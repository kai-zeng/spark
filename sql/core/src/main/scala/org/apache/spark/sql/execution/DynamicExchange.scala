package org.apache.spark.sql.execution

import org.apache.spark.rdd.{CoalescedPartitioner, ShuffledRDD2Partition, RDD}
import org.apache.spark.shuffle.hash.HashShuffleManager
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.shuffle.unsafe.UnsafeShuffleManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.util.{ThreadUtils, MutablePair}
import org.apache.spark._
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.expressions.{RowOrdering, Attribute}
import org.apache.spark.sql.catalyst.plans.physical.{SinglePartition, RangePartitioning, HashPartitioning, Partitioning}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

case class DynamicExchange(newPartitioning: Partitioning, child: SparkPlan) extends UnaryNode {

  override def outputPartitioning: Partitioning = newPartitioning

  override def output: Seq[Attribute] = child.output

  override def outputsUnsafeRows: Boolean = child.outputsUnsafeRows

  override def canProcessSafeRows: Boolean = true

  override def canProcessUnsafeRows: Boolean = true

  /**
   * Determines whether records must be defensively copied before being sent to the shuffle.
   * Several of Spark's shuffle components will buffer deserialized Java objects in memory. The
   * shuffle code assumes that objects are immutable and hence does not perform its own defensive
   * copying. In Spark SQL, however, operators' iterators return the same mutable `Row` object. In
   * order to properly shuffle the output of these operators, we need to perform our own copying
   * prior to sending records to the shuffle. This copying is expensive, so we try to avoid it
   * whenever possible. This method encapsulates the logic for choosing when to copy.
   *
   * In the long run, we might want to push this logic into core's shuffle APIs so that we don't
   * have to rely on knowledge of core internals here in SQL.
   *
   * See SPARK-2967, SPARK-4479, and SPARK-7375 for more discussion of this issue.
   *
   * @param partitioner the partitioner for the shuffle
   * @param serializer the serializer that will be used to write rows
   * @return true if rows should be copied before being shuffled, false otherwise
   */
  private def needToCopyObjectsBeforeShuffle(
                                              partitioner: Partitioner,
                                              serializer: Serializer): Boolean = {
    // Note: even though we only use the partitioner's `numPartitions` field, we require it to be
    // passed instead of directly passing the number of partitions in order to guard against
    // corner-cases where a partitioner constructed with `numPartitions` partitions may output
    // fewer partitions (like RangePartitioner, for example).
    val conf = child.sqlContext.sparkContext.conf
    val shuffleManager = SparkEnv.get.shuffleManager
    val sortBasedShuffleOn = shuffleManager.isInstanceOf[SortShuffleManager] ||
      shuffleManager.isInstanceOf[UnsafeShuffleManager]
    val bypassMergeThreshold = conf.getInt("spark.shuffle.sort.bypassMergeThreshold", 200)
    val serializeMapOutputs = conf.getBoolean("spark.shuffle.sort.serializeMapOutputs", true)
    if (sortBasedShuffleOn) {
      val bypassIsSupported = SparkEnv.get.shuffleManager.isInstanceOf[SortShuffleManager]
      if (bypassIsSupported && partitioner.numPartitions <= bypassMergeThreshold) {
        // If we're using the original SortShuffleManager and the number of output partitions is
        // sufficiently small, then Spark will fall back to the hash-based shuffle write path, which
        // doesn't buffer deserialized records.
        // Note that we'll have to remove this case if we fix SPARK-6026 and remove this bypass.
        false
      } else if (serializeMapOutputs && serializer.supportsRelocationOfSerializedObjects) {
        // SPARK-4550 extended sort-based shuffle to serialize individual records prior to sorting
        // them. This optimization is guarded by a feature-flag and is only applied in cases where
        // shuffle dependency does not specify an aggregator or ordering and the record serializer
        // has certain properties. If this optimization is enabled, we can safely avoid the copy.
        //
        // Exchange never configures its ShuffledRDDs with aggregators or key orderings, so we only
        // need to check whether the optimization is enabled and supported by our serializer.
        //
        // This optimization also applies to UnsafeShuffleManager (added in SPARK-7081).
        false
      } else {
        // Spark's SortShuffleManager uses `ExternalSorter` to buffer records in memory. This code
        // path is used both when SortShuffleManager is used and when UnsafeShuffleManager falls
        // back to SortShuffleManager to perform a shuffle that the new fast path can't handle. In
        // both cases, we must copy.
        true
      }
    } else if (shuffleManager.isInstanceOf[HashShuffleManager]) {
      // We're using hash-based shuffle, so we don't need to copy.
      false
    } else {
      // Catch-all case to safely handle any future ShuffleManager implementations.
      true
    }
  }

  @transient private lazy val sparkConf = child.sqlContext.sparkContext.getConf

  private val serializer: Serializer = {
    val rowDataTypes = child.output.map(_.dataType).toArray
    // It is true when there is no field that needs to be write out.
    // For now, we will not use SparkSqlSerializer2 when noField is true.
    val noField = rowDataTypes == null || rowDataTypes.length == 0

    val useSqlSerializer2 =
      child.sqlContext.conf.useSqlSerializer2 &&   // SparkSqlSerializer2 is enabled.
        SparkSqlSerializer2.support(rowDataTypes) &&  // The schema of row is supported.
        !noField

    if (child.outputsUnsafeRows) {
      logInfo("Using UnsafeRowSerializer.")
      new UnsafeRowSerializer(child.output.size)
    } else if (useSqlSerializer2) {
      logInfo("Using SparkSqlSerializer2.")
      new SparkSqlSerializer2(rowDataTypes)
    } else {
      logInfo("Using SparkSqlSerializer.")
      new SparkSqlSerializer(sparkConf)
    }
  }

  protected override def doExecute(): RDD[InternalRow] = attachTree(this , "execute") {
    val rdd = child.execute()
    val part: Partitioner = newPartitioning match {
      case HashPartitioning(expressions, numPartitions) => new HashPartitioner(numPartitions)
      case RangePartitioning(sortingExpressions, numPartitions) =>
        // Internally, RangePartitioner runs a job on the RDD that samples keys to compute
        // partition bounds. To get accurate samples, we need to copy the mutable keys.
        val rddForSampling = rdd.mapPartitions { iter =>
          val mutablePair = new MutablePair[InternalRow, Null]()
          iter.map(row => mutablePair.update(row.copy(), null))
        }
        implicit val ordering = new RowOrdering(sortingExpressions, child.output)
        new RangePartitioner(numPartitions, rddForSampling, ascending = true)
      case SinglePartition =>
        new Partitioner {
          override def numPartitions: Int = 1
          override def getPartition(key: Any): Int = 0
        }
      case _ => sys.error(s"Exchange not implemented for $newPartitioning")
      // TODO: Handle BroadcastPartitioning.
    }
    def getPartitionKeyExtractor(): InternalRow => InternalRow = newPartitioning match {
      case HashPartitioning(expressions, _) => newMutableProjection(expressions, child.output)()
      case RangePartitioning(_, _) | SinglePartition => identity
      case _ => sys.error(s"Exchange not implemented for $newPartitioning")
    }
    val rddWithPartitionIds: RDD[Product2[Int, InternalRow]] = {
      if (needToCopyObjectsBeforeShuffle(part, serializer)) {
        rdd.mapPartitions { iter =>
          val getPartitionKey = getPartitionKeyExtractor()
          iter.map { row => (part.getPartition(getPartitionKey(row)), row.copy()) }
        }
      } else {
        rdd.mapPartitions { iter =>
          val getPartitionKey = getPartitionKeyExtractor()
          val mutablePair = new MutablePair[Int, InternalRow]()
          iter.map { row => mutablePair.update(part.getPartition(getPartitionKey(row)), row) }
        }
      }
    }
    val dep =
      new ShuffleDependency[Int, InternalRow, InternalRow](
        rddWithPartitionIds,
        new PartitionIdPassthrough(part.numPartitions))

    val future = sparkContext.submitMapStage(dep)

    val partitionStartIndices = ArrayBuffer[Int]()

    future.onComplete {
      case scala.util.Success(mapOutputStats) =>
        val bytesByPartitionId = mapOutputStats.bytesByPartitionId
        val targetSizePerReducer = 64 * 1024 * 1024

        var i = 0
        var currentStartIndex = 0
        var currentSize = 0L
        while (i < bytesByPartitionId.length) {
          // Get the output size of mapper i.
          val size = bytesByPartitionId(i)
          // Add the size to currentSize.
          currentSize += size
          // Check if currentSize is greater or equal than targetSizePerReducer.
          if (currentSize >= targetSizePerReducer) {
            // If so, we add currentStartIndex to partitionStartIndices.
            partitionStartIndices += currentStartIndex
            // Reset currentSize and currentStartIndex.
            currentSize = 0L
            currentStartIndex = i + 1
          }

          i += 1
        }
        if (partitionStartIndices.length == 0) {
          partitionStartIndices += 0
        }
      case scala.util.Failure(t) => sys.error("What to do when we have failure?")
    }(ThreadUtils.sameThread)

    new ShuffledRowRDD2(dep, partitionStartIndices.toArray)
  }
}

class ShuffledRowRDD2(
    var dependency: ShuffleDependency[Int, InternalRow, InternalRow],
    val partitionStartIndices: Array[Int])
  extends RDD[InternalRow](dependency.rdd.context, Nil) {

  override def getDependencies: Seq[Dependency[_]] = List(dependency)

  override val partitioner = {
    Some(new CoalescedPartitioner(dependency.partitioner, partitionStartIndices))
  }

  override def getPartitions: Array[Partition] = {
    val n = dependency.partitioner.numPartitions
    Array.tabulate[Partition](partitionStartIndices.length) { i =>
      val startIndex = partitionStartIndices(i)
      val endIndex = if (i < partitionStartIndices.length - 1) partitionStartIndices(i + 1) else n
      new ShuffledRDD2Partition(i, startIndex, endIndex)
    }
  }

  override def compute(p: Partition, context: TaskContext): Iterator[InternalRow] = {
    val sp = p.asInstanceOf[ShuffledRDD2Partition]
    SparkEnv.get.shuffleManager.getReader(
      dependency.shuffleHandle, sp.startIndexInParent, sp.endIndexInParent, context)
      .read()
      .asInstanceOf[Iterator[Product2[Int, InternalRow]]]
      .map(_._2)
  }

  override def clearDependencies() {
    super.clearDependencies()
    dependency = null
  }
}

