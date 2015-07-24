/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.dynamic

import org.apache.spark._
import org.apache.spark.rdd.{ShuffledRDD2Partition, CoalescedPartitioner, RDD}
import org.apache.spark.sql.catalyst.InternalRow

/**
 * A ShuffledRDD that only works with Spark SQL InternalRow and can fetch multiple Map output
 * partitions.
 */
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
