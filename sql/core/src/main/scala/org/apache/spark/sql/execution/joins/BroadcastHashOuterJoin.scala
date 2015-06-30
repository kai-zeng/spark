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

package org.apache.spark.sql.execution.joins

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.util.collection.CompactBuffer

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._

/**
 * :: DeveloperApi ::
 * Performs a outer hash join for two child relations.  When the output RDD of this operator is
 * being constructed, a Spark job is asynchronously started to calculate the values for the
 * broadcasted relation.  This data is then placed in a Spark broadcast variable.  The streamed
 * relation is not shuffled.
 *
 * TODO: A better comment about buildSide
 * Comment:In left(right) outer join only the right(left) table has an
 * estimated physical size smaller than the user-settable threshold
 * [[org.apache.spark.sql.SQLConf.AUTO_BROADCASTJOIN_THRESHOLD]] the planner
 * would mark it as the broadcasted relation.
 */
@DeveloperApi
case class BroadcastHashOuterJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    buildSide: BuildSide,
    left: SparkPlan,
    right: SparkPlan)
  extends BinaryNode with HashJoin {

  val timeout = {
    val timeoutValue = sqlContext.conf.broadcastTimeout
    if (timeoutValue < 0) {
      Duration.Inf
    } else {
      timeoutValue.seconds
    }
  }

  override def output = buildSide match {
    case BuildLeft => left.output.map(_.withNullability(true)) ++ right.output
    case BuildRight => left.output ++ right.output.map(_.withNullability(true))
  }

  override def outputPartitioning: Partitioning = streamedPlan.outputPartitioning

  override def requiredChildDistribution =
    UnspecifiedDistribution :: UnspecifiedDistribution :: Nil

  @transient
  private val broadcastFuture = future {
    // Note that we use .execute().collect() because we don't want to convert data to Scala types
    val input: Array[Row] = buildPlan.execute().map(_.copy()).collect()
    val nullRow = buildSide match {
      case BuildLeft => new GenericRow(left.output.length)
      case BuildRight => new GenericRow(right.output.length)
    }
    val hashed = HashedRelationWithDefault(
      HashedRelation(input.iterator, buildSideKeyGenerator, input.length), CompactBuffer(nullRow))
    sparkContext.broadcast(hashed)
  }

  override def execute() = {
    val broadcastRelation = Await.result(broadcastFuture, timeout)

    streamedPlan.execute().mapPartitions { streamedIter =>
      hashJoin(streamedIter, broadcastRelation.value)
    }
  }
}
