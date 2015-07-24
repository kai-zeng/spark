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

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{Exchange, SparkPlan}
import org.apache.spark.sql.execution.aggregate.Aggregate2Sort

/**
 * Ensures that the [[org.apache.spark.sql.catalyst.plans.physical.Partitioning Partitioning]]
 * of input data meets the
 * [[org.apache.spark.sql.catalyst.plans.physical.Distribution Distribution]] requirements for
 * each operator by inserting [[AddDynamicExchange]] Operators where required.  Also ensure that the
 * required input partition ordering requirements are met.
 */
private[sql] case class AddDynamicExchange(sqlContext: SQLContext) extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = plan.transformDown {
    case ExtractAggregateChildAndExchange(aggregate, child, hashPartitioning, exchangeChild) =>
      val dynamicExchangeForAggregate = DynamicHashExchange(hashPartitioning, exchangeChild)
      aggregate.withNewChildren(child.withNewChildren(dynamicExchangeForAggregate :: Nil) :: Nil)
    case aggregate @ Aggregate2Sort(_, _, _, _, _, Exchange(hash: HashPartitioning, child)) =>
      val dynamicExchangeForAggregate = DynamicHashExchange(hash, child)
      aggregate.withNewChildren(dynamicExchangeForAggregate :: Nil)
  }
}

private[sql] object ExtractAggregateChildAndExchange {
  type ReturnType = (Aggregate2Sort, SparkPlan, HashPartitioning, SparkPlan)

  def unapply(plan: SparkPlan): Option[ReturnType] = plan match {
    case aggregate: Aggregate2Sort =>
      val child = aggregate.child
      if (child.children.length == 1) {
        child.children.head match {
          case Exchange(hash: HashPartitioning, exchangeChild) =>
            Some((aggregate, child, hash, exchangeChild))
          case _ => None
        }
      } else {
        None
      }
    case _ => None
  }
}
