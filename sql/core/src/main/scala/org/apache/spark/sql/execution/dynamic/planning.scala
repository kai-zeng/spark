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
    case ExtractAggregateAndExchange(aggregate, child, exchange) =>
      val dynamicExchangeForAggregate = DynamicExchange(exchange.newPartitioning, exchange.child)
      aggregate.withNewChildren(child.withNewChildren(dynamicExchangeForAggregate :: Nil) :: Nil)
  }
}

private[sql] object ExtractAggregateAndExchange {
  def unapply(plan: SparkPlan): Option[(Aggregate2Sort, SparkPlan, Exchange)] = plan match {
    case aggregate: Aggregate2Sort =>
      val child = aggregate.child
      if (child.children.length == 1) {
        child.children.head match {
          case exchange: Exchange => Some((aggregate, child, exchange))
          case _ => None
        }
      } else {
        None
      }
    case _ => None
  }
}