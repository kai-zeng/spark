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

import java.util.{HashMap => JavaHashMap}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.catalyst.plans.{FullOuter, JoinType, LeftOuter, RightOuter}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.util.collection.CompactBuffer

@DeveloperApi
trait HashOuterJoin {
  self: SparkPlan =>

  val leftKeys: Seq[Expression]
  val rightKeys: Seq[Expression]
  val joinType: JoinType
  val condition: Option[Expression]
  val left: SparkPlan
  val right: SparkPlan

  override def outputPartitioning: Partitioning = joinType match {
    case LeftOuter => left.outputPartitioning
    case RightOuter => right.outputPartitioning
    case FullOuter => UnknownPartitioning(left.outputPartitioning.numPartitions)
    case x => throw new Exception(s"HashOuterJoin should not take $x as the JoinType")
  }

  override def output = {
    joinType match {
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case FullOuter =>
        left.output.map(_.withNullability(true)) ++ right.output.map(_.withNullability(true))
      case x =>
        throw new Exception(s"HashOuterJoin should not take $x as the JoinType")
    }
  }

  @transient private[this] lazy val DUMMY_LIST = Seq[Row](null)
  @transient protected[this] lazy val EMPTY_LIST = Seq.empty[Row]

  @transient private[this] lazy val leftNullRow = new GenericRow(left.output.length)
  @transient private[this] lazy val rightNullRow = new GenericRow(right.output.length)
  @transient private[this] lazy val boundCondition =
    condition.map(newPredicate(_, left.output ++ right.output)).getOrElse((row: Row) => true)

  // TODO we need to rewrite all of the iterators with our own implementation instead of the Scala
  // iterator for performance purpose.

  protected[this] def leftOuterIterator(
      key: Row, joinedRow: JoinedRow, rightIter: Iterable[Row]): Iterator[Row] = {
    val ret: Iterable[Row] = (
      if (!key.anyNull) {
        val temp = rightIter.collect {
          case r if (boundCondition(joinedRow.withRight(r))) => joinedRow.copy
        }
        if (temp.size  == 0) {
          joinedRow.withRight(rightNullRow).copy :: Nil
        } else {
          temp
        }
      } else {
        joinedRow.withRight(rightNullRow).copy :: Nil
      }
    )
    ret.iterator
  }

  protected[this] def rightOuterIterator(
      key: Row, leftIter: Iterable[Row], joinedRow: JoinedRow): Iterator[Row] = {

    val ret: Iterable[Row] = (
      if (!key.anyNull) {
        val temp = leftIter.collect {
          case l if (boundCondition(joinedRow.withLeft(l))) => joinedRow.copy
        }
        if (temp.size  == 0) {
          joinedRow.withLeft(leftNullRow).copy :: Nil
        } else {
          temp
        }
      } else {
        joinedRow.withLeft(leftNullRow).copy :: Nil
      }
    )
    ret.iterator
  }

  protected[this] def fullOuterIterator(
      key: Row, leftIter: Iterable[Row], rightIter: Iterable[Row],
      joinedRow: JoinedRow): Iterator[Row] = {

    if (!key.anyNull) {
      // Store the positions of records in right, if one of its associated row satisfy
      // the join condition.
      val rightMatchedSet = scala.collection.mutable.Set[Int]()
      leftIter.iterator.flatMap[Row] { l =>
        joinedRow.withLeft(l)
        var matched = false
        rightIter.zipWithIndex.collect {
          // 1. For those matched (satisfy the join condition) records with both sides filled,
          //    append them directly

          case (r, idx) if (boundCondition(joinedRow.withRight(r)))=> {
            matched = true
            // if the row satisfy the join condition, add its index into the matched set
            rightMatchedSet.add(idx)
            joinedRow.copy
          }
        } ++ DUMMY_LIST.filter(_ => !matched).map( _ => {
          // 2. For those unmatched records in left, append additional records with empty right.

          // DUMMY_LIST.filter(_ => !matched) is a tricky way to add additional row,
          // as we don't know whether we need to append it until finish iterating all
          // of the records in right side.
          // If we didn't get any proper row, then append a single row with empty right.
          joinedRow.withRight(rightNullRow).copy
        })
      } ++ rightIter.zipWithIndex.collect {
        // 3. For those unmatched records in right, append additional records with empty left.

        // Re-visiting the records in right, and append additional row with empty left, if its not
        // in the matched set.
        case (r, idx) if (!rightMatchedSet.contains(idx)) => {
          joinedRow(leftNullRow, r).copy
        }
      }
    } else {
      leftIter.iterator.map[Row] { l =>
        joinedRow(l, rightNullRow).copy
      } ++ rightIter.iterator.map[Row] { r =>
        joinedRow(leftNullRow, r).copy
      }
    }
  }

  protected[this] def buildHashTable(
      iter: Iterator[Row], keyGenerator: Projection): JavaHashMap[Row, CompactBuffer[Row]] = {
    val hashTable = new JavaHashMap[Row, CompactBuffer[Row]]()
    while (iter.hasNext) {
      val currentRow = iter.next()
      val rowKey = keyGenerator(currentRow)

      var existingMatchList = hashTable.get(rowKey)
      if (existingMatchList == null) {
        existingMatchList = new CompactBuffer[Row]()
        hashTable.put(rowKey, existingMatchList)
      }

      existingMatchList += currentRow.copy()
    }

    hashTable
  }
}
