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

package org.apache.spark.sql.execution.cost

import java.util.Locale

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.expressions.{PredicateHelper, RowOrdering}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide, JoinSelectionHelper}
import org.apache.spark.sql.catalyst.planning.{ExtractEquiJoinKeys, ExtractSingleColumnNullAwareAntiJoin}
import org.apache.spark.sql.catalyst.plans.{FullOuter, InnerLike, JoinType, LeftAnti}
import org.apache.spark.sql.catalyst.plans.logical.{HintInfo, Join, JoinHint, LogicalPlan}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, CartesianProductExec, ShuffledHashJoinExec, SortMergeJoinExec}

/**
 * Select the proper physical plan for join based on join strategy hints, the availability of
 * equi-join keys and the sizes of joining relations. Below are the existing join strategies,
 * their characteristics and their limitations.
 *
 * - Broadcast hash join (BHJ):
 *     Only supported for equi-joins, while the join keys do not need to be sortable.
 *     Supported for all join types except full outer joins.
 *     BHJ usually performs faster than the other join algorithms when the broadcast side is
 *     small. However, broadcasting tables is a network-intensive operation and it could cause
 *     OOM or perform badly in some cases, especially when the build/broadcast side is big.
 *
 * - Shuffle hash join:
 *     Only supported for equi-joins, while the join keys do not need to be sortable.
 *     Supported for all join types.
 *     Building hash map from table is a memory-intensive operation and it could cause OOM
 *     when the build side is big.
 *
 * - Shuffle sort merge join (SMJ):
 *     Only supported for equi-joins and the join keys have to be sortable.
 *     Supported for all join types.
 *
 * - Broadcast nested loop join (BNLJ):
 *     Supports both equi-joins and non-equi-joins.
 *     Supports all the join types, but the implementation is optimized for:
 *       1) broadcasting the left side in a right outer join;
 *       2) broadcasting the right side in a left outer, left semi, left anti or existence join;
 *       3) broadcasting either side in an inner-like join.
 *     For other cases, we need to scan the data multiple times, which can be rather slow.
 *
 * - Shuffle-and-replicate nested loop join (a.k.a. cartesian product join):
 *     Supports both equi-joins and non-equi-joins.
 *     Supports only inner like joins.
 */
class LearnableCostJoinSelection(session : SparkSession) extends Strategy
  with PredicateHelper
  with JoinSelectionHelper
  with Logging{
  private val sqlConf = session.sqlContext.conf
  private val hintErrorHandler = sqlConf.hintErrorHandler

  private def checkHintBuildSide(
                                  onlyLookingAtHint: Boolean,
                                  buildSide: Option[BuildSide],
                                  joinType: JoinType,
                                  hint: JoinHint,
                                  isBroadcast: Boolean): Unit = {
    def invalidBuildSideInHint(hintInfo: HintInfo, buildSide: String): Unit = {
      hintErrorHandler.joinHintNotSupported(hintInfo,
        s"build $buildSide for ${joinType.sql.toLowerCase(Locale.ROOT)} join")
    }

    if (onlyLookingAtHint && buildSide.isEmpty) {
      if (isBroadcast) {
        // check broadcast hash join
        if (hintToBroadcastLeft(hint)) invalidBuildSideInHint(hint.leftHint.get, "left")
        if (hintToBroadcastRight(hint)) invalidBuildSideInHint(hint.rightHint.get, "right")
      } else {
        // check shuffle hash join
        if (hintToShuffleHashJoinLeft(hint)) invalidBuildSideInHint(hint.leftHint.get, "left")
        if (hintToShuffleHashJoinRight(hint)) invalidBuildSideInHint(hint.rightHint.get, "right")
      }
    }
  }

  private def checkHintNonEquiJoin(hint: JoinHint): Unit = {
    if (hintToShuffleHashJoin(hint) || hintToPreferShuffleHashJoin(hint) ||
      hintToSortMergeJoin(hint)) {
      assert(hint.leftHint.orElse(hint.rightHint).isDefined)
      hintErrorHandler.joinHintNotSupported(hint.leftHint.orElse(hint.rightHint).get,
        "no equi-join keys")
    }
  }

  private def log2(num : BigInt) : BigInt = {
    if (num <= 1) 0 else BigDecimal(math.log(num.doubleValue()) / math.log(2.0)).toBigInt
  }

  private def costOfBroadcastHashJoin(left: LogicalPlan, right: LogicalPlan): BigInt = {
    val leftSize = left.stats.sizeInBytes
    val rightSize = right.stats.sizeInBytes
    if (leftSize > sqlConf.adaptiveMaxValidCost || rightSize > sqlConf.adaptiveMaxValidCost) {
      logInfo(s"costOfBroadcastHashJoin left plan size: $leftSize, " +
        s"right plan size: $rightSize, exceeding max valid cost ${sqlConf.adaptiveMaxValidCost}")
      return -1
    }
    val leftRow: BigInt = left.stats.rowCount.getOrElse(1)
    val rightRow: BigInt = right.stats.rowCount.getOrElse(1)
    val smaller = leftSize.min(rightSize)
    val numPartitions = sqlConf.numShufflePartitions
    val compCost = numPartitions * smaller + leftSize + rightSize
    val networkCost = smaller * (numPartitions - 1)
    logInfo(s"costOfBroadcastHashJoin left plan size: $leftSize, " +
      s"right plan size: $rightSize, " +
      s"w: ${sqlConf.adaptiveNetworkWeight}, " +
      s"left row: $leftRow, right row: $rightRow, numPartitions: $numPartitions, " +
      s"compCost: $compCost, networkCost: $networkCost, total: ${compCost + networkCost}")
    sqlConf.adaptiveNetworkWeight * networkCost + compCost
  }

  private def costOfShuffleHashJoin(left: LogicalPlan, right: LogicalPlan) : BigInt = {
    val leftSize = left.stats.sizeInBytes
    val rightSize = right.stats.sizeInBytes
    if (leftSize > sqlConf.adaptiveMaxValidCost || rightSize > sqlConf.adaptiveMaxValidCost) {
      logInfo(s"costOfShuffleHashJoin left plan size: $leftSize, " +
        s"right plan size: $rightSize, exceeding max valid cost ${sqlConf.adaptiveMaxValidCost}")
      return -1
    }
    val smaller = leftSize.min(rightSize)
    val numPartitions = sqlConf.numShufflePartitions
    val compCost = smaller + leftSize + rightSize
    val networkCost = (leftSize + rightSize) * (numPartitions - 1) / numPartitions
    logInfo(s"costOfShuffleHashJoin " +
      s"compCost: $compCost, networkCost: $networkCost, total: ${compCost + networkCost}")
    sqlConf.adaptiveNetworkWeight * networkCost + compCost
  }

  private def costOfSortMerge(left: LogicalPlan, right: LogicalPlan) : BigInt = {
    val leftSize = left.stats.sizeInBytes
    val rightSize = right.stats.sizeInBytes
    if (leftSize > sqlConf.adaptiveMaxValidCost || rightSize > sqlConf.adaptiveMaxValidCost) {
      logInfo(s"costOfSortMerge left plan size: $leftSize, " +
        s"right plan size: $rightSize, exceeding max valid cost ${sqlConf.adaptiveMaxValidCost}")
      return -1
    }
    val leftRow: BigInt = left.stats.rowCount.getOrElse(1)
    val rightRow: BigInt = right.stats.rowCount.getOrElse(1)
    val numPartitions = sqlConf.numShufflePartitions
    val compCost = leftSize * log2(leftRow / numPartitions) +
      rightSize * log2(rightRow / numPartitions) + leftSize + rightSize
    val networkCost = (leftSize + rightSize) * (numPartitions - 1) / numPartitions
    logInfo(s"costOfSortMerge " +
      s"compCost: $compCost, networkCost: $networkCost, total: ${compCost + networkCost}")
    sqlConf.adaptiveNetworkWeight * networkCost + compCost
  }

  private def costOfBroadcastNL(left: LogicalPlan, right: LogicalPlan) : BigInt = {
    val leftSize = left.stats.sizeInBytes
    val rightSize = right.stats.sizeInBytes
    if (leftSize > sqlConf.adaptiveMaxValidCost || rightSize > sqlConf.adaptiveMaxValidCost) {
      logInfo(s"costOfBroadcastNL left plan size: $leftSize, " +
        s"right plan size: $rightSize, exceeding max valid cost ${sqlConf.adaptiveMaxValidCost}")
      return -1
    }
    val leftRow: BigInt = left.stats.rowCount.getOrElse(1)
    val rightRow: BigInt = right.stats.rowCount.getOrElse(1)
    val smaller = leftSize.min(rightSize)
    val bigger = leftSize.max(rightSize)
    val rowOfBiggerSide = if (leftSize == bigger) leftRow else rightRow
    val numPartitions = sqlConf.numShufflePartitions
    val compCost = bigger + rowOfBiggerSide * smaller
    val networkCost = smaller * (numPartitions - 1)
    logInfo(s"costOfBroadcastNL " +
      s"compCost: $compCost, networkCost: $networkCost, total: ${compCost + networkCost}")
    sqlConf.adaptiveNetworkWeight * networkCost + compCost
  }

  private def costOfCartesian(left: LogicalPlan, right: LogicalPlan) : BigInt = {
    val leftSize = left.stats.sizeInBytes
    val rightSize = right.stats.sizeInBytes
    if (leftSize > sqlConf.adaptiveMaxValidCost || rightSize > sqlConf.adaptiveMaxValidCost) {
      logInfo(s"costOfCartesian left plan size: $leftSize, " +
        s"right plan size: $rightSize, exceeding max valid cost ${sqlConf.adaptiveMaxValidCost}")
      return -1
    }
    val leftRow: BigInt = left.stats.rowCount.getOrElse(1)
    val rightRow: BigInt = right.stats.rowCount.getOrElse(1)
    val smaller = leftSize.min(rightSize)
    val bigger = leftSize.max(rightSize)
    val rowOfBiggerSide = if (leftSize == bigger) leftRow else rightRow
    val numPartitions = sqlConf.numShufflePartitions
    val compCost = bigger + rowOfBiggerSide * smaller / numPartitions
    val networkCost = (leftSize + rightSize) * (numPartitions - 1) / numPartitions
    logInfo(s"costOfCartesian " +
      s"compCost: $compCost, networkCost: $networkCost, total: ${compCost + networkCost}")
    sqlConf.adaptiveNetworkWeight * networkCost + compCost
  }

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

    // If it is an equi-join, we first look at the join hints w.r.t. the following order:
    //   1. broadcast hint: pick broadcast hash join if the join type is supported. If both sides
    //      have the broadcast hints, choose the smaller side (based on stats) to broadcast.
    //   2. sort merge hint: pick sort merge join if join keys are sortable.
    //   3. shuffle hash hint: We pick shuffle hash join if the join type is supported. If both
    //      sides have the shuffle hash hints, choose the smaller side (based on stats) as the
    //      build side.
    //   4. shuffle replicate NL hint: pick cartesian product if join type is inner like.
    //
    // If there is no hint or the hints are not applicable, we follow these rules one by one:
    //   1. Pick broadcast hash join if one side is small enough to broadcast, and the join type
    //      is supported. If both sides are small, choose the smaller side (based on stats)
    //      to broadcast.
    //   2. Pick shuffle hash join if one side is small enough to build local hash map, and is
    //      much smaller than the other side, and `spark.sql.join.preferSortMergeJoin` is false.
    //   3. Pick sort merge join if the join keys are sortable.
    //   4. Pick cartesian product if join type is inner like.
    //   5. Pick broadcast nested loop join as the final solution. It may OOM but we don't have
    //      other choice.
    case j @ ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, nonEquiCond,
    _, left, right, hint) =>

      def createBroadcastHashJoin(onlyLookingAtHint: Boolean, useAdaptiveCost: Boolean = false) = {
        logInfo(s"createBroadcastHashJoin, hint: $onlyLookingAtHint")
        val buildSide = if (!useAdaptiveCost) {
          getBroadcastBuildSide(left, right, joinType, hint, onlyLookingAtHint, sqlConf)
        } else {
          getCostBasedBroadcastBuildSide(
            left, right, joinType, hint, onlyLookingAtHint, sqlConf)
        }
        checkHintBuildSide(onlyLookingAtHint, buildSide, joinType, hint, true)
        buildSide.map {
          buildSide =>
            Seq(BroadcastHashJoinExec(
              leftKeys,
              rightKeys,
              joinType,
              buildSide,
              nonEquiCond,
              planLater(left),
              planLater(right)))
        }
      }

      def createShuffleHashJoin(onlyLookingAtHint: Boolean, useAdaptiveCost: Boolean = false) = {
        logInfo(s"createShuffleHashJoin, hint: $onlyLookingAtHint")
        val buildSide = if (!useAdaptiveCost) {
          getShuffleHashJoinBuildSide(
            left, right, joinType, hint, onlyLookingAtHint, sqlConf)
        } else {
          getCostBasedShuffleHashJoinBuildSide(
            left, right, joinType, hint, onlyLookingAtHint, sqlConf)
        }
        checkHintBuildSide(onlyLookingAtHint, buildSide, joinType, hint, false)
        buildSide.map {
          buildSide =>
            Seq(ShuffledHashJoinExec(
              leftKeys,
              rightKeys,
              joinType,
              buildSide,
              nonEquiCond,
              planLater(left),
              planLater(right)))
        }
      }

      def createSortMergeJoin() = {
        logInfo(s"createSortMergeJoin")
        if (RowOrdering.isOrderable(leftKeys)) {
          Some(Seq(SortMergeJoinExec(
            leftKeys, rightKeys, joinType, nonEquiCond, planLater(left), planLater(right))))
        } else None
      }

      def createCartesianProduct() = {
        logInfo(s"createCartesianProduct")
        if (joinType.isInstanceOf[InnerLike]) {
          // `CartesianProductExec` can't implicitly evaluate equal join condition, here we should
          // pass the original condition which includes both equal and non-equal conditions.
          Some(Seq(CartesianProductExec(planLater(left), planLater(right), j.condition)))
        } else None
      }

      def createJoinWithoutHint() = {
        val costBroadcastHash = costOfBroadcastHashJoin(left, right)
        val costShuffleHash = costOfShuffleHashJoin(left, right)
        val costSortMerge = costOfSortMerge(left, right)
        val costCartesian = costOfCartesian(left, right)
        val costBroadcastNL = costOfBroadcastNL(left, right)
        val minCost = costBroadcastHash.min(costShuffleHash).min(costSortMerge)
        val useAdaptiveCost = if (sqlConf.adaptiveCostJoinEnable && minCost != -1) {
          true
        } else false

        val broadcastHash = if (minCost == costBroadcastHash || !useAdaptiveCost) {
          createBroadcastHashJoin(false, useAdaptiveCost)
        } else None
        broadcastHash.orElse {
          if (minCost == costShuffleHash  || !useAdaptiveCost) {
            createShuffleHashJoin(false, useAdaptiveCost)
          } else None
        }.orElse {
          createSortMergeJoin()
        }.orElse {
          if (costCartesian <= costBroadcastNL || !useAdaptiveCost) {
            createCartesianProduct()
          } else None
        }.getOrElse {
          val buildSide = getSmallerSide(left, right)
          Seq(BroadcastNestedLoopJoinExec(
            planLater(left), planLater(right), buildSide, joinType, j.condition))
        }
      }

      if (hint.isEmpty) {
        createJoinWithoutHint()
      } else {
        createBroadcastHashJoin(true)
          .orElse { if (hintToSortMergeJoin(hint)) createSortMergeJoin() else None }
          .orElse(createShuffleHashJoin(true))
          .orElse { if (hintToShuffleReplicateNL(hint)) createCartesianProduct() else None }
          .getOrElse(createJoinWithoutHint())
      }

    case j @ ExtractSingleColumnNullAwareAntiJoin(leftKeys, rightKeys)
      if (sqlConf.adaptiveCostJoinEnable) =>
      Seq(BroadcastHashJoinExec(leftKeys, rightKeys, LeftAnti, BuildRight,
        None, planLater(j.left), planLater(j.right), isNullAwareAntiJoin = true))

    // If it is not an equi-join, we first look at the join hints w.r.t. the following order:
    //   1. broadcast hint: pick broadcast nested loop join. If both sides have the broadcast
    //      hints, choose the smaller side (based on stats) to broadcast for inner and full joins,
    //      choose the left side for right join, and choose right side for left join.
    //   2. shuffle replicate NL hint: pick cartesian product if join type is inner like.
    //
    // If there is no hint or the hints are not applicable, we follow these rules one by one:
    //   1. Pick broadcast nested loop join if one side is small enough to broadcast. If only left
    //      side is broadcast-able and it's left join, or only right side is broadcast-able and
    //      it's right join, we skip this rule. If both sides are small, broadcasts the smaller
    //      side for inner and full joins, broadcasts the left side for right join, and broadcasts
    //      right side for left join.
    //   2. Pick cartesian product if join type is inner like.
    //   3. Pick broadcast nested loop join as the final solution. It may OOM but we don't have
    //      other choice. It broadcasts the smaller side for inner and full joins, broadcasts the
    //      left side for right join, and broadcasts right side for left join.
    case Join(left, right, joinType, condition, hint) =>
      checkHintNonEquiJoin(hint)
      val desiredBuildSide = if (joinType.isInstanceOf[InnerLike] || joinType == FullOuter) {
        getSmallerSide(left, right)
      } else {
        // For perf reasons, `BroadcastNestedLoopJoinExec` prefers to broadcast left side if
        // it's a right join, and broadcast right side if it's a left join.
        // TODO: revisit it. If left side is much smaller than the right side, it may be better
        // to broadcast the left side even if it's a left join.
        if (canBuildBroadcastLeft(joinType)) BuildLeft else BuildRight
      }

      def createBroadcastNLJoin(buildLeft: Boolean, buildRight: Boolean) = {
        logInfo(s"createBroadcastNLJoin")
        val maybeBuildSide = if (buildLeft && buildRight) {
          Some(desiredBuildSide)
        } else if (buildLeft) {
          Some(BuildLeft)
        } else if (buildRight) {
          Some(BuildRight)
        } else {
          None
        }

        maybeBuildSide.map { buildSide =>
          Seq(BroadcastNestedLoopJoinExec(
            planLater(left), planLater(right), buildSide, joinType, condition))
        }
      }

      def createCartesianProduct() = {
        logInfo(s"createCartesianProduct")
        if (joinType.isInstanceOf[InnerLike]) {
          Some(Seq(CartesianProductExec(planLater(left), planLater(right), condition)))
        } else {
          None
        }
      }

      def createJoinWithoutHint() = {
        val useAdaptiveCost = if (sqlConf.adaptiveCostJoinEnable) true else false
        val costCartesian = costOfCartesian(left, right)
        val costBroadcastNL = costOfBroadcastNL(left, right)
        val minCost = costCartesian.min(costBroadcastNL)
        val exec = if (!useAdaptiveCost) {
          createBroadcastNLJoin(canBroadcastBySize(left, sqlConf),
            canBroadcastBySize(right, sqlConf))
        } else if (minCost != -1 && minCost == costBroadcastNL) {
          createBroadcastNLJoin(true, true)
        } else {
          None
        }
        exec.orElse{
          createCartesianProduct()
        }.getOrElse{
          Seq(BroadcastNestedLoopJoinExec(
            planLater(left), planLater(right), desiredBuildSide, joinType, condition))
        }
      }

      if (hint.isEmpty) {
        createJoinWithoutHint()
      } else {
        createBroadcastNLJoin(hintToBroadcastLeft(hint), hintToBroadcastRight(hint))
          .orElse { if (hintToShuffleReplicateNL(hint)) createCartesianProduct() else None }
          .getOrElse(createJoinWithoutHint())
      }

    // --- Cases where this strategy does not apply ---------------------------------------------
    case _ => Nil
  }
}
