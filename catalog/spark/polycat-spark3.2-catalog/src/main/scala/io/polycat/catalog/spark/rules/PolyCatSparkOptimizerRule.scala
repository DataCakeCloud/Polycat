/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.polycat.catalog.spark.rules

import org.apache.spark.sql.{SparkSession, sources}
import org.apache.spark.sql.catalyst.catalog.CatalogStatistics
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, AttributeSet, Expression, ExpressionSet, NamedExpression, Predicate, PredicateHelper, ProjectionOverSchema, SubqueryExpression, aggregate}
import org.apache.spark.sql.catalyst.planning.{PhysicalOperation, ScanOperation}
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.FilterEstimation
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LeafNode, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{PolyCatPartitionTable, SparkHelper}
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.{Batch, Scan, ScanBuilder, SupportsPushDownAggregates, SupportsPushDownFilters, V1Scan}
import org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown.{logInfo, splitConjunctivePredicates}
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.execution.datasources.{CatalogFileIndex, DataSourceStrategy, HadoopFsRelation, PolyCatFileIndex, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Implicits, DataSourceV2Relation, DataSourceV2ScanRelation, FileScan, PushDownUtils}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

case class PolyCatSparkOptimizerRule(spark: SparkSession) extends Rule[LogicalPlan] with PredicateHelper {

  import DataSourceV2Implicits._

  def normalizeExprs(exprs: Seq[Expression],
                     attributes: Seq[Attribute]): Seq[Expression] = {
    exprs.map { e =>
      e transform {
        case a: AttributeReference =>
          a.withName(attributes.find(_.semanticEquals(a)).getOrElse(a).name)
      }
    }
  }

  def restoreOriginalOutputNames(
                                  projectList: Seq[NamedExpression],
                                  originalNames: Seq[String]): Seq[NamedExpression] = {
    projectList.zip(originalNames).map {
      case (attr: Attribute, name) => attr.withName(name)
      case (alias: Alias, name) => alias.withName(name)
      case (other, _) => other
    }
  }

  /*override def apply(plan: LogicalPlan): LogicalPlan = plan.transformDown {
    case Filter(condition, v2Relation @ DataSourceV2ScanRelation(table, scan: FileScan, output)) =>
      val filters = splitConjunctivePredicates(condition)
      logInfo(s"scan.fileIndex: ${scan.fileIndex}")
      val normalizedFilters =
        normalizeExprs(filters, output)
      if (scan.readPartitionSchema.nonEmpty) {
        val (normalizedFiltersWithSubquery, normalizedFiltersWithoutSubquery) =
          normalizedFilters.partition(SubqueryExpression.hasSubquery)
        val (partitionKeyFilters, dataFilters) =
          getPartitionKeyFiltersAndDataFilters(scan.sparkSession, v2Relation,
            scan.readPartitionSchema, filters, output)
        logInfo(s"scan.fileIndex: ${scan.fileIndex}")

        scan.fileIndex match {
          case index: PolyCatFileIndex =>
            index.refresh1(partitionKeyFilters.toSeq)
          case _ =>
        }
        val filterCondition = partitionKeyFilters.reduceLeftOption(And)
        filterCondition.map(Filter(_, v2Relation)).getOrElse(v2Relation)
      }
      val filterCondition = normalizedFilters.reduceLeftOption(And)
      filterCondition.map(Filter(_, v2Relation)).getOrElse(v2Relation)
  }*/



  private def getPartitionKeyFiltersAndDataFilters(
                                                    sparkSession: SparkSession,
                                                    relation: LeafNode,
                                                    partitionSchema: StructType,
                                                    filters: Seq[Expression],
                                                    output: Seq[AttributeReference]): (ExpressionSet, Seq[Expression]) = {
    val normalizedFilters = normalizeExprs(
      filters.filter(f => f.deterministic && !SubqueryExpression.hasSubquery(f)), output)
    val partitionColumns =
      relation.resolve(partitionSchema, sparkSession.sessionState.analyzer.resolver)
    val partitionSet = AttributeSet(partitionColumns)
    val (partitionFilters, dataFilters) = normalizedFilters.partition(f =>
      f.references.subsetOf(partitionSet)
    )
    val extraPartitionFilter =
      dataFilters.flatMap(extractPredicatesWithinOutputSet(_, partitionSet))

    (ExpressionSet(partitionFilters ++ extraPartitionFilter), dataFilters)
  }

  private def rebuildPhysicalOperation(
                                        projects: Seq[NamedExpression],
                                        filters: Seq[Expression],
                                        relation: LeafNode): Project = {
    val withFilter = if (filters.nonEmpty) {
      val filterExpression = filters.reduceLeft(And)
      Filter(filterExpression, relation)
    } else {
      relation
    }
    Project(projects, withFilter)
  }



  def apply(plan: LogicalPlan): LogicalPlan = {
    applyColumnPruning(pushDownAggregates(pushDownFilters(createScanBuilder(plan))))
  }

  private def createScanBuilder(plan: LogicalPlan) = plan.transform {
    case r: DataSourceV2Relation =>
      ScanBuilderHolder(r.output, r, r.table.asReadable.newScanBuilder(r.options))
  }

  private def pushDownFilters(plan: LogicalPlan) = plan.transform {
    // update the scan builder with filter push down and return a new plan with filter pushed
    case Filter(condition, sHolder: ScanBuilderHolder) =>
      val filters = splitConjunctivePredicates(condition)
      val normalizedFilters =
        normalizeExprs(filters, sHolder.relation.output)
      val (normalizedFiltersWithSubquery, normalizedFiltersWithoutSubquery) =
        normalizedFilters.partition(SubqueryExpression.hasSubquery)


      // `pushedFilters` will be pushed down and evaluated in the underlying data sources.
      // `postScanFilters` need to be evaluated after the scan.
      // `postScanFilters` and `pushedFilters` can overlap, e.g. the parquet row group filter.
      val (pushedFilters, postScanFiltersWithoutSubquery) = PushDownUtils.pushFilters(
        sHolder.builder, normalizedFiltersWithoutSubquery)
      val postScanFilters = postScanFiltersWithoutSubquery ++ normalizedFiltersWithSubquery

      val partitionColumns =
        sHolder.relation.resolve(sHolder.schema, spark.sessionState.analyzer.resolver)
      val partitionSet = AttributeSet(partitionColumns)
      val (partitionFilters, dataFilters) = normalizedFilters.partition(f =>
        f.references.subsetOf(partitionSet)
      )
      logInfo(
        s"""
           |Pushing operators to ${sHolder.relation.name}
           |Pushed Filters: ${pushedFilters.mkString(", ")}
           |Post-Scan Filters: ${postScanFilters.mkString(",")}
         """.stripMargin)

      val filterCondition = postScanFilters.reduceLeftOption(And)
      filterCondition.map(Filter(_, sHolder)).getOrElse(sHolder)
  }

  def pushDownAggregates(plan: LogicalPlan): LogicalPlan = plan.transform {
    // update the scan builder with agg pushdown and return a new plan with agg pushed
    case aggNode @ Aggregate(groupingExpressions, resultExpressions, child) =>
      child match {
        case ScanOperation(project, filters, sHolder: ScanBuilderHolder)
          if filters.isEmpty && project.forall(_.isInstanceOf[AttributeReference]) =>
          sHolder.builder match {
            case _: SupportsPushDownAggregates =>
              val aggExprToOutputOrdinal = mutable.HashMap.empty[Expression, Int]
              var ordinal = 0
              val aggregates = resultExpressions.flatMap { expr =>
                expr.collect {
                  // Do not push down duplicated aggregate expressions. For example,
                  // `SELECT max(a) + 1, max(a) + 2 FROM ...`, we should only push down one
                  // `max(a)` to the data source.
                  case agg: AggregateExpression
                    if !aggExprToOutputOrdinal.contains(agg.canonicalized) =>
                    aggExprToOutputOrdinal(agg.canonicalized) = ordinal
                    ordinal += 1
                    agg
                }
              }
              val normalizedAggregates = normalizeExprs(
                aggregates, sHolder.relation.output).asInstanceOf[Seq[AggregateExpression]]
              val normalizedGroupingExpressions = normalizeExprs(
                groupingExpressions, sHolder.relation.output)
              val pushedAggregates = PushDownUtils.pushAggregates(
                sHolder.builder, normalizedAggregates, normalizedGroupingExpressions)
              if (pushedAggregates.isEmpty) {
                aggNode // return original plan node
              } else {
                // No need to do column pruning because only the aggregate columns are used as
                // DataSourceV2ScanRelation output columns. All the other columns are not
                // included in the output.
                val scan = sHolder.builder.build()

                // scalastyle:off
                // use the group by columns and aggregate columns as the output columns
                // e.g. TABLE t (c1 INT, c2 INT, c3 INT)
                // SELECT min(c1), max(c1) FROM t GROUP BY c2;
                // Use c2, min(c1), max(c1) as output for DataSourceV2ScanRelation
                // We want to have the following logical plan:
                // == Optimized Logical Plan ==
                // Aggregate [c2#10], [min(min(c1)#21) AS min(c1)#17, max(max(c1)#22) AS max(c1)#18]
                // +- RelationV2[c2#10, min(c1)#21, max(c1)#22]
                // scalastyle:on
                val newOutput = SparkHelper.convertAttribute(scan.readSchema())
                assert(newOutput.length == groupingExpressions.length + aggregates.length)
                val groupAttrs = normalizedGroupingExpressions.zip(newOutput).map {
                  case (a: Attribute, b: Attribute) => b.withExprId(a.exprId)
                  case (_, b) => b
                }
                val output = groupAttrs ++ newOutput.drop(groupAttrs.length)

                logInfo(
                  s"""
                     |Pushing operators to ${sHolder.relation.name}
                     |Pushed Aggregate Functions:
                     | ${pushedAggregates.get.aggregateExpressions.mkString(", ")}
                     |Pushed Group by:
                     | ${pushedAggregates.get.groupByColumns.mkString(", ")}
                     |Output: ${output.mkString(", ")}
                      """.stripMargin)

                val wrappedScan = getWrappedScan(scan, sHolder, pushedAggregates)

                val scanRelation = DataSourceV2ScanRelation(sHolder.relation, wrappedScan, output)

                val plan = Aggregate(
                  output.take(groupingExpressions.length), resultExpressions, scanRelation)

                // scalastyle:off
                // Change the optimized logical plan to reflect the pushed down aggregate
                // e.g. TABLE t (c1 INT, c2 INT, c3 INT)
                // SELECT min(c1), max(c1) FROM t GROUP BY c2;
                // The original logical plan is
                // Aggregate [c2#10],[min(c1#9) AS min(c1)#17, max(c1#9) AS max(c1)#18]
                // +- RelationV2[c1#9, c2#10] ...
                //
                // After change the V2ScanRelation output to [c2#10, min(c1)#21, max(c1)#22]
                // we have the following
                // !Aggregate [c2#10], [min(c1#9) AS min(c1)#17, max(c1#9) AS max(c1)#18]
                // +- RelationV2[c2#10, min(c1)#21, max(c1)#22] ...
                //
                // We want to change it to
                // == Optimized Logical Plan ==
                // Aggregate [c2#10], [min(min(c1)#21) AS min(c1)#17, max(max(c1)#22) AS max(c1)#18]
                // +- RelationV2[c2#10, min(c1)#21, max(c1)#22] ...
                // scalastyle:on
                val aggOutput = output.drop(groupAttrs.length)
                plan.transformExpressions {
                  case agg: AggregateExpression =>
                    val ordinal = aggExprToOutputOrdinal(agg.canonicalized)
                    val aggFunction: aggregate.AggregateFunction =
                      agg.aggregateFunction match {
                        case max: aggregate.Max => max.copy(child = aggOutput(ordinal))
                        case min: aggregate.Min => min.copy(child = aggOutput(ordinal))
                        case sum: aggregate.Sum => sum.copy(child = aggOutput(ordinal))
                        case _: aggregate.Count => aggregate.Sum(aggOutput(ordinal))
                        case other => other
                      }
                    agg.copy(aggregateFunction = aggFunction)
                }
              }
            case _ => aggNode
          }
        case _ => aggNode
      }
  }

  def applyColumnPruning(plan: LogicalPlan): LogicalPlan = plan.transform {
    case ScanOperation(project, filters, sHolder: ScanBuilderHolder) =>
      // column pruning
      val normalizedProjects = normalizeExprs(project, sHolder.output)
        .asInstanceOf[Seq[NamedExpression]]
      val (scan, output) = PushDownUtils.pruneColumns(
        sHolder.builder, sHolder.relation, normalizedProjects, filters)

      logInfo(
        s"""
           |Output pushdown: ${output.mkString(", ")}
         """.stripMargin)

      val wrappedScan = getWrappedScan(scan, sHolder, Option.empty[Aggregation])

      val scanRelation = DataSourceV2ScanRelation(sHolder.relation, wrappedScan, output)

      val projectionOverSchema =
        ProjectionOverSchema(output.toStructType, AttributeSet(output))
      val projectionFunc = (expr: Expression) => expr transformDown {
        case projectionOverSchema(newExpr) => newExpr
      }

      val filterCondition = filters.reduceLeftOption(And)
      val newFilterCondition = filterCondition.map(projectionFunc)
      val withFilter = newFilterCondition.map(Filter(_, scanRelation)).getOrElse(scanRelation)

      val withProjection = if (withFilter.output != project) {
        val newProjects = normalizedProjects
          .map(projectionFunc)
          .asInstanceOf[Seq[NamedExpression]]
        Project(restoreOriginalOutputNames(newProjects, project.map(_.name)), withFilter)
      } else {
        withFilter
      }
      withProjection
  }

  private def getWrappedScan(
                              scan: Scan,
                              sHolder: ScanBuilderHolder,
                              aggregation: Option[Aggregation]): Scan = {
    scan match {
      case v1: V1Scan =>
        val pushedFilters = sHolder.builder match {
          case f: SupportsPushDownFilters =>
            f.pushedFilters()
          case _ => Array.empty[sources.Filter]
        }
        V1ScanWrapper(v1, pushedFilters, aggregation)
      /*case fileScan: ParquetScan =>
        val pushedFilters = sHolder.builder match {
          case f: SupportsPushDownFilters =>
            f.pushedFilters()
          case _ => Array.empty[sources.Filter]
        }
        FileScanWrapper(fileScan, pushedFilters, aggregation)*/
      case _ =>
        scan match {
          case scan1: FileScan =>
            /*if (scan1.readPartitionSchema.nonEmpty) {
              val (normalizedFiltersWithSubquery, normalizedFiltersWithoutSubquery) =
                normalizedFilters.partition(SubqueryExpression.hasSubquery)
              val (partitionKeyFilters, dataFilters) =
                getPartitionKeyFiltersAndDataFilters(scan1.sparkSession, v2Relation,
                  scan1.readPartitionSchema, filters, output)
              logInfo(s"scan.fileIndex: ${scan1.fileIndex}")*/
            /*scan1.fileIndex match {
              case index: PolyCatFileIndex =>
                index.refresh1(partitionFilters)
              case _ =>
            }*/
          case _ =>
        }

        logInfo(s"pushedFilters scan: ${scan}")
        scan
    }
  }
}

case class ScanBuilderHolder(
                              output: Seq[AttributeReference],
                              relation: DataSourceV2Relation,
                              builder: ScanBuilder) extends LeafNode

// A wrapper for v1 scan to carry the translated filters and the handled ones. This is required by
// the physical v1 scan node.
case class V1ScanWrapper(
                          v1Scan: V1Scan,
                          handledFilters: Seq[sources.Filter],
                          pushedAggregate: Option[Aggregation]) extends Scan {
  override def readSchema(): StructType = v1Scan.readSchema()
}

case class FileScanWrapper(
                            fileScan: ParquetScan,
                            handledFilters: Seq[sources.Filter],
                            pushedAggregate: Option[Aggregation]) extends Scan {
  override def readSchema(): StructType = fileScan.readSchema()
}
