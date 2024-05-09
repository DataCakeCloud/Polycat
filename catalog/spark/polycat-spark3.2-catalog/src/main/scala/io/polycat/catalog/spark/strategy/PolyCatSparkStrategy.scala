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
package io.polycat.catalog.spark.strategy

import io.polycat.catalog.client.PolyCatClientHelper
import io.polycat.catalog.spark.execution.RecoverPartitionsExec
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.ResolvedTable
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTablePartition, CatalogTableType, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Attribute, AttributeReference, AttributeSet, Expression, Literal, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.planning.{PhysicalOperation, ScanOperation}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.catalog.SupportFileFormat._
import org.apache.spark.sql.connector.catalog.{PolyCatPartitionTable, SparkHelper, SparkWrite}
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, PolyCatFileIndex}
import org.apache.spark.sql.execution.datasources.v2.text.TextScan
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation, OverwritePartitionsDynamicExec}
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.hive.SparkHiveHelper
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{SparkSession, Strategy, catalyst}
import org.apache.spark.unsafe.types.UTF8String

import java.net.URI
import scala.jdk.CollectionConverters.mapAsScalaMapConverter


/**
 * @author liangyouze
 * @date 2024/3/6
 */

case class PolyCatSparkStrategy(spark: SparkSession) extends Strategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {


    case PhysicalOperation(project, filters, relation@DataSourceV2ScanRelation(
    DataSourceV2Relation(table: PolyCatPartitionTable, _, _, _, _), scan: TextScan, output))
    if (table.schema().size > 1) =>
      val partitionAttributes = output.filter(a => table.partitionSchema().exists(partition => partition.name == a.name))
      val partitionKeyIds = AttributeSet(partitionAttributes)
      val polycatTable = PolyCatClientHelper.getTable(table.getClient, table.getTableName)
      val sd = polycatTable.getStorageDescriptor
      val dataCols = output.filter(attribute => !table.partitionSchema().exists(partition => partition.name == attribute.name))
      val partitionCols = output.filter(attribute => table.partitionSchema().exists(partition => partition.name == attribute.name))
      val tableMeta = CatalogTable(TableIdentifier(table.getTableName.getTableName, Option(table.getTableName.getDatabaseName)),
        tableType = CatalogTableType.EXTERNAL,
        schema = (dataCols ++ partitionCols).toStructType,
        partitionColumnNames = table.partitionSchema().map(_.name),
        storage = CatalogStorageFormat(
          locationUri = Option(new URI(sd.getLocation)),
          inputFormat = Option(sd.getInputFormat),
          outputFormat = Option(sd.getOutputFormat),
          serde = Option(sd.getSerdeInfo.getSerializationLibrary),
          compressed = sd.getCompressed,
          properties = Option(sd.getSerdeInfo.getParameters).map(_.asScala.toMap).orNull
        ),
        properties = table.properties().asScala.toMap
      )

      val prunedPartitions = scan.fileIndex.asInstanceOf[PolyCatFileIndex].getCachePartitionPaths().map(p => {
        val partitionSpec = table.partitionSchema().map(_.name).zip(p.values.toSeq(table.partitionSchema()).map(_.toString)).toMap
        val storage = tableMeta.storage.copy(locationUri = Option(p.path.toUri))
        CatalogTablePartition(
          spec = partitionSpec,
          storage = storage,
          parameters = table.properties().asScala.toMap
        )
      })
      val hiveTableRelation = HiveTableRelation(tableMeta,
        dataCols = dataCols,
        partitionCols = partitionCols,
        prunedPartitions = Option(prunedPartitions)
      )
      pruneFilterProject(
        project,
        filters.filter(f => f.references.isEmpty || !f.references.subsetOf(partitionKeyIds)),
        identity[Seq[Expression]],
        SparkHiveHelper.createHiveTableScanExec(_, hiveTableRelation, scan.partitionFilters)(spark)) :: Nil

    case RecoverPartitions(_ @ ResolvedTable(_, _, table: PolyCatPartitionTable, _)) =>
      RecoverPartitionsExec(
        table = table,
        sparkSession = spark,
        enableAddPartitions = true,
        enableDropPartitions = false,
        cmd = "ALTER TABLE RECOVER PARTITIONS") :: Nil
    case RepairTable(_ @ ResolvedTable(_, _, table: PolyCatPartitionTable, _), enableAddPartitions, enableDropPartitions) =>
      RecoverPartitionsExec(
        table = table,
        sparkSession = spark,
        enableAddPartitions = enableAddPartitions,
        enableDropPartitions = enableDropPartitions,
        cmd = "MSCK REPAIR TABLE"
      ) :: Nil

    /*case AppendData(r @ DataSourceV2Relation(table: PolyCatPartitionTable, output: Seq[AttributeReference], _, _, _), query, _, _, Some(write)) =>
      AppendDataExec(planLater(orderedQueryPlan(table, output, query)), refreshCache(r), write) :: Nil
    case OverwriteByExpression(r @ DataSourceV2Relation(table: PolyCatPartitionTable, output: Seq[AttributeReference], _, _, _), _, query, _, _, Some(write)) =>
      OverwriteByExpressionExec(planLater(orderedQueryPlan(table, output, query)), refreshCache(r), write) :: Nil

    case OverwritePartitionsDynamic(r @ DataSourceV2Relation(table: PolyCatPartitionTable, output: Seq[AttributeReference], _, _, _), query, _, _, Some(write)) =>
      OverwritePartitionsDynamicExec(planLater(orderedQueryPlan(table, output, query)), refreshCache(r), write) :: Nil*/


    case OverwritePartitionsDynamic(r @ DataSourceV2Relation(table: PolyCatPartitionTable, output: Seq[AttributeReference], _, _, _), query, _, _, Some(write: SparkWrite)) =>
      query.transform {
        case plan @ Project(seq: Seq[NamedExpression], _) =>
          seq.foreach {
            case Alias(literal: Literal, name: String) =>
              if (table.partitionSchema().count(partition => partition.name == name && literal.dataType == DataTypes.StringType) > 0) {
                val value = literal.value.asInstanceOf[UTF8String]
                write.staticPartitions += (name -> value.toString)
              }
            case _ =>
          }
          plan
        case plan => plan
      }
      OverwritePartitionsDynamicExec(planLater(query), refreshCache(r), write) :: Nil
    case _ =>
      Nil
  }

  def orderedQueryPlan(table: PolyCatPartitionTable, output: Seq[AttributeReference], query: LogicalPlan): LogicalPlan = {
    val requiredOrdering = output.filter(a => table.partitionSchema().exists(partition => partition.name == a.name))
    val actualOrdering = query.outputOrdering.map(_.child)
    val orderingMatched = if (requiredOrdering.length > actualOrdering.length) {
      false
    } else {
      requiredOrdering.zip(actualOrdering).forall {
        case (requiredOrder, childOutputOrder) =>
          requiredOrder.semanticEquals(childOutputOrder)
      }
    }
    if (orderingMatched) {
      query
    } else {
      val orderingExpr = bindReferences(
        requiredOrdering.map(SortOrder(_, Ascending)), output)
      Sort(orderingExpr, false, query)
    }
  }

  private def refreshCache(r: DataSourceV2Relation)(): Unit = {
    spark.sharedState.cacheManager.recacheByPlan(spark, r)
  }

  def pruneFilterProject(
                          projectList: Seq[NamedExpression],
                          filterPredicates: Seq[Expression],
                          prunePushedDownFilters: Seq[Expression] => Seq[Expression],
                          scanBuilder: Seq[Attribute] => SparkPlan): SparkPlan = {

    val projectSet = AttributeSet(projectList.flatMap(_.references))
    val filterSet = AttributeSet(filterPredicates.flatMap(_.references))
    val filterCondition: Option[Expression] =
      prunePushedDownFilters(filterPredicates).reduceLeftOption(catalyst.expressions.And)

    // Right now we still use a projection even if the only evaluation is applying an alias
    // to a column.  Since this is a no-op, it could be avoided. However, using this
    // optimization with the current implementation would change the output schema.
    // TODO: Decouple final output schema from expression evaluation so this copy can be
    // avoided safely.

    if (AttributeSet(projectList.map(_.toAttribute)) == projectSet &&
      filterSet.subsetOf(projectSet)) {
      // When it is possible to just use column pruning to get the right projection and
      // when the columns of this projection are enough to evaluate all filter conditions,
      // just do a scan followed by a filter, with no extra project.
      val scan = scanBuilder(projectList.asInstanceOf[Seq[Attribute]])
      filterCondition.map(FilterExec(_, scan)).getOrElse(scan)
    } else {
      val scan = scanBuilder((projectSet ++ filterSet).toSeq)
      ProjectExec(projectList, filterCondition.map(FilterExec(_, scan)).getOrElse(scan))
    }
  }

}
