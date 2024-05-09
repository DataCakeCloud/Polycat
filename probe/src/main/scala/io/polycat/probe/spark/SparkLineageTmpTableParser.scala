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
package io.polycat.probe.spark

import io.polycat.catalog.common.lineage.{EDbType, EDependWay, ELineageObjectType, ELineageSourceType, ELineageType, LineageNode}
import io.polycat.catalog.common.plugin.request.input.LineageInfoInput
import io.polycat.catalog.common.utils.LineageUtils
import io.polycat.probe.ProbeConstants
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OverwriteByExpression, OverwritePartitionsDynamic, Project, SubqueryAlias}
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable}

import java.util
import scala.collection.JavaConverters._

class SparkLineageTmpTableParser(conf: Configuration) {

  def parsePlan(sparkPlan: LogicalPlan, parentTable: String, lineageInfo: LineageInfoInput, catalog: String): Unit = {
    var parentTableName = parentTable
    sparkPlan match {
      case cdt: CreateDataSourceTableAsSelectCommand =>
        val lineageQualifiedName = generateLineageQualifiedNameFromTableIdentifier(catalog, cdt.table.identifier)
        parentTableName = lineageQualifiedName
        addLineageNode(lineageInfo, lineageQualifiedName)

      case ctas: CreateHiveTableAsSelectCommand =>
        val lineageQualifiedName = generateLineageQualifiedNameFromTableIdentifier(catalog, ctas.tableDesc.identifier)
        parentTableName = lineageQualifiedName
        addLineageNode(lineageInfo, lineageQualifiedName)

      case iiht: InsertIntoHiveTable =>
        val lineageQualifiedName = generateLineageQualifiedNameFromTableIdentifier(catalog, iiht.table.identifier)
        parentTableName = lineageQualifiedName
        addLineageNode(lineageInfo, lineageQualifiedName)

      case overwrite: OverwriteByExpression =>
        val identifier = overwrite.table.asInstanceOf[DataSourceV2Relation].identifier.get
        val lineageQualifiedName = LineageUtils.getTableQualifiedName(catalog, identifier.namespace()(0), identifier.name())
        parentTableName = lineageQualifiedName
        addLineageNode(lineageInfo, lineageQualifiedName)

      case overwrite: OverwritePartitionsDynamic =>
        val identifier = overwrite.table.asInstanceOf[DataSourceV2Relation].identifier.get
        val lineageQualifiedName = LineageUtils.getTableQualifiedName(catalog, identifier.namespace()(0), identifier.name())
        parentTableName = lineageQualifiedName
        addLineageNode(lineageInfo, lineageQualifiedName)

      case insert: InsertIntoHadoopFsRelationCommand if insert.catalogTable.isDefined =>
        val lineageQualifiedName = generateLineageQualifiedNameFromTableIdentifier(catalog, insert.catalogTable.get.identifier)
        parentTableName = lineageQualifiedName
        addLineageNode(lineageInfo, lineageQualifiedName)
      case subquery: SubqueryAlias =>
        if (!subquery.identifier.qualifier.contains("spark_catalog")) {
          val tmpTableQualifiedName = s"SPARK_${System.currentTimeMillis()}_TMP_${subquery.identifier}"
          lineageInfo.addNode(new LineageNode(tmpTableQualifiedName, EDbType.SPARKSQL, ELineageObjectType.TABLE, ELineageSourceType.LINEAGE, true))
          // add table dependency for tmpTable and parentTable
          addSparkTableDependencies(lineageInfo, tmpTableQualifiedName, parentTableName)

          parentTableName = tmpTableQualifiedName
        }
      case project: Project => {
        project.projectList.foreach {
          case attribute: Attribute =>
            // merge columns from two consecutive projects
            val lineageColumnInfo = searchExistColumnInfoFromCache(lineageInfo, parentTableName, attribute.qualifiedName)
            if (lineageColumnInfo.isEmpty) {
              lineageInfo.addNode(new LineageNode(LineageUtils.getColumnQualifiedName(parentTableName, attribute.name),
                EDbType.SPARKSQL, ELineageObjectType.COLUMN, ELineageSourceType.LINEAGE, parentTableName.contains("_TMP_")))
              // cache column
              val columnInfo = new LineageInfoInput.LineageColumnInfo
              columnInfo.setTf(false)
              columnInfo.setTfName(attribute.qualifiedName)
              columnInfo.setTfSourceExpress(attribute.treeString)
              lineageInfo.cacheTableColumns(parentTableName, columnInfo)
            }
          case _ => None
        }
        project.projectList.foreach {
          case attribute: Attribute => dealSparkColumnDependencies(lineageInfo, parentTableName, attribute.name)
          case alias: Alias =>
            val hiddenCols = parseColNameFromExpress(alias.child)
            // merge columns from two consecutive projects
            val lineageColumnInfo = searchExistColumnInfoFromCache(lineageInfo, parentTableName, alias.name)
            lineageColumnInfo.foreach(col => {
              col.setTfSourceName(hiddenCols)
              col.setTfSourceExpress(alias.child.treeString)
            })
            if (lineageColumnInfo.isEmpty) {
              lineageInfo.addNode(new LineageNode(LineageUtils.getColumnQualifiedName(parentTableName, alias.name),
                EDbType.SPARKSQL, ELineageObjectType.COLUMN, ELineageSourceType.LINEAGE, true))
              // cache column
              val columnInfo = new LineageInfoInput.LineageColumnInfo
              columnInfo.setTf(true)
              columnInfo.setTfName(alias.name)
              columnInfo.setTfSourceExpress(alias.child.treeString)
              columnInfo.setTfSourceName(hiddenCols)
              lineageInfo.cacheTableColumns(parentTableName, columnInfo)
              // create field lineage relationship
              dealSparkColumnDependencies(lineageInfo, parentTableName, alias.name)
            }
        }
      }

      case relation: LogicalRelation => {
        val lineageQualifiedName = generateLineageQualifiedNameFromTableIdentifier(catalog, relation.catalogTable.get.identifier)
        lineageInfo.addNode(new LineageNode(lineageQualifiedName, EDbType.SPARKSQL, ELineageObjectType.TABLE, ELineageSourceType.LINEAGE, false))
        addSparkTableDependencies(lineageInfo, lineageQualifiedName, parentTableName)
        // deal  from db.table  tmp, parent is tmp and no fill col
        val parentTableNotDeal = Option(lineageInfo.getTableColumnsCache).exists(_.get(parentTableName) == null)

        relation.outputSet.foreach {
          case attribute: Attribute =>
            lineageInfo.addNode(new LineageNode(LineageUtils.getColumnQualifiedName(lineageQualifiedName, attribute.name),
              EDbType.SPARKSQL, ELineageObjectType.COLUMN, ELineageSourceType.LINEAGE, false))
            if (parentTableNotDeal) {
              lineageInfo.addNode(new LineageNode(LineageUtils.getColumnQualifiedName(parentTableName, attribute.name),
                EDbType.SPARKSQL, ELineageObjectType.COLUMN, ELineageSourceType.LINEAGE, true))
              addSparkColumnDependencies(lineageInfo, lineageQualifiedName, attribute.name, parentTableName, attribute.name, attribute.name)
            }
        }

        relation.outputSet.foreach {
          case attribute: Attribute =>
            if (parentTableNotDeal) {
              val tables = lineageInfo.getDownstreamCacheTableByUpstreamTableNode(EDbType.SPARKSQL, parentTableName)
              tables.forEach(table => dealSparkRelationOutDependencies(lineageInfo, table, parentTableName, attribute))
            } else {
              dealSparkRelationOutDependencies(lineageInfo, parentTableName, lineageQualifiedName, attribute)
            }
        }
      }
      case _ => None
    }
    sparkPlan.children.foreach(plan => parsePlan(plan, parentTableName, lineageInfo, catalog))
  }

  private def dealSparkRelationOutDependencies(lineageInfo: LineageInfoInput, parentTableName: String, lineageQualifiedName: String, attribute: Attribute) = {
    val downstreamColumns = lineageInfo.getCachedTableColumns(parentTableName)
    downstreamColumns.forEach(dsCol => {
      val originDownStreamTable = convertLineageTableNameToOriginTable(parentTableName)
      val originDownStream = extractTableOrColumnName(dsCol.getTfName)
      if (dsCol.isTf) {
        dsCol.getTfSourceName.forEach({
          sourceColumn =>
            // todo: opt column name compare
            if (sourceColumn.equalsIgnoreCase(attribute.name)
              || sourceColumn.equalsIgnoreCase(s"${getOriginTableName(parentTableName)}.${attribute.name}")
              || sourceColumn.equalsIgnoreCase(s"${getOriginTableName(lineageQualifiedName)}.${attribute.name}")
              || replaceSparkCatalog(sourceColumn).equalsIgnoreCase(s"${parentTableName}.${attribute.name}")
              || replaceSparkCatalog(sourceColumn).equalsIgnoreCase(s"${lineageQualifiedName}.${attribute.name}")
              || replaceSparkCatalog(sourceColumn).equalsIgnoreCase(s"${getOriginTableName(parentTableName)}.${attribute.name}")
            ) {
              val sourceColumnName = extractTableOrColumnName(sourceColumn)
              addSparkColumnDependencies(lineageInfo, lineageQualifiedName, sourceColumnName, originDownStreamTable, originDownStream, dsCol.getTfSourceExpress)
            }
        })
      } else {
        if (dsCol.getTfName.equalsIgnoreCase(attribute.name)
          || dsCol.getTfName.equalsIgnoreCase(s"${getOriginTableName(parentTableName)}.${attribute.name}")
          || dsCol.getTfName.equalsIgnoreCase(s"${getOriginTableName(lineageQualifiedName)}.${attribute.name}")
          || replaceSparkCatalog(dsCol.getTfName).equalsIgnoreCase(s"${parentTableName}.${attribute.name}")
          || replaceSparkCatalog(dsCol.getTfName).equalsIgnoreCase(s"${lineageQualifiedName}.${attribute.name}")
          || replaceSparkCatalog(dsCol.getTfName).equalsIgnoreCase(s"${getOriginTableName(parentTableName)}.${attribute.name}")) {
          addSparkColumnDependencies(lineageInfo, lineageQualifiedName, attribute.name, originDownStreamTable, originDownStream, dsCol.getTfSourceExpress)
        }
      }
    })
  }

  def addLineageNode(lineageInfo: LineageInfoInput, lineageQualifiedName: String) = {
    lineageInfo.addNode(new LineageNode(lineageQualifiedName, EDbType.SPARKSQL, ELineageObjectType.TABLE, ELineageSourceType.LINEAGE, false))
    lineageInfo.setTargetTable(LineageUtils.getLineageUniqueName(EDbType.SPARKSQL, ELineageObjectType.TABLE, lineageQualifiedName))
  }

  def dealSparkColumnDependencies(lineageInfo: LineageInfoInput, upstreamTable: String, upstreamColumn: String) = {
    val downstreamTables = lineageInfo.getDownstreamCacheTableByUpstreamTableNode(EDbType.SPARKSQL, upstreamTable)
    downstreamTables.forEach { dnTable =>
      lineageInfo.getCachedTableColumns(dnTable).forEach { dnCol =>
        val tfNameMatches = dnCol.getTfName.equalsIgnoreCase(upstreamColumn) ||
          dnCol.getTfName.equalsIgnoreCase(s"${getOriginTableName(upstreamTable)}.${upstreamColumn}") ||
          dnCol.isTf && dnCol.getTfSourceName.contains(upstreamColumn) ||
          dnCol.isTf && dnCol.getTfSourceName.contains(s"${getOriginTableName(upstreamTable)}.${upstreamColumn}")

        if (tfNameMatches) {
          val originDownStreamTable = convertLineageTableNameToOriginTable(dnTable)
          val originDownStreamColName = extractTableOrColumnName(dnCol.getTfName)
          if (dnCol.isTf) {
            dnCol.getTfSourceName.forEach { sourceColumn =>
              val sourceColumnName = extractTableOrColumnName(sourceColumn)
              addSparkColumnDependencies(lineageInfo, upstreamTable, sourceColumnName, originDownStreamTable, originDownStreamColName, dnCol.getTfSourceExpress)
            }
          } else {
            addSparkColumnDependencies(lineageInfo, upstreamTable, upstreamColumn, originDownStreamTable, originDownStreamColName, dnCol.getTfSourceExpress)
          }
        }
      }
    }
  }

  def addSparkColumnDependencies(lineageInfo: LineageInfoInput,
                                 upstreamTable: String,
                                 upstreamColumn: String,
                                 downstreamTable: String,
                                 downstreamColumn: String,
                                 dsCodeSegment: String) = {
    val lineageRs = new LineageInfoInput.LineageRsInput(ELineageType.FIELD_DEPEND_FIELD, EDependWay.EXPRESSION, dsCodeSegment)
    lineageRs.addUpstreamNode(EDbType.SPARKSQL, ELineageObjectType.COLUMN, LineageUtils.getColumnQualifiedName(upstreamTable, upstreamColumn))
    lineageRs.setDownstreamNode(EDbType.SPARKSQL, ELineageObjectType.COLUMN, LineageUtils.getColumnQualifiedName(downstreamTable, downstreamColumn))
    lineageInfo.addRelationShipMap(lineageRs)
  }

  def parseColNameFromExpress(expression: Expression): util.ArrayList[String] = {
    val columns = new util.ArrayList[String]()
    expression.collectLeaves().foreach({
      case attribute: Attribute => {
        columns.add(attribute.qualifiedName)
      }
      case _ => None
    })
    columns
  }

  def replaceSparkCatalog(tableName: String): String = {
    tableName.replace("spark_catalog", conf.get(ProbeConstants.POLYCAT_CATALOG))
  }

  def getOriginTableName(tableName: String): String = {
    // convert lineageTempQualifiedName to origin table
    Option(StringUtils.substringAfterLast(tableName, "_TMP_")).filterNot(_.isEmpty).getOrElse(tableName)
  }

  // convert lineage table to origin table, e.g.g:  5:5@xxxxxxxx_ue1.db.table =>  xxxxxxxx_ue1.db.table
  def convertLineageTableNameToOriginTable(lineageTable: String): String = {
    Option(StringUtils.substringAfterLast(lineageTable, "@")).filterNot(_.isEmpty).getOrElse(lineageTable)
  }

  def extractTableOrColumnName(identifier: String): String = {
    Option(StringUtils.substringAfterLast(identifier, ".")).filterNot(_.isEmpty).getOrElse(identifier)
  }

  def generateLineageQualifiedNameFromTableIdentifier(catalog: String, identifier: TableIdentifier): String = {
    val database = identifier.database.getOrElse("default")
    val table = identifier.table
    LineageUtils.getTableQualifiedName(catalog, database, table)
  }

  def searchExistColumnInfoFromCache(lineageInfo: LineageInfoInput, parentTable: String, aliasName: String): Option[LineageInfoInput.LineageColumnInfo] = {
    Option(lineageInfo.getTableColumnsCache)
      .flatMap(cache => Option(cache.get(parentTable)))
      .flatMap { columns =>
        columns.asScala.find { column =>
          column.isTf && Option(column.getTfSourceName).exists(_.contains(aliasName))
        }
      }
  }

  def addSparkTableDependencies(lineageInfo: LineageInfoInput, upstreamNode: String, downstreamNode: String) = {
    val lineageRs = new LineageInfoInput.LineageRsInput(ELineageType.TABLE_DEPEND_TABLE)
    lineageRs.addUpstreamNode(EDbType.SPARKSQL, ELineageObjectType.TABLE, upstreamNode)
    lineageRs.setDownstreamNode(EDbType.SPARKSQL, ELineageObjectType.TABLE, downstreamNode)
    lineageInfo.addRelationShipMap(lineageRs)
  }
}
