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
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Expand, LogicalPlan, Project, Union, V2WriteCommand, Window}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable}

import scala.collection.mutable
import scala.collection.mutable.{Map, Set}

class SparkLineageParser(catalog: String) extends Logging {
  private val targetTableName: Set[String] = Set()
  private val sourceTablesName: Set[String] = Set()
  private val targetTableColumns: Map[Long, String] = Map()
  private val sourceTablesColumns: Map[Long, String] = Map()
  private val fieldProcess: Map[Long, mutable.Set[Long]] = Map()
  private val commonLevelId: Map[Long, mutable.Set[Long]] = Map()
  private val fieldLineage: Map[String, mutable.Set[String]] = mutable.Map();

  def parsePlan(sparkPlan: LogicalPlan, lineageInfo: LineageInfoInput): Unit = {
    parseTableAndColumn(sparkPlan)
    connectSourceFieldAndTargetField()
    if (targetTableName.isEmpty) {
      return
    }
    // add table node
    (targetTableName ++ sourceTablesName).foreach(tableName => {
      val qualifiedName = catalog + "." + tableName
      lineageInfo.addNode(new LineageNode(qualifiedName, EDbType.SPARKSQL, ELineageObjectType.TABLE, ELineageSourceType.LINEAGE, false))
    })
    // add table relation
    sourceTablesName.foreach(sourceTable => {
      targetTableName.foreach(targetTable => {
        val sourceQualifiedName = s"$catalog.$sourceTable"
        val targetQualifiedName = s"$catalog.$targetTable"
        val lineageRs = new LineageInfoInput.LineageRsInput(ELineageType.TABLE_DEPEND_TABLE)
        lineageRs.addUpstreamNode(EDbType.SPARKSQL, ELineageObjectType.TABLE, sourceQualifiedName)
        lineageRs.setDownstreamNode(EDbType.SPARKSQL, ELineageObjectType.TABLE, targetQualifiedName)
        lineageInfo.addRelationShipMap(lineageRs)
      })
    })
    // add table column and add column relation
    fieldLineage.foreach {
      case (targetCol, sourceCols) =>
        val targetQualifiedName = s"$catalog.$targetCol"
        createNodeAndAddToLineage(targetQualifiedName)

        sourceCols.foreach { sourceCol =>
          val sourceQualifiedName = s"$catalog.$sourceCol"
          createNodeAndAddToLineage(sourceQualifiedName)

          val lineageRs = new LineageInfoInput.LineageRsInput(
            ELineageType.FIELD_DEPEND_FIELD,
            EDependWay.EXPRESSION,
            ""
          )
          lineageRs.addUpstreamNode(EDbType.SPARKSQL, ELineageObjectType.COLUMN, sourceQualifiedName)
          lineageRs.setDownstreamNode(EDbType.SPARKSQL, ELineageObjectType.COLUMN, targetQualifiedName)
          lineageInfo.addRelationShipMap(lineageRs)
        }
    }

    def createNodeAndAddToLineage(qualifiedName: String): Unit = {
      val lineageNode = new LineageNode(
        qualifiedName,
        EDbType.SPARKSQL,
        ELineageObjectType.COLUMN,
        ELineageSourceType.LINEAGE,
        false
      )
      lineageInfo.addNode(lineageNode)
    }
  }

  def parseTableAndColumn(plan: LogicalPlan): Unit = {
    plan.collect {
      case plan: LogicalRelation => {
        val calalogTable = plan.catalogTable.get
        val tableName = calalogTable.database + "." + calalogTable.identifier.table
        sourceTablesName += tableName
        plan.output.foreach(columnAttribute => {
          val columnFullName = tableName + "." + columnAttribute.name
          sourceTablesColumns += (columnAttribute.exprId.id -> columnFullName)
        })

      }
      case plan: HiveTableRelation => {
        val tableName = plan.tableMeta.database + "." + plan.tableMeta.identifier.table
        sourceTablesName += tableName
        plan.output.foreach(columnAttribute => {
          val columnFullName = tableName + "." + columnAttribute.name
          sourceTablesColumns += (columnAttribute.exprId.id -> columnFullName)
        })
      }
      case plan: DataSourceV2Relation => {
        val identifier = plan.identifier.get
        val tableName = identifier.namespace()(0) + "." + identifier.name()
        sourceTablesName += tableName
        plan.output.foreach(columnAttribute => {
          val columnFullName = tableName + "." + columnAttribute.name
          sourceTablesColumns += (columnAttribute.exprId.id -> columnFullName)
        })
      }
      case plan: InsertIntoHiveTable => {
        val tableName = plan.table.database + "." + plan.table.identifier.table
        targetTableName += tableName
        extTargetTable(tableName, plan.query)
      }
      case plan: InsertIntoHadoopFsRelationCommand => {
        val catalogTable: CatalogTable = plan.catalogTable.get
        val tableName = catalogTable.database + "." + catalogTable.identifier.table
        targetTableName += tableName
        extTargetTable(tableName, plan.query)
      }
      case plan: CreateHiveTableAsSelectCommand => {
        val tableName = plan.tableDesc.database + "." + plan.tableDesc.identifier.table
        targetTableName += tableName
        extTargetTable(tableName, plan.query)
      }
      case plan: V2WriteCommand => {
        val tableName = plan.table.name.split("\\.") match {
          case Array(lastPart) => lastPart
          case splits => splits(splits.length - 2) + "." + splits.last
        }
        targetTableName += tableName
        extTargetTable(tableName, plan.query)
      }
      case plan: Aggregate => plan.aggregateExpressions.foreach(extFieldProcess)
      case plan: Window => plan.windowExpressions.foreach(extFieldProcess)
      case plan: Project => plan.projectList.toList.foreach(extFieldProcess)
      case plan: Expand  => {
        plan.output.groupBy(_.name)
          .filter(_._2.length == 2)
          .foreach(attr => {
            val sourceFieldId = attr._2.seq(1).exprId.id
            val targetFieldId = attr._2.seq(0).exprId.id
            val targetFieldIdSet: mutable.Set[Long] = fieldProcess.getOrElse(sourceFieldId, mutable.Set.empty)
            targetFieldIdSet += targetFieldId
            fieldProcess += (sourceFieldId -> targetFieldIdSet)
          })
      }
      case plan: Union => {
        plan.outputSet.zipWithIndex.foreach {
          case (att, index) =>
            val elderBrother = att.exprId.id
            val brothers: mutable.Set[Long] = commonLevelId.getOrElse(elderBrother, mutable.Set.empty)
            plan.children.foreach {
              case p: Project =>
                brothers += (p.projectList(index).exprId.id)
              case p: Aggregate =>
                parseTableAndColumn(p)
                brothers += p.aggregateExpressions(index).exprId.id
            }
            commonLevelId += (elderBrother -> brothers)
        }
      }
    }
  }

  def extFieldProcess(namedExpression: NamedExpression): Unit = {
    if ("alias".equals(namedExpression.prettyName)) {
      val sourceFieldId = namedExpression.exprId.id
      val targetFieldIdSet: mutable.Set[Long] = fieldProcess.getOrElse(sourceFieldId, mutable.Set.empty)
      namedExpression.references.foreach(attribute => targetFieldIdSet += attribute.exprId.id)
      fieldProcess += (sourceFieldId -> targetFieldIdSet)
    }
  }

  def extTargetTable(tableName: String, plan: LogicalPlan): Unit = {
    plan.output.foreach(columnAttribute => {
      val columnFullName = tableName + "." + columnAttribute.name
      targetTableColumns += (columnAttribute.exprId.id -> columnFullName)
    })
  }

  def connectSourceFieldAndTargetField(): Unit = {
    val fieldIds = targetTableColumns.keySet
    fieldIds.foreach(fieldId => {
      val resTargetFieldName = targetTableColumns(fieldId)
      val resSourceFieldSet: mutable.Set[String] = mutable.Set.empty[String]
      if (sourceTablesColumns.contains(fieldId)) {
        val sourceFieldId = sourceTablesColumns.getOrElse(fieldId, "")
        resSourceFieldSet += sourceFieldId
        resSourceFieldSet ++= findCommonLevelField(fieldId)
      } else {
        val targetIdsTmp = findSourceField(fieldId)
        resSourceFieldSet ++= targetIdsTmp
      }
      fieldLineage += (resTargetFieldName -> resSourceFieldSet)
    })
  }

  def findSourceField(fieldId: Long): mutable.Set[String] = {
    val resSourceFieldSet: mutable.Set[String] = mutable.Set.empty[String]
    if (fieldProcess.contains(fieldId)) {
      val fieldIds: mutable.Set[Long] = fieldProcess.getOrElse(fieldId, mutable.Set.empty)
      fieldIds.foreach(fieldId => {
        if (sourceTablesColumns.contains(fieldId)) {
          resSourceFieldSet += sourceTablesColumns(fieldId)
          resSourceFieldSet ++= findCommonLevelField(fieldId)
        } else {
          val sourceFieldSet = findSourceField(fieldId)
          if (sourceFieldSet.nonEmpty) {
            resSourceFieldSet ++= findCommonLevelField(fieldId)
          }
          resSourceFieldSet ++= sourceFieldSet
        }
      })
    }
    resSourceFieldSet
  }

  def findCommonLevelField(fieldId:Long) = {
    val commonLevelField: mutable.Set[String] = mutable.Set.empty[String]
    commonLevelId.getOrElse(fieldId, mutable.Set.empty).foreach { fid: Long =>
      commonLevelField ++= findSourceField(fid)
    }
    commonLevelField
  }

}
