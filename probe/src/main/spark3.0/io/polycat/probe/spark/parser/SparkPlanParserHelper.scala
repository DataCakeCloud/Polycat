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
package io.polycat.probe.spark.parser

import io.polycat.catalog.common.Operation
import io.polycat.catalog.common.model.CatalogInnerObject
import io.polycat.probe.model.CatalogOperationObject
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{ResolvedNamespace, ResolvedTable, UnresolvedV2Relation}
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.datasources.{RefreshTable => _, _}
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveDirCommand, InsertIntoHiveTable, OptimizedCreateHiveTableAsSelectCommand}


class SparkPlanParserHelper(projectId: String, polyCatCatalog: String) {

  def parsePlan(plan: LogicalPlan): java.util.HashSet[CatalogOperationObject] = {
    val catalogOperationObjectList = new java.util.HashSet[CatalogOperationObject]()
    plan match {
      case runnableCommand: RunnableCommand =>
        SparkPlanParserHelper.collectRunnableCommandInfo(projectId, polyCatCatalog, runnableCommand, catalogOperationObjectList)
      case writeCommand: V2WriteCommand =>
        SparkPlanParserHelper.collectV2WriteCommandInfo(projectId, polyCatCatalog, writeCommand, catalogOperationObjectList)
      case writeCommand: DataWritingCommand =>
        SparkPlanParserHelper.collectDataWritingCommandInfo(projectId, polyCatCatalog, writeCommand, catalogOperationObjectList)
      case command: Command =>
        SparkPlanParserHelper.collectOtherCommandInfo(projectId, polyCatCatalog, command, catalogOperationObjectList)
      case query: LogicalPlan =>
        SparkPlanParserHelper.collectQueryInfo(projectId, polyCatCatalog, query, catalogOperationObjectList)
    }
    catalogOperationObjectList
  }
}

object SparkPlanParserHelper  {

  def collectRunnableCommandInfo(projectId: String, polyCatCatalog: String, runnableCommand: RunnableCommand,
                                 catalogOperationObjectList: java.util.HashSet[CatalogOperationObject]): Unit = {
    val catalogOperationObject: Option[CatalogOperationObject] = runnableCommand match {
      case _: CreateDatabaseCommand =>
        catalogOperation(projectId, polyCatCatalog, Operation.CREATE_DATABASE)
      case adp: AlterDatabasePropertiesCommand =>
        databaseOperation(projectId, Some(polyCatCatalog), adp.databaseName, Operation.ALTER_DATABASE)
      case ddc: DropDatabaseCommand =>
        databaseOperation(projectId, Some(polyCatCatalog), ddc.databaseName, Operation.DROP_DATABASE)
      case desc: DescribeDatabaseCommand =>
        databaseOperation(projectId, Some(polyCatCatalog), desc.databaseName, Operation.DESC_DATABASE)

      case adsl: AlterDatabaseSetLocationCommand =>
        databaseOperation(projectId, Some(polyCatCatalog), adsl.databaseName, Operation.ALTER_DATABASE)
      case cdst: CreateDataSourceTableCommand =>
        databaseOperation(projectId, Some(polyCatCatalog), cdst.table.database, Operation.CREATE_TABLE)
      case ctlc: CreateTableLikeCommand =>
        val database = ctlc.asInstanceOf[CreateTableLikeCommand].targetTable.database.get
        databaseOperation(projectId, Some(polyCatCatalog), database, Operation.CREATE_TABLE)
      case ctc: CreateTableCommand =>
        val database = runnableCommand.asInstanceOf[CreateTableCommand].table.database
        databaseOperation(projectId, Some(polyCatCatalog), database, Operation.CREATE_TABLE)
      case atrc: AlterTableRenameCommand =>
        val oldTable = atrc.asInstanceOf[AlterTableRenameCommand].oldName
        tableOperation(projectId, polyCatCatalog, oldTable.database, oldTable.table, Operation.ALTER_TABLE)
      case atac: AlterTableAddColumnsCommand =>
        tableOperation(projectId, polyCatCatalog, atac.table.database, atac.table.table, Operation.ALTER_TABLE)
      case ldc: LoadDataCommand =>
        tableOperation(projectId, polyCatCatalog, ldc.table.database, ldc.table.table, Operation.INSERT_TABLE)
      case _: TruncateTableCommand => None
      case dt: DescribeTableCommand =>
        tableOperation(projectId, polyCatCatalog, dt.table.database, dt.table.table, Operation.DESC_TABLE)
      case _: DescribeQueryCommand => None
      case _: DescribeColumnCommand => None
      case stc: ShowTablesCommand => None
        //databaseOperation(projectId, Some(polyCatCatalog), stc.asInstanceOf[ShowTablesCommand].databaseName.get, Operation.SHOW_TABLE)
      case _: ShowTablePropertiesCommand => None
      case scc: ShowColumnsCommand => None
        // val table = scc.asInstanceOf[ShowColumnsCommand].tableName
        // tableOperation(projectId, polyCatCatalog, table.database, table.table, Operation.SHOW_TABLE)
      case _: ShowPartitionsCommand => None
      case sctc: ShowCreateTableCommand =>
        val table = sctc.asInstanceOf[ShowCreateTableCommand].table
        tableOperation(projectId, polyCatCatalog, table.database, table.table, Operation.DESC_TABLE)
      case sctasc: ShowCreateTableAsSerdeCommand => None
        // val table = sctasc.asInstanceOf[ShowCreateTableAsSerdeCommand].table
        // tableOperation(projectId, polyCatCatalog, table.database, table.table, Operation.SHOW_TABLE)
      // enlarge operation type to alter_table
      case command: AlterTableSetPropertiesCommand =>
        val table = command.asInstanceOf[AlterTableSetPropertiesCommand].tableName
        tableOperation(projectId, polyCatCatalog, table.database, table.table, Operation.ALTER_TABLE)
      case command: AlterTableUnsetPropertiesCommand =>
        val table = command.asInstanceOf[AlterTableUnsetPropertiesCommand].tableName
        tableOperation(projectId, polyCatCatalog, table.database, table.table, Operation.ALTER_TABLE)
      case command: AlterTableChangeColumnCommand =>
        val table = command.asInstanceOf[AlterTableChangeColumnCommand].tableName
        tableOperation(projectId, polyCatCatalog, table.database, table.table, Operation.ALTER_TABLE)
      case command: AlterTableSerDePropertiesCommand =>
        val table = command.asInstanceOf[AlterTableSerDePropertiesCommand].tableName
        tableOperation(projectId, polyCatCatalog, table.database, table.table, Operation.ALTER_TABLE)
      case command: AlterTableAddPartitionCommand =>
        val table = command.asInstanceOf[AlterTableAddPartitionCommand].tableName
        tableOperation(projectId, polyCatCatalog, table.database, table.table, Operation.ALTER_TABLE)
      case command: AlterTableRenamePartitionCommand =>
        val table = command.asInstanceOf[AlterTableRenamePartitionCommand].tableName
        tableOperation(projectId, polyCatCatalog, table.database, table.table, Operation.ALTER_TABLE)
      case command: AlterTableDropPartitionCommand =>
        val table = command.asInstanceOf[AlterTableDropPartitionCommand].tableName
        tableOperation(projectId, polyCatCatalog, table.database, table.table, Operation.ALTER_TABLE)
      case command: AlterTableRecoverPartitionsCommand =>
        val table = command.asInstanceOf[AlterTableRecoverPartitionsCommand].tableName
        tableOperation(projectId, polyCatCatalog, table.database, table.table, Operation.ALTER_TABLE)
      case command: AlterTableSetLocationCommand => None
        val table = command.asInstanceOf[AlterTableSetLocationCommand].tableName
        tableOperation(projectId, polyCatCatalog, table.database, table.table, Operation.ALTER_TABLE)
      case dt: DropTableCommand =>
        tableOperation(projectId, polyCatCatalog, dt.asInstanceOf[DropTableCommand].tableName, Operation.DROP_TABLE)
      case insert: InsertIntoDataSourceCommand if insert.logicalRelation.catalogTable.isDefined =>
        tableOperation(projectId, polyCatCatalog, insert.logicalRelation.catalogTable.get.identifier, Operation.INSERT_TABLE)
      case _: InsertIntoDataSourceCommand => None
      case _: InsertIntoDataSourceDirCommand => None
      case insert: SaveIntoDataSourceCommand => None
      case ac: AnalyzeColumnCommand => None
      case ap: AnalyzePartitionCommand => None
      case at: AnalyzeTableCommand => None
      case ct: CacheTableCommand => None
      case uct: UncacheTableCommand => None
      case ClearCacheCommand => None
      case explain: ExplainCommand => None
      case se: StreamingExplainCommand => None
      case ece: ExternalCommandExecutor => None
      case _: CreateFunctionCommand => None
      case _: DescribeFunctionCommand => None
      case _: DropFunctionCommand => None
      case _: ShowFunctionsCommand => None
      case _: AddJarCommand => None
      case _: AddFileCommand => None
      case _: ListFilesCommand => None
      case _: ListJarsCommand => None
      case _: SetCommand => None
      case _: CreateViewCommand => None
      case _: AlterViewAsCommand => None
      case _: ShowViewsCommand => None
      case _: CreateTempViewUsing => None
      case _: RefreshTable => None
      case _: RefreshResource => None
      case _ => None
//        throw new CatalogException("No permission. Unknown RunnableCommand: " + runnableCommand.getClass.getName);
    }
    catalogOperationObject.foreach(catalogOperationObjectList.add)
  }

  def collectV2WriteCommandInfo(projectId: String, polyCatCatalog: String, writeCommand: V2WriteCommand,
                                authorizationList: java.util.HashSet[CatalogOperationObject]): Unit = {
    val authorizationInput: Option[CatalogOperationObject] = writeCommand match {
      case rt: ReplaceTable if rt.tableName.namespace().length == 1 =>
        databaseOperation(projectId, Some(polyCatCatalog), rt.tableName.namespace()(0), Operation.CREATE_TABLE)
      case _: ReplaceTable => None
      case ad: AppendData if ad.table.asInstanceOf[DataSourceV2Relation].identifier.isDefined &&
        ad.table.asInstanceOf[DataSourceV2Relation].identifier.get.namespace().length == 1 =>
        val identifier = ad.table.asInstanceOf[DataSourceV2Relation].identifier.get
        tableOperation(projectId, polyCatCatalog, TableIdentifier(identifier.name(), Some(identifier.namespace()(0))), Operation.INSERT_TABLE)
      case _: AppendData => None
      case overwrite: OverwriteByExpression =>
        val identifier = overwrite.table.asInstanceOf[DataSourceV2Relation].identifier.get
        tableOperation(projectId, polyCatCatalog, TableIdentifier(identifier.name(), Some(identifier.namespace()(0))), Operation.INSERT_TABLE)
      case overwrite: OverwritePartitionsDynamic =>
        val identifier = overwrite.table.asInstanceOf[DataSourceV2Relation].identifier.get
        tableOperation(projectId, polyCatCatalog, TableIdentifier(identifier.name(), Some(identifier.namespace()(0))), Operation.INSERT_TABLE)
      case _ => None
//        throw new CatalogException("No permission. Unknown V2WriteCommand: " + writeCommand.getClass.getName);
    }
    authorizationInput.foreach(authorizationList.add)
  }

  def collectDataWritingCommandInfo(projectId: String, polyCatCatalog: String, writeCommand: DataWritingCommand, authorizationList: java.util.HashSet[CatalogOperationObject]): Unit = {
    val authorizationInput: Option[CatalogOperationObject] = writeCommand match {
      case ctas: CreateDataSourceTableAsSelectCommand =>
        collectQueryInfo(projectId, polyCatCatalog, ctas.query, authorizationList)
        val database = ctas.asInstanceOf[CreateDataSourceTableAsSelectCommand].table.database
        databaseOperation(projectId, Some(polyCatCatalog), database, Operation.CREATE_TABLE)
      case ctas: CreateHiveTableAsSelectCommand =>
        collectQueryInfo(projectId, polyCatCatalog, ctas.query, authorizationList)
        val database = ctas.asInstanceOf[CreateHiveTableAsSelectCommand].tableDesc.database
        databaseOperation(projectId, Some(polyCatCatalog), database, Operation.CREATE_TABLE)
      case _: OptimizedCreateHiveTableAsSelectCommand => None
      case _: InsertIntoHiveDirCommand => None
      case insert: InsertIntoHiveTable =>
        collectQueryInfo(projectId, polyCatCatalog, insert.query, authorizationList)
        tableOperation(projectId, polyCatCatalog, Some(insert.table.identifier.database.get), insert.table.identifier.table, Operation.INSERT_TABLE)
      case insert: InsertIntoHadoopFsRelationCommand if insert.catalogTable.isDefined =>
        val table = insert.catalogTable.get.identifier
        collectQueryInfo(projectId, polyCatCatalog, insert.query, authorizationList)
        tableOperation(projectId, polyCatCatalog, table, Operation.INSERT_TABLE)
      case insert: InsertIntoHadoopFsRelationCommand if insert.fileIndex.isDefined =>
        collectQueryInfo(projectId, polyCatCatalog, insert.query, authorizationList)
        val tableInfo = extractTable(insert.fileIndex.get)
        if (tableInfo.isDefined) {
          tableOperation(projectId, polyCatCatalog, tableInfo.get._2, Operation.INSERT_TABLE)
        } else {
          None
        }
      case _: InsertIntoHadoopFsRelationCommand => None
      case _ => None
//        throw new CatalogException("No permission. Unknown DataWritingCommand: " + writeCommand.getClass.getName);
    }
    authorizationInput.foreach(authorizationList.add)
  }

  def extractTable(fileIndex: FileIndex): Option[(String, TableIdentifier)] = {
    if (fileIndex.isInstanceOf[InMemoryFileIndex]) {
      val field = classOf[org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex].
        getDeclaredField("parameters");
      field.setAccessible(true)
      val options = field.get(fileIndex).asInstanceOf[Map[String, String]]
      val catalog = options.get("catalog_name")
      val database = options.get("database_name")
      val table = options.get("table_name")
      if (catalog.isDefined && database.isDefined && table.isDefined) {
        Some((catalog.get, TableIdentifier(table.get, database)))
      } else {
        None
      }
    } else {
      None
    }
  }

  def collectOtherCommandInfo(projectId: String, polyCatCatalog: String, command: Command, authorizationList: java.util.HashSet[CatalogOperationObject]): Unit = {
    val authorizationInput: Option[CatalogOperationObject] = command match {
      case cn: CreateNamespace if cn.namespace.length == 1 =>
        catalogOperation(projectId, polyCatCatalog, Operation.CREATE_DATABASE)
      case _: CreateNamespace => None
      case dn: DropNamespace if dn.namespace.asInstanceOf[ResolvedNamespace].namespace.length == 1 =>
        databaseOperation(projectId, dn.namespace.asInstanceOf[ResolvedNamespace], Operation.DROP_DATABASE)
      case _: DropNamespace => None
      case dn: DescribeNamespace if dn.namespace.asInstanceOf[ResolvedNamespace].namespace.length == 1 =>
        databaseOperation(projectId, dn.namespace.asInstanceOf[ResolvedNamespace], Operation.DESC_DATABASE)
      case _: DescribeNamespace => None
      case sn: ShowCurrentNamespace => None
        //catalogOperation(projectId, polyCatCatalog, Operation.SHOW_DATABASE)
      case sn: ShowNamespaces if sn.namespace.asInstanceOf[ResolvedNamespace].namespace.isEmpty => None
        // catalogOperation(projectId, polyCatCatalog, Operation.SHOW_DATABASE)
      case con: CommentOnNamespace => None
      case _: ShowNamespaces => None
      case an: AlterNamespaceSetProperties if an.namespace.asInstanceOf[ResolvedNamespace].namespace.length == 1 =>
        databaseOperation(projectId, an.namespace.asInstanceOf[ResolvedNamespace], Operation.ALTER_DATABASE)
      case _: AlterNamespaceSetProperties => None
      case an: AlterNamespaceSetLocation if an.namespace.asInstanceOf[ResolvedNamespace].namespace.length == 1 =>
        databaseOperation(projectId, an.namespace.asInstanceOf[ResolvedNamespace], Operation.ALTER_DATABASE)
      case _: AlterNamespaceSetLocation => None
      case use: SetCatalogAndNamespace if use.namespace.isDefined && use.namespace.get.length == 1 => None
        // databaseOperation(projectId, Some(polyCatCatalog), use.namespace.get.head, Operation.USE_DATABASE)
      case _: SetCatalogAndNamespace => None
      case ct: CreateV2Table if ct.tableName.namespace().length == 1 =>
        databaseOperation(projectId, Some(ct.catalog.name()), ct.tableName.namespace()(0), Operation.CREATE_TABLE)
      case _: CreateV2Table => None
      case ctas: CreateTableAsSelect if ctas.tableName.namespace().length == 1 =>
        collectQueryInfo(projectId, polyCatCatalog, ctas.query, authorizationList)
        databaseOperation(projectId, Some(ctas.catalog.name()), ctas.tableName.namespace()(0), Operation.CREATE_TABLE)
      case _: CreateTableAsSelect => None
      case rt: ReplaceTable => None
      case rtas: ReplaceTableAsSelect => None
      case dr: DescribeRelation =>
        val table = dr.relation.asInstanceOf[ResolvedTable]
        tableOperation(projectId, polyCatCatalog, Some(table.identifier.namespace()(0)), table.identifier.name(), Operation.DESC_TABLE)
      case _: DescribeRelation => None
      case _: ShowTableProperties => None
      case _: CommentOnTable => None
      case at: AlterTable if at.table.isInstanceOf[UnresolvedV2Relation] =>
        val tableName = at.table.asInstanceOf[UnresolvedV2Relation].tableName.name()
        val database = at.table.asInstanceOf[UnresolvedV2Relation].tableName.namespace()(0)
        tableOperation(projectId, polyCatCatalog, Some(database), tableName, Operation.ALTER_TABLE)
      case at: AlterTable if at.table.isInstanceOf[DataSourceV2Relation] =>
        val identifier = at.table.asInstanceOf[DataSourceV2Relation].identifier
        tableOperation(projectId, polyCatCatalog, Some(identifier.get.namespace()(0)), identifier.get.name(), Operation.ALTER_TABLE)
      case _: RenameTable => None
      case _: ShowTables => None
      case _: ShowViews => None
      case delete: DeleteFromTable => None
      case merge: MergeIntoTable => None
      case _: DropTable => None
      case _: RefreshTable => None
      case other => None
//        throw new CatalogException("No permission. Unknown Command: " + other.getClass.getName);
    }
    authorizationInput.foreach(authorizationList.add)
  }

  def collectQueryInfo(projectId: String, polyCatCatalog: String, query: LogicalPlan, authorizationList: java.util.HashSet[CatalogOperationObject]): Unit = {
    val authorizationInputs = query.collectLeaves().collect {
      case relation: DataSourceV2Relation if relation.identifier.isDefined && relation.identifier.get.namespace().length == 1 =>
        val identifier = relation.identifier.get
        tableOperation(projectId, polyCatCatalog, Some(identifier.namespace()(0)), identifier.name(), Operation.SELECT_TABLE)
      case hiveTableRelation: HiveTableRelation =>
        val identifier = hiveTableRelation.tableMeta.identifier
        tableOperation(projectId, polyCatCatalog, Some(identifier.database.get), identifier.table, Operation.SELECT_TABLE)
      case logicalRelation: LogicalRelation if logicalRelation.catalogTable.isDefined =>
        val table = logicalRelation.catalogTable.get.identifier
        tableOperation(projectId, polyCatCatalog, Some(table.database.get), table.table, Operation.SELECT_TABLE)
      case logicalPlan: LogicalPlan =>
        println(s"un processed logicalPlan:$logicalPlan")
        Option.empty
    }
    authorizationInputs.filter(_.isDefined).map(_.get).foreach(authorizationList.add)
  }


  def tableOperation(projectId: String, catalogName: String, table: TableIdentifier, operation: Operation): Option[CatalogOperationObject] = {
    val databaseName = table.database.get
    tableOperation(projectId, catalogName, Some(databaseName), table.table, operation)
  }

  def tableOperation(projectId: String, catalogName: String, databaseNameOption: Option[String],
                     tableName: String, operation: Operation): Option[CatalogOperationObject] = {
    val databaseName = databaseNameOption.get
    val catalogObject = new CatalogInnerObject(projectId, catalogName, databaseName, tableName)
    createCatalogOperationObject(catalogObject, operation)
  }

  def databaseOperation(projectId: String, resolvedNamespace: ResolvedNamespace, operation: Operation): Option[CatalogOperationObject] = {
    val catalogName = resolvedNamespace.catalog.name()
    val databaseName = resolvedNamespace.namespace.head
    databaseOperation(projectId, Some(catalogName), databaseName, operation)
  }

  def databaseOperation(projectId: String, catalog: Option[String], database: String, operation: Operation): Option[CatalogOperationObject] = {
    createCatalogOperationObject(new CatalogInnerObject(projectId, catalog.get, database, database), operation)
  }

  def catalogOperation(projectId: String, polyCatCatalog: String, operation: Operation): Option[CatalogOperationObject] = {
    createCatalogOperationObject(new CatalogInnerObject(projectId, polyCatCatalog, null, polyCatCatalog), operation)
  }

  def createCatalogOperationObject(catalogObject: CatalogInnerObject, operation: Operation): Option[CatalogOperationObject] = {
    val operationObject = new CatalogOperationObject(catalogObject,operation)
    Some(operationObject)
  }
}
