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
package org.apache.spark.sql

import io.polycat.catalog.client.PolyCatClient
import io.polycat.catalog.common.Operation
import io.polycat.catalog.common.exception.CatalogException
import io.polycat.catalog.common.model.{AuthorizationType, CatalogInnerObject, GrantObject}
import io.polycat.catalog.common.plugin.request.AuthenticationRequest
import io.polycat.catalog.common.plugin.request.input.AuthorizationInput
import io.polycat.catalog.common.plugin.{CatalogContext, CatalogPlugin}
import io.polycat.catalog.spark.PolyCatClientHelper
import io.polycat.common.sql.AuthSubject
import org.apache.spark.adapter.{MatchResetCommand, SparkAdapter}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{ResolvedNamespace, ResolvedTable}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.command.catalog.{CreateCatalog, ShowCatalogs, UseCatalog}
import org.apache.spark.sql.command.database.{CreateDatabase, UseDatabase}
import org.apache.spark.sql.command.delegate.{CreateDelegate, ShowDelegates}
import org.apache.spark.sql.command.policy.{GrantPrivilegeToPrincipal, RevokePrivilegeFromPrincipal, ShowPoliciesOfPrincipal}
import org.apache.spark.sql.command.privilege.{GrantRoleToUser, GrantShareToUser, RevokeRoleFromUser, RevokeShareFromUser}
import org.apache.spark.sql.command.role.{CreateRole, DescRole, DropRole, ShowRoles}
import org.apache.spark.sql.command.share.{AlterShareAddAccounts, AlterShareRemoveAccounts, CreateShare, DescShare, DropShare, ShowShares}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.datasources.{CreateTempViewUsing, FileIndex, InMemoryFileIndex, InsertIntoDataSourceCommand, InsertIntoHadoopFsRelationCommand, PartitioningAwareFileIndex, RefreshResource, SaveIntoDataSourceCommand}
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveDirCommand, InsertIntoHiveTable, OptimizedCreateHiveTableAsSelectCommand}

class PolyCatCheckPermission(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (sparkSession.conf.get("spark.polycat.authorization.enabled", "false").toBoolean) {
      doAuthorization(plan)
    }
    plan
  }

  private def doAuthorization(plan: LogicalPlan): Unit = {
    val client = PolyCatClientHelper.getClientFromSession(sparkSession)
    val authorizationList = new java.util.ArrayList[AuthorizationInput]()
    plan match {
      case authSubject: AuthSubject if PolyCatCheckPermission.skipAuthorization(authSubject) =>
      case authSubject: AuthSubject =>
          authorizationList.addAll(authSubject.getAuthorizationInputList(client.getContext))
      case runnableCommand: RunnableCommand =>
        PolyCatCheckPermission.collectRunnableCommandAuth(client, runnableCommand, authorizationList)
      case writeCommand: V2WriteCommand =>
        PolyCatCheckPermission.collectV2WriteCommandAuth(client, writeCommand, authorizationList)
      case writeCommand: DataWritingCommand =>
        PolyCatCheckPermission.collectDataWritingCommandAuth(client, writeCommand, authorizationList)
      case command: Command =>
        PolyCatCheckPermission.collectOtherCommandAuth(client, command, authorizationList)
      case query: LogicalPlan =>
        PolyCatCheckPermission.collectQueryAuth(client, query, authorizationList)
    }
    if (!authorizationList.isEmpty) {
      val response = client.authenticate(new AuthenticationRequest(client.getProjectId, authorizationList))
      if (!response.getAllowed) {
        throw new CatalogException("No permission. " + response.getOpType.getPrintName)
      }
    }
  }
}

object PolyCatCheckPermission {

  val excludePlans = Seq(
    classOf[CreateCatalog],
    classOf[CreateDelegate],
    classOf[ShowDelegates],
    classOf[CreateRole],
    classOf[CreateShare],
    classOf[ShowCatalogs],
    classOf[ShowRoles],
    classOf[ShowShares])

  def skipAuthorization(authSubject: AuthSubject): Boolean = {
    excludePlans.exists(x => authSubject.getClass.getName.equals(x.getName))
  }

  def collectRunnableCommandAuth(client: CatalogPlugin, runnableCommand: RunnableCommand, authorizationList: java.util.List[AuthorizationInput]): Unit = {
    val authorizationInput: Option[AuthorizationInput] = runnableCommand match {
      case _: CreateDatabaseCommand =>
        catalogOperation(client, None, Operation.CREATE_DATABASE)
      case _: AlterDatabasePropertiesCommand => None
      case _: AlterDatabaseSetLocationCommand => None
      case _: DescribeDatabaseCommand => None
      case _: DropDatabaseCommand => None
      case _: CreateDataSourceTableCommand => None
      case _: CreateTableLikeCommand => None
      case _: CreateTableCommand => None
      case _: AlterTableRenameCommand => None
      case _: AlterTableAddColumnsCommand => None
      case _: LoadDataCommand => None
      case _: TruncateTableCommand => None
      case _: DescribeTableCommand => None
      case _: DescribeQueryCommand => None
      case _: DescribeColumnCommand => None
      case _: ShowTablesCommand => None
      case _: ShowTablePropertiesCommand => None
      case _: ShowColumnsCommand => None
      case _: ShowPartitionsCommand => None
      case _: ShowCreateTableCommand => None
      case _: ShowCreateTableAsSerdeCommand => None
      case _: AlterTableSetPropertiesCommand => None
      case _: AlterTableUnsetPropertiesCommand => None
      case _: AlterTableChangeColumnCommand => None
      case _: AlterTableSerDePropertiesCommand => None
      case _: AlterTableAddPartitionCommand => None
      case _: AlterTableRenamePartitionCommand => None
      case _: AlterTableDropPartitionCommand => None
      case _: AlterTableRecoverPartitionsCommand => None
      case _: AlterTableSetLocationCommand => None
      case _: DropTableCommand => None
      case insert: InsertIntoDataSourceCommand if insert.logicalRelation.catalogTable.isDefined =>
        val operation = if (insert.overwrite) Operation.INSERT_OVERWRITE else Operation.INSERT_TABLE
        tableOperation(client, insert.logicalRelation.catalogTable.get.identifier, operation)
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
      case MatchResetCommand(_) => None
      case _: CreateViewCommand => None
      case _: AlterViewAsCommand => None
      case _: ShowViewsCommand => None
      case _: CreateTempViewUsing => None
      case _: RefreshTable => None
      case _: RefreshResource => None
      case _ =>
        throw new CatalogException("No permission. Unknown RunnableCommand: " + runnableCommand.getClass.getName);
    }
    authorizationInput.foreach(authorizationList.add)
  }

  def collectV2WriteCommandAuth(client: PolyCatClient, writeCommand: V2WriteCommand,
                                authorizationList: java.util.ArrayList[AuthorizationInput]): Unit = {
    val authorizationInput: Option[AuthorizationInput] = writeCommand match {
      case rt: ReplaceTable if rt.tableName.namespace().length == 1 =>
        databaseOperation(client, Some(rt.catalog.name()), rt.tableName.namespace()(0), Operation.CREATE_TABLE)
      case _: ReplaceTable => None
      case ad: AppendData if ad.table.asInstanceOf[DataSourceV2Relation].identifier.isDefined &&
        ad.table.asInstanceOf[DataSourceV2Relation].identifier.get.namespace().length == 1 =>
        val identifier = ad.table.asInstanceOf[DataSourceV2Relation].identifier.get
        tableOperation(client, TableIdentifier(identifier.name(), Some(identifier.namespace()(0))), Operation.INSERT_TABLE)
      case _: AppendData => None
      case overwrite: OverwriteByExpression =>
        val identifier = overwrite.table.asInstanceOf[DataSourceV2Relation].identifier.get
        tableOperation(client, TableIdentifier(identifier.name(), Some(identifier.namespace()(0))), Operation.INSERT_OVERWRITE)
      case overwrite: OverwritePartitionsDynamic =>
        val identifier = overwrite.table.asInstanceOf[DataSourceV2Relation].identifier.get
        tableOperation(client, TableIdentifier(identifier.name(), Some(identifier.namespace()(0))), Operation.INSERT_OVERWRITE)
      case _ =>
        throw new CatalogException("No permission. Unknown V2WriteCommand: " + writeCommand.getClass.getName);
    }
    authorizationInput.foreach(authorizationList.add)
  }

  def collectDataWritingCommandAuth(client: PolyCatClient, writeCommand: DataWritingCommand, authorizationList: java.util.ArrayList[AuthorizationInput]): Unit = {
    val authorizationInput: Option[AuthorizationInput] = writeCommand match {
      case ctas: CreateDataSourceTableAsSelectCommand => None
      case ctas: CreateHiveTableAsSelectCommand => None
      case ctas: OptimizedCreateHiveTableAsSelectCommand => None
      case insert: InsertIntoHiveDirCommand => None
      case insert: InsertIntoHiveTable => None
      case insert: InsertIntoHadoopFsRelationCommand if insert.catalogTable.isDefined =>
        val table = insert.catalogTable.get.identifier
        val operation = if (insert.mode == SaveMode.Overwrite) Operation.INSERT_OVERWRITE else Operation.INSERT_TABLE
        tableOperation(client, table, operation)
      case insert: InsertIntoHadoopFsRelationCommand if insert.fileIndex.isDefined =>
        collectQueryAuth(client, insert.query, authorizationList)
        val tableInfo = extractTable(insert.fileIndex.get)
        if (tableInfo.isDefined) {
          val operation = if (insert.mode == SaveMode.Overwrite) Operation.INSERT_OVERWRITE else Operation.INSERT_TABLE
          tableOperation(client, tableInfo.get._1, tableInfo.get._2, operation)
        } else {
          None
        }
      case _: InsertIntoHadoopFsRelationCommand => None
      case _ =>
        throw new CatalogException("No permission. Unknown DataWritingCommand: " + writeCommand.getClass.getName);
    }
    authorizationInput.foreach(authorizationList.add)
  }

  def extractTable(fileIndex: FileIndex) : Option[(String, TableIdentifier)] = {
    if (fileIndex.isInstanceOf[InMemoryFileIndex]) {
      val field =classOf[org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex].
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

  def collectOtherCommandAuth(client: PolyCatClient, command: Command, authorizationList: java.util.ArrayList[AuthorizationInput]): Unit = {
    val authorizationInput: Option[AuthorizationInput] = command match {
      case cn: CreateNamespace if cn.namespace.length == 1 =>
        catalogOperation(client, None, Operation.CREATE_DATABASE)
      case _: CreateNamespace => None
      case dn: DropNamespace if dn.namespace.asInstanceOf[ResolvedNamespace].namespace.length == 1 =>
        databaseOperation(client, dn.namespace.asInstanceOf[ResolvedNamespace], Operation.DROP_DATABASE)
      case _: DropNamespace => None
      case dn: DescribeNamespace if dn.namespace.asInstanceOf[ResolvedNamespace].namespace.length == 1 =>
        databaseOperation(client, dn.namespace.asInstanceOf[ResolvedNamespace], Operation.DESC_DATABASE)
      case _: DescribeNamespace => None
      case sn: ShowCurrentNamespace =>
        catalogOperation(client, None, Operation.SHOW_DATABASE)
      case sn: ShowNamespaces if sn.namespace.asInstanceOf[ResolvedNamespace].namespace.isEmpty =>
        catalogOperation(client, Some(sn.namespace.asInstanceOf[ResolvedNamespace].catalog.name()), Operation.SHOW_DATABASE)
      case con: CommentOnNamespace => None
      case _: ShowNamespaces => None
      case an: AlterNamespaceSetProperties if an.namespace.asInstanceOf[ResolvedNamespace].namespace.length == 1 =>
        databaseOperation(client, an.namespace.asInstanceOf[ResolvedNamespace], Operation.ALTER_DATABASE)
      case _: AlterNamespaceSetProperties => None
      case an: AlterNamespaceSetLocation if an.namespace.asInstanceOf[ResolvedNamespace].namespace.length == 1 =>
        databaseOperation(client, an.namespace.asInstanceOf[ResolvedNamespace], Operation.ALTER_DATABASE)
      case _: AlterNamespaceSetLocation => None
      case use: SetCatalogAndNamespace if use.namespace.isDefined && use.namespace.get.length == 1 =>
        databaseOperation(client, use.catalogName, use.namespace.get.head, Operation.USE_DATABASE)
      case _: SetCatalogAndNamespace => None
      case ct: CreateV2Table if ct.tableName.namespace().length == 1 =>
        databaseOperation(client, Some(ct.catalog.name()), ct.tableName.namespace()(0), Operation.CREATE_TABLE)
      case _: CreateV2Table => None
      case ctas: CreateTableAsSelect if ctas.tableName.namespace().length == 1 =>
        collectQueryAuth(client, ctas.query, authorizationList)
        databaseOperation(client, Some(ctas.catalog.name()), ctas.tableName.namespace()(0), Operation.CREATE_TABLE)
      case _: CreateTableAsSelect => None
      case rt: ReplaceTable => None
      case rtas: ReplaceTableAsSelect => None
      case dr: DescribeRelation if dr.relation.asInstanceOf[ResolvedTable].identifier.namespace().length == 1 =>
        val table = dr.relation.asInstanceOf[ResolvedTable]
        tableOperation(client, table.catalog.name(), Some(table.identifier.namespace()(0)), table.identifier.name(), Operation.DESC_TABLE)
      case _: DescribeRelation => None
      case _: ShowTableProperties => None
      case _: CommentOnTable => None
      case _: AlterTable => None
      case _: RenameTable => None
      case _: ShowTables => None
      case _: ShowViews => None
      case delete: DeleteFromTable => None
      case update: UpdateTable => None
      case merge: MergeIntoTable => None
      case dt: DropTable if SparkAdapter.getTable(dt).namespace().length == 1 =>
        val table = SparkAdapter.getTable(dt)
        val catalogName = SparkAdapter.getTableCatalog(dt).name()
        tableOperation(client, catalogName, Some(table.namespace()(0)), table.name(), Operation.DROP_TABLE)
      case _: DropTable => None
      case _: RefreshTable => None
      case other =>
        throw new CatalogException("No permission. Unknown Command: " + other.getClass.getName);
    }
    authorizationInput.foreach(authorizationList.add)
  }

  def collectQueryAuth(client: PolyCatClient, query: LogicalPlan, authorizationList: java.util.ArrayList[AuthorizationInput]): Unit = {
    val authorizationInputs = query collect {
      case relation: DataSourceV2Relation if relation.identifier.isDefined && relation.identifier.get.namespace().length == 1 =>
        val identifier = relation.identifier.get
        tableOperation(client, TableIdentifier(identifier.name(), Some(identifier.namespace()(0))), Operation.SELECT_TABLE)
    }
    authorizationInputs.filter(_.isDefined).map(_.get).foreach(authorizationList.add)
  }

  def tableOperation(catalog: CatalogPlugin, table: TableIdentifier, operation: Operation): Option[AuthorizationInput] = {
    val catalogName = catalog.getContext.getCurrentCatalogName
    tableOperation(catalog, catalogName, table, operation)
  }

  def tableOperation(catalog: CatalogPlugin, catalogName: String, table: TableIdentifier, operation: Operation): Option[AuthorizationInput] = {
    val databaseName = table.database.getOrElse(catalog.getContext.getCurrentDatabaseName)
    tableOperation(catalog, catalogName, Some(databaseName), table.table, operation)
  }

  def tableOperation(catalog: CatalogPlugin, catalogName: String, databaseNameOption: Option[String],
                     tableName: String, operation: Operation): Option[AuthorizationInput] = {
    if (PolyCatClientHelper.isSparkDefaultCatalog(catalogName)) {
      return None
    }
    val databaseName = databaseNameOption.getOrElse(catalog.getContext.getCurrentDatabaseName)
    val catalogObject = new CatalogInnerObject(catalog.getContext.getProjectId, catalogName, databaseName, tableName)
    createAuthorizationInput(catalog.getContext, operation, catalogObject, null)
  }

  def databaseOperation(client: CatalogPlugin, resolvedNamespace: ResolvedNamespace, operation: Operation): Option[AuthorizationInput] = {
    val catalogName = resolvedNamespace.catalog.name()
    val databaseName = resolvedNamespace.namespace.head
    databaseOperation(client, Some(catalogName), databaseName, operation)
  }

  def databaseOperation(client: CatalogPlugin, catalog: Option[String], database: String,  operation: Operation): Option[AuthorizationInput] = {
    val catalogName = catalog.getOrElse(client.getContext.getCurrentCatalogName)
    if (PolyCatClientHelper.isSparkDefaultCatalog(catalogName)) {
      return None
    }
    val catalogObject = new CatalogInnerObject(client.getContext.getProjectId, catalogName, database, database)
    createAuthorizationInput(client.getContext, operation, catalogObject, null)
  }

  def catalogOperation(client: CatalogPlugin, catalogNameOption: Option[String], operation: Operation): Option[AuthorizationInput] = {
    val catalogName = catalogNameOption.getOrElse(client.getContext.getCurrentCatalogName)
    if (PolyCatClientHelper.isSparkDefaultCatalog(catalogName)) {
      return None
    }
    val catalogObject = new CatalogInnerObject(client.getContext.getProjectId, catalogName, null, catalogName)
    createAuthorizationInput(client.getContext, operation, catalogObject, null)
  }

  def createAuthorizationInput(context: CatalogContext, operation: Operation, catalogObject: CatalogInnerObject,
                               grantObject: GrantObject): Option[AuthorizationInput] = {
    val authorizationInput = new AuthorizationInput
    authorizationInput.setAuthorizationType(AuthorizationType.NORMAL_OPERATION)
    authorizationInput.setOperation(operation)
    authorizationInput.setCatalogInnerObject(catalogObject)
    authorizationInput.setGrantObject(grantObject)
    authorizationInput.setUser(context.getUser)
    authorizationInput.setToken(context.getToken)
    Some(authorizationInput)
  }
}
