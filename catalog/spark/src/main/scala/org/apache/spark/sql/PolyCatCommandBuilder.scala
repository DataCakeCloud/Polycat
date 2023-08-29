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

import io.polycat.common.sql.{ParserUtil, PolicyParserUtil}
import io.polycat.sql.{PolyCatSQLBaseVisitor, PolyCatSQLParser}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.command._
import org.apache.spark.sql.command.branch.{CreateBranch, MergeBranch, ShowBranches}
import org.apache.spark.sql.command.catalog.{CreateCatalog, DescCatalog, DropCatalog, ShowCatalogHistory, ShowCatalogs, UseCatalog}
import org.apache.spark.sql.command.database.{CreateDatabase, DescDatabase, DropDatabase, ShowDatabases, UndropDatabase, UseDatabase}
import org.apache.spark.sql.command.delegate.{CreateDelegate, DescDelegate, DropDelegate, ShowDelegates}
import org.apache.spark.sql.command.usageProfile.{DescAccessStatsForTable, ShowAccessStatsForCatalog}
import org.apache.spark.sql.command.dataLineage.ShowDataLineageForTable
import org.apache.spark.sql.command.policy.{GrantPrivilegeToPrincipal, RevokePrivilegeFromPrincipal, ShowPoliciesOfPrincipal}
import org.apache.spark.sql.command.privilege.{GrantAllObjectPrivilegeToRole, GrantPrivilegeToRole, GrantPrivilegeToShare, GrantRoleToUser, GrantShareToUser, RevokeAllObjectPrivilegeFromRole, RevokeAllPrivilegeFromRole, RevokePrivilegeFromRole, RevokePrivilegeFromShare, RevokeRoleFromUser, RevokeShareFromUser, ShowGrantsToRole}
import org.apache.spark.sql.command.role.{CreateRole, DescRole, DropRole, ShowRoles}
import org.apache.spark.sql.command.share.{AlterShareAddAccounts, AlterShareRemoveAccounts, CreateShare, DescShare, DropShare, ShowShares}
import org.apache.spark.sql.command.table.{AlterColumn, AlterTableRename, PurgeTable, SetTableProperty, ShowTablePartitions, ShowTables, UndropTable, UnsetTableProperty}

class PolyCatCommandBuilder extends PolyCatSQLBaseVisitor[LogicalPlan] {

  override def visitCatalogStatement(ctx: PolyCatSQLParser.CatalogStatementContext): LogicalPlan = {
    visit(ctx.catalogCommand())
  }

  override def visitCreateCatalog(ctx: PolyCatSQLParser.CreateCatalogContext): LogicalPlan = {
    CreateCatalog(ParserUtil.buildCreateCatalogRequest(ctx))
  }

  override def visitUseCatalog(ctx: PolyCatSQLParser.UseCatalogContext): LogicalPlan = {
    UseCatalog(ParserUtil.buildGetCatalogRequest(ctx))
  }

  override def visitShowTableHistory(ctx: PolyCatSQLParser.ShowTableHistoryContext): LogicalPlan = {
    ShowTableHistory(ParserUtil.buildListTableCommitsRequest(ctx))
  }

  override def visitRestoreTableWithVer(ctx: PolyCatSQLParser.RestoreTableWithVerContext): LogicalPlan = {
    RestoreTable(ParserUtil.buildRestoreTableRequest(ctx))
  }

  override def visitCopyIntoTable(ctx: PolyCatSQLParser.CopyIntoTableContext): LogicalPlan = {
    val tableNameInput = ParserUtil.parseTableName(ctx.tableName())
    val delegateName = ParserUtil.parseIdentifier(ctx.delegateName)
    val location = ParserUtil.parseString(ctx.location)
    val fileFormat = ParserUtil.parseIdentifier(ctx.source)
    if (!fileFormat.equalsIgnoreCase("csv")) {
      throw new UnsupportedOperationException("other formats except CSV are not supported yet")
    }
    val with_header = ParserUtil.parseBooleanDefaultFalse(ctx.with_header)
    val delimiter = ParserUtil.parseString(ctx.delimiter)
    CopyIntoTable(tableNameInput, delegateName, location, fileFormat, with_header, delimiter)
  }

  //delegate
  override def visitCreateDelegate(ctx: PolyCatSQLParser.CreateDelegateContext): LogicalPlan = {
    CreateDelegate(ParserUtil.buildCreateDelegateRequest(ctx))
  }
  override def visitDescDelegate(ctx: PolyCatSQLParser.DescDelegateContext): LogicalPlan = {
    DescDelegate(ParserUtil.buildGetDelegateRequest(ctx))
  }
  override def visitShowDelegates(ctx: PolyCatSQLParser.ShowDelegatesContext): LogicalPlan = {
    ShowDelegates(ParserUtil.buildListDelegatesRequest(ctx))
  }
  override def visitDropDelegate(ctx: PolyCatSQLParser.DropDelegateContext): LogicalPlan = {
    DropDelegate(ParserUtil.buildDeleteDelegateRequest(ctx))
  }

  //branch
  override def visitCreateBranch(ctx: PolyCatSQLParser.CreateBranchContext): LogicalPlan = {
    CreateBranch(ParserUtil.buildCreateBranchRequest(ctx))
  }
  override def visitMergeBranch(ctx: PolyCatSQLParser.MergeBranchContext): LogicalPlan = {
    MergeBranch(ParserUtil.buildMergeBranchRequest(ctx))
  }
  override def visitShowBranches(ctx: PolyCatSQLParser.ShowBranchesContext): LogicalPlan = {
    ShowBranches(ParserUtil.buildListBranchesRequest(ctx))
  }
  override def visitUseBranch(ctx: PolyCatSQLParser.UseBranchContext): LogicalPlan = {
    UseCatalog(ParserUtil.buildGetCatalogRequest(ctx))
  }

  // catalog
  override def visitDescCatalog(ctx: PolyCatSQLParser.DescCatalogContext): LogicalPlan = {
    DescCatalog(ParserUtil.buildDescCatalogRequest(ctx))
  }
  override def visitDropCatalog(ctx: PolyCatSQLParser.DropCatalogContext): LogicalPlan = {
    DropCatalog(ParserUtil.buildDeleteCatalogRequest(ctx))
  }
  override def visitShowCatalogs(ctx: PolyCatSQLParser.ShowCatalogsContext): LogicalPlan = {
    ShowCatalogs(ParserUtil.buildListCatalogsRequest(ctx))
  }
  override def visitShowCatalogHistory(ctx: PolyCatSQLParser.ShowCatalogHistoryContext): LogicalPlan = {
    ShowCatalogHistory(ParserUtil.buildListCatalogCommitsRequest(ctx))
  }

  //database
  override def visitCreateDatabase(ctx: PolyCatSQLParser.CreateDatabaseContext): LogicalPlan = {
    CreateDatabase(ParserUtil.buildCreateDatabaseRequest(ctx))
  }
  override def visitDescDatabase(ctx: PolyCatSQLParser.DescDatabaseContext): LogicalPlan = {
    DescDatabase(ParserUtil.buildGetDatabaseRequest(ctx))
  }
  override def visitDropDatabase(ctx: PolyCatSQLParser.DropDatabaseContext): LogicalPlan = {
    DropDatabase(ParserUtil.buildDeleteDatabaseRequest(ctx))
  }
  override def visitShowDatabases(ctx: PolyCatSQLParser.ShowDatabasesContext): LogicalPlan = {
    ShowDatabases(ParserUtil.buildListDatabasesRequest(ctx))
  }
  override def visitUndropDatabase(ctx: PolyCatSQLParser.UndropDatabaseContext): LogicalPlan = {
    UndropDatabase(ParserUtil.buildUndropDatabaseRequest(ctx))
  }
  override def visitUseDatabase(ctx: PolyCatSQLParser.UseDatabaseContext): LogicalPlan = {
    UseDatabase(ParserUtil.buildGetDatabaseRequest(ctx))
  }

  //history
  override def visitDescAccessStatsForTable(ctx: PolyCatSQLParser.DescAccessStatsForTableContext): LogicalPlan = {
    DescAccessStatsForTable(ParserUtil.buildGetTableUsageProfileRequest(ctx))
  }
  override def visitShowAccessStatsForCatalog(ctx: PolyCatSQLParser.ShowAccessStatsForCatalogContext): LogicalPlan = {
    ShowAccessStatsForCatalog(ParserUtil.buldListCatalogUsageProfilesRequest(ctx))
  }
  override def visitShowDataLineageForTable(ctx: PolyCatSQLParser.ShowDataLineageForTableContext): LogicalPlan = {
    ShowDataLineageForTable(ParserUtil.buildListDataLineageRequest(ctx))
  }

  //privilege
  override def visitGrantAllObjectPrivilegeToRole(ctx: PolyCatSQLParser.GrantAllObjectPrivilegeToRoleContext): LogicalPlan = {
    GrantAllObjectPrivilegeToRole(ParserUtil.buildAlterRoleRequest(ctx))
  }
  override def visitGrantPrivilegeToRole(ctx: PolyCatSQLParser.GrantPrivilegeToRoleContext): LogicalPlan = {
    GrantPrivilegeToRole(ParserUtil.buildAlterRoleRequest(ctx))
  }
  override def visitGrantPrivilegeToShare(ctx: PolyCatSQLParser.GrantPrivilegeToShareContext): LogicalPlan = {
    GrantPrivilegeToShare(ParserUtil.buildAlterShareRequest(ctx))
  }
  override def visitGrantRoleToUser(ctx: PolyCatSQLParser.GrantRoleToUserContext): LogicalPlan = {
    GrantRoleToUser(ParserUtil.buildAlterRoleRequest(ctx))
  }
  override def visitGrantShareToUser(ctx: PolyCatSQLParser.GrantShareToUserContext): LogicalPlan = {
    GrantShareToUser(ParserUtil.buildAlterShareRequest(ctx))
  }
  override def visitRevokeAllObjectPrivilegeFromRole(ctx: PolyCatSQLParser.RevokeAllObjectPrivilegeFromRoleContext): LogicalPlan = {
    RevokeAllObjectPrivilegeFromRole(ParserUtil.buildAlterRoleRequest(ctx))
  }
  override def visitRevokeAllPrivilegeFromRole(ctx: PolyCatSQLParser.RevokeAllPrivilegeFromRoleContext): LogicalPlan = {
    RevokeAllPrivilegeFromRole(ParserUtil.buildAlterRoleRequest(ctx))
  }
  override def visitRevokePrivilegeFromRole(ctx: PolyCatSQLParser.RevokePrivilegeFromRoleContext): LogicalPlan = {
    RevokePrivilegeFromRole(ParserUtil.buildAlterRoleRequest(ctx))
  }
  override def visitRevokePrivilegeFromShare(ctx: PolyCatSQLParser.RevokePrivilegeFromShareContext): LogicalPlan = {
    RevokePrivilegeFromShare(ParserUtil.buildAlterShareRequest(ctx))
  }
  override def visitRevokeRoleFromUser(ctx: PolyCatSQLParser.RevokeRoleFromUserContext): LogicalPlan = {
    RevokeRoleFromUser(ParserUtil.buildAlterRoleRequest(ctx))
  }
  override def visitRevokeShareFromUser(ctx: PolyCatSQLParser.RevokeShareFromUserContext): LogicalPlan = {
    RevokeShareFromUser(ParserUtil.buildAlterShareRequest(ctx))
  }
  override def visitShowGrantsToRole(ctx: PolyCatSQLParser.ShowGrantsToRoleContext): LogicalPlan = {
    ShowGrantsToRole(ParserUtil.buildShowGrantsToRoleRequest(ctx))
  }

  // role
  override def visitCreateRole(ctx: PolyCatSQLParser.CreateRoleContext): LogicalPlan = {
    CreateRole(ParserUtil.buildCreateRoleRequest(ctx))
  }
  override def visitDescRole(ctx: PolyCatSQLParser.DescRoleContext): LogicalPlan = {
    DescRole(ParserUtil.buildGetRoleRequest(ctx))
  }
  override def visitDropRole(ctx: PolyCatSQLParser.DropRoleContext): LogicalPlan = {
    DropRole(ParserUtil.buildDropRoleRequest(ctx))
  }
  override def visitShowRoles(ctx: PolyCatSQLParser.ShowRolesContext): LogicalPlan = {
    ShowRoles(ParserUtil.buildShowRolesRequest(ctx))
  }

  // share
  override def visitAlterShareAddAccounts(ctx: PolyCatSQLParser.AlterShareAddAccountsContext): LogicalPlan = {
    AlterShareAddAccounts(ParserUtil.buildAlterShareRequest(ctx))
  }
  override def visitAlterShareRemoveAccounts(ctx: PolyCatSQLParser.AlterShareRemoveAccountsContext): LogicalPlan = {
    AlterShareRemoveAccounts(ParserUtil.buildAlterShareRequest(ctx))
  }
  override def visitCreateShare(ctx: PolyCatSQLParser.CreateShareContext): LogicalPlan = {
    CreateShare(ParserUtil.buildCreateShareRequest(ctx))
  }
  override def visitDescShare(ctx: PolyCatSQLParser.DescShareContext): LogicalPlan = {
    DescShare(ParserUtil.buildGetShareRequest(ctx))
  }
  override def visitDropShare(ctx: PolyCatSQLParser.DropShareContext): LogicalPlan = {
    DropShare(ParserUtil.buildDropShareRequest(ctx))
  }
  override def visitShowShares(ctx: PolyCatSQLParser.ShowSharesContext): LogicalPlan = {
    ShowShares(ParserUtil.buildShowSharesRequest(ctx))
  }

  //table
  override def visitAlterColumn(ctx: PolyCatSQLParser.AlterColumnContext): LogicalPlan = {
    AlterColumn(ParserUtil.buildAlterColumnRequest(ctx))
  }
  override def visitRenameTable(ctx: PolyCatSQLParser.RenameTableContext): LogicalPlan = {
    AlterTableRename(ParserUtil.buildAlterTableRequest(ctx))
  }
  override def visitPurgeTable(ctx: PolyCatSQLParser.PurgeTableContext): LogicalPlan = {
    PurgeTable(ParserUtil.buildPurgeTableRequest(ctx))
  }
  override def visitSetTableProperties(ctx: PolyCatSQLParser.SetTablePropertiesContext): LogicalPlan = {
    SetTableProperty(ParserUtil.buildSetTablePropertyRequest(ctx))
  }
  override def visitShowTablePartitions(ctx: PolyCatSQLParser.ShowTablePartitionsContext): LogicalPlan = {
    ShowTablePartitions(ParserUtil.buildListTablePartitionsRequest(ctx))
  }
  override def visitShowTables(ctx: PolyCatSQLParser.ShowTablesContext): LogicalPlan = {
    ShowTables(ParserUtil.buildListTablesRequest(ctx))
  }
  override def visitUndropTable(ctx: PolyCatSQLParser.UndropTableContext): LogicalPlan = {
    UndropTable(ParserUtil.buildUndropTableRequest(ctx))
  }
  override def visitUnsetTableProperties(ctx: PolyCatSQLParser.UnsetTablePropertiesContext): LogicalPlan = {
    UnsetTableProperty(ParserUtil.buildUnsetTablePropertyRequest(ctx))
  }



  //policy
  override def visitGrantPrivilegeToPrincipal(ctx: PolyCatSQLParser.GrantPrivilegeToPrincipalContext) : LogicalPlan = {
    GrantPrivilegeToPrincipal(PolicyParserUtil.buildAlterPrivilegeRequest(ctx))
  }

  override def visitRevokePrivilegeFromPrincipal(ctx: PolyCatSQLParser.RevokePrivilegeFromPrincipalContext) : LogicalPlan = {
    RevokePrivilegeFromPrincipal(PolicyParserUtil.buildAlterPrivilegeRequest(ctx))
  }

  override def visitGrantRsPrincipalToUgPrincipal(ctx: PolyCatSQLParser.GrantRsPrincipalToUgPrincipalContext): LogicalPlan = {
    if (ctx.rsPrincipalInfo().getChild(0).getText.toUpperCase() == "ROLE") {
      GrantRoleToUser(PolicyParserUtil.buildAlterRoleRequest(ctx))
    } else {
      GrantShareToUser(PolicyParserUtil.buildAlterShareRequest(ctx))
    }
  }

  override def visitRevokeRsPrincipalFromUgPrincipal(ctx: PolyCatSQLParser.RevokeRsPrincipalFromUgPrincipalContext): LogicalPlan = {
    if (ctx.rsPrincipalInfo().getChild(0).getText.toUpperCase() == "ROLE") {
      RevokeRoleFromUser(PolicyParserUtil.buildAlterRoleRequest(ctx))
    } else {
      RevokeShareFromUser(PolicyParserUtil.buildAlterShareRequest(ctx))
    }
  }

  override def visitShowPoliciesOfPrincipal(ctx: PolyCatSQLParser.ShowPoliciesOfPrincipalContext): LogicalPlan = {
    ShowPoliciesOfPrincipal(PolicyParserUtil.buildShowPoliciesOfPrincipalRequest(ctx))
  }

  override def visitCreateShareOnCatalog(ctx: PolyCatSQLParser.CreateShareOnCatalogContext): LogicalPlan = {
    CreateShare(PolicyParserUtil.buildCreateShareRequest(ctx))
  }

}
