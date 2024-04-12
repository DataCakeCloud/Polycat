/*
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
package io.polycat.catalog.common.plugin;

import java.util.List;

import io.polycat.catalog.common.model.discovery.CatalogTableCount;
import io.polycat.catalog.common.model.discovery.DatabaseSearch;
import io.polycat.catalog.common.model.discovery.DiscoverySearchBase;
import io.polycat.catalog.common.model.discovery.ObjectCount;
import io.polycat.catalog.common.model.discovery.TableCategories;
import io.polycat.catalog.common.model.discovery.TableSearch;
import io.polycat.catalog.common.model.glossary.Category;
import io.polycat.catalog.common.model.glossary.Glossary;
import io.polycat.catalog.common.model.stats.PartitionStatisticData;
import io.polycat.catalog.common.plugin.request.CreateMaterializedViewRequest;
import io.polycat.catalog.common.plugin.request.base.AcceleratorRequestBase;
import io.polycat.catalog.common.plugin.request.input.AcceleratorInput;
import io.polycat.catalog.common.plugin.request.input.FunctionInput;
import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.lineage.LineageFact;
import io.polycat.catalog.common.model.*;

import io.polycat.catalog.common.model.stats.AggrStatisticData;
import io.polycat.catalog.common.model.stats.ColumnStatisticsObj;
import io.polycat.catalog.common.plugin.request.*;

/**
 * Catalog interface
 */
public interface CatalogPlugin {

    /**
     * return the context of Catalog
     */
    CatalogContext getContext();

    void setContext(CatalogContext context);

    // ---------------------------------------------------------------------------------------------------
    //                                  Catalog related interface
    // ---------------------------------------------------------------------------------------------------

    /**
     * Create a new catalog
     */
    Catalog createCatalog(CreateCatalogRequest createCatalogRequest) throws CatalogException;

    /**
     * Delete a catalog
     */
    void dropCatalog(DropCatalogRequest dropCatalogRequest) throws CatalogException;

    /**
     * Return a list of catalogs
     */
    PagedList<Catalog> listCatalogs(ListCatalogsRequest listCatalogsRequest) throws CatalogException;

    Catalog getCatalog(GetCatalogRequest getCatalogRequest) throws CatalogException;

    PagedList<CatalogCommit> listCatalogCommits(ListCatalogCommitsRequest listCatalogCommitsRequest);

    void restoreCatalog(RestoreCatalogRequest restoreCatalogRequest);

    void undropCatalog(UndropCatalogRequest undropCatalogRequest);

    void alterCatalog(AlterCatalogRequest request);

    // ---------------------------------------------------------------------------------------------------
    //                                  Branch related interface

    // ---------------------------------------------------------------------------------------------------
    Catalog createBranch(CreateBranchRequest createBranchRequest);

    PagedList<Catalog> listBranch(ListBranchesRequest listBranchesRequest);

    void mergeBranch(MergeBranchRequest mergeBranchRequest);

    // ---------------------------------------------------------------------------------------------------
    //                                  Database related interface

    // ---------------------------------------------------------------------------------------------------
    /**
     * create Database
     *
     * @param createDatabaseRequest
     * @return createDatabaseResult
     */
    Database createDatabase(CreateDatabaseRequest createDatabaseRequest) throws CatalogException;

    /**
     * delete database
     *
     * @param deleteDatabaseRequest
     * @return deleteDatabaseResult
     */
    void deleteDatabase(DeleteDatabaseRequest deleteDatabaseRequest) throws CatalogException;

    /**
     * alter database
     *
     * @param updateDatabaseRequest
     * @return alterDatabaseResult
     */
    void alterDatabase(AlterDatabaseRequest updateDatabaseRequest) throws CatalogException;

    /**
     * list databases
     *
     * @param listDatabasesRequest
     * @return
     */
    PagedList<Database> listDatabases(ListDatabasesRequest listDatabasesRequest) throws CatalogException;

    /**
     * list databases
     *
     * @param listDatabasesRequest
     * @return
     */
    PagedList<String> getDatabases(ListDatabasesRequest listDatabasesRequest) throws CatalogException;

    /**
     * get datbase
     *
     * @param getDatabaseRequest
     * @return getDatabaseResult
     */
    Database getDatabase(GetDatabaseRequest getDatabaseRequest) throws CatalogException;

    void restoreDatabase(RestoreDatabaseRequest restoreDatabaseRequest);

    /**
     * undrop database
     *
     * @param undropDatabaseRequest
     * @return
     */
    void undropDatabase(UndropDatabaseRequest undropDatabaseRequest) throws CatalogException;

    PagedList<TableBrief> getTableMeta(GetTableMetaRequest request) throws  CatalogException;
    // ---------------------------------------------------------------------------------------------------

    //                                  Table related interface

    // ---------------------------------------------------------------------------------------------------
    Table createTable(CreateTableRequest createTableRequest) throws CatalogException;

    void createTableWithLocation(CreateTableWithInsertRequest insertSegmentRequest) throws CatalogException;

    void deleteTable(DeleteTableRequest deleteTableRequest) throws CatalogException;

    void purgeTable(PurgeTableRequest purgeTableRequest) throws CatalogException;

    void batchDeleteTable(BatchDeleteTableRequest batchDeleteTableRequest) throws CatalogException;

    @Deprecated
    PagedList<Table> listTables(ListTablesRequest listTablesRequest) throws CatalogException;

    PagedList<Table> listTables(ListTableObjectsRequest listTableObjectsRequest) throws CatalogException;

    PagedList<String> listTableNames(ListTablesRequest listTablesRequest) throws CatalogException;

    PagedList<String> getTableNames(GetTableNamesRequest request);

    Table getTable(GetTableRequest getTableRequest) throws CatalogException;

    TableStats getTableStats(GetTableStatRequest getTableStatRequest);

    TableCommit getTableVersions(GetTableVersionsRequest getTableVersionsRequest)
        throws CatalogException;

    PagedList<TableCommit> listTableCommits(ListTableCommitsRequest listTableCommitsRequest) throws CatalogException;

    PagedList<Partition> showTablePartitions(ListTablePartitionsRequest listTablePartitionsRequest)
        throws CatalogException;

    void alterTable(AlterTableRequest alterTableRequest) throws CatalogException;

    void restoreTable(RestoreTableRequest restoreTableRequest) throws CatalogException;

    void undropTable(UndropTableRequest undropTableRequest) throws CatalogException;

    void alterColumn(AlterColumnRequest alterColumnRequest) throws CatalogException;

    void setTableProperty(SetTablePropertyRequest request) throws CatalogException;

    void unsetTableProperty(UnsetTablePropertyRequest request) throws CatalogException;

    MetaObjectName getObjectFromNameMap(GetObjectMapRequest request) throws CatalogException;

    PagedList<Table> getTableObjectsByName(GetTableObjectsByNameRequest request) throws CatalogException;

    void truncateTable(TruncateTableRequest request);

    LakeProfile getLakeProfile(GetLakeProfileRequest request);

    // ---------------------------------------------------------------------------------------------------
    //                                  Segment related interface
    // ---------------------------------------------------------------------------------------------------

    void addPartition(AddPartitionRequest addPartitionRequest) throws CatalogException;

    List<Partition> addPartitions(AddPartitionRequest addPartitionRequest) throws CatalogException;

    boolean doesPartitionExist(DoesPartitionExistsRequest doesPartitionExistsRequest) throws CatalogException;

    void alterPartitions(AlterPartitionRequest alterPartitionRequest) throws CatalogException;

    void dropPartition(DropPartitionRequest dropPartitionRequest) throws CatalogException;

    void truncatePartition(TruncatePartitionRequest truncatePartitionRequest) throws CatalogException;

    List<Partition> listPartitions(ListFileRequest listFileRequest) throws CatalogException;

    List<Partition> listPartitionPs(ListTablePartitionsRequest request);

    List<Partition> getPartitionsByFilter(ListPartitionByFilterRequest listTablePartitionsRequest)
        throws CatalogException;

    List<Partition> getPartitionsByNames(GetTablePartitionsByNamesRequest request) throws CatalogException;

    Partition getPartitionWithAuth(GetPartitionWithAuthRequest request) throws CatalogException;

    List<String> listPartitionNames(ListTablePartitionsRequest request) throws CatalogException;

    List<String> listPartitionNamesByFilter(ListPartitionNamesByFilterRequest request) throws CatalogException;

    List<Partition> listPartitionsPsWithAuth(ListPartitionsWithAuthRequest request) throws CatalogException;

    List<Partition> listPartitionsByExpr(ListPartitionsByExprRequest request) throws CatalogException;

    List<String> listPartitionNamesPs(ListPartitionNamesPsRequest request) throws CatalogException;

    List<String> listFiles(ListFileRequest listFileRequest) throws CatalogException;

    Partition getPartition(GetPartitionRequest request) throws CatalogException;

    Partition appendPartition(AppendPartitionRequest request);

    void renamePartition(RenamePartitionRequest request);

    Partition[] dropPartitionsByExpr(DropPartitionsRequest request);

    Integer getPartitionCount(GetPartitionCountRequest request) throws CatalogException;

    String getLatestPartitionName(GetLatestPartitionNameRequest request) throws CatalogException;

    //---------------------------------------------------------------------------------------------------

    //                                  share related interface
    //---------------------------------------------------------------------------------------------------

    void createShare(CreateShareRequest createShareRequest) throws CatalogException;

    void alterShare(AlterShareRequest alterShareRequest) throws CatalogException;

    void alterShareAddAccounts(AlterShareRequest alterShareRequest) throws CatalogException;

    void alterShareRemoveAccounts(AlterShareRequest alterShareRequest) throws CatalogException;

    void grantPrivilegeToShare(AlterShareRequest alterShareRequest) throws CatalogException;

    void revokePrivilegeFromShare(AlterShareRequest alterShareRequest) throws CatalogException;

    void dropShare(DropShareRequest dropShareRequest) throws CatalogException;

    Share getShare(GetShareRequest getShareRequest) throws CatalogException;

    void grantShareToUser(AlterShareRequest alterShareRequest) throws CatalogException;
    void revokeShareFromUser(AlterShareRequest alterShareRequest) throws CatalogException;
    PagedList<Share> showShares(ShowSharesRequest showSharesRequest) throws CatalogException;
    //---------------------------------------------------------------------------------------------------

    //                                  Role related interface

    //----------------------------------------------------------------------------------------------------

    void createRole(CreateRoleRequest createRoleRequest) throws CatalogException;

    void alterRole(AlterRoleRequest alterRoleRequest) throws CatalogException;

    void dropRole(DropRoleRequest dropRoleRequest) throws CatalogException;

    void grantRoleToUser(AlterRoleRequest alterRoleRequest) throws CatalogException;

    void revokeRoleFromUser(AlterRoleRequest alterRoleRequest) throws CatalogException;

    void grantPrivilegeToRole(AlterRoleRequest alterRoleRequest) throws CatalogException;

    void grantPrivilegeToPrincipal(AlterPrivilegeRequest alterPrivilegeRequest) throws CatalogException;

    void revokePrivilegeFromPrincipal(AlterPrivilegeRequest request) throws CatalogException;

    PagedList<Policy> showPoliciesOfPrincipal(ShowPoliciesOfPrincipalRequest request) throws CatalogException;

    PagedList<MetaPolicyHistory> getPolicyHistoryByTime(GetPolicyChangeRecordByTimeRequest request) throws CatalogException;

    MetaPrivilegePolicyAggrData getAggrPolicyById(GetPrivilegesByIdRequest request) throws CatalogException;

    void revokePrivilegeFromRole(AlterRoleRequest alterRoleRequest) throws CatalogException;

    Role getRole(GetRoleRequest getRoleRequest) throws CatalogException;

    PagedList<Role> showRoles(ShowRolesRequest showRolesRequest) throws CatalogException;

    /**
     * The display says that some character names are accompanied by keywords
     *
     * @param request
     * @return
     * @throws CatalogException
     */
    PagedList<Role> showAllRoleName(ShowRoleNamesRequest request) throws CatalogException;

    /**
     * Display a list of all permissions by user
     *
     * @param request
     * @return
     * @throws CatalogException
     */
    PagedList<String> showPermObjectsByUser(ShowPermObjectsRequest request) throws CatalogException;

    PagedList<Role> showRolePrivileges(ShowRolePrivilegesRequest request) throws CatalogException;

    PagedList<PrivilegeRoles> showPrivilegeRoles(ShowRolePrivilegesRequest request) throws CatalogException;

    Role showGrantsToRole(ShowGrantsToRoleRequest showGrantsToRoleRequest) throws CatalogException;

    AuthorizationResponse authenticate(AuthenticationRequest request) throws CatalogException;

    void grantAllObjectPrivilegeToRole(AlterRoleRequest request) throws CatalogException;

    void revokeAllObjectPrivilegeFromRole(AlterRoleRequest request) throws CatalogException;
    // ---------------------------------------------------------------------------------------------------

    //                                  Delegate related interface

    // ---------------------------------------------------------------------------------------------------

    DelegateOutput createDelegate(CreateDelegateRequest request) throws CatalogException;

    DelegateOutput getDelegate(GetDelegateRequest request) throws CatalogException;

    void deleteDelegate(DeleteDelegateRequest request) throws CatalogException;

    PagedList<DelegateBriefInfo> listDelegates(ListDelegatesRequest request) throws CatalogException;

    void createAccelerator(CreateAcceleratorRequest request) throws CatalogException;

    PagedList<AcceleratorObject> showAccelerator(ShowAcceleratorsRequest request) throws CatalogException;

    void dropAccelerator(DropAcceleratorRequest request) throws CatalogException;
    void alterAccelerator(AcceleratorRequestBase<AcceleratorInput> request);
    // ---------------------------------------------------------------------------------------------------

    //                                  UsageProfile related interface

    // ---------------------------------------------------------------------------------------------------

    void insertUsageProfile(InsertUsageProfileRequest request) throws CatalogException;
    PagedList<TableUsageProfile> listCatalogUsageProfiles(ListCatalogUsageProfilesRequest request)
        throws CatalogException;
    PagedList<TableUsageProfile> getTableUsageProfile(GetTableUsageProfileRequest request) throws CatalogException;

    PagedList<TableUsageProfile> getUsageProfileDetails(GetUsageProfileDetailsRequest request) throws CatalogException;

    List<TableAccessUsers> getTableAccessUsers(GetTableAccessUsersRequest request) throws CatalogException;

    List<TableUsageProfile> getUsageProfileGroupByUser(GetUsageProfilesGroupByUserRequest request) throws CatalogException;

    // ---------------------------------------------------------------------------------------------------

    //                                  DataLineage related interface

    // ---------------------------------------------------------------------------------------------------
    //void insertDataLineage(InsertDataLineageRequest request) throws CatalogException;
    //PagedList<DataLineage> listDataLineages(ListDataLineageRequest request) throws CatalogException;

    /**
     * update data lineage
     *
     * @param request request
     * @throws CatalogException
     */
    void updateDataLineage(UpdateDataLineageRequest request) throws CatalogException;

    /**
     * search lineage graph
     * @param request request
     * @return {@link LineageInfo}
     * @throws CatalogException
     */
    LineageInfo searchDataLineageGraph(SearchDataLineageRequest request) throws CatalogException;

    /**
     * lineage job fact by job fact id.
     *
     * @param request request
     * @return {@link LineageFact}
     * @throws CatalogException
     */
    LineageFact getDataLineageFact(GetDataLineageFactRequest request) throws CatalogException;

    // ---------------------------------------------------------------------------------------------------
    //                                  Function related interface
    // ---------------------------------------------------------------------------------------------------
    void createFunction(CreateFunctionRequest request) throws CatalogException;

    void dropFunction(FunctionRequestBase request) throws CatalogException;

    FunctionInput getFunction(GetFunctionRequest request) throws CatalogException;

    void alterFunction(AlterFunctionRequest request);

    PagedList<String> listFunctions(ListFunctionRequest request) throws CatalogException;

    PagedList<FunctionInput> getAllFunctions(GetAllFunctionRequest request) throws CatalogException;

    // ---------------------------------------------------------------------------------------------------
    //                                  Materialized view related interface
    // ---------------------------------------------------------------------------------------------------

    void createMaterializedView(CreateMaterializedViewRequest request) throws CatalogException;

    void dropMaterializedView(DropMaterializedViewRequest request) throws CatalogException;

    PagedList<IndexInfo> listIndexes(ListMaterializedViewsRequest listMaterializedViewsRequest)
        throws CatalogException;

    IndexInfo getMaterializedView(GetMaterializedViewRequest getMaterializedViewRequest) throws CatalogException;

    void alterMaterializedView(AlterMaterializedViewRequest alterMaterializedViewRequest) throws CatalogException;

    // ---------------------------------------------------------------------------------------------------
    //                                  Statistics related interface
    // ---------------------------------------------------------------------------------------------------
    void deleteTableColumnStatistics(DeleteColumnStatisticsRequest request) throws CatalogException;

    ColumnStatisticsObj[] getTableColumnsStatistics(GetTableColumnStatisticRequest request);

    boolean updateTableColumnStatistics(UpdateTableColumnStatisticRequest request);

    void deletePartitionColumnStatistics(DeletePartitionColumnStatisticsRequest request);

    AggrStatisticData getAggrColStats(GetAggregateColumnStatisticsRequest request);

    PartitionStatisticData getPartitionColumnStatistics(GetPartitionColumnStatisticsRequest request);

    void setPartitionsColumnStatistics(SetPartitionColumnStatisticsRequest request);

    boolean updatePartitionColumnStatistics(UpdatePartitionColumnStatisticRequest request);
    
    // ---------------------------------------------------------------------------------------------------
    //                                  Hive  related interface
    // ---------------------------------------------------------------------------------------------------

    KerberosToken addToken(AddTokenRequest request) throws CatalogException;

    KerberosToken alterToken(AlterKerberosTokenRequest request) throws CatalogException;

    void deleteToken(DeleteTokenRequest request) throws CatalogException;

    PagedList<KerberosToken>  ListToken(ListTokenRequest request) throws CatalogException;

    KerberosToken getToken(GetTokenRequest request) throws CatalogException;

    KerberosToken getTokenWithRenewer(GetTokenWithRenewerRequest request) throws CatalogException;

    // ---------------------------------------------------------------------------------------------------
    //                                  constraint interface
    // ---------------------------------------------------------------------------------------------------


    PagedList<PrimaryKey> getPrimaryKeys(GetPrimaryKeysRequest request);

    PagedList<ForeignKey> getForeignKeys(GetForeignKeysReq request);

    PagedList<Constraint> getConstraints(GetConstraintsRequest request);

    void dropConstraint(DeleteConstraintRequest request);

    void addPrimaryKey(AddPrimaryKeysRequest request);

    void addForeignKey(AddForeignKeysRequest request);

    void addConstraint(AddConstraintsRequest request);

    // ---------------------------------------------------------------------------------------------------
    //                                  policy interface
    // ---------------------------------------------------------------------------------------------------



    // ---------------------------------------------------------------------------------------------------
    //                                  discovery interface
    // ---------------------------------------------------------------------------------------------------

    /**
     * discovery fulltext
     *
     * @param request
     * @return
     * @throws CatalogException
     */
    PagedList<DiscoverySearchBase> search(SearchBaseRequest request) throws CatalogException;

    /**
     * table fulltext
     *
     * @param request
     * @return
     * @throws CatalogException
     */
    PagedList<TableSearch> searchTable(TableSearchRequest request) throws CatalogException;

    PagedList<TableCategories> searchTableWithCategories(TableSearchRequest request) throws CatalogException;

    PagedList<DatabaseSearch> searchDatabase(DatabaseSearchRequest request) throws CatalogException;

    PagedList<String> searchDiscoveryNames(SearchDiscoveryNamesRequest request) throws CatalogException;

    void addCategoryRelation(AddCategoryRelationRequest request) throws CatalogException;
    void removeCategoryRelation(RemoveCategoryRelationRequest request) throws CatalogException;
    ObjectCount getObjectCountByCategory(GetObjectCountRequest request) throws CatalogException;
    List<CatalogTableCount> getTableCountByCatalog(GetCatalogTableCountRequest request) throws CatalogException;

    TableCategories getTableCategories(GetTableCategoriesRequest request) throws CatalogException;

    // ---------------------------------------------------------------------------------------------------
    //                                  glossary interface
    // ---------------------------------------------------------------------------------------------------
    Glossary createGlossary(CreateGlossaryRequest request) throws CatalogException;

    void alterGlossary(AlterGlossaryRequest request) throws CatalogException;

    void deleteGlossary(DeleteGlossaryRequest request) throws CatalogException;

    Glossary getGlossary(GetGlossaryRequest request) throws CatalogException;

    PagedList<Glossary> listGlossaryWithoutCategory(ListGlossaryRequest request) throws CatalogException;

    Category createCategory(CreateCategoryRequest request) throws CatalogException;

    void alterCategory(AlterCategoryRequest request) throws CatalogException;

    void deleteCategory(DeleteCategoryRequest request) throws CatalogException;

    Category getCategory(GetCategoryRequest request) throws CatalogException;
}
