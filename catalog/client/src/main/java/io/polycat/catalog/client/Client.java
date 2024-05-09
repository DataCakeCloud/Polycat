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
package io.polycat.catalog.client;

import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.lineage.LineageFact;
import io.polycat.catalog.common.model.AuthorizationResponse;
import io.polycat.catalog.common.model.Catalog;
import io.polycat.catalog.common.model.Database;
import io.polycat.catalog.common.model.LineageInfo;
import io.polycat.catalog.common.model.PagedList;
import io.polycat.catalog.common.model.Partition;
import io.polycat.catalog.common.model.Role;
import io.polycat.catalog.common.model.Table;
import io.polycat.catalog.common.model.TableAccessUsers;
import io.polycat.catalog.common.model.TableBrief;
import io.polycat.catalog.common.model.TableUsageProfile;
import io.polycat.catalog.common.model.discovery.CatalogTableCount;
import io.polycat.catalog.common.model.discovery.DatabaseSearch;
import io.polycat.catalog.common.model.discovery.DiscoverySearchBase;
import io.polycat.catalog.common.model.discovery.ObjectCount;
import io.polycat.catalog.common.model.discovery.TableCategories;
import io.polycat.catalog.common.model.discovery.TableSearch;
import io.polycat.catalog.common.model.glossary.Category;
import io.polycat.catalog.common.model.glossary.Glossary;
import io.polycat.catalog.common.model.lock.LockInfo;
import io.polycat.catalog.common.model.stats.AggrStatisticData;
import io.polycat.catalog.common.model.stats.ColumnStatisticsObj;
import io.polycat.catalog.common.model.stats.PartitionStatisticData;
import io.polycat.catalog.common.plugin.request.AddCategoryRelationRequest;
import io.polycat.catalog.common.plugin.request.AddPartitionRequest;
import io.polycat.catalog.common.plugin.request.AlterCatalogRequest;
import io.polycat.catalog.common.plugin.request.AlterCategoryRequest;
import io.polycat.catalog.common.plugin.request.AlterColumnRequest;
import io.polycat.catalog.common.plugin.request.AlterDatabaseRequest;
import io.polycat.catalog.common.plugin.request.AlterFunctionRequest;
import io.polycat.catalog.common.plugin.request.AlterGlossaryRequest;
import io.polycat.catalog.common.plugin.request.AlterPartitionRequest;
import io.polycat.catalog.common.plugin.request.AlterRoleRequest;
import io.polycat.catalog.common.plugin.request.AlterTableRequest;
import io.polycat.catalog.common.plugin.request.AppendPartitionRequest;
import io.polycat.catalog.common.plugin.request.AuthenticationRequest;
import io.polycat.catalog.common.plugin.request.CheckLockRequest;
import io.polycat.catalog.common.plugin.request.CreateCatalogRequest;
import io.polycat.catalog.common.plugin.request.CreateCategoryRequest;
import io.polycat.catalog.common.plugin.request.CreateDatabaseRequest;
import io.polycat.catalog.common.plugin.request.CreateFunctionRequest;
import io.polycat.catalog.common.plugin.request.CreateGlossaryRequest;
import io.polycat.catalog.common.plugin.request.CreateLockRequest;
import io.polycat.catalog.common.plugin.request.CreateRoleRequest;
import io.polycat.catalog.common.plugin.request.CreateTableRequest;
import io.polycat.catalog.common.plugin.request.DatabaseSearchRequest;
import io.polycat.catalog.common.plugin.request.DeleteCategoryRequest;
import io.polycat.catalog.common.plugin.request.DeleteColumnStatisticsRequest;
import io.polycat.catalog.common.plugin.request.DeleteDatabaseRequest;
import io.polycat.catalog.common.plugin.request.DeleteGlossaryRequest;
import io.polycat.catalog.common.plugin.request.DeletePartitionColumnStatisticsRequest;
import io.polycat.catalog.common.plugin.request.DeleteTableRequest;
import io.polycat.catalog.common.plugin.request.DoesPartitionExistsRequest;
import io.polycat.catalog.common.plugin.request.DropCatalogRequest;
import io.polycat.catalog.common.plugin.request.DropPartitionRequest;
import io.polycat.catalog.common.plugin.request.DropPartitionsRequest;
import io.polycat.catalog.common.plugin.request.DropRoleRequest;
import io.polycat.catalog.common.plugin.request.FunctionRequestBase;
import io.polycat.catalog.common.plugin.request.GetAggregateColumnStatisticsRequest;
import io.polycat.catalog.common.plugin.request.GetAllFunctionRequest;
import io.polycat.catalog.common.plugin.request.GetCatalogRequest;
import io.polycat.catalog.common.plugin.request.GetCatalogTableCountRequest;
import io.polycat.catalog.common.plugin.request.GetCategoryRequest;
import io.polycat.catalog.common.plugin.request.GetDataLineageFactRequest;
import io.polycat.catalog.common.plugin.request.GetDatabaseRequest;
import io.polycat.catalog.common.plugin.request.GetFunctionRequest;
import io.polycat.catalog.common.plugin.request.GetGlossaryRequest;
import io.polycat.catalog.common.plugin.request.GetLatestPartitionNameRequest;
import io.polycat.catalog.common.plugin.request.GetObjectCountRequest;
import io.polycat.catalog.common.plugin.request.GetPartitionColumnStatisticsRequest;
import io.polycat.catalog.common.plugin.request.GetPartitionCountRequest;
import io.polycat.catalog.common.plugin.request.GetPartitionRequest;
import io.polycat.catalog.common.plugin.request.GetPartitionWithAuthRequest;
import io.polycat.catalog.common.plugin.request.GetRoleRequest;
import io.polycat.catalog.common.plugin.request.GetTableAccessUsersRequest;
import io.polycat.catalog.common.plugin.request.GetTableCategoriesRequest;
import io.polycat.catalog.common.plugin.request.GetTableColumnStatisticRequest;
import io.polycat.catalog.common.plugin.request.GetTableMetaRequest;
import io.polycat.catalog.common.plugin.request.GetTableNamesRequest;
import io.polycat.catalog.common.plugin.request.GetTableObjectsByNameRequest;
import io.polycat.catalog.common.plugin.request.GetTablePartitionsByNamesRequest;
import io.polycat.catalog.common.plugin.request.GetTableRequest;
import io.polycat.catalog.common.plugin.request.GetTableUsageProfileRequest;
import io.polycat.catalog.common.plugin.request.GetUsageProfileDetailsRequest;
import io.polycat.catalog.common.plugin.request.GetUsageProfilesGroupByUserRequest;
import io.polycat.catalog.common.plugin.request.InsertUsageProfileRequest;
import io.polycat.catalog.common.plugin.request.ListCatalogUsageProfilesRequest;
import io.polycat.catalog.common.plugin.request.ListCatalogsRequest;
import io.polycat.catalog.common.plugin.request.ListDatabasesRequest;
import io.polycat.catalog.common.plugin.request.ListFunctionRequest;
import io.polycat.catalog.common.plugin.request.ListGlossaryRequest;
import io.polycat.catalog.common.plugin.request.ListPartitionByFilterRequest;
import io.polycat.catalog.common.plugin.request.ListPartitionNamesByFilterRequest;
import io.polycat.catalog.common.plugin.request.ListPartitionNamesPsRequest;
import io.polycat.catalog.common.plugin.request.ListPartitionsByExprRequest;
import io.polycat.catalog.common.plugin.request.ListPartitionsWithAuthRequest;
import io.polycat.catalog.common.plugin.request.ListTableObjectsRequest;
import io.polycat.catalog.common.plugin.request.ListTablePartitionsRequest;
import io.polycat.catalog.common.plugin.request.ListTablesRequest;
import io.polycat.catalog.common.plugin.request.LockHeartbeatRequest;
import io.polycat.catalog.common.plugin.request.RemoveCategoryRelationRequest;
import io.polycat.catalog.common.plugin.request.RenamePartitionRequest;
import io.polycat.catalog.common.plugin.request.SearchBaseRequest;
import io.polycat.catalog.common.plugin.request.SearchDataLineageRequest;
import io.polycat.catalog.common.plugin.request.SearchDiscoveryNamesRequest;
import io.polycat.catalog.common.plugin.request.SetPartitionColumnStatisticsRequest;
import io.polycat.catalog.common.plugin.request.ShowGrantsToRoleRequest;
import io.polycat.catalog.common.plugin.request.ShowLocksRequest;
import io.polycat.catalog.common.plugin.request.ShowPermObjectsRequest;
import io.polycat.catalog.common.plugin.request.ShowRoleNamesRequest;
import io.polycat.catalog.common.plugin.request.ShowRolesRequest;
import io.polycat.catalog.common.plugin.request.TableSearchRequest;
import io.polycat.catalog.common.plugin.request.UnlockRequest;
import io.polycat.catalog.common.plugin.request.UpdateDataLineageRequest;
import io.polycat.catalog.common.plugin.request.UpdatePartitionColumnStatisticRequest;
import io.polycat.catalog.common.plugin.request.UpdateTableColumnStatisticRequest;
import io.polycat.catalog.common.plugin.request.input.FunctionInput;

import java.util.List;

/**
 * @author liangyouze
 * @date 2024/2/28
 */
public interface Client {

    String getProjectId();

    String getUserName();

    String getEndpoint();

    Catalog createCatalog(CreateCatalogRequest createCatalogRequest) throws CatalogException;

    /**
     * Delete a catalog
     */
    void dropCatalog(DropCatalogRequest dropCatalogRequest) throws CatalogException;

    /**
     * Return a list of catalogs
     */
    PagedList<Catalog> listCatalogs(ListCatalogsRequest listCatalogsRequest) throws CatalogException;

    void alterCatalog(AlterCatalogRequest request) throws CatalogException;

    Catalog getCatalog(GetCatalogRequest getCatalogRequest) throws CatalogException;

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

    PagedList<TableBrief> getTableMeta(GetTableMetaRequest request) throws  CatalogException;
    // ---------------------------------------------------------------------------------------------------

    //                                  Table related interface

    // ---------------------------------------------------------------------------------------------------
    Table createTable(CreateTableRequest createTableRequest) throws CatalogException;

    void deleteTable(DeleteTableRequest deleteTableRequest) throws CatalogException;

    @Deprecated
    PagedList<Table> listTables(ListTablesRequest listTablesRequest) throws CatalogException;

    PagedList<Table> listTables(ListTableObjectsRequest listTableObjectsRequest) throws CatalogException;

    PagedList<String> listTableNames(ListTablesRequest listTablesRequest) throws CatalogException;

    PagedList<String> getTableNames(GetTableNamesRequest request);

    Table getTable(GetTableRequest getTableRequest) throws CatalogException;

    void alterTable(AlterTableRequest alterTableRequest) throws CatalogException;

    void alterColumn(AlterColumnRequest alterColumnRequest) throws CatalogException;

    PagedList<Table> getTableObjectsByName(GetTableObjectsByNameRequest request) throws CatalogException;


    // ---------------------------------------------------------------------------------------------------
    //                                  Segment related interface
    // ---------------------------------------------------------------------------------------------------

    void addPartition(AddPartitionRequest addPartitionRequest) throws CatalogException;

    List<Partition> addPartitions(AddPartitionRequest addPartitionRequest) throws CatalogException;

    boolean doesPartitionExist(DoesPartitionExistsRequest doesPartitionExistsRequest) throws CatalogException;

    void alterPartitions(AlterPartitionRequest alterPartitionRequest) throws CatalogException;

    void dropPartition(DropPartitionRequest dropPartitionRequest) throws CatalogException;

    List<Partition> listPartitions(ListTablePartitionsRequest listTablePartitionsRequest) throws CatalogException;

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

    Partition getPartition(GetPartitionRequest request) throws CatalogException;

    Partition appendPartition(AppendPartitionRequest request);

    void renamePartition(RenamePartitionRequest request);

    Partition[] dropPartitionsByExpr(DropPartitionsRequest request);

    Integer getPartitionCount(GetPartitionCountRequest request) throws CatalogException;

    String getLatestPartitionName(GetLatestPartitionNameRequest request) throws CatalogException;

    //---------------------------------------------------------------------------------------------------

    //                                  Role related interface

    //----------------------------------------------------------------------------------------------------

    void createRole(CreateRoleRequest createRoleRequest) throws CatalogException;

    void alterRole(AlterRoleRequest alterRoleRequest) throws CatalogException;

    void dropRole(DropRoleRequest dropRoleRequest) throws CatalogException;

    void grantRoleToUser(AlterRoleRequest alterRoleRequest) throws CatalogException;

    void revokeRoleFromUser(AlterRoleRequest alterRoleRequest) throws CatalogException;

    void grantPrivilegeToRole(AlterRoleRequest alterRoleRequest) throws CatalogException;

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

    Role showGrantsToRole(ShowGrantsToRoleRequest showGrantsToRoleRequest) throws CatalogException;

    AuthorizationResponse authenticate(AuthenticationRequest request) throws CatalogException;

    void grantAllObjectPrivilegeToRole(AlterRoleRequest request) throws CatalogException;

    void revokeAllObjectPrivilegeFromRole(AlterRoleRequest request) throws CatalogException;

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

    // ---------------------------------------------------------------------------------------------------
    //                                  lock interface
    // ---------------------------------------------------------------------------------------------------

    LockInfo lock(CreateLockRequest request) throws CatalogException;

    void heartbeat(LockHeartbeatRequest request) throws CatalogException;

    LockInfo checkLock(CheckLockRequest request) throws CatalogException;

    void unlock(UnlockRequest request) throws CatalogException;

    List<LockInfo> showLocks(ShowLocksRequest request) throws CatalogException;

}
