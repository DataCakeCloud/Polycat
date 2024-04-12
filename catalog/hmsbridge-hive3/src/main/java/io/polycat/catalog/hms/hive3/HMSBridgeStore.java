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
package io.polycat.catalog.hms.hive3;

import io.polycat.catalog.common.exception.CatalogException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.FileMetadataHandler;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FileMetadataExprType;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.ISchema;
import org.apache.hadoop.hive.metastore.api.ISchemaName;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
import org.apache.hadoop.hive.metastore.api.RuntimeStat;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.SchemaVersion;
import org.apache.hadoop.hive.metastore.api.SchemaVersionDescriptor;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMMapping;
import org.apache.hadoop.hive.metastore.api.WMNullablePool;
import org.apache.hadoop.hive.metastore.api.WMNullableResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMPool;
import org.apache.hadoop.hive.metastore.api.WMResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMTrigger;
import org.apache.hadoop.hive.metastore.api.WMValidateResourcePlanResponse;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.ColStatsObjWithSourceInfo;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.FullTableName;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class HMSBridgeStore implements RawStore {

    private final ObjectStore objectStore;

    private final CatalogStore catalogStore;

    private boolean doesDefaultCatalogExist;

    private Configuration conf;

    public HMSBridgeStore() {
        objectStore = new ObjectStore();
        catalogStore = new CatalogStore();
    }

    public HMSBridgeStore(Configuration configuration) {
        this.objectStore = new ObjectStore();
        this.catalogStore = new CatalogStore(configuration);
    }

    @Override
    public void shutdown() {
        objectStore.shutdown();
    }

    @Override
    public boolean openTransaction() {
        return objectStore.openTransaction();
    }

    @Override
    public boolean commitTransaction() {
        return objectStore.commitTransaction();
    }

    @Override
    public boolean isActiveTransaction() {
        return objectStore.isActiveTransaction();
    }

    @Override
    public void rollbackTransaction() {
        objectStore.rollbackTransaction();
    }

    @Override
    public void createCatalog(Catalog catalog) throws MetaException {
        catalogStore.createCatalog(catalog);
    }

    @Override
    public void alterCatalog(String s, Catalog catalog) throws MetaException, InvalidOperationException {
        catalogStore.alterCatalog(s, catalog);
    }

    @Override
    public Catalog getCatalog(String s) throws NoSuchObjectException, MetaException {
        return catalogStore.getCatalog(s);
    }

    @Override
    public List<String> getCatalogs() throws MetaException {
        return catalogStore.getCatalogs();
    }

    @Override
    public void dropCatalog(String s) throws NoSuchObjectException, MetaException {
        catalogStore.dropCatalog(s);
    }

    @Override
    public void createDatabase(Database database) throws InvalidObjectException, MetaException {
        catalogStore.createDatabase(database);
    }

    @Override
    public Database getDatabase(String catalogName, String name) throws NoSuchObjectException {
        return catalogStore.getDatabase(catalogName, name);
    }

    @Override
    public boolean dropDatabase(String catalogName, String dbname) throws NoSuchObjectException, MetaException {
        return catalogStore.dropDatabase(catalogName, dbname);
    }

    @Override
    public boolean alterDatabase(String s, String s1, Database database) throws NoSuchObjectException, MetaException {
        return catalogStore.alterDatabase(s, s1, database);
    }

    @Override
    public List<String> getDatabases(String s, String s1) throws MetaException {
        return catalogStore.getDatabases(s, s1);
    }

    @Override
    public List<String> getAllDatabases(String catalogName) throws MetaException {
        return catalogStore.getAllDatabases(catalogName);
    }

    @Override
    public boolean createType(Type type) {
        return objectStore.createType(type);
    }

    @Override
    public Type getType(String s) {
        return objectStore.getType(s);
    }

    @Override
    public boolean dropType(String s) {
        return objectStore.dropType(s);
    }

    @Override
    public void createTable(Table table) throws InvalidObjectException, MetaException {
        catalogStore.createTable(table);
    }

    @Override
    public boolean dropTable(String catalogName, String dbName, String tableName)
        throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        return catalogStore.dropTable(catalogName, dbName, tableName);
    }

    @Override
    public Table getTable(String catalogName, String dbName, String tableName) throws MetaException {
        return catalogStore.getTable(catalogName, dbName, tableName);
    }

    @Override
    public boolean addPartition(Partition partition) throws InvalidObjectException, MetaException {
        return catalogStore.addPartition(partition);
    }

    @Override
    public boolean addPartitions(String catName, String dbName, String tblName, List<Partition> parts)
        throws InvalidObjectException, MetaException {
        return catalogStore.addPartitions(catName, dbName, tblName, parts);
    }

    @Override
    public boolean addPartitions(String catName, String dbName, String tblName,
                                 PartitionSpecProxy partitionSpec, boolean ifNotExists)
        throws InvalidObjectException, MetaException {
        return catalogStore.addPartitions(catName, dbName, tblName, partitionSpec, ifNotExists);
    }

    @Override
    public Partition getPartition(String catName, String dbName, String tableName,
                                  List<String> partVals)
        throws MetaException, NoSuchObjectException {
        return catalogStore.getPartition(catName, dbName, tableName, partVals);
    }

    @Override
    public boolean doesPartitionExist(String catName, String dbName, String tableName,
                                      List<String> partVals)
        throws MetaException, NoSuchObjectException {
        return catalogStore.doesPartitionExist(catName, dbName, tableName, partVals);
    }

    @Override
    public boolean dropPartition(String catName, String dbName, String tableName,
                                 List<String> partVals)
        throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        return catalogStore.dropPartition(catName, dbName, tableName, partVals);
    }

    @Override
    public List<Partition> getPartitions(String catName, String dbName, String tableName, int max)
        throws MetaException, NoSuchObjectException {
        return catalogStore.getPartitions(catName, dbName, tableName, max);
    }

    @Override
    public void alterTable(String catName, String dbName, String tblName, Table newTable) throws InvalidObjectException, MetaException {
        catalogStore.alterTable(catName, dbName, tblName, newTable);
    }

    @Override
    public void updateCreationMetadata(String catName, String dbName, String tableName, CreationMetadata cm)
        throws MetaException {
        objectStore.updateCreationMetadata(catName, dbName, tableName, cm);
    }

    @Override
    public List<String> getTables(String catName, String dbName, String pattern) throws MetaException {
        return catalogStore.getTables(catName, dbName, pattern);
    }

    @Override
    public List<String> getTables(String catName, String dbName, String pattern, TableType tableType) throws MetaException {
        return catalogStore.getTables(catName, dbName, pattern, tableType);
    }

    @Override
    public List<String> getMaterializedViewsForRewriting(String catName, String dbName)
        throws MetaException, NoSuchObjectException {
        return catalogStore.getMaterializedViewsForRewriting(catName, dbName);
    }

    @Override
    public List<TableMeta> getTableMeta(String catName, String dbNames, String tableNames,
                                        List<String> tableTypes) throws MetaException {
        return objectStore.getTableMeta(catName, dbNames, tableNames, tableTypes);
    }

    @Override
    public List<Table> getTableObjectsByName(String catName, String dbName, List<String> tableNames)
        throws MetaException, UnknownDBException {
        return catalogStore.getTableObjectsByName(catName, dbName, tableNames);
    }

    @Override
    public List<String> getAllTables(String catName, String dbName) throws MetaException {
        return catalogStore.getAllTables(catName, dbName);
    }

    @Override
    public List<String> listTableNamesByFilter(String catName, String dbName, String filter,
                                               short maxTables)
        throws MetaException, UnknownDBException {
        return catalogStore.listTableNamesByFilter(catName, dbName, filter, maxTables);
    }

    @Override
    public List<String> listPartitionNames(String catName, String dbName,
                                           String tblName, short maxParts) throws MetaException {
        return catalogStore.listPartitionNames(catName, dbName, tblName, maxParts);
    }

    @Override
    public PartitionValuesResponse listPartitionValues(String catName, String dbName, String tblName,
                                                       List<FieldSchema> cols, boolean applyDistinct, String filter, boolean ascending,
                                                       List<FieldSchema> order, long maxParts) throws MetaException {
         return catalogStore.listPartitionValues(catName, dbName, tblName, cols, applyDistinct, filter, ascending, order, maxParts);
    }

    @Override
    public void alterPartition(String catName, String dbName, String tblName, List<String> partVals,
                               Partition newPart)
        throws InvalidObjectException, MetaException {
        catalogStore.alterPartition(catName, dbName, tblName, partVals, newPart);
    }

    @Override
    public void alterPartitions(String catName, String dbName, String tblName,
                                List<List<String>> partValsList, List<Partition> newParts)
        throws InvalidObjectException, MetaException {
        catalogStore.alterPartitions(catName, dbName, tblName, partValsList, newParts);
    }

    @Override
    public List<Partition> getPartitionsByFilter(String catName, String dbName, String tblName, String filter, short maxParts)
        throws MetaException, NoSuchObjectException {
        return catalogStore.getPartitionsByFilter(catName, dbName, tblName, filter, maxParts);
    }

    @Override
    public boolean getPartitionsByExpr(String catName, String dbName, String tblName,
                                       byte[] expr, String defaultPartitionName, short maxParts, List<Partition> result) throws TException {
        return catalogStore.getPartitionsByExpr(catName, dbName, tblName, expr, defaultPartitionName, maxParts, result);
    }

    @Override
    public int getNumPartitionsByFilter(String catName, String dbName, String tblName, String filter)
        throws MetaException, NoSuchObjectException {
        return catalogStore.getNumPartitionsByFilter(catName, dbName, tblName, filter);
    }

    @Override
    public int getNumPartitionsByExpr(String catName, String dbName, String tblName, byte[] expr)
        throws MetaException, NoSuchObjectException {
        return catalogStore.getNumPartitionsByExpr(catName, dbName, tblName, expr);
    }

    @Override
    public List<Partition> getPartitionsByNames(String catName, String dbName, String tblName,
                                                List<String> partNames)
        throws MetaException, NoSuchObjectException {
        return catalogStore.getPartitionsByNames(catName, dbName, tblName, partNames);
    }

    @Override
    public Table markPartitionForEvent(String s, String s1, String s2, Map<String, String> map,
        PartitionEventType partitionEventType)
        throws MetaException, UnknownTableException, InvalidPartitionException, UnknownPartitionException {
        return objectStore.markPartitionForEvent(s, s1, s2, map, partitionEventType);
    }

    @Override
    public boolean isPartitionMarkedForEvent(String s, String s1, String s2, Map<String, String> map,
        PartitionEventType partitionEventType)
        throws MetaException, UnknownTableException, InvalidPartitionException, UnknownPartitionException {
        return objectStore.isPartitionMarkedForEvent(s, s1, s2, map, partitionEventType);
    }

    @Override
    public boolean addRole(String s, String s1) throws InvalidObjectException, MetaException, NoSuchObjectException {
        return objectStore.addRole(s, s1);
    }

    @Override
    public boolean removeRole(String s) throws MetaException, NoSuchObjectException {
        return objectStore.removeRole(s);
    }

    @Override
    public boolean grantRole(Role role, String s, PrincipalType principalType, String s1, PrincipalType principalType1,
        boolean b) throws MetaException, NoSuchObjectException, InvalidObjectException {
        return objectStore.grantRole(role, s, principalType, s1, principalType1, b);
    }

    @Override
    public boolean revokeRole(Role role, String s, PrincipalType principalType, boolean b)
        throws MetaException, NoSuchObjectException {
        return objectStore.revokeRole(role, s, principalType, b);
    }

    @Override
    public PrincipalPrivilegeSet getUserPrivilegeSet(String s, List<String> list)
        throws InvalidObjectException, MetaException {
        return objectStore.getUserPrivilegeSet(s, list);
    }

    @Override
    public PrincipalPrivilegeSet getDBPrivilegeSet(String s, String s1, String s2, List<String> list)
        throws InvalidObjectException, MetaException {
        return objectStore.getDBPrivilegeSet(s, s1, s2, list);
    }

    @Override
    public PrincipalPrivilegeSet getTablePrivilegeSet(String s, String s1, String s2, String s3, List<String> list)
        throws InvalidObjectException, MetaException {
        return objectStore.getTablePrivilegeSet(s, s1, s2, s3, list);
    }

    @Override
    public PrincipalPrivilegeSet getPartitionPrivilegeSet(String s, String s1, String s2, String s3, String s4,
        List<String> list) throws InvalidObjectException, MetaException {
        return objectStore.getPartitionPrivilegeSet(s, s1, s2, s3, s4, list);
    }

    @Override
    public PrincipalPrivilegeSet getColumnPrivilegeSet(String s, String s1, String s2, String s3, String s4, String s5,
        List<String> list) throws InvalidObjectException, MetaException {
        return objectStore.getColumnPrivilegeSet(s, s1, s2, s3, s4, s5, list);
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalGlobalGrants(String s, PrincipalType principalType) {
        return objectStore.listPrincipalGlobalGrants(s, principalType);
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalDBGrants(String s, PrincipalType principalType, String s1,
        String s2) {
        return objectStore.listPrincipalDBGrants(s, principalType, s1, s2);
    }

    @Override
    public List<HiveObjectPrivilege> listAllTableGrants(String s, PrincipalType principalType, String s1, String s2,
        String s3) {
        return objectStore.listAllTableGrants(s, principalType, s1, s2, s3);
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalPartitionGrants(String s, PrincipalType principalType, String s1,
        String s2, String s3, List<String> list, String s4) {
        return objectStore.listPrincipalPartitionGrants(s, principalType, s1 , s2, s3, list, s4);
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalTableColumnGrants(String s, PrincipalType principalType, String s1,
        String s2, String s3, String s4) {
        return objectStore.listPrincipalTableColumnGrants(s, principalType, s1, s2, s3, s4);
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalPartitionColumnGrants(String s, PrincipalType principalType,
        String s1, String s2, String s3, List<String> list, String s4, String s5) {
        return objectStore.listPrincipalPartitionColumnGrants(s, principalType, s1, s2, s3, list, s4, s5);
    }

    @Override
    public boolean grantPrivileges(PrivilegeBag privilegeBag)
        throws InvalidObjectException, MetaException, NoSuchObjectException {
        return objectStore.grantPrivileges(privilegeBag);
    }

    @Override
    public boolean revokePrivileges(PrivilegeBag privilegeBag, boolean b)
        throws InvalidObjectException, MetaException, NoSuchObjectException {
        return objectStore.revokePrivileges(privilegeBag, b);
    }

    @Override
    public boolean refreshPrivileges(HiveObjectRef hiveObjectRef, String s, PrivilegeBag privilegeBag)
        throws InvalidObjectException, MetaException, NoSuchObjectException {
        return objectStore.refreshPrivileges(hiveObjectRef, s, privilegeBag);
    }

    @Override
    public Role getRole(String s) throws NoSuchObjectException {
        return objectStore.getRole(s);
    }

    @Override
    public List<String> listRoleNames() {
        return objectStore.listRoleNames();
    }

    @Override
    public List<Role> listRoles(String s, PrincipalType principalType) {
        return objectStore.listRoles(s, principalType);
    }

    @Override
    public List<RolePrincipalGrant> listRolesWithGrants(String s, PrincipalType principalType) {
        return objectStore.listRolesWithGrants(s, principalType);
    }

    @Override
    public List<RolePrincipalGrant> listRoleMembers(String s) {
        return objectStore.listRoleMembers(s);
    }

    @Override
    public Partition getPartitionWithAuth(String catName, String dbName, String tblName, List<String> partVals, String userName,
                                          List<String> groupNames) throws MetaException, NoSuchObjectException, InvalidObjectException {
        return catalogStore.getPartitionWithAuth(catName, dbName, tblName, partVals, userName, groupNames);
    }

    @Override
    public List<Partition> getPartitionsWithAuth(String catName, String dbName, String tblName, short maxParts, String userName, List<String> groupNames)
        throws MetaException, NoSuchObjectException, InvalidObjectException {
        return catalogStore.getPartitionsWithAuth(catName, dbName, tblName, maxParts, userName, groupNames);
    }

    @Override
    public List<String> listPartitionNamesPs(String catName, String dbName, String tblName, List<String> partVals, short maxParts)
        throws MetaException, NoSuchObjectException {
        return catalogStore.listPartitionNamesPs(catName, dbName, tblName, partVals, maxParts);
    }

    @Override
    public List<Partition> listPartitionsPsWithAuth(String catName, String dbName, String tblName, List<String> partVals, short maxParts,
                                                    String userName, List<String> groupNames) throws MetaException, InvalidObjectException, NoSuchObjectException {
        return catalogStore.listPartitionsPsWithAuth(catName, dbName, tblName, partVals, maxParts, userName, groupNames);
    }

    @Override
    public boolean updateTableColumnStatistics(ColumnStatistics columnStatistics)
        throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
        return catalogStore.updateTableColumnStatistics(columnStatistics);
    }

    @Override
    public boolean updatePartitionColumnStatistics(ColumnStatistics statsObj, List<String> partVals)
        throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
        return catalogStore.updatePartitionColumnStatistics(statsObj, partVals);
    }

    @Override
    public ColumnStatistics getTableColumnStatistics(String catName, String dbName, String tableName, List<String> colNames)
        throws MetaException, NoSuchObjectException {
        return catalogStore.getTableColumnStatistics(catName, dbName, tableName, colNames);
    }

    @Override
    public List<ColumnStatistics> getPartitionColumnStatistics(String catName, String dbName, String tblName, List<String> partNames, List<String> colNames) throws MetaException, NoSuchObjectException {
        return catalogStore.getPartitionColumnStatistics(catName, dbName, tblName, partNames, colNames);
    }

    @Override
    public boolean deletePartitionColumnStatistics(String catName, String dbName, String tableName,
                                                   String partName, List<String> partVals, String colName) throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
        return catalogStore.deletePartitionColumnStatistics(catName, dbName, tableName, partName, partVals, colName);
    }

    @Override
    public boolean deleteTableColumnStatistics(String catName, String dbName, String tableName, String colName)
        throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
        return catalogStore.deleteTableColumnStatistics(catName, dbName, tableName, colName);
    }

    @Override
    public long cleanupEvents() {
        return objectStore.cleanupEvents();
    }

    @Override
    public boolean addToken(String s, String s1) {
        return objectStore.addToken(s, s1);
    }

    @Override
    public boolean removeToken(String s) {
        return objectStore.removeToken(s);
    }

    @Override
    public String getToken(String s) {
        return objectStore.getToken(s);
    }

    @Override
    public List<String> getAllTokenIdentifiers() {
        return objectStore.getAllTokenIdentifiers();
    }

    @Override
    public int addMasterKey(String s) throws MetaException {
        return objectStore.addMasterKey(s);
    }

    @Override
    public void updateMasterKey(Integer integer, String s) throws NoSuchObjectException, MetaException {
        objectStore.updateMasterKey(integer, s);
    }

    @Override
    public boolean removeMasterKey(Integer integer) {
        return objectStore.removeMasterKey(integer);
    }

    @Override
    public String[] getMasterKeys() {
        return objectStore.getMasterKeys();
    }

    @Override
    public void verifySchema() throws MetaException {
        objectStore.verifySchema();
    }

    @Override
    public String getMetaStoreSchemaVersion() throws MetaException {
        return objectStore.getMetaStoreSchemaVersion();
    }

    @Override
    public void setMetaStoreSchemaVersion(String version, String comment) throws MetaException {
        objectStore.setMetaStoreSchemaVersion(version, comment);
    }

    @Override
    public void dropPartitions(String catName, String dbName, String tblName, List<String> partNames)
        throws MetaException, NoSuchObjectException {
        catalogStore.dropPartitions(catName, dbName, tblName, partNames);
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalDBGrantsAll(String s, PrincipalType principalType) {
        return objectStore.listPrincipalDBGrantsAll(s, principalType);
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalTableGrantsAll(String s, PrincipalType principalType) {
        return objectStore.listPrincipalTableGrantsAll(s, principalType);
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalPartitionGrantsAll(String s, PrincipalType principalType) {
        return objectStore.listPrincipalPartitionGrantsAll(s, principalType);
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalTableColumnGrantsAll(String s, PrincipalType principalType) {
        return objectStore.listPrincipalTableColumnGrantsAll(s, principalType);
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalPartitionColumnGrantsAll(String s, PrincipalType principalType) {
        return objectStore.listPrincipalPartitionColumnGrantsAll(s, principalType);
    }

    @Override
    public List<HiveObjectPrivilege> listGlobalGrantsAll() {
        return objectStore.listGlobalGrantsAll();
    }

    @Override
    public List<HiveObjectPrivilege> listDBGrantsAll(String s, String s1) {
        return objectStore.listDBGrantsAll(s, s1);
    }

    @Override
    public List<HiveObjectPrivilege> listPartitionColumnGrantsAll(String s, String s1, String s2, String s3,
        String s4) {
        return objectStore.listPartitionColumnGrantsAll(s, s1, s2, s3, s4);
    }

    @Override
    public List<HiveObjectPrivilege> listTableGrantsAll(String s, String s1, String s2) {
        return objectStore.listTableGrantsAll(s, s1, s2);
    }

    @Override
    public List<HiveObjectPrivilege> listPartitionGrantsAll(String s, String s1, String s2, String s3) {
        return objectStore.listPartitionGrantsAll(s, s1, s2, s3);
    }

    @Override
    public List<HiveObjectPrivilege> listTableColumnGrantsAll(String s, String s1, String s2, String s3) {
        return objectStore.listTableColumnGrantsAll(s, s1, s2, s3);
    }

    @Override
    public void createFunction(Function function) throws InvalidObjectException, MetaException {
        catalogStore.createFunction(function);
    }

    @Override
    public void alterFunction(String catName, String dbName, String funcName, Function newFunction)
        throws InvalidObjectException, MetaException {
        throw new UnsupportedOperationException();
        // objectStore.alterFunction(catName, dbName, funcName, newFunction);
    }

    @Override
    public void dropFunction(String catName, String dbName, String funcName)
        throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        Function function = catalogStore.getFunction(catName, dbName, funcName);
        if (function != null) {
            catalogStore.dropFunction(catName, dbName, funcName);
        } else {
            objectStore.dropFunction(catName, dbName, funcName);
        }
    }

    @Override
    public Function getFunction(String catName, String dbName, String funcName) throws MetaException {
        Function function = catalogStore.getFunction(catName, dbName, funcName);
        if (function == null) {
            return objectStore.getFunction(catName, dbName, funcName);
        }
        return function;
    }

    @Override
    public List<Function> getAllFunctions(String catName) throws MetaException {
        List<Function> functions = catalogStore.getAllFunctions(catName);
        functions.addAll(objectStore.getAllFunctions(catName));
        return functions;
    }

    @Override
    public List<String> getFunctions(String catName, String dbName, String pattern) throws MetaException {
        return catalogStore.getFunctions(catName, dbName, pattern);
    }

    @Override
    public AggrStats get_aggr_stats_for(String catName, String dbName, String tblName,
                                        List<String> partNames, List<String> colNames)
        throws MetaException, NoSuchObjectException {
        return catalogStore.get_aggr_stats_for(catName, dbName, tblName, partNames, colNames);
    }

    @Override
    public List<ColStatsObjWithSourceInfo> getPartitionColStatsForDatabase(String catName, String dbName)
        throws MetaException, NoSuchObjectException {
        return objectStore.getPartitionColStatsForDatabase(catName, dbName);
    }

    @Override
    public NotificationEventResponse getNextNotification(NotificationEventRequest notificationEventRequest) {
        return objectStore.getNextNotification(notificationEventRequest);
    }

    @Override
    public void addNotificationEvent(NotificationEvent notificationEvent) {
        objectStore.addNotificationEvent(notificationEvent);
    }

    @Override
    public void cleanNotificationEvents(int i) {
        objectStore.cleanNotificationEvents(i);
    }

    @Override
    public CurrentNotificationEventId getCurrentNotificationEventId() {
        return objectStore.getCurrentNotificationEventId();
    }

    @Override
    public NotificationEventsCountResponse getNotificationEventsCount(
        NotificationEventsCountRequest notificationEventsCountRequest) {
        return objectStore.getNotificationEventsCount(notificationEventsCountRequest);
    }

    @Override
    public void flushCache() {
        objectStore.flushCache();
    }

    @Override
    public ByteBuffer[] getFileMetadata(List<Long> list) throws MetaException {
        return objectStore.getFileMetadata(list);
    }

    @Override
    public void putFileMetadata(List<Long> list, List<ByteBuffer> list1, FileMetadataExprType fileMetadataExprType)
        throws MetaException {
        objectStore.putFileMetadata(list, list1, fileMetadataExprType);
    }

    @Override
    public boolean isFileMetadataSupported() {
        return objectStore.isFileMetadataSupported();
    }

    @Override
    public void getFileMetadataByExpr(List<Long> list, FileMetadataExprType fileMetadataExprType, byte[] bytes,
        ByteBuffer[] byteBuffers, ByteBuffer[] byteBuffers1, boolean[] booleans) throws MetaException {
        objectStore.getFileMetadataByExpr(list, fileMetadataExprType, bytes, byteBuffers, byteBuffers1, booleans);
    }

    @Override
    public FileMetadataHandler getFileMetadataHandler(FileMetadataExprType fileMetadataExprType) {
        return objectStore.getFileMetadataHandler(fileMetadataExprType);
    }

    @Override
    public int getTableCount() throws MetaException {
        return objectStore.getTableCount();
    }

    @Override
    public int getPartitionCount() throws MetaException {
        return objectStore.getPartitionCount();
    }

    @Override
    public int getDatabaseCount() throws MetaException {
        return objectStore.getDatabaseCount();
    }

    @Override
    public List<SQLPrimaryKey> getPrimaryKeys(String s, String s1, String s2) throws MetaException {
        return objectStore.getPrimaryKeys(s, s1, s2);
    }

    @Override
    public List<SQLForeignKey> getForeignKeys(String s, String s1, String s2, String s3, String s4)
        throws MetaException {
        return objectStore.getForeignKeys(s, s1, s2, s3, s4);
    }

    @Override
    public List<SQLUniqueConstraint> getUniqueConstraints(String catName, String dbName,
                                                          String tblName) throws MetaException {
        return objectStore.getUniqueConstraints(catName, dbName, tblName);
    }

    @Override
    public List<SQLNotNullConstraint> getNotNullConstraints(String catName, String dbName,
                                                            String tblName) throws MetaException {
        return objectStore.getNotNullConstraints(catName, dbName, tblName);
    }

    @Override
    public List<SQLDefaultConstraint> getDefaultConstraints(String catName, String dbName,
                                                            String tblName) throws MetaException {
        return objectStore.getDefaultConstraints(catName, dbName, tblName);
    }

    @Override
    public List<SQLCheckConstraint> getCheckConstraints(String catName, String dbName,
                                                        String tblName) throws MetaException {
        return objectStore.getCheckConstraints(catName, dbName, tblName);
    }

    @Override
    public List<String> createTableWithConstraints(Table table, List<SQLPrimaryKey> list, List<SQLForeignKey> list1,
        List<SQLUniqueConstraint> list2, List<SQLNotNullConstraint> list3, List<SQLDefaultConstraint> list4,
        List<SQLCheckConstraint> list5) throws InvalidObjectException, MetaException {
        return objectStore.createTableWithConstraints(table, list, list1, list2, list3, list4, list5);
    }

    @Override
    public void dropConstraint(String catName, String dbName, String tableName, String constraintName,
                               boolean missingOk) throws NoSuchObjectException {
        objectStore.dropConstraint(catName, dbName, tableName, constraintName, missingOk);
    }

    @Override
    public List<String> addPrimaryKeys(List<SQLPrimaryKey> list) throws InvalidObjectException, MetaException {
        return objectStore.addPrimaryKeys(list);
    }

    @Override
    public List<String> addForeignKeys(List<SQLForeignKey> list) throws InvalidObjectException, MetaException {
        return objectStore.addForeignKeys(list);
    }

    @Override
    public List<String> addUniqueConstraints(List<SQLUniqueConstraint> list)
        throws InvalidObjectException, MetaException {
        return objectStore.addUniqueConstraints(list);
    }

    @Override
    public List<String> addNotNullConstraints(List<SQLNotNullConstraint> list)
        throws InvalidObjectException, MetaException {
        return objectStore.addNotNullConstraints(list);
    }

    @Override
    public List<String> addDefaultConstraints(List<SQLDefaultConstraint> list)
        throws InvalidObjectException, MetaException {
        return objectStore.addDefaultConstraints(list);
    }

    @Override
    public List<String> addCheckConstraints(List<SQLCheckConstraint> list)
        throws InvalidObjectException, MetaException {
        return objectStore.addCheckConstraints(list);
    }

    @Override
    public String getMetastoreDbUuid() throws MetaException {
        return objectStore.getMetastoreDbUuid();
    }

    @Override
    public void createResourcePlan(WMResourcePlan wmResourcePlan, String s, int i)
        throws AlreadyExistsException, MetaException, InvalidObjectException, NoSuchObjectException {
        objectStore.createResourcePlan(wmResourcePlan, s, i);
    }

    @Override
    public WMFullResourcePlan getResourcePlan(String s) throws NoSuchObjectException, MetaException {
        return objectStore.getResourcePlan(s);
    }

    @Override
    public List<WMResourcePlan> getAllResourcePlans() throws MetaException {
        return objectStore.getAllResourcePlans();
    }

    @Override
    public WMFullResourcePlan alterResourcePlan(String s, WMNullableResourcePlan wmNullableResourcePlan, boolean b,
        boolean b1, boolean b2)
        throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException, MetaException {
        return objectStore.alterResourcePlan(s, wmNullableResourcePlan, b, b1, b2);
    }

    @Override
    public WMFullResourcePlan getActiveResourcePlan() throws MetaException {
        return objectStore.getActiveResourcePlan();
    }

    @Override
    public WMValidateResourcePlanResponse validateResourcePlan(String s)
        throws NoSuchObjectException, InvalidObjectException, MetaException {
        return objectStore.validateResourcePlan(s);
    }

    @Override
    public void dropResourcePlan(String s) throws NoSuchObjectException, MetaException {
        objectStore.dropResourcePlan(s);
    }

    @Override
    public void createWMTrigger(WMTrigger wmTrigger)
        throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException, MetaException {
        objectStore.createWMTrigger(wmTrigger);
    }

    @Override
    public void alterWMTrigger(WMTrigger wmTrigger)
        throws NoSuchObjectException, InvalidOperationException, MetaException {
        objectStore.alterWMTrigger(wmTrigger);
    }

    @Override
    public void dropWMTrigger(String s, String s1)
        throws NoSuchObjectException, InvalidOperationException, MetaException {
        objectStore.dropWMTrigger(s, s1);
    }

    @Override
    public List<WMTrigger> getTriggersForResourcePlan(String s) throws NoSuchObjectException, MetaException {
        return objectStore.getTriggersForResourcePlan(s);
    }

    @Override
    public void createPool(WMPool wmPool)
        throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException, MetaException {
        objectStore.createPool(wmPool);
    }

    @Override
    public void alterPool(WMNullablePool wmNullablePool, String s)
        throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException, MetaException {
        objectStore.alterPool(wmNullablePool, s);
    }

    @Override
    public void dropWMPool(String s, String s1) throws NoSuchObjectException, InvalidOperationException, MetaException {
        objectStore.dropWMPool(s, s1);
    }

    @Override
    public void createOrUpdateWMMapping(WMMapping wmMapping, boolean b)
        throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException, MetaException {
        objectStore.createOrUpdateWMMapping(wmMapping, b);
    }

    @Override
    public void dropWMMapping(WMMapping wmMapping)
        throws NoSuchObjectException, InvalidOperationException, MetaException {
        objectStore.dropWMMapping(wmMapping);
    }

    @Override
    public void createWMTriggerToPoolMapping(String s, String s1, String s2)
        throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException, MetaException {
        objectStore.createWMTriggerToPoolMapping(s, s1, s2);
    }

    @Override
    public void dropWMTriggerToPoolMapping(String s, String s1, String s2)
        throws NoSuchObjectException, InvalidOperationException, MetaException {
        objectStore.dropWMTriggerToPoolMapping(s, s1, s2);
    }

    @Override
    public void createISchema(ISchema iSchema) throws AlreadyExistsException, MetaException, NoSuchObjectException {
        objectStore.createISchema(iSchema);
    }

    @Override
    public void alterISchema(ISchemaName iSchemaName, ISchema iSchema) throws NoSuchObjectException, MetaException {
        objectStore.alterISchema(iSchemaName, iSchema);
    }

    @Override
    public ISchema getISchema(ISchemaName iSchemaName) throws MetaException {
        return objectStore.getISchema(iSchemaName);
    }

    @Override
    public void dropISchema(ISchemaName iSchemaName) throws NoSuchObjectException, MetaException {
        objectStore.dropISchema(iSchemaName);
    }

    @Override
    public void addSchemaVersion(SchemaVersion schemaVersion)
        throws AlreadyExistsException, InvalidObjectException, NoSuchObjectException, MetaException {
        objectStore.addSchemaVersion(schemaVersion);
    }

    @Override
    public void alterSchemaVersion(SchemaVersionDescriptor schemaVersionDescriptor, SchemaVersion schemaVersion)
        throws NoSuchObjectException, MetaException {
        objectStore.alterSchemaVersion(schemaVersionDescriptor, schemaVersion);
    }

    @Override
    public SchemaVersion getSchemaVersion(SchemaVersionDescriptor schemaVersionDescriptor) throws MetaException {
        return objectStore.getSchemaVersion(schemaVersionDescriptor);
    }

    @Override
    public SchemaVersion getLatestSchemaVersion(ISchemaName iSchemaName) throws MetaException {
        return objectStore.getLatestSchemaVersion(iSchemaName);
    }

    @Override
    public List<SchemaVersion> getAllSchemaVersion(ISchemaName iSchemaName) throws MetaException {
        return objectStore.getAllSchemaVersion(iSchemaName);
    }

    @Override
    public List<SchemaVersion> getSchemaVersionsByColumns(String colName, String colNamespace, String type) throws MetaException {
        return objectStore.getSchemaVersionsByColumns(colName, colNamespace, type);
    }

    @Override
    public void dropSchemaVersion(SchemaVersionDescriptor schemaVersionDescriptor)
        throws NoSuchObjectException, MetaException {
        objectStore.dropSchemaVersion(schemaVersionDescriptor);
    }

    @Override
    public SerDeInfo getSerDeInfo(String serDeName) throws NoSuchObjectException, MetaException {
        return objectStore.getSerDeInfo(serDeName);
    }

    @Override
    public void addSerde(SerDeInfo serDeInfo) throws AlreadyExistsException, MetaException {
        objectStore.addSerde(serDeInfo);
    }

    @Override
    public void addRuntimeStat(RuntimeStat runtimeStat) throws MetaException {
        objectStore.addRuntimeStat(runtimeStat);
    }

    @Override
    public List<RuntimeStat> getRuntimeStats(int maxEntries, int maxCreateTime) throws MetaException {
        return objectStore.getRuntimeStats(maxEntries, maxCreateTime);
    }

    @Override
    public int deleteRuntimeStats(int i) throws MetaException {
        return objectStore.deleteRuntimeStats(i);
    }

    @Override
    public List<FullTableName> getTableNamesWithStats() throws MetaException, NoSuchObjectException {
        return objectStore.getTableNamesWithStats();
    }

    @Override
    public List<FullTableName> getAllTableNamesForStats() throws MetaException, NoSuchObjectException {
        return objectStore.getAllTableNamesForStats();
    }

    @Override
    public Map<String, List<String>> getPartitionColsWithStats(String catName, String dbName,
                                                               String tableName)
        throws MetaException, NoSuchObjectException {
        return objectStore.getPartitionColsWithStats(catName, dbName, tableName);
    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
        catalogStore.setConf(this.conf);
        objectStore.setConf(this.conf);
        if (!doesDefaultCatalogExist) {
            String catName = MetastoreConf.getVar(this.conf, MetastoreConf.ConfVars.CATALOG_DEFAULT);
            if (catName == null || catName.isEmpty()) {
                catName = Warehouse.DEFAULT_CATALOG_NAME;
            }
            try {
                if (catName.equals(Warehouse.DEFAULT_CATALOG_NAME)) {
                    return;
                }
                catalogStore.getCatalog(catName);
            } catch (NoSuchObjectException e ) {
                final String location = MetastoreConf.getVar(this.conf, MetastoreConf.ConfVars.WAREHOUSE);
                final Catalog catalog = new Catalog(catName, location);
                catalog.setDescription(Warehouse.DEFAULT_CATALOG_COMMENT);
                catalogStore.createCatalog(catalog);
                doesDefaultCatalogExist = true;
            } catch (MetaException e) {
                throw new CatalogException(e);
            }
        }
    }

    @Override
    public Configuration getConf() {
        return catalogStore.getConf();
    }

}
