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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.polycat.catalog.common.model.base.PartitionInput;
import io.polycat.catalog.common.plugin.request.AddPartitionRequest;
import io.polycat.catalog.common.plugin.request.input.AddPartitionInput;
import io.polycat.catalog.common.model.Column;

import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.FileMetadataHandler;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.TableType;
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
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
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

public class HMSBridgeStore implements RawStore {

    private static final String metaLocation = "meta_location";

    private static final String pcMetaStore = "pc_meta_store";

    private static final String hiveMetaStore = "hive_meta_store";

    private static final String isMigrating = "is_migrating";

    private static final String migrateFlag = "migrate_flag";

    private final ObjectStore objectStore;

    private final CatalogStore dashRawStore;

    private Configuration conf;

    public HMSBridgeStore() {
        objectStore = new ObjectStore();
        dashRawStore = new CatalogStore();
    }

    @Override
    public void shutdown() {

    }

    @Override
    public boolean openTransaction() {
        return false;
    }

    @Override
    public boolean commitTransaction() {
        return false;
    }

    @Override
    public boolean isActiveTransaction() {
        return false;
    }

    @Override
    public void rollbackTransaction() {

    }

    @Override
    public void createCatalog(Catalog catalog) throws MetaException {

    }

    @Override
    public void alterCatalog(String s, Catalog catalog) throws MetaException, InvalidOperationException {

    }

    @Override
    public Catalog getCatalog(String s) throws NoSuchObjectException, MetaException {
        return null;
    }

    @Override
    public List<String> getCatalogs() throws MetaException {
        return null;
    }

    @Override
    public void dropCatalog(String s) throws NoSuchObjectException, MetaException {

    }

    @Override
    public void createDatabase(Database database) throws InvalidObjectException, MetaException {
        objectStore.createDatabase(database);
    }

    @Override
    public Database getDatabase(String s, String s1) throws NoSuchObjectException {
        return objectStore.getDatabase(s, s1);
    }

    @Override
    public boolean dropDatabase(String s, String s1) throws NoSuchObjectException, MetaException {
        return objectStore.dropDatabase(s, s1);
    }

    @Override
    public boolean alterDatabase(String s, String s1, Database database) throws NoSuchObjectException, MetaException {
        return objectStore.alterDatabase(s, s1, database);
    }

    @Override
    public List<String> getDatabases(String s, String s1) throws MetaException {
        return objectStore.getDatabases(s, s1);
    }

    @Override
    public List<String> getAllDatabases(String s) throws MetaException {
        return objectStore.getAllDatabases(s);
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
        objectStore.createTable(table);
    }

    @Override
    public boolean dropTable(String s, String s1, String s2)
        throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        return objectStore.dropTable(s, s1, s2);
    }

    private boolean isLms(String s, String s1, String s2) throws MetaException {
        Table table = objectStore.getTable(s, s1, s2);
        String value = table.getParameters().get(metaLocation);
        if (value != null && value.equals(pcMetaStore)) {
            return true;
        }
        return false;
    }

    @Override
    public Table getTable(String s, String s1, String s2) throws MetaException {
        if (isLms(s, s1, s2)) {
            return dashRawStore.getTable(s, s1, s2);
        }
        return objectStore.getTable(s, s1, s2);
    }

    @Override
    public boolean addPartition(Partition partition) throws InvalidObjectException, MetaException {
        return objectStore.addPartition(partition);
    }

    @Override
    public boolean addPartitions(String s, String s1, String s2, List<Partition> list)
        throws InvalidObjectException, MetaException {
        return objectStore.addPartitions(s, s1, s2, list);
    }

    @Override
    public boolean addPartitions(String s, String s1, String s2, PartitionSpecProxy partitionSpecProxy, boolean b)
        throws InvalidObjectException, MetaException {
        return objectStore.addPartitions(s, s1, s2, partitionSpecProxy, b);
    }

    @Override
    public Partition getPartition(String s, String s1, String s2, List<String> list)
        throws MetaException, NoSuchObjectException {
        // TODO:need catch
        return null;
    }

    @Override
    public boolean doesPartitionExist(String s, String s1, String s2, List<String> list)
        throws MetaException, NoSuchObjectException {
        return objectStore.doesPartitionExist(s, s1, s2, list);
    }

    @Override
    public boolean dropPartition(String s, String s1, String s2, List<String> list)
        throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        return objectStore.dropPartition(s, s1, s2, list);
    }

    @Override
    public List<Partition> getPartitions(String s, String s1, String s2, int i)
        throws MetaException, NoSuchObjectException {
        return objectStore.getPartitions(s, s1, s2, i);
    }

    private AddPartitionInput buildPartition(Table table) {
        AddPartitionInput partitionInput = new AddPartitionInput();
        StorageDescriptor sd = table.getSd();
        PartitionInput[] partitionBases = new PartitionInput[]{new PartitionInput()};
        List<Column> columnInputs = new ArrayList();
        for (FieldSchema fieldSchema : sd.getCols()) {
            Column columnInput = new Column();
            columnInput.setColumnName(fieldSchema.getName());
            columnInput.setComment(fieldSchema.getComment());
            columnInput.setColType(fieldSchema.getType());
            columnInputs.add(columnInput);
        }
        partitionBases[0].setFileIndexUrl(sd.getLocation());
        io.polycat.catalog.common.model.StorageDescriptor lmsSd = new io.polycat.catalog.common.model.StorageDescriptor();
        lmsSd.setColumns(columnInputs);
        lmsSd.setLocation(sd.getLocation());
        partitionBases[0].setStorageDescriptor(lmsSd);
        partitionInput.setPartitions(partitionBases);
        return partitionInput;
    }

    @SneakyThrows
    @Override
    public void alterTable(String s, String s1, String s2, Table table) throws InvalidObjectException, MetaException {
        // if exist migrate flag, set table properties, start migrate.
        String startFlag = table.getParameters().get(migrateFlag);
        String migrating = table.getParameters().get(isMigrating);
        if (startFlag != null && startFlag.equals("true") &&  migrating == null) {
            table.getParameters().put(metaLocation, hiveMetaStore);
            table.getParameters().put(isMigrating, "true");
            objectStore.alterTable(s, s1, s2, table);
            Database database = objectStore.getDatabase(table.getCatName(), table.getDbName());
            Catalog catalog = new Catalog();
            catalog.setName(getMigratingCatalog());
            catalog.setLocationUriIsSet(false);
            dashRawStore.createCatalog(catalog);
            dashRawStore.createDatabase(database);
            dashRawStore.createTable(table);
            AddPartitionInput addPartitionInput = buildPartition(table);
            AddPartitionRequest request = new AddPartitionRequest(getProjectId(), getMigratingCatalog(),
                table.getDbName(), table.getTableName(), addPartitionInput);
            dashRawStore.getCatalogPlugin().addPartition(request);
            table.getParameters().replace(metaLocation, pcMetaStore);
            objectStore.alterTable(s, s1, s2, table);
            return;
        } else if (migrating != null && migrating.equals("true")) {
            return;
        } else if (isLms(s, s1, s2)) {
            dashRawStore.alterTable(s, s1, s2, table);
        } else {
            objectStore.alterTable(s, s1, s2, table);
        }
    }

    @Override
    public void updateCreationMetadata(String s, String s1, String s2, CreationMetadata creationMetadata)
        throws MetaException {
        objectStore.updateCreationMetadata(s, s1, s2, creationMetadata);
    }

    @Override
    public List<String> getTables(String s, String s1, String s2) throws MetaException {
        return objectStore.getTables(s, s1, s2);
    }

    @Override
    public List<String> getTables(String s, String s1, String s2, TableType tableType) throws MetaException {
        return objectStore.getTables(s, s1, s2, tableType);
    }

    @Override
    public List<String> getMaterializedViewsForRewriting(String s, String s1)
        throws MetaException, NoSuchObjectException {
        return objectStore.getMaterializedViewsForRewriting(s, s1);
    }

    @Override
    public List<TableMeta> getTableMeta(String s, String s1, String s2, List<String> list) throws MetaException {
        return objectStore.getTableMeta(s, s1, s2, list);
    }

    @Override
    public List<Table> getTableObjectsByName(String s, String s1, List<String> list)
        throws MetaException, UnknownDBException {
        return objectStore.getTableObjectsByName(s, s1, list);
    }

    @Override
    public List<String> getAllTables(String s, String s1) throws MetaException {
        return objectStore.getAllTables(s, s1);
    }

    @Override
    public List<String> listTableNamesByFilter(String s, String s1, String s2, short i)
        throws MetaException, UnknownDBException {
        return objectStore.listTableNamesByFilter(s, s1, s2, i);
    }

    @Override
    public List<String> listPartitionNames(String s, String s1, String s2, short i) throws MetaException {
        return objectStore.listPartitionNames(s, s1, s2, i);
    }

    @Override
    public PartitionValuesResponse listPartitionValues(String s, String s1, String s2, List<FieldSchema> list,
        boolean b, String s3, boolean b1, List<FieldSchema> list1, long l) throws MetaException {
        return objectStore.listPartitionValues(s, s1, s2, list, b, s3, b1, list1, l);
    }

    @Override
    public void alterPartition(String s, String s1, String s2, List<String> list, Partition partition)
        throws InvalidObjectException, MetaException {
        objectStore.alterPartition(s, s1, s2, list, partition);
    }

    @Override
    public void alterPartitions(String s, String s1, String s2, List<List<String>> list, List<Partition> list1)
        throws InvalidObjectException, MetaException {
        objectStore.alterPartitions(s, s1, s2, list, list1);
    }

    @Override
    public List<Partition> getPartitionsByFilter(String s, String s1, String s2, String s3, short i)
        throws MetaException, NoSuchObjectException {
        return objectStore.getPartitionsByFilter(s, s1, s2, s3, i);
    }

    @Override
    public boolean getPartitionsByExpr(String s, String s1, String s2, byte[] bytes, String s3, short i,
        List<Partition> list) throws TException {
        return objectStore.getPartitionsByExpr(s, s1, s2, bytes, s3, i, list);
    }

    @Override
    public int getNumPartitionsByFilter(String s, String s1, String s2, String s3)
        throws MetaException, NoSuchObjectException {
        return objectStore.getNumPartitionsByFilter(s, s1, s2, s3);
    }

    @Override
    public int getNumPartitionsByExpr(String s, String s1, String s2, byte[] bytes)
        throws MetaException, NoSuchObjectException {
        return objectStore.getNumPartitionsByExpr(s, s1, s2, bytes);
    }

    @Override
    public List<Partition> getPartitionsByNames(String s, String s1, String s2, List<String> list)
        throws MetaException, NoSuchObjectException {
        return objectStore.getPartitionsByNames(s, s1, s2, list);
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
    public Partition getPartitionWithAuth(String s, String s1, String s2, List<String> list, String s3,
        List<String> list1) throws MetaException, NoSuchObjectException, InvalidObjectException {
        return objectStore.getPartitionWithAuth(s, s1, s2, list, s3, list1);
    }

    @Override
    public List<Partition> getPartitionsWithAuth(String s, String s1, String s2, short i, String s3, List<String> list)
        throws MetaException, NoSuchObjectException, InvalidObjectException {
        return objectStore.getPartitionsWithAuth(s, s1, s2, i, s3, list);
    }

    @Override
    public List<String> listPartitionNamesPs(String s, String s1, String s2, List<String> list, short i)
        throws MetaException, NoSuchObjectException {
        return objectStore.listPartitionNamesPs(s, s1, s2, list, i);
    }

    @Override
    public List<Partition> listPartitionsPsWithAuth(String s, String s1, String s2, List<String> list, short i,
        String s3, List<String> list1) throws MetaException, InvalidObjectException, NoSuchObjectException {
        return objectStore.listPartitionsPsWithAuth(s, s1, s2, list, i, s3, list1);
    }

    @Override
    public boolean updateTableColumnStatistics(ColumnStatistics columnStatistics)
        throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
        return objectStore.updateTableColumnStatistics(columnStatistics);
    }

    @Override
    public boolean updatePartitionColumnStatistics(ColumnStatistics columnStatistics, List<String> list)
        throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
        return objectStore.updatePartitionColumnStatistics(columnStatistics, list);
    }

    @Override
    public ColumnStatistics getTableColumnStatistics(String s, String s1, String s2, List<String> list)
        throws MetaException, NoSuchObjectException {
        return objectStore.getTableColumnStatistics(s, s1, s2, list);
    }

    @Override
    public List<ColumnStatistics> getPartitionColumnStatistics(String s, String s1, String s2, List<String> list,
        List<String> list1) throws MetaException, NoSuchObjectException {
        return objectStore.getPartitionColumnStatistics(s, s1, s2, list, list1);
    }

    @Override
    public boolean deletePartitionColumnStatistics(String s, String s1, String s2, String s3, List<String> list,
        String s4) throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
        return objectStore.deletePartitionColumnStatistics(s, s1, s2, s3, list, s4);
    }

    @Override
    public boolean deleteTableColumnStatistics(String s, String s1, String s2, String s3)
        throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
        return objectStore.deleteTableColumnStatistics(s, s1, s2, s3);
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
    public void setMetaStoreSchemaVersion(String s, String s1) throws MetaException {
        objectStore.setMetaStoreSchemaVersion(s, s1);
    }

    @Override
    public void dropPartitions(String s, String s1, String s2, List<String> list)
        throws MetaException, NoSuchObjectException {
        objectStore.dropPartitions(s, s1, s2, list);
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
        objectStore.createFunction(function);
    }

    @Override
    public void alterFunction(String s, String s1, String s2, Function function)
        throws InvalidObjectException, MetaException {
        objectStore.alterFunction(s, s1, s2, function);
    }

    @Override
    public void dropFunction(String s, String s1, String s2)
        throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        objectStore.dropFunction(s, s1, s2);
    }

    @Override
    public Function getFunction(String s, String s1, String s2) throws MetaException {
        return objectStore.getFunction(s, s1, s2);
    }

    @Override
    public List<Function> getAllFunctions(String s) throws MetaException {
        return objectStore.getAllFunctions(s);
    }

    @Override
    public List<String> getFunctions(String s, String s1, String s2) throws MetaException {
        return objectStore.getFunctions(s, s1, s2);
    }

    @Override
    public AggrStats get_aggr_stats_for(String s, String s1, String s2, List<String> list, List<String> list1)
        throws MetaException, NoSuchObjectException {
        return objectStore.get_aggr_stats_for(s, s1, s2, list, list1);
    }

    @Override
    public List<ColStatsObjWithSourceInfo> getPartitionColStatsForDatabase(String s, String s1)
        throws MetaException, NoSuchObjectException {
        return objectStore.getPartitionColStatsForDatabase(s, s1);
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
    public List<SQLUniqueConstraint> getUniqueConstraints(String s, String s1, String s2) throws MetaException {
        return objectStore.getUniqueConstraints(s, s1, s2);
    }

    @Override
    public List<SQLNotNullConstraint> getNotNullConstraints(String s, String s1, String s2) throws MetaException {
        return objectStore.getNotNullConstraints(s, s1, s2);
    }

    @Override
    public List<SQLDefaultConstraint> getDefaultConstraints(String s, String s1, String s2) throws MetaException {
        return objectStore.getDefaultConstraints(s, s1, s2);
    }

    @Override
    public List<SQLCheckConstraint> getCheckConstraints(String s, String s1, String s2) throws MetaException {
        return objectStore.getCheckConstraints(s, s1, s2);
    }

    @Override
    public List<String> createTableWithConstraints(Table table, List<SQLPrimaryKey> list, List<SQLForeignKey> list1,
        List<SQLUniqueConstraint> list2, List<SQLNotNullConstraint> list3, List<SQLDefaultConstraint> list4,
        List<SQLCheckConstraint> list5) throws InvalidObjectException, MetaException {
        return objectStore.createTableWithConstraints(table, list, list1, list2, list3, list4, list5);
    }

    @Override
    public void dropConstraint(String s, String s1, String s2, String s3, boolean b) throws NoSuchObjectException {
        objectStore.dropConstraint(s, s1, s2, s3, b);
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
    public List<SchemaVersion> getSchemaVersionsByColumns(String s, String s1, String s2) throws MetaException {
        return objectStore.getSchemaVersionsByColumns(s, s1, s2);
    }

    @Override
    public void dropSchemaVersion(SchemaVersionDescriptor schemaVersionDescriptor)
        throws NoSuchObjectException, MetaException {
        objectStore.dropSchemaVersion(schemaVersionDescriptor);
    }

    @Override
    public SerDeInfo getSerDeInfo(String s) throws NoSuchObjectException, MetaException {
        return objectStore.getSerDeInfo(s);
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
    public List<RuntimeStat> getRuntimeStats(int i, int i1) throws MetaException {
        return objectStore.getRuntimeStats(i, i1);
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
    public Map<String, List<String>> getPartitionColsWithStats(String s, String s1, String s2)
        throws MetaException, NoSuchObjectException {
        return objectStore.getPartitionColsWithStats(s, s1, s2);
    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
        if (dashRawStore != null) {
            dashRawStore.setConf(configuration);
        }
    }

    @Override
    public Configuration getConf() {
        return null;
    }

    private String getCurrentCatalog() {
        return MetastoreConf.getVar(conf, MetastoreConf.ConfVars.CATALOG_DEFAULT);
    }

    private String getMigratingCatalog() {
        return MetastoreConf.getVar(conf, MetastoreConf.ConfVars.valueOf(ConfVars.MIGRATION_CATALOG.name()));
    }

    private String getProjectId() {
        return MetastoreConf.getVar(conf, MetastoreConf.ConfVars.valueOf(ConfVars.PROJECT_ID.name()));
    }

    private String getUserId() {
        return MetastoreConf.getVar(conf, MetastoreConf.ConfVars.valueOf(ConfVars.USER_ID.name()));
    }
}
