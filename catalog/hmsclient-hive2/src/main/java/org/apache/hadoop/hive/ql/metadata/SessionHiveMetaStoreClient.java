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
package org.apache.hadoop.hive.ql.metadata;

import io.polycat.catalog.client.PolyCatClient;
import io.polycat.catalog.common.PolyCatConf;
import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.model.Database;
import io.polycat.catalog.common.model.PagedList;
import io.polycat.catalog.common.plugin.request.CreateDatabaseRequest;
import io.polycat.catalog.common.plugin.request.GetDatabaseRequest;
import io.polycat.catalog.common.plugin.request.ListDatabasesRequest;
import io.polycat.catalog.common.plugin.request.base.CatalogRequestBase;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.FireEventResponse;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleRequest;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleResponse;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.MetadataPpdResult;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PartitionValuesRequest;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SessionHiveMetaStoreClient implements IMetaStoreClient{

    PolyCatClient client;
    protected final HiveConf conf;
    private final HiveMetaHookLoader hookLoader;
    String polyCatCatalog;
    String polyCatProjectId;
    String userName;
    String password;
    Map<String, String> polyCatConfMap = new HashMap<>();

    public SessionHiveMetaStoreClient(HiveConf conf, HiveMetaHookLoader hookLoader, Boolean allowEmbedded) {
        this.conf = conf;
        this.hookLoader = hookLoader;
        String polycatConfPath = conf.get("polycat.configFile.path", System.getenv("POLYCAT_CONFIG_PATH"));
        if (StringUtils.isNotEmpty(polycatConfPath)) {
            PolyCatConf polyCatConf = new PolyCatConf(polycatConfPath);
            polyCatConfMap.putAll(polyCatConf.getConf());
        }
        String metastoreUrisString = conf.get("polycat.metastore.uri",
                polyCatConfMap.getOrDefault("polycat.metastore.uri", "127.0.0.1"));
        int metastorePort = conf.getInt("polycat.metastore.port",
                Integer.parseInt(
                        polyCatConfMap.getOrDefault("polycat.metastore.port", "8082")));
        polyCatCatalog = conf.get("polycat.catalog.name",
                polyCatConfMap.getOrDefault("polycat.catalog.name", "default"));
        polyCatProjectId = conf.get("polycat.projectId",
                polyCatConfMap.getOrDefault("polycat.projectId", "project1"));
        userName = conf.get("polycat.auth.username", "test");
        password = conf.get("polycat.auth.password", "dash");
        if (localHost(metastoreUrisString)) {
            this.client = new PolyCatClient(metastoreUrisString, metastorePort, userName, password);
        } else {
            this.client = new PolyCatClient(metastoreUrisString, metastorePort);
        }

    }

    public SessionHiveMetaStoreClient(HiveConf conf, HiveMetaHookLoader hookLoader) {
        this(conf, hookLoader, true);
    }

    private boolean localHost(String catalogHost) {
        return catalogHost.contains("127.0.0.1");
    }

    private DatabaseInput convertHiveDB2PolyCatDB(org.apache.hadoop.hive.metastore.api.Database db) {
        DatabaseInput databaseInput = new DatabaseInput();
        databaseInput.setDatabaseName(db.getName());
        databaseInput.setCatalogName(polyCatCatalog);
        databaseInput.setLocationUri(db.getLocationUri());
        databaseInput.setDescription(db.getDescription());
        databaseInput.setUserId(userName);
        databaseInput.setParameters(db.getParameters());
        return databaseInput;
    }

    private org.apache.hadoop.hive.metastore.api.Database convertPolyCatDB2HiveDB(Database database) {
        org.apache.hadoop.hive.metastore.api.Database db = new org.apache.hadoop.hive.metastore.api.Database();
        db.setName(database.getDatabaseName());
        db.setDescription(database.getDescription());
        db.setLocationUri(database.getLocationUri());
        db.setOwnerName(database.getOwner());
        db.setParameters(database.getParameters());
        return db;
    }

    private void initRequest(CatalogRequestBase request) {
        request.setProjectId(polyCatProjectId);
        request.setCatalogName(polyCatCatalog);
        request.setToken(client.getToken());
    }


    @Override
    public boolean isCompatibleWith(HiveConf conf) {
        return false;
    }

    @Override
    public void setHiveAddedJars(String s) {

    }

    @Override
    public boolean isLocalMetaStore() {
        return false;
    }

    @Override
    public void reconnect() throws MetaException {

    }

    @Override
    public void close() {

    }

    @Override
    public void setMetaConf(String s, String s1) throws MetaException, TException {

    }

    @Override
    public String getMetaConf(String s) throws MetaException, TException {
        return null;
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Database getDatabase(String databaseName) throws NoSuchObjectException, MetaException, TException {
        GetDatabaseRequest getDatabaseRequest = new GetDatabaseRequest();
        initRequest(getDatabaseRequest);
        getDatabaseRequest.setDatabaseName(databaseName);
        Database database = null;
        try {
            database = client.getDatabase(getDatabaseRequest);
        } catch (CatalogException e) {
            if (e.getStatusCode() == 404) {
                throw new NoSuchObjectException(e.getMessage());
            }
            else {
                throw e;
            }
        }
        return convertPolyCatDB2HiveDB(database);
    }



    @Override
    public List<String> getDatabases(String s) throws MetaException, TException {
        List<String> databases = new ArrayList<>();
        ListDatabasesRequest listDatabasesRequest = new ListDatabasesRequest();
        initRequest(listDatabasesRequest);
        listDatabasesRequest.setFilter(s);
        PagedList<Database> databasePagedList = client.listDatabases(listDatabasesRequest);
        Arrays.stream(databasePagedList.getObjects()).forEach(database -> databases.add(database.getDatabaseName()));
        return databases;
    }

    @Override
    public List<String> getAllDatabases() throws MetaException, TException {
        List<String> databases = new ArrayList<>();
        ListDatabasesRequest listDatabasesRequest = new ListDatabasesRequest();
        initRequest(listDatabasesRequest);
        PagedList<Database> databasePagedList = client.listDatabases(listDatabasesRequest);
        Arrays.stream(databasePagedList.getObjects()).forEach(database -> databases.add(database.getDatabaseName()));
        return databases;
    }

    @Override
    public void createDatabase(org.apache.hadoop.hive.metastore.api.Database db) throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        CreateDatabaseRequest createDatabaseRequest = new CreateDatabaseRequest();
        initRequest(createDatabaseRequest);
        createDatabaseRequest.setInput(convertHiveDB2PolyCatDB(db));
        client.createDatabase(createDatabaseRequest);
    }

    @Override
    public List<String> getTables(String dbName, String tablePattern) throws MetaException, TException, UnknownDBException {
        return null;
    }

    @Override
    public List<String> getTables(String dbName, String tablePattern, TableType tableType) throws MetaException, TException, UnknownDBException {
        return null;
    }

    @Override
    public List<TableMeta> getTableMeta(String dbPatterns, String tablePatterns, List<String> tableTypes) throws MetaException, TException, UnknownDBException {
        return null;
    }

    @Override
    public List<String> getAllTables(String dbName) throws MetaException, TException, UnknownDBException {
        return null;
    }

    @Override
    public List<String> listTableNamesByFilter(String dbName, String filter, short maxTables) throws MetaException, TException, InvalidOperationException, UnknownDBException {
        return null;
    }

    @Override
    public void dropTable(String dbname, String tableName, boolean deleteData, boolean ignoreUnknownTab) throws MetaException, TException, NoSuchObjectException {

    }

    @Override
    public void dropTable(String dbname, String tableName, boolean deleteData, boolean ignoreUnknownTab, boolean ifPurge) throws MetaException, TException, NoSuchObjectException {

    }

    @Override
    public void dropTable(String tableName, boolean deleteData) throws MetaException, UnknownTableException, TException, NoSuchObjectException {

    }

    @Override
    public void dropTable(String dbname, String tableName) throws MetaException, TException, NoSuchObjectException {

    }

    @Override
    public boolean tableExists(String databaseName, String tableName) throws MetaException, TException, UnknownDBException {
        return false;
    }

    @Override
    public boolean tableExists(String tableName) throws MetaException, TException, UnknownDBException {
        return false;
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Table getTable(String tableName) throws MetaException, TException, NoSuchObjectException {
        return null;
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Table getTable(String dbName, String tableName) throws MetaException, TException, NoSuchObjectException {
        return null;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Table> getTableObjectsByName(String dbName, List<String> tableNames) throws MetaException, InvalidOperationException, UnknownDBException, TException {
        return null;
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition appendPartition(String tableName, String dbName, List<String> partVals) throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return null;
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition appendPartition(String tableName, String dbName, String name) throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return null;
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition add_partition(org.apache.hadoop.hive.metastore.api.Partition partition) throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return null;
    }

    @Override
    public int add_partitions(List<org.apache.hadoop.hive.metastore.api.Partition> partitions) throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return 0;
    }

    @Override
    public int add_partitions_pspec(PartitionSpecProxy partitionSpec) throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return 0;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> add_partitions(List<org.apache.hadoop.hive.metastore.api.Partition> partitions, boolean ifNotExists, boolean needResults) throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return null;
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition getPartition(String dbName, String tblName, List<String> partVals) throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition exchange_partition(Map<String, String> partitionSpecs, String sourceDb, String sourceTable, String destdb, String destTableName) throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
        return null;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> exchange_partitions(Map<String, String> partitionSpecs, String sourceDb, String sourceTable, String destdb, String destTableName) throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
        return null;
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition getPartition(String dbName, String tblName, String name) throws MetaException, UnknownTableException, NoSuchObjectException, TException {
        return null;
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition getPartitionWithAuthInfo(String dbName, String tableName, List<String> pvals, String userName, List<String> groupNames) throws MetaException, UnknownTableException, NoSuchObjectException, TException {
        return null;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitions(String db_name, String tbl_name, short max_parts) throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public PartitionSpecProxy listPartitionSpecs(String dbName, String tableName, int maxParts) throws TException {
        return null;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitions(String db_name, String tbl_name, List<String> part_vals, short max_parts) throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public List<String> listPartitionNames(String db_name, String tbl_name, short max_parts) throws MetaException, TException {
        return null;
    }

    @Override
    public List<String> listPartitionNames(String db_name, String tbl_name, List<String> part_vals, short max_parts) throws MetaException, TException, NoSuchObjectException {
        return null;
    }

    @Override
    public PartitionValuesResponse listPartitionValues(PartitionValuesRequest request) throws MetaException, TException, NoSuchObjectException {
        return null;
    }

    @Override
    public int getNumPartitionsByFilter(String dbName, String tableName, String filter) throws MetaException, NoSuchObjectException, TException {
        return 0;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitionsByFilter(String db_name, String tbl_name, String filter, short max_parts) throws MetaException, NoSuchObjectException, TException {
        return null;
    }

    @Override
    public PartitionSpecProxy listPartitionSpecsByFilter(String db_name, String tbl_name, String filter, int max_parts) throws MetaException, NoSuchObjectException, TException {
        return null;
    }

    @Override
    public boolean listPartitionsByExpr(String db_name, String tbl_name, byte[] expr, String default_partition_name, short max_parts, List<org.apache.hadoop.hive.metastore.api.Partition> result) throws TException {
        return false;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitionsWithAuthInfo(String dbName, String tableName, short s, String userName, List<String> groupNames) throws MetaException, TException, NoSuchObjectException {
        return null;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> getPartitionsByNames(String db_name, String tbl_name, List<String> part_names) throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitionsWithAuthInfo(String dbName, String tableName, List<String> partialPvals, short s, String userName, List<String> groupNames) throws MetaException, TException, NoSuchObjectException {
        return null;
    }

    @Override
    public void markPartitionForEvent(String db_name, String tbl_name, Map<String, String> partKVs, PartitionEventType eventType) throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException, UnknownPartitionException, InvalidPartitionException {

    }

    @Override
    public boolean isPartitionMarkedForEvent(String db_name, String tbl_name, Map<String, String> partKVs, PartitionEventType eventType) throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException, UnknownPartitionException, InvalidPartitionException {
        return false;
    }

    @Override
    public void validatePartitionNameCharacters(List<String> partVals) throws TException, MetaException {

    }

    @Override
    public void createTable(org.apache.hadoop.hive.metastore.api.Table tbl) throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {

    }

    @Override
    public void alter_table(String defaultDatabaseName, String tblName, org.apache.hadoop.hive.metastore.api.Table table) throws InvalidOperationException, MetaException, TException {

    }

    @Override
    public void alter_table(String defaultDatabaseName, String tblName, org.apache.hadoop.hive.metastore.api.Table table, boolean cascade) throws InvalidOperationException, MetaException, TException {

    }

    @Override
    public void alter_table_with_environmentContext(String defaultDatabaseName, String tblName, org.apache.hadoop.hive.metastore.api.Table table, EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {

    }

    @Override
    public void dropDatabase(String name) throws NoSuchObjectException, InvalidOperationException, MetaException, TException {

    }

    @Override
    public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb) throws NoSuchObjectException, InvalidOperationException, MetaException, TException {

    }

    @Override
    public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb, boolean cascade) throws NoSuchObjectException, InvalidOperationException, MetaException, TException {

    }

    @Override
    public void alterDatabase(String name, org.apache.hadoop.hive.metastore.api.Database db) throws NoSuchObjectException, MetaException, TException {

    }

    @Override
    public boolean dropPartition(String db_name, String tbl_name, List<String> part_vals, boolean deleteData) throws NoSuchObjectException, MetaException, TException {
        return false;
    }

    @Override
    public boolean dropPartition(String db_name, String tbl_name, List<String> part_vals, PartitionDropOptions options) throws TException {
        return false;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> dropPartitions(String dbName, String tblName, List<ObjectPair<Integer, byte[]>> partExprs, boolean deleteData, boolean ifExists) throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> dropPartitions(String dbName, String tblName, List<ObjectPair<Integer, byte[]>> partExprs, boolean deleteData, boolean ifExists, boolean needResults) throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> dropPartitions(String dbName, String tblName, List<ObjectPair<Integer, byte[]>> partExprs, PartitionDropOptions options) throws TException {
        return null;
    }

    @Override
    public boolean dropPartition(String db_name, String tbl_name, String name, boolean deleteData) throws NoSuchObjectException, MetaException, TException {
        return false;
    }

    @Override
    public void alter_partition(String dbName, String tblName, org.apache.hadoop.hive.metastore.api.Partition newPart) throws InvalidOperationException, MetaException, TException {

    }

    @Override
    public void alter_partition(String dbName, String tblName, org.apache.hadoop.hive.metastore.api.Partition newPart, EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {

    }

    @Override
    public void alter_partitions(String dbName, String tblName, List<org.apache.hadoop.hive.metastore.api.Partition> newParts) throws InvalidOperationException, MetaException, TException {

    }

    @Override
    public void alter_partitions(String dbName, String tblName, List<org.apache.hadoop.hive.metastore.api.Partition> newParts, EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {

    }

    @Override
    public void renamePartition(String dbname, String name, List<String> part_vals, Partition newPart) throws InvalidOperationException, MetaException, TException {

    }

    @Override
    public List<FieldSchema> getFields(String db, String tableName) throws MetaException, TException, UnknownTableException, UnknownDBException {
        return null;
    }

    @Override
    public List<FieldSchema> getSchema(String db, String tableName) throws MetaException, TException, UnknownTableException, UnknownDBException {
        return null;
    }

    @Override
    public String getConfigValue(String name, String defaultValue) throws TException, ConfigValSecurityException {
        return null;
    }

    @Override
    public List<String> partitionNameToVals(String name) throws MetaException, TException {
        return null;
    }

    @Override
    public Map<String, String> partitionNameToSpec(String name) throws MetaException, TException {
        return null;
    }

    @Override
    public void createIndex(Index index, org.apache.hadoop.hive.metastore.api.Table indexTable) throws InvalidObjectException, MetaException, NoSuchObjectException, TException, AlreadyExistsException {

    }

    @Override
    public void alter_index(String dbName, String tblName, String indexName, Index index) throws InvalidOperationException, MetaException, TException {

    }

    @Override
    public Index getIndex(String dbName, String tblName, String indexName) throws MetaException, UnknownTableException, NoSuchObjectException, TException {
        return null;
    }

    @Override
    public List<Index> listIndexes(String db_name, String tbl_name, short max) throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public List<String> listIndexNames(String db_name, String tbl_name, short max) throws MetaException, TException {
        return null;
    }

    @Override
    public boolean dropIndex(String db_name, String tbl_name, String name, boolean deleteData) throws NoSuchObjectException, MetaException, TException {
        return false;
    }

    @Override
    public boolean updateTableColumnStatistics(ColumnStatistics statsObj) throws NoSuchObjectException, InvalidObjectException, MetaException, TException, InvalidInputException {
        return false;
    }

    @Override
    public boolean updatePartitionColumnStatistics(ColumnStatistics statsObj) throws NoSuchObjectException, InvalidObjectException, MetaException, TException, InvalidInputException {
        return false;
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName, List<String> colNames) throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String dbName, String tableName, List<String> partNames, List<String> colNames) throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public boolean deletePartitionColumnStatistics(String dbName, String tableName, String partName, String colName) throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
        return false;
    }

    @Override
    public boolean deleteTableColumnStatistics(String dbName, String tableName, String colName) throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
        return false;
    }

    @Override
    public boolean create_role(Role role) throws MetaException, TException {
        return false;
    }

    @Override
    public boolean drop_role(String role_name) throws MetaException, TException {
        return false;
    }

    @Override
    public List<String> listRoleNames() throws MetaException, TException {
        return null;
    }

    @Override
    public boolean grant_role(String role_name, String user_name, PrincipalType principalType, String grantor, PrincipalType grantorType, boolean grantOption) throws MetaException, TException {
        return false;
    }

    @Override
    public boolean revoke_role(String role_name, String user_name, PrincipalType principalType, boolean grantOption) throws MetaException, TException {
        return false;
    }

    @Override
    public List<Role> list_roles(String principalName, PrincipalType principalType) throws MetaException, TException {
        return null;
    }

    @Override
    public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObject, String user_name, List<String> group_names) throws MetaException, TException {
        return null;
    }

    @Override
    public List<HiveObjectPrivilege> list_privileges(String principal_name, PrincipalType principal_type, HiveObjectRef hiveObject) throws MetaException, TException {
        return null;
    }

    @Override
    public boolean grant_privileges(PrivilegeBag privileges) throws MetaException, TException {
        return false;
    }

    @Override
    public boolean revoke_privileges(PrivilegeBag privileges, boolean grantOption) throws MetaException, TException {
        return false;
    }

    @Override
    public String getDelegationToken(String owner, String renewerKerberosPrincipalName) throws MetaException, TException {
        return null;
    }

    @Override
    public long renewDelegationToken(String tokenStrForm) throws MetaException, TException {
        return 0;
    }

    @Override
    public void cancelDelegationToken(String tokenStrForm) throws MetaException, TException {

    }

    @Override
    public String getTokenStrForm() throws IOException {
        return null;
    }

    @Override
    public boolean addToken(String tokenIdentifier, String delegationToken) throws TException {
        return false;
    }

    @Override
    public boolean removeToken(String tokenIdentifier) throws TException {
        return false;
    }

    @Override
    public String getToken(String tokenIdentifier) throws TException {
        return null;
    }

    @Override
    public List<String> getAllTokenIdentifiers() throws TException {
        return null;
    }

    @Override
    public int addMasterKey(String key) throws MetaException, TException {
        return 0;
    }

    @Override
    public void updateMasterKey(Integer seqNo, String key) throws NoSuchObjectException, MetaException, TException {

    }

    @Override
    public boolean removeMasterKey(Integer keySeq) throws TException {
        return false;
    }

    @Override
    public String[] getMasterKeys() throws TException {
        return new String[0];
    }

    @Override
    public void createFunction(Function func) throws InvalidObjectException, MetaException, TException {

    }

    @Override
    public void alterFunction(String dbName, String funcName, Function newFunction) throws InvalidObjectException, MetaException, TException {

    }

    @Override
    public void dropFunction(String dbName, String funcName) throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException, TException {

    }

    @Override
    public Function getFunction(String dbName, String funcName) throws MetaException, TException {
        return null;
    }

    @Override
    public List<String> getFunctions(String dbName, String pattern) throws MetaException, TException {
        return null;
    }

    @Override
    public GetAllFunctionsResponse getAllFunctions() throws MetaException, TException {
        return null;
    }

    @Override
    public ValidTxnList getValidTxns() throws TException {
        return null;
    }

    @Override
    public ValidTxnList getValidTxns(long currentTxn) throws TException {
        return null;
    }

    @Override
    public long openTxn(String user) throws TException {
        return 0;
    }

    @Override
    public OpenTxnsResponse openTxns(String user, int numTxns) throws TException {
        return null;
    }

    @Override
    public void rollbackTxn(long txnid) throws NoSuchTxnException, TException {

    }

    @Override
    public void commitTxn(long txnid) throws NoSuchTxnException, TxnAbortedException, TException {

    }

    @Override
    public void abortTxns(List<Long> txnids) throws TException {

    }

    @Override
    public GetOpenTxnsInfoResponse showTxns() throws TException {
        return null;
    }

    @Override
    public LockResponse lock(LockRequest request) throws NoSuchTxnException, TxnAbortedException, TException {
        return null;
    }

    @Override
    public LockResponse checkLock(long lockid) throws NoSuchTxnException, TxnAbortedException, NoSuchLockException, TException {
        return null;
    }

    @Override
    public void unlock(long lockid) throws NoSuchLockException, TxnOpenException, TException {

    }

    @Override
    public ShowLocksResponse showLocks() throws TException {
        return null;
    }

    @Override
    public ShowLocksResponse showLocks(ShowLocksRequest showLocksRequest) throws TException {
        return null;
    }

    @Override
    public void heartbeat(long txnid, long lockid) throws NoSuchLockException, NoSuchTxnException, TxnAbortedException, TException {

    }

    @Override
    public HeartbeatTxnRangeResponse heartbeatTxnRange(long min, long max) throws TException {
        return null;
    }

    @Override
    public void compact(String dbname, String tableName, String partitionName, CompactionType type) throws TException {

    }

    @Override
    public void compact(String dbname, String tableName, String partitionName, CompactionType type, Map<String, String> tblproperties) throws TException {

    }

    @Override
    public CompactionResponse compact2(String dbname, String tableName, String partitionName, CompactionType type, Map<String, String> tblproperties) throws TException {
        return null;
    }

    @Override
    public ShowCompactResponse showCompactions() throws TException {
        return null;
    }

    @Override
    public void addDynamicPartitions(long txnId, String dbName, String tableName, List<String> partNames) throws TException {

    }

    @Override
    public void addDynamicPartitions(long txnId, String dbName, String tableName, List<String> partNames, DataOperationType operationType) throws TException {

    }

    @Override
    public void insertTable(org.apache.hadoop.hive.metastore.api.Table table, boolean overwrite) throws MetaException {

    }

    @Override
    public NotificationEventResponse getNextNotification(long lastEventId, int maxEvents, IMetaStoreClient.NotificationFilter filter) throws TException {
        return null;
    }

    @Override
    public CurrentNotificationEventId getCurrentNotificationEventId() throws TException {
        return null;
    }

    @Override
    public FireEventResponse fireListenerEvent(FireEventRequest request) throws TException {
        return null;
    }

    @Override
    public GetPrincipalsInRoleResponse get_principals_in_role(GetPrincipalsInRoleRequest getPrincRoleReq) throws MetaException, TException {
        return null;
    }

    @Override
    public GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(GetRoleGrantsForPrincipalRequest getRolePrincReq) throws MetaException, TException {
        return null;
    }

    @Override
    public AggrStats getAggrColStatsFor(String dbName, String tblName, List<String> colNames, List<String> partName) throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public boolean setPartitionColumnStatistics(SetPartitionsStatsRequest request) throws NoSuchObjectException, InvalidObjectException, MetaException, TException, InvalidInputException {
        return false;
    }

    @Override
    public void flushCache() {

    }

    @Override
    public Iterable<Map.Entry<Long, ByteBuffer>> getFileMetadata(List<Long> fileIds) throws TException {
        return null;
    }

    @Override
    public Iterable<Map.Entry<Long, MetadataPpdResult>> getFileMetadataBySarg(List<Long> fileIds, ByteBuffer sarg, boolean doGetFooters) throws TException {
        return null;
    }

    @Override
    public void clearFileMetadata(List<Long> fileIds) throws TException {

    }

    @Override
    public void putFileMetadata(List<Long> fileIds, List<ByteBuffer> metadata) throws TException {

    }

    @Override
    public boolean isSameConfObj(HiveConf c) {
        return false;
    }

    @Override
    public boolean cacheFileMetadata(String dbName, String tableName, String partName, boolean allParts) throws TException {
        return false;
    }

    @Override
    public List<SQLPrimaryKey> getPrimaryKeys(PrimaryKeysRequest request) throws MetaException, NoSuchObjectException, TException {
        return null;
    }

    @Override
    public List<SQLForeignKey> getForeignKeys(ForeignKeysRequest request) throws MetaException, NoSuchObjectException, TException {
        return null;
    }

    @Override
    public void createTableWithConstraints(Table tTbl, List<SQLPrimaryKey> primaryKeys, List<SQLForeignKey> foreignKeys) throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {

    }

    @Override
    public void dropConstraint(String dbName, String tableName, String constraintName) throws MetaException, NoSuchObjectException, TException {

    }

    @Override
    public void addPrimaryKey(List<SQLPrimaryKey> primaryKeyCols) throws MetaException, NoSuchObjectException, TException {

    }

    @Override
    public void addForeignKey(List<SQLForeignKey> foreignKeyCols) throws MetaException, NoSuchObjectException, TException {

    }
}
