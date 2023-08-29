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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.polycat.catalog.client.PolyCatClient;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.model.Database;
import io.polycat.catalog.common.model.PagedList;
import io.polycat.catalog.common.model.StorageDescriptor;
import io.polycat.catalog.common.plugin.CatalogPlugin;
import io.polycat.catalog.common.plugin.request.AlterDatabaseRequest;
import io.polycat.catalog.common.plugin.request.CreateCatalogRequest;
import io.polycat.catalog.common.plugin.request.CreateDatabaseRequest;
import io.polycat.catalog.common.plugin.request.CreateTableRequest;
import io.polycat.catalog.common.plugin.request.DeleteDatabaseRequest;
import io.polycat.catalog.common.plugin.request.DeleteTableRequest;
import io.polycat.catalog.common.plugin.request.GetCatalogRequest;
import io.polycat.catalog.common.plugin.request.GetDatabaseRequest;
import io.polycat.catalog.common.plugin.request.GetTableRequest;
import io.polycat.catalog.common.plugin.request.ListDatabasesRequest;
import io.polycat.catalog.common.plugin.request.ListTablesRequest;
import io.polycat.catalog.common.plugin.request.input.CatalogInput;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;
import io.polycat.catalog.common.plugin.request.input.TableInput;
import io.polycat.catalog.common.model.Column;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.FileMetadataHandler;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
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
import org.apache.hadoop.hive.ql.io.StorageFormatDescriptor;
import org.apache.hadoop.hive.ql.io.StorageFormatFactory;
import org.apache.hadoop.hive.ql.io.TextFileStorageFormatDescriptor;
import org.apache.thrift.TException;

import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;

/**
 * A RawStore implementation that uses PolyCatClient
 */
public class CatalogStore implements RawStore {

    private static final Logger LOG = Logger.getLogger(CatalogStore.class);
    private PolyCatClient client;

    // TODO 从配置文件获取catalog url，当前默认本地
    private static final String CATALOG_URL = "http://127.0.0.1:8082/v1/projects/";

    private static HashMap<String,String> convertTypeMap = new HashMap() {
        {
            put("integer", "int");
        }
    };


    private static HashMap<String,String> lmsToHiveFileFormatMap = new HashMap() {
        {
            put("csv", "TEXTFILE");
        }
    };

    private static HashMap<String,String> serializationLibToSourceNameMap = new HashMap() {
        {
            put("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", "parquet");
            put("org.apache.hadoop.hive.ql.io.orc.OrcSerde", "ORC");
            put("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", "textfile");
        }
    };

    private Configuration conf;

    public CatalogStore() {
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
    public void createCatalog(Catalog mCat) {
        CreateCatalogRequest request = new CreateCatalogRequest();
        CatalogInput catalogInput = new CatalogInput();
        catalogInput.setCatalogName(mCat.getName());
        catalogInput.setOwner(getUserId());
        request.setInput(catalogInput);
        request.setProjectId(getProjectId());
        client.createCatalog(request);
    }

    @Override
    public void alterCatalog(String s, Catalog catalog) throws MetaException, InvalidOperationException {

    }

    private boolean isCatExist(String catalogName) {
        Catalog mCat = null;
        try {
            mCat = getCatalog(catalogName);
        } catch (NoSuchObjectException | MetaException e) {
            LOG.info("Catalog {} is not existed", catalogName);
        }
        return StringUtils.isNotBlank(mCat.getName());
    }

    @Override
    public Catalog getCatalog(String catalogName) throws NoSuchObjectException, MetaException {
        try {
            GetCatalogRequest request = new GetCatalogRequest(getProjectId(), catalogName);
            io.polycat.catalog.common.model.Catalog dCat = client.getCatalog(request);
            return lmsCatTomCat(dCat);
        } catch (CatalogException e) {
            throw new NoSuchObjectException("No catalog " + catalogName);
        }
    }

    // Convert lms catalog to HMS catalog
    private Catalog lmsCatTomCat(io.polycat.catalog.common.model.Catalog dCat) {
        Catalog mCat = new Catalog();
        mCat.setName(normalizeIdentifier(dCat.getCatalogName()));
        // hms中catalog需要字段locationUri，lms中需要适配
        mCat.setLocationUri("");
        return mCat;
    }

    @Override
    public List<String> getCatalogs() throws MetaException {
        return new ArrayList<>();
    }

    @Override
    public void dropCatalog(String s) throws NoSuchObjectException, MetaException {

    }

    @Override
    public void createDatabase(org.apache.hadoop.hive.metastore.api.Database hiveDatabase) {
        LOG.info("createDatabase,db name is:" + hiveDatabase.getName());
        String currentCatalog = getCurrentCatalog();
        CreateDatabaseRequest createDatabaseRequest = new CreateDatabaseRequest();
        createDatabaseRequest.setProjectId(getProjectId());
        createDatabaseRequest.setCatalogName(currentCatalog);
        DatabaseInput databaseInput = new DatabaseInput();
        databaseInput.setDatabaseName(hiveDatabase.getName());
        databaseInput.setDescription(hiveDatabase.getDescription());
        databaseInput.setLocationUri(hiveDatabase.getLocationUri());
        databaseInput.setOwner(getUserId());
        createDatabaseRequest.setInput(databaseInput);
        ListDatabasesRequest request = new ListDatabasesRequest();
        request.setProjectId(getProjectId());
        request.setCatalogName(currentCatalog);
        client.createDatabase(createDatabaseRequest);
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Database getDatabase(String catName, String dbname) throws NoSuchObjectException {
        catName = normalizeIdentifier(getCurrentCatalog());
        return dDbTomDb(getDDatabase(catName,dbname));
    }

    private org.apache.hadoop.hive.metastore.api.Database dDbTomDb(Database lmsDatabase) {
        org.apache.hadoop.hive.metastore.api.Database mDb = new org.apache.hadoop.hive.metastore.api.Database();
        mDb.setCatalogName(lmsDatabase.getCatalogName());
        mDb.setDescription(lmsDatabase.getDescription());
        mDb.setLocationUri(lmsDatabase.getLocationUri());
        mDb.setName(lmsDatabase.getDatabaseName());
        mDb.setParameters(lmsDatabase.getParameters());
        mDb.setOwnerName(lmsDatabase.getOwner());
        mDb.setOwnerType(PrincipalType.valueOf(lmsDatabase.getOwnerType()));
        return mDb;
    }

    @Override
    public boolean dropDatabase(String catName, String dbname) throws NoSuchObjectException, MetaException {
        boolean success = true;
        LOG.info("Dropping database {}.{} along with all tables", catName, dbname);
        dbname = normalizeIdentifier(dbname);
        catName = normalizeIdentifier(getCurrentCatalog());
        Database mDb = getDDatabase(catName, dbname);
        if (mDb != null) {
            DeleteDatabaseRequest deleteDatabaseRequest = new DeleteDatabaseRequest();
            deleteDatabaseRequest.setCatalogName(catName);
            deleteDatabaseRequest.setDatabaseName(dbname);
            deleteDatabaseRequest.setProjectId(getProjectId());
            try {
                client.deleteDatabase(deleteDatabaseRequest);
            } catch (CatalogException e) {
                success = false;
            }
        }
        return success;
    }

    private Database getDDatabase(String catName, String dbname) throws NoSuchObjectException {
        GetDatabaseRequest request = new GetDatabaseRequest();
        request.setCatalogName(catName);
        request.setDatabaseName(dbname);
        request.setProjectId(getProjectId());
        Database dDb;
        try {
            dDb = client.getDatabase(request);
        } catch (CatalogException e) {
            LOG.warn("Failed to get database {}.{}, returning NoSuchObjectException",
                    catName, dbname, e);
            throw new NoSuchObjectException(dbname + ":" + e.getMessage());
        }
        return dDb;
    }

    @Override
    public boolean alterDatabase(String catName, String dbName, org.apache.hadoop.hive.metastore.api.Database mDb) throws NoSuchObjectException, MetaException {
        boolean success = true;
        catName = normalizeIdentifier(getCurrentCatalog());
        getDDatabase(catName, dbName);
        DatabaseInput databaseInput = new DatabaseInput();
        databaseInput.setDescription(mDb.getDescription());
        databaseInput.setLocationUri(mDb.getLocationUri());
        databaseInput.setParameters(mDb.getParameters());
        databaseInput.setCatalogName(mDb.getCatalogName());
        databaseInput.setDatabaseName(mDb.getName());
        databaseInput.setOwner(getUserId());

        AlterDatabaseRequest alterDatabaseRequest = new AlterDatabaseRequest();
        alterDatabaseRequest.setInput(databaseInput);
        alterDatabaseRequest.setDatabaseName(mDb.getName());
        alterDatabaseRequest.setProjectId(getProjectId());
        alterDatabaseRequest.setCatalogName(catName);
        try {
            client.alterDatabase(alterDatabaseRequest);
        } catch (CatalogException e) {
            LOG.error("Alter database {}.{} failed", catName, dbName, e);
            success = false;
        }
        return success;
    }

    @Override
    public List<String> getDatabases(String catName, String pattern) throws MetaException {
        return getAllDatabases(catName);
    }

    @Override
    public List<String> getAllDatabases(String catName) throws MetaException {
        catName = normalizeIdentifier(getCurrentCatalog());
        LOG.info("show databases,catalog is:" + catName);
        List<String> list = new ArrayList<>();
        ListDatabasesRequest request = new ListDatabasesRequest();
        request.setProjectId(getProjectId());
        request.setCatalogName(catName);
        PagedList<Database> response = client.listDatabases(request);
        Database[] databases = response.getObjects();
        for (Database database : databases) {
            list.add(database.getDatabaseName());
        }
        return list;
    }

    @Override
    public boolean createType(Type type) {
        return false;
    }

    @Override
    public Type getType(String s) {
        return new Type();
    }

    @Override
    public boolean dropType(String s) {
        return false;
    }

    @Override
    public void createTable(Table table) throws InvalidObjectException, MetaException {

        TableInput tableInput = new TableInput();
        org.apache.hadoop.hive.metastore.api.StorageDescriptor sd = table.getSd();
        tableInput.setTableName(table.getTableName());
        tableInput.setPartitionKeys(table.getPartitionKeys().stream()
                .map(this::convertToTableInput).collect(Collectors.toList()));
        tableInput.setTableType(table.getTableType());
        tableInput.setParameters(table.getParameters());
        tableInput.setRetention(table.getRetention());
        tableInput.setOwner(getUserId());

        StorageDescriptor storageInput = fillInStorageDescriptor(sd);
        tableInput.setStorageDescriptor(storageInput);
        tableInput.setOwner(getUserId());
        CreateTableRequest createTableRequest = new CreateTableRequest();
        createTableRequest.setInput(tableInput);
        createTableRequest.setCatalogName(getCurrentCatalog());
        createTableRequest.setDatabaseName(table.getDbName());
        createTableRequest.setProjectId(getProjectId());
        try {
            client.createTable(createTableRequest);
            LOG.info("create table {}.{},sourceShortName is {}", table.getDbName(), table.getTableName(),
                storageInput.getSourceShortName());
        } catch (CatalogException e) {
            LOG.error("table:{} create failed",table.getTableName(),e);
        }
    }

    private StorageDescriptor fillInStorageDescriptor(org.apache.hadoop.hive.metastore.api.StorageDescriptor sd) {
        if (sd == null) {
            return null;
        }
        StorageDescriptor storageInput = new StorageDescriptor();
        io.polycat.catalog.common.model.SerDeInfo serDeInfo = fillInSerDeInfo(sd);
        storageInput.setLocation(sd.getLocation());
        storageInput.setCompressed(sd.isCompressed());
        String serializationLib = sd.getSerdeInfo().getSerializationLib();
        String sourceName = serializationLibToSourceNameMap.getOrDefault(serializationLib, serializationLib);
        storageInput.setSourceShortName(sourceName);
        storageInput.setInputFormat(sd.getInputFormat());
        storageInput.setOutputFormat(sd.getOutputFormat());
        storageInput.setSerdeInfo(serDeInfo);
        storageInput.setNumberOfBuckets(sd.getNumBuckets());
        storageInput.setColumns(sd.getCols().stream().map(this::convertToTableInput).collect(Collectors.toList()));
        storageInput.setParameters(new HashMap<>());
        return storageInput;
    }

    private io.polycat.catalog.common.model.SerDeInfo fillInSerDeInfo(
        org.apache.hadoop.hive.metastore.api.StorageDescriptor sd) {
        SerDeInfo hiveSerDe = sd.getSerdeInfo();
        if (hiveSerDe != null) {
            io.polycat.catalog.common.model.SerDeInfo serDeInfo = new io.polycat.catalog.common.model.SerDeInfo();
            serDeInfo.setName(hiveSerDe.getName());
            serDeInfo.setSerializationLibrary(hiveSerDe.getSerializationLib());
            serDeInfo.setParameters(hiveSerDe.getParameters());
            return serDeInfo;
        }
        return null;
    }

    private Column convertToTableInput(FieldSchema fieldSchema) {

        Column columnInput = new Column();
        columnInput.setColumnName(fieldSchema.getName());
        columnInput.setColType(fieldSchema.getType());
        return columnInput;
    }

    @Override
    public boolean dropTable(String catName, String dbName, String tableName)
            throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        DeleteTableRequest deleteTableRequest = new DeleteTableRequest();
        deleteTableRequest.setCatalogName(getCurrentCatalog());
        deleteTableRequest.setDatabaseName(dbName);
        deleteTableRequest.setTableName(tableName);
        deleteTableRequest.setProjectId(getProjectId());
        client.deleteTable(deleteTableRequest);
        return true;
    }

    @Override
    public Table getTable(String catName, String dbName, String tableName) throws MetaException {
        GetTableRequest getTableRequest = new GetTableRequest(getProjectId(),getCurrentCatalog(),dbName,tableName);
        io.polycat.catalog.common.model.Table lmsTable = null;
        try {
            lmsTable = client.getTable(getTableRequest);
        } catch (CatalogException e) {
            LOG.info("table:{}.{}.{} doesn't exist,{}",getCurrentCatalog(),dbName,tableName,e.getMessage());
        }

        return mTblToLmsTbl(lmsTable);
    }

    public Table mTblToLmsTbl(io.polycat.catalog.common.model.Table lmsTable) throws MetaException {
        return HiveDataAccessor.toTable(lmsTable);
    }

    private StorageFormatDescriptor getBySourceName(String name) {
        StorageFormatFactory formatFactory = new StorageFormatFactory();
        return formatFactory.get(lmsToHiveFileFormatMap.getOrDefault(name,name));
    }

    private String getSerde(String fileFormat) {
        StorageFormatDescriptor formatDescriptor = getBySourceName(fileFormat);
        if (fileFormat.equalsIgnoreCase("csv")) {
            return "org.apache.hadoop.hive.serde2.OpenCSVSerde";
        }
        if (formatDescriptor instanceof TextFileStorageFormatDescriptor) {
            return "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
        } else {
            return formatDescriptor.getSerde();
        }

    }

    private FieldSchema lmsSchemaToMsFieldSchema(Column schemaField) {
        FieldSchema fieldSchema = new FieldSchema();
        fieldSchema.setComment(schemaField.getComment());
        fieldSchema.setName(schemaField.getColumnName());
        fieldSchema.setType(lmsDataTypeTohmsDataType(schemaField.getColType().toLowerCase()));
        return fieldSchema;
    }

    private String lmsDataTypeTohmsDataType(String lmsDataType) {

        return convertTypeMap.getOrDefault(lmsDataType, lmsDataType);

    }

    @Override
    public boolean addPartition(Partition partition) throws InvalidObjectException, MetaException {
        return false;
    }

    @Override
    public boolean addPartitions(String s, String s1, String s2, List<Partition> list)
            throws InvalidObjectException, MetaException {
        return false;
    }

    @Override
    public boolean addPartitions(String s, String s1, String s2, PartitionSpecProxy partitionSpecProxy, boolean b)
            throws InvalidObjectException, MetaException {
        return false;
    }

    @Override
    public Partition getPartition(String s, String s1, String s2, List<String> list)
            throws MetaException, NoSuchObjectException {
        return new Partition();
    }

    @Override
    public boolean doesPartitionExist(String s, String s1, String s2, List<String> list)
            throws MetaException, NoSuchObjectException {
        return false;
    }

    @Override
    public boolean dropPartition(String s, String s1, String s2, List<String> list)
            throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        return false;
    }

    @Override
    public List<Partition> getPartitions(String s, String s1, String s2, int i)
            throws MetaException, NoSuchObjectException {
        return new ArrayList<>();
    }

    @Override
    public void alterTable(String s, String s1, String s2, Table table) throws InvalidObjectException, MetaException {

    }

    @Override
    public void updateCreationMetadata(String s, String s1, String s2, CreationMetadata creationMetadata)
            throws MetaException {

    }

    @Override
    public List<String> getTables(String catName, String dbName, String pattern) throws MetaException {
        return getTables(catName, dbName, pattern, null);
    }

    @Override
    public List<String> getTables(String catName, String dbName, String pattern, TableType tableType) throws MetaException {
        catName = normalizeIdentifier(getCurrentCatalog());
        ListTablesRequest listTablesRequest = new ListTablesRequest();
        listTablesRequest.setCatalogName(catName);
        listTablesRequest.setDatabaseName(dbName);
        listTablesRequest.setProjectId(getProjectId());
        listTablesRequest.setExpression(pattern);
        PagedList<io.polycat.catalog.common.model.Table> lmsTbls = null;
        try {
            lmsTbls = client.listTables(listTablesRequest);
        } catch (CatalogException e) {
            LOG.warn("getTables failed,catalog:{}.{}",catName,dbName,e);
        }
        return Arrays.stream(lmsTbls.getObjects())
                .map(io.polycat.catalog.common.model.Table::getTableName).collect(Collectors.toList());

    }

    @Override
    public List<String> getMaterializedViewsForRewriting(String s, String s1)
            throws MetaException, NoSuchObjectException {
        return new ArrayList<>();
    }

    @Override
    public List<TableMeta> getTableMeta(String s, String s1, String s2, List<String> list) throws MetaException {
        return new ArrayList<>();
    }

    @Override
    public List<Table> getTableObjectsByName(String s, String s1, List<String> list)
            throws MetaException, UnknownDBException {
        return new ArrayList<>();
    }

    @Override
    public List<String> getAllTables(String s, String s1) throws MetaException {
        return new ArrayList<>();
    }

    @Override
    public List<String> listTableNamesByFilter(String s, String s1, String s2, short i)
            throws MetaException, UnknownDBException {
        return new ArrayList<>();
    }

    @Override
    public List<String> listPartitionNames(String s, String s1, String s2, short i) throws MetaException {
        return new ArrayList<>();
    }

    @Override
    public PartitionValuesResponse listPartitionValues(String s, String s1, String s2, List<FieldSchema> list,
            boolean b, String s3, boolean b1, List<FieldSchema> list1, long l) throws MetaException {
        return new PartitionValuesResponse();
    }

    @Override
    public void alterPartition(String s, String s1, String s2, List<String> list, Partition partition)
            throws InvalidObjectException, MetaException {

    }

    @Override
    public void alterPartitions(String s, String s1, String s2, List<List<String>> list, List<Partition> list1)
            throws InvalidObjectException, MetaException {

    }

    @Override
    public List<Partition> getPartitionsByFilter(String s, String s1, String s2, String s3, short i)
            throws MetaException, NoSuchObjectException {
        return new ArrayList<>();
    }

    @Override
    public boolean getPartitionsByExpr(String s, String s1, String s2, byte[] bytes, String s3, short i,
            List<Partition> list) throws TException {
        return false;
    }

    @Override
    public int getNumPartitionsByFilter(String s, String s1, String s2, String s3)
            throws MetaException, NoSuchObjectException {
        return 0;
    }

    @Override
    public int getNumPartitionsByExpr(String s, String s1, String s2, byte[] bytes)
            throws MetaException, NoSuchObjectException {
        return 0;
    }

    @Override
    public List<Partition> getPartitionsByNames(String s, String s1, String s2, List<String> list)
            throws MetaException, NoSuchObjectException {
        return new ArrayList<>();
    }

    @Override
    public Table markPartitionForEvent(String s, String s1, String s2, Map<String, String> map,
            PartitionEventType partitionEventType)
            throws MetaException, UnknownTableException, InvalidPartitionException, UnknownPartitionException {
        return new Table();
    }

    @Override
    public boolean isPartitionMarkedForEvent(String s, String s1, String s2, Map<String, String> map,
            PartitionEventType partitionEventType)
            throws MetaException, UnknownTableException, InvalidPartitionException, UnknownPartitionException {
        return false;
    }

    @Override
    public boolean addRole(String s, String s1) throws InvalidObjectException, MetaException, NoSuchObjectException {
        return false;
    }

    @Override
    public boolean removeRole(String s) throws MetaException, NoSuchObjectException {
        return false;
    }

    @Override
    public boolean grantRole(Role role, String s, PrincipalType principalType, String s1, PrincipalType principalType1,
            boolean b) throws MetaException, NoSuchObjectException, InvalidObjectException {
        return false;
    }

    @Override
    public boolean revokeRole(Role role, String s, PrincipalType principalType, boolean b)
            throws MetaException, NoSuchObjectException {
        return false;
    }

    @Override
    public PrincipalPrivilegeSet getUserPrivilegeSet(String s, List<String> list)
            throws InvalidObjectException, MetaException {
        return new PrincipalPrivilegeSet();
    }

    @Override
    public PrincipalPrivilegeSet getDBPrivilegeSet(String s, String s1, String s2, List<String> list)
            throws InvalidObjectException, MetaException {
        return new PrincipalPrivilegeSet();
    }

    @Override
    public PrincipalPrivilegeSet getTablePrivilegeSet(String s, String s1, String s2, String s3, List<String> list)
            throws InvalidObjectException, MetaException {
        return new PrincipalPrivilegeSet();
    }

    @Override
    public PrincipalPrivilegeSet getPartitionPrivilegeSet(String s, String s1, String s2, String s3, String s4,
            List<String> list) throws InvalidObjectException, MetaException {
        return new PrincipalPrivilegeSet();
    }

    @Override
    public PrincipalPrivilegeSet getColumnPrivilegeSet(String s, String s1, String s2, String s3, String s4, String s5,
            List<String> list) throws InvalidObjectException, MetaException {
        return new PrincipalPrivilegeSet();
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalGlobalGrants(String s, PrincipalType principalType) {
        return new ArrayList<>();
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalDBGrants(String s, PrincipalType principalType, String s1,
            String s2) {
        return new ArrayList<>();
    }

    @Override
    public List<HiveObjectPrivilege> listAllTableGrants(String s, PrincipalType principalType, String s1, String s2,
            String s3) {
        return new ArrayList<>();
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalPartitionGrants(String s, PrincipalType principalType, String s1,
            String s2, String s3, List<String> list, String s4) {
        return new ArrayList<>();
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalTableColumnGrants(String s, PrincipalType principalType, String s1,
            String s2, String s3, String s4) {
        return new ArrayList<>();
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalPartitionColumnGrants(String s, PrincipalType principalType,
            String s1, String s2, String s3, List<String> list, String s4, String s5) {
        return new ArrayList<>();
    }

    @Override
    public boolean grantPrivileges(PrivilegeBag privilegeBag)
            throws InvalidObjectException, MetaException, NoSuchObjectException {
        return false;
    }

    @Override
    public boolean revokePrivileges(PrivilegeBag privilegeBag, boolean b)
            throws InvalidObjectException, MetaException, NoSuchObjectException {
        return false;
    }

    @Override
    public boolean refreshPrivileges(HiveObjectRef hiveObjectRef, String s, PrivilegeBag privilegeBag)
            throws InvalidObjectException, MetaException, NoSuchObjectException {
        return false;
    }

    @Override
    public Role getRole(String s) throws NoSuchObjectException {
        return new Role();
    }

    @Override
    public List<String> listRoleNames() {
        return new ArrayList<>();
    }

    @Override
    public List<Role> listRoles(String s, PrincipalType principalType) {
        return new ArrayList<>();
    }

    @Override
    public List<RolePrincipalGrant> listRolesWithGrants(String s, PrincipalType principalType) {
        return new ArrayList<>();
    }

    @Override
    public List<RolePrincipalGrant> listRoleMembers(String s) {
        return new ArrayList<>();
    }

    @Override
    public Partition getPartitionWithAuth(String s, String s1, String s2, List<String> list, String s3,
            List<String> list1) throws MetaException, NoSuchObjectException, InvalidObjectException {
        return new Partition();
    }

    @Override
    public List<Partition> getPartitionsWithAuth(String s, String s1, String s2, short i, String s3, List<String> list)
            throws MetaException, NoSuchObjectException, InvalidObjectException {
        return new ArrayList<>();
    }

    @Override
    public List<String> listPartitionNamesPs(String s, String s1, String s2, List<String> list, short i)
            throws MetaException, NoSuchObjectException {
        return new ArrayList<>();
    }

    @Override
    public List<Partition> listPartitionsPsWithAuth(String s, String s1, String s2, List<String> list, short i,
            String s3, List<String> list1) throws MetaException, InvalidObjectException, NoSuchObjectException {
        return new ArrayList<>();
    }

    @Override
    public boolean updateTableColumnStatistics(ColumnStatistics columnStatistics)
            throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
        return false;
    }

    @Override
    public boolean updatePartitionColumnStatistics(ColumnStatistics columnStatistics, List<String> list)
            throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
        return false;
    }

    @Override
    public ColumnStatistics getTableColumnStatistics(String s, String s1, String s2, List<String> list)
            throws MetaException, NoSuchObjectException {
        return new ColumnStatistics();
    }

    @Override
    public List<ColumnStatistics> getPartitionColumnStatistics(String s, String s1, String s2, List<String> list,
            List<String> list1) throws MetaException, NoSuchObjectException {
        return new ArrayList<>();
    }

    @Override
    public boolean deletePartitionColumnStatistics(String s, String s1, String s2, String s3, List<String> list,
            String s4) throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
        return false;
    }

    @Override
    public boolean deleteTableColumnStatistics(String s, String s1, String s2, String s3)
            throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
        return false;
    }

    @Override
    public long cleanupEvents() {
        return 0;
    }

    @Override
    public boolean addToken(String s, String s1) {
        return false;
    }

    @Override
    public boolean removeToken(String s) {
        return false;
    }

    @Override
    public String getToken(String s) {
        return "";
    }

    @Override
    public List<String> getAllTokenIdentifiers() {
        return new ArrayList<>();
    }

    @Override
    public int addMasterKey(String s) throws MetaException {
        return 0;
    }

    @Override
    public void updateMasterKey(Integer integer, String s) throws NoSuchObjectException, MetaException {

    }

    @Override
    public boolean removeMasterKey(Integer integer) {
        return false;
    }

    @Override
    public String[] getMasterKeys() {
        return new String[0];
    }

    @Override
    public void verifySchema() throws MetaException {

    }

    @Override
    public String getMetaStoreSchemaVersion() throws MetaException {
        return "";
    }

    @Override
    public void setMetaStoreSchemaVersion(String s, String s1) throws MetaException {

    }

    @Override
    public void dropPartitions(String s, String s1, String s2, List<String> list)
            throws MetaException, NoSuchObjectException {

    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalDBGrantsAll(String s, PrincipalType principalType) {
        return new ArrayList<>();
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalTableGrantsAll(String s, PrincipalType principalType) {
        return new ArrayList<>();
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalPartitionGrantsAll(String s, PrincipalType principalType) {
        return new ArrayList<>();
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalTableColumnGrantsAll(String s, PrincipalType principalType) {
        return new ArrayList<>();
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalPartitionColumnGrantsAll(String s, PrincipalType principalType) {
        return new ArrayList<>();
    }

    @Override
    public List<HiveObjectPrivilege> listGlobalGrantsAll() {
        return new ArrayList<>();
    }

    @Override
    public List<HiveObjectPrivilege> listDBGrantsAll(String s, String s1) {
        return new ArrayList<>();
    }

    @Override
    public List<HiveObjectPrivilege> listPartitionColumnGrantsAll(String s, String s1, String s2, String s3,
            String s4) {
        return new ArrayList<>();
    }

    @Override
    public List<HiveObjectPrivilege> listTableGrantsAll(String s, String s1, String s2) {
        return new ArrayList<>();
    }

    @Override
    public List<HiveObjectPrivilege> listPartitionGrantsAll(String s, String s1, String s2, String s3) {
        return new ArrayList<>();
    }

    @Override
    public List<HiveObjectPrivilege> listTableColumnGrantsAll(String s, String s1, String s2, String s3) {
        return new ArrayList<>();
    }

    @Override
    public void createFunction(Function function) throws InvalidObjectException, MetaException {

    }

    @Override
    public void alterFunction(String s, String s1, String s2, Function function)
            throws InvalidObjectException, MetaException {

    }

    @Override
    public void dropFunction(String s, String s1, String s2)
            throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {

    }

    @Override
    public Function getFunction(String s, String s1, String s2) throws MetaException {
        return new Function();
    }

    @Override
    public List<Function> getAllFunctions(String s) throws MetaException {
        return new ArrayList<>();
    }

    @Override
    public List<String> getFunctions(String s, String s1, String s2) throws MetaException {
        return new ArrayList<>();
    }

    @Override
    public AggrStats get_aggr_stats_for(String s, String s1, String s2, List<String> list, List<String> list1)
            throws MetaException, NoSuchObjectException {
        return new AggrStats();
    }

    @Override
    public List<ColStatsObjWithSourceInfo> getPartitionColStatsForDatabase(String s, String s1)
            throws MetaException, NoSuchObjectException {
        return new ArrayList<>();
    }

    @Override
    public NotificationEventResponse getNextNotification(NotificationEventRequest notificationEventRequest) {
        return new NotificationEventResponse();
    }

    @Override
    public void addNotificationEvent(NotificationEvent notificationEvent) {

    }

    @Override
    public void cleanNotificationEvents(int i) {

    }

    @Override
    public CurrentNotificationEventId getCurrentNotificationEventId() {
        return new CurrentNotificationEventId(new SecureRandom().nextInt());
    }

    @Override
    public NotificationEventsCountResponse getNotificationEventsCount(
            NotificationEventsCountRequest notificationEventsCountRequest) {
        return new NotificationEventsCountResponse();
    }

    @Override
    public void flushCache() {

    }

    @Override
    public ByteBuffer[] getFileMetadata(List<Long> list) throws MetaException {
        return new ByteBuffer[0];
    }

    @Override
    public void putFileMetadata(List<Long> list, List<ByteBuffer> list1, FileMetadataExprType fileMetadataExprType)
            throws MetaException {

    }

    @Override
    public boolean isFileMetadataSupported() {
        return false;
    }

    @Override
    public void getFileMetadataByExpr(List<Long> list, FileMetadataExprType fileMetadataExprType, byte[] bytes,
            ByteBuffer[] byteBuffers, ByteBuffer[] byteBuffers1, boolean[] booleans) throws MetaException {

    }

    @Override
    public FileMetadataHandler getFileMetadataHandler(FileMetadataExprType fileMetadataExprType) {
        return new FileMetadataHandler() {
            @Override
            public void getFileMetadataByExpr(List<Long> list, byte[] bytes, ByteBuffer[] byteBuffers, ByteBuffer[] byteBuffers1, boolean[] booleans) throws IOException {

            }

            @Override
            protected FileMetadataExprType getType() {
                return null;
            }
        };
    }

    @Override
    public int getTableCount() throws MetaException {
        return 0;
    }

    @Override
    public int getPartitionCount() throws MetaException {
        return 0;
    }

    @Override
    public int getDatabaseCount() throws MetaException {
        return 0;
    }

    @Override
    public List<SQLPrimaryKey> getPrimaryKeys(String s, String s1, String s2) throws MetaException {
        return new ArrayList<>();
    }

    @Override
    public List<SQLForeignKey> getForeignKeys(String s, String s1, String s2, String s3, String s4)
            throws MetaException {
        return new ArrayList<>();
    }

    @Override
    public List<SQLUniqueConstraint> getUniqueConstraints(String s, String s1, String s2) throws MetaException {
        return new ArrayList<>();
    }

    @Override
    public List<SQLNotNullConstraint> getNotNullConstraints(String s, String s1, String s2) throws MetaException {
        return new ArrayList<>();
    }

    @Override
    public List<SQLDefaultConstraint> getDefaultConstraints(String s, String s1, String s2) throws MetaException {
        return new ArrayList<>();
    }

    @Override
    public List<SQLCheckConstraint> getCheckConstraints(String s, String s1, String s2) throws MetaException {
        return new ArrayList<>();
    }

    @Override
    public List<String> createTableWithConstraints(Table table, List<SQLPrimaryKey> list, List<SQLForeignKey> list1,
            List<SQLUniqueConstraint> list2, List<SQLNotNullConstraint> list3, List<SQLDefaultConstraint> list4,
            List<SQLCheckConstraint> list5) throws InvalidObjectException, MetaException {
        return new ArrayList<>();
    }

    @Override
    public void dropConstraint(String s, String s1, String s2, String s3, boolean b) throws NoSuchObjectException {

    }

    @Override
    public List<String> addPrimaryKeys(List<SQLPrimaryKey> list) throws InvalidObjectException, MetaException {
        return new ArrayList<>();
    }

    @Override
    public List<String> addForeignKeys(List<SQLForeignKey> list) throws InvalidObjectException, MetaException {
        return new ArrayList<>();
    }

    @Override
    public List<String> addUniqueConstraints(List<SQLUniqueConstraint> list)
            throws InvalidObjectException, MetaException {
        return new ArrayList<>();
    }

    @Override
    public List<String> addNotNullConstraints(List<SQLNotNullConstraint> list)
            throws InvalidObjectException, MetaException {
        return new ArrayList<>();
    }

    @Override
    public List<String> addDefaultConstraints(List<SQLDefaultConstraint> list)
            throws InvalidObjectException, MetaException {
        return new ArrayList<>();
    }

    @Override
    public List<String> addCheckConstraints(List<SQLCheckConstraint> list)
            throws InvalidObjectException, MetaException {
        return new ArrayList<>();
    }

    @Override
    public String getMetastoreDbUuid() throws MetaException {
        return "";
    }

    @Override
    public void createResourcePlan(WMResourcePlan wmResourcePlan, String s, int i)
            throws AlreadyExistsException, MetaException, InvalidObjectException, NoSuchObjectException {

    }

    @Override
    public WMFullResourcePlan getResourcePlan(String s) throws NoSuchObjectException, MetaException {
        return new WMFullResourcePlan();
    }

    @Override
    public List<WMResourcePlan> getAllResourcePlans() throws MetaException {
        return new ArrayList<>();
    }

    @Override
    public WMFullResourcePlan alterResourcePlan(String s, WMNullableResourcePlan wmNullableResourcePlan, boolean b,
            boolean b1, boolean b2)
            throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException, MetaException {
        return new WMFullResourcePlan();
    }

    @Override
    public WMFullResourcePlan getActiveResourcePlan() throws MetaException {
        return new WMFullResourcePlan();
    }

    @Override
    public WMValidateResourcePlanResponse validateResourcePlan(String s)
            throws NoSuchObjectException, InvalidObjectException, MetaException {
        return new WMValidateResourcePlanResponse();
    }

    @Override
    public void dropResourcePlan(String s) throws NoSuchObjectException, MetaException {

    }

    @Override
    public void createWMTrigger(WMTrigger wmTrigger)
            throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException, MetaException {

    }

    @Override
    public void alterWMTrigger(WMTrigger wmTrigger)
            throws NoSuchObjectException, InvalidOperationException, MetaException {

    }

    @Override
    public void dropWMTrigger(String s, String s1)
            throws NoSuchObjectException, InvalidOperationException, MetaException {

    }

    @Override
    public List<WMTrigger> getTriggersForResourcePlan(String s) throws NoSuchObjectException, MetaException {
        return new ArrayList<>();
    }

    @Override
    public void createPool(WMPool wmPool)
            throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException, MetaException {

    }

    @Override
    public void alterPool(WMNullablePool wmNullablePool, String s)
            throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException, MetaException {

    }

    @Override
    public void dropWMPool(String s, String s1) throws NoSuchObjectException, InvalidOperationException, MetaException {

    }

    @Override
    public void createOrUpdateWMMapping(WMMapping wmMapping, boolean b)
            throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException, MetaException {

    }

    @Override
    public void dropWMMapping(WMMapping wmMapping)
            throws NoSuchObjectException, InvalidOperationException, MetaException {

    }

    @Override
    public void createWMTriggerToPoolMapping(String s, String s1, String s2)
            throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException, MetaException {

    }

    @Override
    public void dropWMTriggerToPoolMapping(String s, String s1, String s2)
            throws NoSuchObjectException, InvalidOperationException, MetaException {

    }

    @Override
    public void createISchema(ISchema iSchema) throws AlreadyExistsException, MetaException, NoSuchObjectException {

    }

    @Override
    public void alterISchema(ISchemaName iSchemaName, ISchema iSchema) throws NoSuchObjectException, MetaException {

    }

    @Override
    public ISchema getISchema(ISchemaName iSchemaName) throws MetaException {
        return new ISchema();
    }

    @Override
    public void dropISchema(ISchemaName iSchemaName) throws NoSuchObjectException, MetaException {

    }

    @Override
    public void addSchemaVersion(SchemaVersion schemaVersion)
            throws AlreadyExistsException, InvalidObjectException, NoSuchObjectException, MetaException {

    }

    @Override
    public void alterSchemaVersion(SchemaVersionDescriptor schemaVersionDescriptor, SchemaVersion schemaVersion)
            throws NoSuchObjectException, MetaException {

    }

    @Override
    public SchemaVersion getSchemaVersion(SchemaVersionDescriptor schemaVersionDescriptor) throws MetaException {
        return new SchemaVersion();
    }

    @Override
    public SchemaVersion getLatestSchemaVersion(ISchemaName iSchemaName) throws MetaException {
        return new SchemaVersion();
    }

    @Override
    public List<SchemaVersion> getAllSchemaVersion(ISchemaName iSchemaName) throws MetaException {
        return new ArrayList<>();
    }

    @Override
    public List<SchemaVersion> getSchemaVersionsByColumns(String s, String s1, String s2) throws MetaException {
        return new ArrayList<>();
    }

    @Override
    public void dropSchemaVersion(SchemaVersionDescriptor schemaVersionDescriptor)
            throws NoSuchObjectException, MetaException {

    }

    @Override
    public SerDeInfo getSerDeInfo(String s) throws NoSuchObjectException, MetaException {
        return new SerDeInfo();
    }

    @Override
    public void addSerde(SerDeInfo serDeInfo) throws AlreadyExistsException, MetaException {

    }

    @Override
    public void addRuntimeStat(RuntimeStat runtimeStat) throws MetaException {

    }

    @Override
    public List<RuntimeStat> getRuntimeStats(int i, int i1) throws MetaException {
        return new ArrayList<>();
    }

    @Override
    public int deleteRuntimeStats(int i) throws MetaException {
        return 0;
    }

    @Override
    public List<FullTableName> getTableNamesWithStats() throws MetaException, NoSuchObjectException {
        return new ArrayList<>();
    }

    @Override
    public List<FullTableName> getAllTableNamesForStats() throws MetaException, NoSuchObjectException {
        return new ArrayList<>();
    }

    @Override
    public Map<String, List<String>> getPartitionColsWithStats(String s, String s1, String s2)
            throws MetaException, NoSuchObjectException {
        return new HashMap<>();
    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
        initClientIfNeeded();
    }

    private void initClientIfNeeded() {
        if (client == null) {
            synchronized(this) {
                if (client == null) {
                    client = PolyCatClient.getInstance(conf);
                }
            }
        }
    }

    @Override
    public Configuration getConf() {
        return new Configuration();
    }

    private String getCurrentCatalog() {
        return MetastoreConf.getVar(conf, MetastoreConf.ConfVars.CATALOG_DEFAULT);
    }

    private String getProjectId() {
        return MetastoreConf.getVar(conf, MetastoreConf.ConfVars.valueOf(ConfVars.PROJECT_ID.name()));
    }

    private String getUserId() {
        return MetastoreConf.getVar(conf, MetastoreConf.ConfVars.valueOf(ConfVars.USER_ID.name()));
    }

    public CatalogPlugin getCatalogPlugin() {
        return client;
    }
}
