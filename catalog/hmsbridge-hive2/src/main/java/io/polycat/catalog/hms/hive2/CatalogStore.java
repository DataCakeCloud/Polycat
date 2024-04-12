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
package io.polycat.catalog.hms.hive2;

import io.polycat.catalog.client.PolyCatClient;
import io.polycat.catalog.common.plugin.request.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.polycat.catalog.common.Constants;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.model.Catalog;
import io.polycat.catalog.common.model.Column;
import io.polycat.catalog.common.model.Database;
import io.polycat.catalog.common.model.MetaObjectName;
import io.polycat.catalog.common.model.Order;
import io.polycat.catalog.common.model.PagedList;
import io.polycat.catalog.common.model.Partition;
import io.polycat.catalog.common.model.PartitionAlterContext;
import io.polycat.catalog.common.model.SkewedInfo;
import io.polycat.catalog.common.model.StorageDescriptor;
import io.polycat.catalog.common.model.base.PartitionInput;
import io.polycat.catalog.common.model.stats.PartitionStatisticData;
import io.polycat.catalog.common.plugin.CatalogPlugin;
import io.polycat.catalog.common.plugin.request.AddPartitionRequest;
import io.polycat.catalog.common.plugin.request.AlterColumnRequest;
import io.polycat.catalog.common.plugin.request.AlterDatabaseRequest;
import io.polycat.catalog.common.plugin.request.AlterPartitionRequest;
import io.polycat.catalog.common.plugin.request.AlterTableRequest;
import io.polycat.catalog.common.plugin.request.CreateCatalogRequest;
import io.polycat.catalog.common.plugin.request.CreateDatabaseRequest;
import io.polycat.catalog.common.plugin.request.CreateFunctionRequest;
import io.polycat.catalog.common.plugin.request.CreateTableRequest;
import io.polycat.catalog.common.plugin.request.DeleteColumnStatisticsRequest;
import io.polycat.catalog.common.plugin.request.DeleteDatabaseRequest;
import io.polycat.catalog.common.plugin.request.DeleteTableRequest;
import io.polycat.catalog.common.plugin.request.DoesPartitionExistsRequest;
import io.polycat.catalog.common.plugin.request.DropPartitionRequest;
import io.polycat.catalog.common.plugin.request.FunctionRequestBase;
import io.polycat.catalog.common.plugin.request.GetAllFunctionRequest;
import io.polycat.catalog.common.plugin.request.GetDatabaseRequest;
import io.polycat.catalog.common.plugin.request.GetFunctionRequest;
import io.polycat.catalog.common.plugin.request.GetObjectMapRequest;
import io.polycat.catalog.common.plugin.request.GetPartitionColumnStatisticsRequest;
import io.polycat.catalog.common.plugin.request.GetPartitionRequest;
import io.polycat.catalog.common.plugin.request.GetPartitionWithAuthRequest;
import io.polycat.catalog.common.plugin.request.GetTableColumnStatisticRequest;
import io.polycat.catalog.common.plugin.request.GetTablePartitionsByNamesRequest;
import io.polycat.catalog.common.plugin.request.input.*;
import io.polycat.catalog.common.plugin.request.ListCatalogsRequest;
import io.polycat.catalog.common.plugin.request.ListDatabasesRequest;
import io.polycat.catalog.common.plugin.request.ListFileRequest;
import io.polycat.catalog.common.plugin.request.ListFunctionRequest;
import io.polycat.catalog.common.plugin.request.ListPartitionByFilterRequest;
import io.polycat.catalog.common.plugin.request.ListPartitionNamesByFilterRequest;
import io.polycat.catalog.common.plugin.request.ListPartitionNamesPsRequest;
import io.polycat.catalog.common.plugin.request.ListPartitionsByExprRequest;
import io.polycat.catalog.common.plugin.request.ListPartitionsWithAuthRequest;
import io.polycat.catalog.common.plugin.request.ListTablePartitionsRequest;
import io.polycat.catalog.common.plugin.request.ListTablesRequest;
import io.polycat.catalog.common.plugin.request.UndropDatabaseRequest;
import io.polycat.catalog.common.plugin.request.UpdatePartitionColumnStatisticRequest;
import io.polycat.catalog.common.plugin.request.UpdateTableColumnStatisticRequest;
import io.polycat.catalog.common.plugin.request.input.AddPartitionInput;
import io.polycat.catalog.common.plugin.request.input.AlterPartitionInput;
import io.polycat.catalog.common.plugin.request.input.AlterTableInput;
import io.polycat.catalog.common.plugin.request.input.CatalogInput;
import io.polycat.catalog.common.plugin.request.input.ColumnChangeInput;
import io.polycat.catalog.common.plugin.request.input.ColumnStatisticsInput;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;
import io.polycat.catalog.common.plugin.request.input.DropPartitionInput;
import io.polycat.catalog.common.plugin.request.input.FileInput;
import io.polycat.catalog.common.plugin.request.input.FileStatsInput;
import io.polycat.catalog.common.plugin.request.input.FilterInput;
import io.polycat.catalog.common.plugin.request.input.FunctionInput;
import io.polycat.catalog.common.plugin.request.input.FunctionResourceUri;
import io.polycat.catalog.common.plugin.request.input.GetPartitionWithAuthInput;
import io.polycat.catalog.common.plugin.request.input.GetPartitionsByExprInput;
import io.polycat.catalog.common.plugin.request.input.GetPartitionsWithAuthInput;
import io.polycat.catalog.common.plugin.request.input.PartitionFilterInput;
import io.polycat.catalog.common.plugin.request.input.PartitionValuesInput;
import io.polycat.catalog.common.plugin.request.input.TableInput;
import io.polycat.catalog.common.plugin.request.input.TableTypeInput;
import io.polycat.catalog.common.utils.PartitionUtil;
import io.polycat.catalog.common.utils.TableUtil;
import io.polycat.hivesdk.hive2.tools.HiveDataAccessor;
import io.polycat.hivesdk.hive2.tools.LsmDataAccessor;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.FileMetadataHandler;
import org.apache.hadoop.hive.metastore.PartFilterExprUtil;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FileMetadataExprType;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.ql.io.StorageFormatDescriptor;
import org.apache.hadoop.hive.ql.io.StorageFormatFactory;
import org.apache.hadoop.hive.ql.io.TextFileStorageFormatDescriptor;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionExpressionForMetastore;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.util.Strings;
import org.apache.thrift.TException;
import org.eclipse.jetty.http.HttpStatus;

/**
 * A RawStore implementation that uses PolyCatClient
 */
public class CatalogStore implements RawStore {

    private static final Logger LOG = Logger.getLogger(CatalogStore.class);

    private final static String LMS_NAME = "lms_name";
    private static final HashMap<String, String> serializationLibToSourceNameMap = new HashMap() {
        {
            put("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", "parquet");
            put("org.apache.hadoop.hive.ql.io.orc.OrcSerde", "ORC");
            put("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", "textfile");
            put("org.apache.hadoop.hive.serde2.OpenCSVSerde", "csv");
            put("org.apache.carbondata.hive.CarbonHiveSerDe", "carbondata");
        }
    };
    private static final HashMap<String, String> lmsToHiveFileFormatMap = new HashMap() {
        {
            put("csv", "TEXTFILE");
        }
    };
    private static final HashMap<String, String> convertTypeMap = new HashMap() {
        {
            put("integer", "int");
        }
    };
    private Configuration conf;
    private CatalogPlugin client;

    public CatalogStore() {

    }

    public CatalogStore(Configuration conf) {
        this.conf = conf;
        initClientIfNeeded();
    }

    private static List<String> convertToValueList(String value) {
        if (value == null) {
            return Collections.emptyList();
        }
        String[] split = value.split("\\$");
        List<String> list = new LinkedList<>(Arrays.asList(split));
        list.remove(0);
        return list;
    }

    private static String convertToValueString(List<String> values) {
        if (values == null) {
            return Strings.EMPTY;
        }

        StringBuilder valStr = new StringBuilder();
        for (String cur : values) {
            valStr.append(cur.length()).append("$").append(cur);
        }

        return valStr.toString();
    }

    private static List<Order> fillOrderList(List<org.apache.hadoop.hive.metastore.api.Order> sortCols) {
        if (sortCols == null) {
            return null;
        }
        return sortCols.stream()
            .map(hiveSortCol -> new Order(hiveSortCol.getCol(), hiveSortCol.getOrder()))
            .collect(Collectors.toList());
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

    public List<String> getCatalogs() {
        ListCatalogsRequest request = new ListCatalogsRequest(getProjectId());
        PagedList<Catalog> response = client.listCatalogs(request);
        List<String> catalogs = new ArrayList<>();
        for (Catalog catalog : response.getObjects()) {
            catalogs.add(catalog.getCatalogName());
        }
        return catalogs;
    }

    public void createCatalog(String catalogName) {
        CreateCatalogRequest request = new CreateCatalogRequest();
        CatalogInput catalogInput = new CatalogInput();
        catalogInput.setCatalogName(catalogName);
        catalogInput.setOwner(getUserId());
        request.setInput(catalogInput);
        request.setProjectId(getProjectId());
        client.createCatalog(request);
    }

    public void createDatabase(String catalogName, String databaseName, String desc, String location) {
        CreateDatabaseRequest createDatabaseRequest = new CreateDatabaseRequest();
        createDatabaseRequest.setProjectId(getProjectId());
        createDatabaseRequest.setCatalogName(catalogName);
        DatabaseInput databaseInput = new DatabaseInput();
        databaseInput.setDatabaseName(databaseName);
        databaseInput.setDescription(desc);
        databaseInput.setLocationUri(location);
        databaseInput.setOwner(getUserId());
        databaseInput.setAccountId(getTenantName());
        createDatabaseRequest.setInput(databaseInput);
        client.createDatabase(createDatabaseRequest);
    }

    private String getDefaultDBLocation(org.apache.hadoop.hive.metastore.api.Database db) {
        final GetCatalogRequest getCatalogRequest = new GetCatalogRequest(getProjectId(),
            db.getParameters().get(LMS_NAME));
        final Catalog catalog = client.getCatalog(getCatalogRequest);
        return catalog.getLocation();
    }

    @Override
    public void createDatabase(org.apache.hadoop.hive.metastore.api.Database db)
        throws InvalidObjectException, MetaException {
        if (!db.getParameters().containsKey(LMS_NAME)) {
            return;
        }

        final String warehouseDir = getConf().get("hive.metastore.warehouse.dir");
        LOG.info("warehouseDir: {}, dbLocationUri: {}", warehouseDir, db.getLocationUri());
        if (warehouseDir == null || warehouseDir.equals(db.getLocationUri()) || StringUtils
            .isEmpty(db.getLocationUri())) {
            LOG.info("get real default location from catalog");
            db.setLocationUri(getDefaultDBLocation(db));
        }

        DatabaseInput dbInput = getDBInput(db);
        CreateDatabaseRequest createDatabaseRequest = new CreateDatabaseRequest();
        createDatabaseRequest.setInput(dbInput);
        createDatabaseRequest.setProjectId(getProjectId());
        createDatabaseRequest.setCatalogName(db.getParameters().get(LMS_NAME));

        try {
            client.createDatabase(createDatabaseRequest);
        } catch (CatalogException e) {
            LOG.error("database:{} create failed", db.getName(), e);
            throw new MetaException("database: " + db.getName() + " create failed");
        }
    }

    private DatabaseInput getDBInput(org.apache.hadoop.hive.metastore.api.Database db) {
        DatabaseInput dbInput = new DatabaseInput();
        dbInput.setCatalogName(db.getParameters().get(LMS_NAME));
        dbInput.setDatabaseName(db.getName());
        dbInput.setDescription(db.getDescription());
        dbInput.setLocationUri(db.getLocationUri());
        dbInput.setParameters(db.getParameters());
        dbInput.setOwner(getUserId(db.getOwnerName()));
        // TODO PrincipalType
        dbInput.setOwnerType(checkPrincipalType(db.getOwnerType()));
        dbInput.setAccountId(getTenantName());
        // ownerType
        // auth source type
        return dbInput;
    }

    private String checkPrincipalType(PrincipalType ownerType) {
        if (ownerType == null) {
            ownerType = PrincipalType.USER;
        }
        return ownerType.name();
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Database getDatabase(String name) throws NoSuchObjectException {
        return null;
    }

    public org.apache.hadoop.hive.metastore.api.Database getDatabase(String catName, String dbName)
        throws NoSuchObjectException {
        GetDatabaseRequest getDbRequest = new GetDatabaseRequest(getProjectId(), catName, dbName);
        Database lmsDatabase = null;
        try {
            lmsDatabase = client.getDatabase(getDbRequest);
        } catch (CatalogException e) {
            LOG.info("database:{}.{} doesn't exist,{}", catName, dbName, e.getMessage());
        }

        return lmsDbToHiveDb(lmsDatabase);
    }

    private org.apache.hadoop.hive.metastore.api.Database lmsDbToHiveDb(Database lmsDatabase) {
        if (lmsDatabase == null) {
            return null;
        }
        String dbName = lmsDatabase.getDatabaseName();
        String description = lmsDatabase.getDescription();
        String locationUri = lmsDatabase.getLocationUri();
        Map<String, String> parameters = lmsDatabase.getParameters();
        org.apache.hadoop.hive.metastore.api.Database database = new org.apache.hadoop.hive.metastore.api.Database(
            dbName, description, locationUri, parameters);
        database.setOwnerName(lmsDatabase.getOwner());
        database.setOwnerType(getOwnerType(lmsDatabase.getOwnerType()));
        return database;
    }

    private PrincipalType getOwnerType(String ownerType) {
        if (StringUtils.isNotEmpty(ownerType)) {
            try {
                return PrincipalType.valueOf(ownerType);
            } catch (Exception e) {
                LOG.warn("Unknown ownerType: {}", ownerType);
                return PrincipalType.USER;
            }
        }
        return PrincipalType.USER;
    }

    @Override
    public boolean dropDatabase(String dbname) throws MetaException {
        return false;
    }

    public boolean dropDatabase(String catName, String dbname) throws MetaException {
        DeleteDatabaseRequest request = new DeleteDatabaseRequest(getProjectId(), catName, dbname);
        client.deleteDatabase(request);
        return true;
    }

    public void undropDatabase(String catName, String dbName) {
        UndropDatabaseRequest undropDatabaseRequest = new UndropDatabaseRequest();
        undropDatabaseRequest.setProjectId(getProjectId());
        undropDatabaseRequest.setCatalogName(catName);
        undropDatabaseRequest.setDatabaseName(dbName);
        client.undropDatabase(undropDatabaseRequest);
    }

    @Override
    public boolean alterDatabase(String dbname, org.apache.hadoop.hive.metastore.api.Database db)
        throws NoSuchObjectException, MetaException {
        return false;
    }

    public boolean alterDatabase(String catName, String dbname, org.apache.hadoop.hive.metastore.api.Database db)
        throws NoSuchObjectException, MetaException {
        DatabaseInput dbInput = getDBInput(db);
        AlterDatabaseRequest request = new AlterDatabaseRequest(getProjectId(), catName, dbname, dbInput);
        client.alterDatabase(request);
        return true;
    }

    @Override
    public List<String> getDatabases(String pattern) throws MetaException {
        HashSet<String> dbNameSet = new HashSet<>();

        List<String> catNameList = getCatalogs();
        for (String catName : catNameList) {
            ListDatabasesRequest request = new ListDatabasesRequest(getProjectId(), catName);
            request.setFilter(pattern);
            PagedList<Database> response = client.listDatabases(request);
            for (Database db : response.getObjects()) {
                dbNameSet.add(db.getDatabaseName());
            }
        }
        return new ArrayList<>(dbNameSet);
    }

    public List<String> getDatabases(String pattern, String catalogName) throws MetaException {
        List<String> databases = new ArrayList<>();
        ListDatabasesRequest request = new ListDatabasesRequest(getProjectId(), catalogName);
        request.setFilter(pattern);
        PagedList<Database> response = client.listDatabases(request);
        for (Database db : response.getObjects()) {
            databases.add(db.getDatabaseName());
        }
        return databases;
    }

    @Override
    public List<String> getAllDatabases() throws MetaException {
        HashSet<String> dbNameSet = new HashSet<>();

        List<String> catNameList = getCatalogs();
        for (String catName : catNameList) {
            ListDatabasesRequest request = new ListDatabasesRequest(getProjectId(), catName);
            PagedList<Database> response = client.listDatabases(request);
            for (Database db : response.getObjects()) {
                dbNameSet.add(db.getDatabaseName());
            }
        }
        return new ArrayList<>(dbNameSet);
    }

    public List<String> getAllDatabases(String catalogName) throws MetaException {
        List<String> databases = new ArrayList<>();
        ListDatabasesRequest request = new ListDatabasesRequest(getProjectId(), catalogName);
        PagedList<Database> response = client.listDatabases(request);
        for (Database db : response.getObjects()) {
            databases.add(db.getDatabaseName());
        }
        return databases;
    }

    @Override
    public boolean createType(Type type) {
        return false;
    }

    @Override
    public Type getType(String typeName) {
        return null;
    }

    @Override
    public boolean dropType(String typeName) {
        return false;
    }

    @Override
    public void createTable(Table table) throws InvalidObjectException, MetaException {
        TableInput tableInput = getTableInput(table);
        CreateTableRequest createTableRequest = new CreateTableRequest();
        createTableRequest.setInput(tableInput);
        createTableRequest.setCatalogName(getCatName(table));
        createTableRequest.setDatabaseName(table.getDbName());
        createTableRequest.setProjectId(getProjectId());

        try {
            client.createTable(createTableRequest);
        } catch (CatalogException e) {
            LOG.error("table:{} create failed", table.getTableName(), e);
            throw new MetaException(e.getMessage());
        }
    }

    public void createTable(String catalogName, Table table) {
        TableInput tableInput = getTableInput(table);
        CreateTableRequest createTableRequest = new CreateTableRequest();
        createTableRequest.setInput(tableInput);
        createTableRequest.setCatalogName(catalogName);
        createTableRequest.setDatabaseName(table.getDbName());
        createTableRequest.setProjectId(getProjectId());
        client.createTable(createTableRequest);
    }

    private Column convertToTableInput(FieldSchema fieldSchema) {
        Column columnInput = new Column();
        columnInput.setColumnName(fieldSchema.getName());
        columnInput.setColType(fieldSchema.getType());
        columnInput.setComment(fieldSchema.getComment());
        return columnInput;
    }

    private Column convertToTableInput(Schema fieldSchema) {
        Column columnInput = new Column();
        columnInput.setColumnName(fieldSchema.getName());
        columnInput.setColType(fieldSchema.getType());
        String comment = ((JSONObject) fieldSchema.getMetadata()).getString("comment");
        columnInput.setComment(comment);
        return columnInput;
    }

    @Override
    public boolean dropTable(String dbName, String tableName)
        throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        DeleteTableRequest deleteTableRequest = new DeleteTableRequest();
        deleteTableRequest.setCatalogName(getCurrentCatalog());
        deleteTableRequest.setDatabaseName(dbName);
        deleteTableRequest.setTableName(tableName);
        deleteTableRequest.setProjectId(getProjectId());
        client.deleteTable(deleteTableRequest);
        return true;
    }

    public boolean dropTable(String catName, String dbName, String tableName)
        throws MetaException, InvalidObjectException {
        DeleteTableRequest deleteTableRequest = new DeleteTableRequest();
        deleteTableRequest.setCatalogName(catName);
        deleteTableRequest.setDatabaseName(dbName);
        deleteTableRequest.setTableName(tableName);
        deleteTableRequest.setProjectId(getProjectId());
        deleteTableRequest.setPurgeFlag(false);
        client.deleteTable(deleteTableRequest);
        return true;
    }

    @Override
    public Table getTable(String dbName, String tableName) throws MetaException {
        GetTableRequest getTableRequest = new GetTableRequest(getProjectId(), getCurrentCatalog(), dbName, tableName
        );
        io.polycat.catalog.common.model.Table lmsTable = null;
        try {
            lmsTable = client.getTable(getTableRequest);
        } catch (CatalogException e) {
            LOG.info("table:{}.{}.{} doesn't exist,{}", getCurrentCatalog(), dbName, tableName, e.getMessage());
        }

        return mTblTodTbl(lmsTable);
    }

    public Table getTable(String catName, String dbName, String tableName) throws MetaException {
        GetTableRequest getTableRequest = new GetTableRequest(getProjectId(), catName, dbName, tableName);
        io.polycat.catalog.common.model.Table lmsTable = null;
        try {
            lmsTable = client.getTable(getTableRequest);
        } catch (CatalogException e) {
            LOG.info("table:{}.{}.{} doesn't exist,{}", catName, dbName, tableName, e.getMessage());
        }

        return mTblTodTbl(lmsTable);
    }

    private Table mTblTodTbl(io.polycat.catalog.common.model.Table lmsTable) throws MetaException {
        if (lmsTable == null) {
            return null;
        }
        Table table = new Table();
        table.setDbName(lmsTable.getDatabaseName());
        table.setTableName(lmsTable.getTableName());
        table.setParameters(lmsTable.getParameters());
        org.apache.hadoop.hive.metastore.api.StorageDescriptor storageDescriptor = new org.apache.hadoop.hive.metastore.api.StorageDescriptor();
        StorageDescriptor lmsTableStorageDescriptor = lmsTable.getStorageDescriptor();
        storageDescriptor.setParameters(lmsTableStorageDescriptor.getParameters());
        List<FieldSchema> columns = lmsTableStorageDescriptor.getColumns().stream()
            .map(this::lmsSchemaToMsFieldSchema).collect(Collectors.toList());
        List<FieldSchema> partitions = lmsTable.getPartitionKeys().stream()
            .map(this::lmsSchemaToMsFieldSchema).collect(Collectors.toList());
        for (FieldSchema fieldSchema : partitions) {
            columns.remove(fieldSchema);
        }
        storageDescriptor.setCols(columns);
        storageDescriptor.setColsIsSet(true);
        String sourceShortName = lmsTableStorageDescriptor.getSourceShortName();
        storageDescriptor.setInputFormat(lmsTableStorageDescriptor.getInputFormat());
        storageDescriptor.setOutputFormat(lmsTableStorageDescriptor.getOutputFormat());

        storageDescriptor.setLocation(lmsTableStorageDescriptor.getLocation());
        storageDescriptor.setNumBuckets(lmsTableStorageDescriptor.getNumberOfBuckets());
        storageDescriptor.setCompressed(lmsTableStorageDescriptor.getCompressed());
        storageDescriptor.setCompressedIsSet(lmsTableStorageDescriptor.getCompressed());
        storageDescriptor.setSortCols(convertToSortOrder(lmsTableStorageDescriptor.getSortColumns()));
        storageDescriptor.setSerdeInfo(convertToSerDeInfo(lmsTableStorageDescriptor));
        storageDescriptor.setBucketCols(lmsTableStorageDescriptor.getBucketColumns());
        storageDescriptor.setSkewedInfo(convertToSkewedInfo(lmsTableStorageDescriptor.getSkewedInfo()));
        table.setSd(storageDescriptor);
        table.setTableType(lmsTable.getTableType());

        // TODO
        if (lmsTable.getPartitionKeys() != null) {
            table.setPartitionKeys(
                lmsTable.getPartitionKeys().stream()
                    .map(this::lmsSchemaToMsFieldSchema).collect(Collectors.toList()));
        }
        table.setOwner(lmsTable.getOwner());
        table.setViewExpandedText(lmsTable.getViewExpandedText());
        table.setViewOriginalText(lmsTable.getViewOriginalText());

        return table;
    }

    private List<org.apache.hadoop.hive.metastore.api.Order> convertToSortOrder(List<Order> sortColumns) {
        if (sortColumns == null) {
            return Collections.emptyList();
        }
        return sortColumns.stream().map(x -> {
            return new org.apache.hadoop.hive.metastore.api.Order(x.getColumn(), x.getSortOrder());
        }).collect(Collectors.toList());
    }

    private SerDeInfo convertToSerDeInfo(StorageDescriptor lmsSd) {
        io.polycat.catalog.common.model.SerDeInfo lmsSerDe = lmsSd.getSerdeInfo();
        SerDeInfo hiveSerDe = new SerDeInfo();
        if (lmsSerDe != null) {
            hiveSerDe.setName(lmsSerDe.getName());
            hiveSerDe.setParameters(lmsSerDe.getParameters());
            hiveSerDe.setSerializationLib(lmsSerDe.getSerializationLibrary());
        }

        if (hiveSerDe.getSerializationLib() == null || hiveSerDe.getSerializationLib().isEmpty()) {
            hiveSerDe.setSerializationLib(getSerde(lmsSd.getSourceShortName()));
        }
        return hiveSerDe;
    }

    private FieldSchema lmsSchemaToMsFieldSchema(Column schemaField) {
        FieldSchema fieldSchema = new FieldSchema();
        fieldSchema.setComment(schemaField.getComment());
        fieldSchema.setName(schemaField.getColumnName());
        fieldSchema.setType(lmsDataTypeTohmsDataType(schemaField.getColType().toLowerCase()));
        return fieldSchema;
    }

    private StorageFormatDescriptor getBySourceName(String name) {
        StorageFormatFactory formatFactory = new StorageFormatFactory();
        return formatFactory.get(lmsToHiveFileFormatMap.getOrDefault(name, name));
    }

    private String lmsDataTypeTohmsDataType(String lmsDataType) {

        return convertTypeMap.getOrDefault(lmsDataType, lmsDataType);

    }

    private String getSerde(String fileFormat) {
        if (fileFormat.equalsIgnoreCase("csv")) {
            return "org.apache.hadoop.hive.serde2.OpenCSVSerde";
        }
        if (fileFormat.equalsIgnoreCase("parquet")) {
            return "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
        }
        if (fileFormat.equalsIgnoreCase("orc")) {
            return "org.apache.hadoop.hive.ql.io.orc.OrcSerde";
        }

        if ("org.openx.data.jsonserde.JsonSerDe".equalsIgnoreCase(fileFormat) ||
            "org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe".equalsIgnoreCase(fileFormat) ||
            "org.apache.hadoop.hive.serde2.OpenCSVSerde".equalsIgnoreCase(fileFormat)) {
            return fileFormat;
        }

        StorageFormatDescriptor formatDescriptor = getBySourceName(fileFormat);
        if (formatDescriptor == null) {
            return fileFormat;
        }
        if (formatDescriptor instanceof TextFileStorageFormatDescriptor) {
            return "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
        } else {
            return "";
        }

    }

    @Override
    public boolean addPartition(org.apache.hadoop.hive.metastore.api.Partition part)
            throws InvalidObjectException, MetaException {
        return false;
    }

    private String makePartitionName(Table table, List<String> partValues) {
        List<String> partitionNames = table.getPartitionKeys().stream().map(x -> x.getName())
            .collect(Collectors.toList());

        StringBuilder partitionFolder = new StringBuilder();
        for (int i = 0; i < partitionNames.size(); i++) {
            partitionFolder.append(partitionNames.get(i))
                .append("=")
                .append(FileUtils.escapePathName(partValues.get(i)))
                .append("/");
        }
        return partitionFolder.substring(0, partitionFolder.length() - 1);
    }

    private String makePartitionNameWithoutEscape(Table table, List<String> partValues) {
        List<String> partitionNames = table.getPartitionKeys().stream().map(x -> x.getName())
                .collect(Collectors.toList());

        StringBuilder partitionFolder = new StringBuilder();
        for (int i = 0; i < partitionNames.size(); i++) {
            partitionFolder.append(partitionNames.get(i))
                    .append("=")
                    .append(partValues.get(i))
                    .append("/");
        }
        return partitionFolder.substring(0, partitionFolder.length() - 1);
    }

    private void setPartitionFiles(PartitionInput partitionBase, String location) {
        if (partitionBase.getStorageDescriptor() == null) {
            partitionBase.setStorageDescriptor(new StorageDescriptor());
        }
        partitionBase.getStorageDescriptor().setLocation(location);
        partitionBase.setFiles(new FileInput[]{});
        partitionBase.setIndex(new FileStatsInput[]{});
    }

    public void addPartition(String caName, String dbName, String tblName,
            org.apache.hadoop.hive.metastore.api.Partition part) throws MetaException {
        Table table = getTable(caName, dbName, tblName);
        PartitionInput partitionBase = buildPartitionBaseInput(table, part);
        String partName = makePartitionName(table, part.getValues());
        partitionBase.setPartitionValues(part.getValues());
        if (part.getCreateTime() <= 0) {
            partitionBase.setCreateTime(System.currentTimeMillis() / 1000);
        } else {
            partitionBase.setCreateTime((long) part.getCreateTime());
        }

        String partLocation = null;
        if (part.getSd() != null && !StringUtils.isBlank(part.getSd().getLocation())) {
            partLocation = part.getSd().getLocation();
        } else {
            if (!isViewTableType(table)) {
                partLocation = table.getSd().getLocation() + File.separator + partName;
            }
        }
        setPartitionFiles(partitionBase, partLocation);
        AddPartitionInput partitionInput = new AddPartitionInput();
        partitionInput.setPartitions(new PartitionInput[]{partitionBase});
        AddPartitionRequest request = new AddPartitionRequest(getProjectId(), getCatName(table),
            table.getDbName(), table.getTableName(), partitionInput);
        client.addPartition(request);
    }

    private boolean isViewTableType(Table table) {
        return TableType.VIRTUAL_VIEW.name().equals(table.getTableType());
    }

    public void addPartitions(String catalogName, String dbName, String tableName,
            Table table, List<org.apache.hadoop.hive.metastore.api.Partition> partitions) {

        List<PartitionInput> partitionBaseList = new ArrayList<>();
        for (org.apache.hadoop.hive.metastore.api.Partition part : partitions) {
            PartitionInput partitionBase = buildPartitionBaseInput(table, part);
            String partName = makePartitionName(table, part.getValues());
            partitionBase.setPartitionValues(part.getValues());

            String partLocation;
            if (part.getSd().getLocation() != null && !StringUtils.isBlank(part.getSd().getLocation())) {
                partLocation = part.getSd().getLocation();
            } else {
                partLocation = table.getSd().getLocation() + File.separator + partName;
            }
            partitionBase.setFiles(new FileInput[]{});
            partitionBase.setIndex(new FileStatsInput[]{});
            StorageDescriptor sd = partitionBase.getStorageDescriptor();
            sd.setLocation(partLocation);
            sd.setInputFormat(part.getSd().getInputFormat());
            sd.setOutputFormat(part.getSd().getOutputFormat());
            sd.setSerdeInfo(fillSerDeInfo(part.getSd().getSerdeInfo()));
            partitionBaseList.add(partitionBase);
        }
        AddPartitionInput partitionInput = new AddPartitionInput();
        partitionInput.setPartitions(partitionBaseList.toArray(new PartitionInput[partitionBaseList.size()]));
        AddPartitionRequest request = new AddPartitionRequest(getProjectId(), catalogName, dbName, tableName,
            partitionInput);
        client.addPartition(request);
    }

    public void addPartitions(String catalogName, String dbName, String tableName, Table table,
            List<org.apache.hadoop.hive.metastore.api.Partition> partitions, boolean ifNotExists) throws MetaException {
        List<String> parts = partitions.stream().flatMap(partition -> partition.getValues().stream())
            .collect(Collectors.toList());
        org.apache.hadoop.hive.metastore.api.Partition partition = null;
        try {
            partition = getPartition(catalogName, dbName, tableName, parts);
            throw new MetaException("Partition already exists: " + partition);
        } catch (NoSuchObjectException e) {
            if (ifNotExists) {
                return;
            }
        }
        addPartitions(catalogName, dbName, tableName, table, partitions);
    }

    @Override
    public boolean addPartitions(String dbName, String tblName,
            List<org.apache.hadoop.hive.metastore.api.Partition> parts)
            throws InvalidObjectException, MetaException {
        return false;
    }

    @Override
    public boolean addPartitions(String dbName, String tblName, PartitionSpecProxy partitionSpec,
        boolean ifNotExists) throws InvalidObjectException, MetaException {
        return false;
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition getPartition(String dbName, String tableName, List<String> part_vals)
            throws MetaException, NoSuchObjectException {
        return null;
    }

    public org.apache.hadoop.hive.metastore.api.Partition getPartition(String catalogName, String dbName,
            String tableName, List<String> part_vals)
            throws MetaException, NoSuchObjectException {
        Table table = getTable(catalogName, dbName, tableName);
        Partition lmsPartition = getLmsPartition(catalogName, dbName, tableName, part_vals, table);
        if (lmsPartition == null) {
            throw new NoSuchObjectException("partition values=" + part_vals);
        }
        return convertToPartition(lmsPartition, dbName, tableName, table.getSd());
    }

    private Partition getLmsPartition(String catalogName, String dbName, String tableName, List<String> partVals,
        Table table) throws MetaException {
        String partitionName = makePartitionName(table, partVals);
        String partitionNameWithoutEscape = makePartitionNameWithoutEscape(table, partVals);
        GetPartitionRequest request = new GetPartitionRequest(getProjectId(), catalogName, dbName, tableName,
            partitionName);
        final Partition partition = client.getPartition(request);
        if (partition == null && !partitionName.equals(partitionNameWithoutEscape)) {
            request = new GetPartitionRequest(getProjectId(), catalogName, dbName, tableName,
                partitionNameWithoutEscape);
            return client.getPartition(request);
        }
        return partition;
    }

    @Override
    public boolean doesPartitionExist(String dbName, String tableName, List<String> part_vals)
        throws MetaException, NoSuchObjectException {
        return true;
    }

    public boolean doesPartitionExist(String catName, String dbName, String tableName, List<String> partVals)
        throws MetaException, NoSuchObjectException {
        try {
            final PartitionValuesInput partitionValuesInput = new PartitionValuesInput();
            partitionValuesInput.setPartitionValues(partVals);
            final DoesPartitionExistsRequest doesPartitionExistsRequest = new DoesPartitionExistsRequest(getProjectId(),
                    catName, dbName, tableName, partitionValuesInput);
            return client.doesPartitionExist(doesPartitionExistsRequest);
        } catch (Exception e) {
            return false;
        }
    }

    private FileStatus[] listDataFiles(String location, String partitionFolder) {
        try {
            String partitionLocation = location + File.separator + partitionFolder;
            Path path = new Path(partitionLocation);
            FileSystem fileSystem = path.getFileSystem(conf == null ? new Configuration() : conf);
            return fileSystem.listStatus(path);
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            throw new CatalogException(e);
        }
    }

    private PartitionInput buildPartitionBaseInput(Table table, org.apache.hadoop.hive.metastore.api.Partition part) {
        PartitionInput partitionBase = new PartitionInput();
        org.apache.hadoop.hive.metastore.api.StorageDescriptor sd = table.getSd().deepCopy();
        StorageDescriptor lmsPartSd = new StorageDescriptor();
        org.apache.hadoop.hive.metastore.api.StorageDescriptor partSd = part.getSd();
        if (partSd != null) {
            if (partSd.getParameters() != null && sd.getParameters() != null) {
                sd.getParameters().putAll(partSd.getParameters());
                partSd.setParameters(sd.getParameters());
            }
            if (partSd.getCols() != null) {
                lmsPartSd.setColumns(
                        partSd.getCols().stream().map(this::convertToColumnInput).collect(Collectors.toList()));
            }
            if (partSd.getSerdeInfo() != null) {
                lmsPartSd.setSerdeInfo(convertToLmsSerDeInfo(partSd.getSerdeInfo()));
            }
            if (partSd.getSkewedInfo() != null) {
                lmsPartSd.setSkewedInfo(convertToLmsSkewedInfo(partSd.getSkewedInfo()));
            }
        }
        if (lmsPartSd.getParameters() == null) {
            lmsPartSd.setParameters(sd.getParameters());
        }
        if (lmsPartSd.getColumns() == null && sd.getCols() != null) {
            lmsPartSd.setColumns(sd.getCols().stream().map(this::convertToColumnInput).collect(Collectors.toList()));
        }
        if (lmsPartSd.getSerdeInfo() == null) {
            lmsPartSd.setSerdeInfo(convertToLmsSerDeInfo(sd.getSerdeInfo()));
        }
        if (lmsPartSd.getSkewedInfo() == null) {
            lmsPartSd.setSkewedInfo(convertToLmsSkewedInfo(sd.getSkewedInfo()));
        }
        partitionBase.setParameters(part.getParameters());
        partitionBase.setStorageDescriptor(lmsPartSd);
        return partitionBase;
    }

    private org.apache.hadoop.hive.metastore.api.SkewedInfo convertToSkewedInfo(SkewedInfo lmsSkewedInfo) {
        org.apache.hadoop.hive.metastore.api.SkewedInfo hiveSkewedInfo = new org.apache.hadoop.hive.metastore.api.SkewedInfo();
        if (lmsSkewedInfo == null) {
            return hiveSkewedInfo;
        }
        hiveSkewedInfo.setSkewedColNames(lmsSkewedInfo.getSkewedColumnNames());
        hiveSkewedInfo.setSkewedColValues(lmsSkewedInfo.getSkewedColumnValues());
        Map<List<String>, String> skewedMaps = new HashMap<>(lmsSkewedInfo.getSkewedColumnValueLocationMaps().size());
        Map<String, String> lmsSkewedMaps = lmsSkewedInfo.getSkewedColumnValueLocationMaps();
        for (String values : lmsSkewedMaps.keySet()) {
            skewedMaps.put(convertToValueList(values), lmsSkewedMaps.get(values));
        }
        hiveSkewedInfo.setSkewedColValueLocationMaps(skewedMaps);
        return hiveSkewedInfo;
    }

    private SkewedInfo convertToLmsSkewedInfo(org.apache.hadoop.hive.metastore.api.SkewedInfo hiveSkewedInfo) {
        if (hiveSkewedInfo == null) {
            return null;
        }
        SkewedInfo lmsSkewedInfo = new SkewedInfo();
        lmsSkewedInfo.setSkewedColumnNames(hiveSkewedInfo.getSkewedColNames());
        lmsSkewedInfo.setSkewedColumnValues(hiveSkewedInfo.getSkewedColValues());
        Map<String, String> skewedMaps = new HashMap<>(hiveSkewedInfo.getSkewedColValueLocationMapsSize());
        Map<List<String>, String> hiveSkewedMaps = hiveSkewedInfo.getSkewedColValueLocationMaps();
        for (List<String> values : hiveSkewedMaps.keySet()) {
            skewedMaps.put(convertToValueString(values), hiveSkewedMaps.get(values));
        }
        lmsSkewedInfo.setSkewedColumnValueLocationMaps(skewedMaps);
        return lmsSkewedInfo;
    }

    private io.polycat.catalog.common.model.SerDeInfo convertToLmsSerDeInfo(SerDeInfo serdeInfo) {
        io.polycat.catalog.common.model.SerDeInfo lmsSerDeInfo = new io.polycat.catalog.common.model.SerDeInfo();
        if (serdeInfo != null) {
            lmsSerDeInfo.setName(serdeInfo.getName());
            lmsSerDeInfo.setParameters(serdeInfo.getParameters());
            lmsSerDeInfo.setSerializationLibrary(serdeInfo.getSerializationLib());
        }
        return lmsSerDeInfo;
    }

    boolean checkIsMapNotExistException(CatalogException e) {
        return e.getStatusCode() == HttpStatus.NOT_FOUND_404;
    }

    public MetaObjectName getObjectFromNameMap(String dbName) {
        return getObjectFromNameMap(ObjectType.DATABASE.getNum(), dbName, null);
    }

    public MetaObjectName getObjectFromNameMap(String dbName, String tableName) {
        return getObjectFromNameMap(ObjectType.TABLE.getNum(), dbName, tableName);
    }

    public MetaObjectName getObjectFromNameMap(int objectType, String dbName, String tableName) {
        GetObjectMapRequest request = new GetObjectMapRequest();
        request.setProjectId(getProjectId());
        request.setObjectType(objectType);
        request.setDatabaseName(dbName);
        request.setObjectName(tableName);
        MetaObjectName objectName;
        try {
            objectName = client.getObjectFromNameMap(request);
        } catch (CatalogException e) {
            if (checkIsMapNotExistException(e)) {
                return null;
            }
            throw e;
        }
        return objectName;
    }

    @Override
    public boolean dropPartition(String dbName, String tableName, List<String> part_vals)
        throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        return false;
    }

    public boolean dropPartition(String caName, String dbName, String tblName, List<String> part_vals)
        throws MetaException {
        Table table = getTable(caName, dbName, tblName);
        if (table.getPartitionKeys().size() != part_vals.size() || table.getPartitionKeys().size() == 0) {
            throw new MetaException(
                "The number of input partition column values is not equal to the number of partition table columns");
        }
        List<String> partNames = new ArrayList<>();
        List<String> partitionKeys = table.getPartitionKeys().stream()
            .map(partitionKey -> partitionKey.getName()).collect(Collectors.toList());
        partNames.add(PartitionUtil.makePartitionName(partitionKeys, part_vals));
        DropPartitionInput dropPartitionInput = new DropPartitionInput();
        dropPartitionInput.setPartitionNames(partNames);
        DropPartitionRequest request = new DropPartitionRequest(getProjectId(), caName,
            dbName, tblName, dropPartitionInput);
        try {
            client.dropPartition(request);
        } catch (CatalogException e) {
            LOG.warn(e.getMessage() + "\n try to make deprecated partition names");
            final List<String> deprecatedPartVals = part_vals.stream()
                .map(p -> PartitionUtil.escapePathName(FileUtils.unescapePathName(p)))
                .collect(Collectors.toList());
            final ArrayList<String> deprecatedPartNames = new ArrayList<>();
            deprecatedPartNames.add(PartitionUtil.makePartitionName(partitionKeys, deprecatedPartVals));
            dropPartitionInput.setPartitionNames(deprecatedPartNames);
            client.dropPartition(request);
        }
        return true;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> getPartitions(String dbName, String tableName, int max)
        throws MetaException, NoSuchObjectException {
        Table table = getTable(dbName, tableName);
        ListFileRequest listFileRequest = new ListFileRequest(getProjectId(), getCurrentCatalog(), dbName, tableName,
                getFilterInput(max));
        List<Partition> partitions = client.listPartitions(listFileRequest);
        return getHivePartitions(partitions, dbName, tableName, table.getSd());

    }

    private FilterInput getFilterInput(Integer max) {
        FilterInput filterInput = new FilterInput();
        if (max != null) {
            filterInput.setLimit(max);
        }
        return filterInput;
    }

    public List<org.apache.hadoop.hive.metastore.api.Partition> getPartitions(String catName, String dbName,
            String tableName, int max)
            throws MetaException {
        Table table = getTable(catName, dbName, tableName);
        ListFileRequest listFileRequest = new ListFileRequest(getProjectId(), catName, dbName, tableName,
                getFilterInput(max));
        List<Partition> partitions = client.listPartitions(listFileRequest);
        return getHivePartitions(partitions, dbName, tableName, table.getSd());
    }

    public List<String> getPartNames(String catName, String dbName, String tableName, int max)
            throws MetaException, NoSuchObjectException {
        ListFileRequest listFileRequest = new ListFileRequest(getProjectId(), catName, dbName, tableName,
                getFilterInput(max));
        List<Partition> partitions = client.listPartitions(listFileRequest);
        List<String> partitionKeys = PartitionUtil.getPartitionKeysFromTable(
            client.getTable(new GetTableRequest(getProjectId(), catName, dbName, tableName)));
        return partitions.stream()
            .map(Partition::getPartitionValues)
            .map(values -> PartitionUtil.makePartitionName(partitionKeys, values))
            .collect(Collectors.toList());
    }

    private org.apache.hadoop.hive.metastore.api.Partition convertToPartition(Partition partition, String dbName,
            String tableName,
            org.apache.hadoop.hive.metastore.api.StorageDescriptor sd) {
        return new org.apache.hadoop.hive.metastore.api.Partition(partition.getPartitionValues(), dbName, tableName,
            partition.getCreateTime().intValue(), 0,
            convertToStorageDescriptor(partition.getStorageDescriptor(), sd), partition.getParameters());
    }

    private org.apache.hadoop.hive.metastore.api.StorageDescriptor convertToStorageDescriptor(
            StorageDescriptor lmsPartitionSd, org.apache.hadoop.hive.metastore.api.StorageDescriptor sd) {
        org.apache.hadoop.hive.metastore.api.StorageDescriptor storageDescriptor = new org.apache.hadoop.hive.metastore.api.StorageDescriptor();
        if (lmsPartitionSd != null) {
            if (CollectionUtils.isNotEmpty(lmsPartitionSd.getColumns())) {
                storageDescriptor.setCols(lmsPartitionSd.getColumns().stream().map(this::lmsSchemaToMsFieldSchema)
                        .collect(Collectors.toList()));
            }
            if (CollectionUtils.isEmpty(storageDescriptor.getCols())) {
                storageDescriptor.setCols(sd.getCols());
            }

            if (lmsPartitionSd.getSkewedInfo() != null) {
                storageDescriptor.setSkewedInfo(convertToSkewedInfo(lmsPartitionSd.getSkewedInfo()));
            }
            storageDescriptor.setLocation(lmsPartitionSd.getLocation());
            if (storageDescriptor.getSkewedInfo() == null) {
                storageDescriptor.setSkewedInfo(sd.getSkewedInfo());
            }
            if (lmsPartitionSd.getSerdeInfo() != null) {
                storageDescriptor.setSerdeInfo(convertToSerDeInfo(lmsPartitionSd));
            }
            if (storageDescriptor.getSerdeInfo() == null || StringUtils.isEmpty(
                    storageDescriptor.getSerdeInfo().getSerializationLib())) {
                storageDescriptor.setSerdeInfo(sd.getSerdeInfo());
            }
            storageDescriptor.setCompressed(lmsPartitionSd.getCompressed());
            if (!storageDescriptor.isCompressed()) {
                storageDescriptor.setCompressed(sd.isCompressed());
            }
            storageDescriptor.setNumBuckets(lmsPartitionSd.getNumberOfBuckets());
            if (storageDescriptor.getNumBuckets() <= 0) {
                storageDescriptor.setNumBuckets(sd.getNumBuckets());
            }
            storageDescriptor.setBucketCols(lmsPartitionSd.getBucketColumns());
            if (CollectionUtils.isEmpty(storageDescriptor.getBucketCols())) {
                storageDescriptor.setBucketCols(sd.getBucketCols());
            }
            storageDescriptor.setInputFormat(lmsPartitionSd.getInputFormat());
            if (StringUtils.isEmpty(storageDescriptor.getInputFormat())) {
                storageDescriptor.setInputFormat(sd.getInputFormat());
            }
            storageDescriptor.setOutputFormat(lmsPartitionSd.getOutputFormat());
            if (StringUtils.isEmpty(storageDescriptor.getOutputFormat())) {
                storageDescriptor.setOutputFormat(sd.getOutputFormat());
            }
            storageDescriptor.setSortCols(convertToSortOrder(lmsPartitionSd.getSortColumns()));
            if (CollectionUtils.isEmpty(storageDescriptor.getSortCols())) {
                storageDescriptor.setSortCols(sd.getSortCols());
            }
            return storageDescriptor;
        }
        return storageDescriptor;
    }

    @Override
    public void alterTable(String dbName, String tblName, Table newt)
        throws InvalidObjectException, MetaException {
        String catName = newt.getParameters().get("lms_name");
        if (catName == null || catName.isEmpty()) {
            throw new MetaException("catalog name can not be null");
        }
        alterTable(catName, dbName, tblName, newt);
    }

    private List<FieldSchema> removePartitionSchemas(List<FieldSchema> columnSchemas,
            List<FieldSchema> partitionSchemas) {
        Set<String> names = partitionSchemas.stream().map(FieldSchema::getName).collect(Collectors.toSet());
        return columnSchemas.stream().filter(x -> !names.contains(x.getName())).collect(Collectors.toList());
    }

    private FieldSchema convertToFieldSchema(Schema schema) {
        String comment = ((JSONObject) schema.getMetadata()).getString("comment");
        return new FieldSchema(schema.getName(), schema.getType(), comment);
    }

    private List<FieldSchema> getFieldSchemas(Table table, int idx) {
        String struct = table.getParameters().get("spark.sql.sources.schema.part." + idx);
        JSONObject structJson = JSONObject.parseObject(struct);
        JSONArray schemas = structJson.getJSONArray("fields");
        Schema[] fieldSchemas = schemas.toJavaObject(Schema[].class);
        return Arrays.stream(fieldSchemas).map(this::convertToFieldSchema).collect(Collectors.toList());
    }

    private List<FieldSchema> buildColumnSchema(Table table) {
        // if (table.getParameters().containsKey("spark.sql.sources.provider") &&
        //     table.getParameters().get("spark.sql.sources.provider").equalsIgnoreCase("csv")) {
        //     int partNum = Integer.parseInt(table.getParameters().get("spark.sql.sources.schema.numParts"));
        //     List<FieldSchema> columnSchemas = new ArrayList<>();
        //     for (int i = 0; i < partNum; i++) {
        //         columnSchemas.addAll(getFieldSchemas(table, i));
        //     }
        //     return removePartitionSchemas(columnSchemas, table.getPartitionKeys());
        // } else {
            return table.getSd().getCols();
        // }
    }

    private boolean isAlterTableColumn(String catName, String dbName, String tblName,
            Table newTable, AlterColumnContext context)
            throws MetaException {
        Table oldTable = getTable(catName, dbName, tblName);
        List<FieldSchema> oldColumns = buildColumnSchema(oldTable);
        List<FieldSchema> newColumns = buildColumnSchema(newTable);
        if (oldColumns.size() > newColumns.size()) {
            throw new MetaException(String.format("The number(%s) of modified columns is less than " +
                "the number of the original table", newColumns.size()));
        }

        for (int i = 0; i < oldColumns.size(); i++) {
            if (!newColumns.get(i).getName().equals(oldColumns.get(i).getName())) {
                context.setColumnOperation(Operation.RENAME_COLUMN);
                context.setModifyColumn(newColumns.get(i));
                context.setOriginalColumn(oldColumns.get(i));
                return true;
            }

            if (newColumns.get(i).getComment() != null &&
                !newColumns.get(i).getComment().equals(oldColumns.get(i).getComment()) ||
                !newColumns.get(i).getType().equals(oldColumns.get(i).getType())) {
                context.setColumnOperation(Operation.CHANGE_COLUMN);
                context.setModifyColumn(newColumns.get(i));
                return true;
            }
        }

        List<FieldSchema> addColumnList = new ArrayList<>();
        for (int i = oldColumns.size(); i < newColumns.size(); i++) {
            addColumnList.add(newColumns.get(i));
        }

        if (addColumnList.size() != 0) {
            context.setAddColumns(addColumnList.toArray(new FieldSchema[0]));
            context.setColumnOperation(Operation.ADD_COLUMN);
            return true;
        }
        return false;
    }

    private Column convertToColumnInput(FieldSchema fieldSchema) {
        Column columnInput = new Column();
        columnInput.setColumnName(fieldSchema.getName());
        columnInput.setColType(fieldSchema.getType());
        columnInput.setComment(fieldSchema.getComment());
        return columnInput;
    }

    private void alterTableColumn(String caName, String dbName, String tblName, AlterColumnContext context) {
        ColumnChangeInput colChangeIn = new ColumnChangeInput();
        colChangeIn.setChangeType(context.getColumnOperation());
        if (context.getColumnOperation() == Operation.ADD_COLUMN) {
            List<Column> addColumnList = new ArrayList<>();
            for (FieldSchema fieldSchema : context.getAddColumns()) {
                addColumnList.add(convertToColumnInput(fieldSchema));
            }
            colChangeIn.setColumnList(addColumnList);
        } else if (context.getColumnOperation() == Operation.CHANGE_COLUMN) {
            Map<String, Column> changeColumnMap = new HashMap<>();
            changeColumnMap.put(context.getModifyColumn().getName(), convertToColumnInput(context.getModifyColumn()));
            colChangeIn.setChangeColumnMap(changeColumnMap);
        } else if (context.getColumnOperation() == Operation.RENAME_COLUMN) {
            Map<String, String> renameColumnMap = new HashMap<>();
            renameColumnMap.put(context.getOriginalColumn().getName(), context.getModifyColumn().getName());
            colChangeIn.setRenameColumnMap(renameColumnMap);
        } else {
            throw new CatalogException(String.format("Unsupported column operation(%s)",
                context.getColumnOperation().toString()));
        }

        AlterColumnRequest request = new AlterColumnRequest(getProjectId(), caName, dbName, tblName,
            colChangeIn);
        client.alterColumn(request);
    }

    public void alterTable(String catName, String dbName, String tblName, Table newt)
        throws InvalidObjectException, MetaException {
        AlterColumnContext context = new AlterColumnContext();
        if (isAlterTableColumn(catName, dbName, tblName, newt, context)) {
            alterTableColumn(catName, dbName, tblName, context);
        }

        if (!newt.getTableName().equals(tblName)) {
            newt.getParameters().put(LMS_NAME, catName);
        }
        AlterTableInput alterTableInput = new AlterTableInput(getTableInput(newt), null);
        AlterTableRequest alterTableRequest = new AlterTableRequest();
        alterTableRequest.setInput(alterTableInput);
        alterTableRequest.setTableName(tblName);
        alterTableRequest.setCatalogName(catName);
        alterTableRequest.setDatabaseName(dbName);
        alterTableRequest.setProjectId(getProjectId());
        try {
            client.alterTable(alterTableRequest);
        } catch (CatalogException e) {
            LOG.error("table:{} alter failed", newt.getTableName(), e);
        }
    }

    private List<Column> getColumnInputs(Table table, int location) {
        String struct = table.getParameters().get("spark.sql.sources.schema.part." + location);
        JSONObject structJson = JSONObject.parseObject(struct);
        JSONArray schemas = structJson.getJSONArray("fields");
        Schema[] fieldSchemas = schemas.toJavaObject(Schema[].class);
        return Arrays.stream(fieldSchemas).map(this::convertToTableInput).collect(Collectors.toList());
    }

    private List<Column> buildColumnInputs(Table table, List<Column> partitions) {
        List<Column> columnInputs;
        // if (table.getParameters().containsKey("spark.sql.sources.provider") &&
        //     table.getParameters().get("spark.sql.sources.provider").equalsIgnoreCase("csv")) {
        //     int partNum = Integer.parseInt(table.getParameters().get("spark.sql.sources.schema.numParts"));
        //     columnInputs = new ArrayList<>();
        //     for (int i = 0; i < partNum; i++) {
        //         columnInputs.addAll(getColumnInputs(table, i));
        //     }
        //     removePartitionColumns(columnInputs, partitions);
        // } else {
        columnInputs = table.getSd().getCols().stream()
                .map(this::convertToTableInput).collect(Collectors.toList());
        // }
        return columnInputs;
    }

    private TableInput getTableInput(Table newt) {
        TableInput tableInput = new TableInput();

        List<Column> partitions = null;
        if (newt.getPartitionKeys() != null) {
            partitions = newt.getPartitionKeys().stream()
                .map(this::convertToTableInput).collect(Collectors.toList());
        } else {
            partitions = Collections.emptyList();
        }
        tableInput.setLmsMvcc(false);

//        List<Column> columnInputs = buildColumnInputs(newt, partitions);
//        tableInput.setColumns(columnInputs);
        tableInput.setTableName(newt.getTableName());
        tableInput.setPartitionKeys(partitions);
        tableInput.setRetention(newt.getRetention());
        String owner = newt.getOwner();
        if (newt.getParameters() != null &&
            TableUtil.isIcebergTableByParams(newt.getParameters()) &&
            newt.getParameters().containsKey(Constants.OWNER_PARAM)) {
            owner = newt.getParameters().get(Constants.OWNER_PARAM);
            owner = getUserId(owner);
            newt.getParameters().put(Constants.OWNER_PARAM, owner);
            tableInput.setOwner(owner);
        } else {
            tableInput.setOwner(getUserId(owner));
        }
        tableInput.setStorageDescriptor(fillInStorageDescriptor(newt, partitions));
        tableInput.setParameters(newt.getParameters());
        setTableTypeParams(tableInput, newt);
        return tableInput;
    }

    private void setTableTypeParams(TableInput tableInput, Table newt) {
        // If the table has property EXTERNAL set, update table type
        // accordingly
        String tableType = newt.getTableType();
        try {
            if (StringUtils.isNotEmpty(tableType)) {
                boolean isExternal = "TRUE".equals(newt.getParameters().get("EXTERNAL"));
                switch (TableTypeInput.valueOf(tableType)) {
                    case VIRTUAL_VIEW:
                        tableInput.setViewExpandedText(newt.getViewExpandedText());
                        tableInput.setViewOriginalText(newt.getViewOriginalText());
                        break;
                    case MANAGED_TABLE:
                        if (isExternal) {
                            tableType = TableTypeInput.EXTERNAL_TABLE.toString();
                        }
                        break;
                    case EXTERNAL_TABLE:
                    /*
                    // Do you still need to keep it in cloud native? For the time being:EXTERNAL_TABLE
                    if (!isExternal) {
                        tableType = TableTypeInput.MANAGED_TABLE.toString();
                    }*/
                        break;
                    case INDEX_TABLE:
                        break;
                    case MATERIALIZED_VIEW:
                        break;
                    default:
                        break;
                }
            }
        } catch (Exception e) {
            //Special engine type support
            LOG.warn("Table: {}.{} type={} unresolved", newt.getDbName(), newt.getTableName(), tableType, e);
        }
        tableInput.setTableType(tableType);
    }

    private StorageDescriptor fillInStorageDescriptor(Table newt, List<Column> partitions) {
        org.apache.hadoop.hive.metastore.api.StorageDescriptor sd = newt.getSd();
        if (sd == null) {
            return null;
        }

        StorageDescriptor storageInput = new StorageDescriptor();
        storageInput.setLocation(sd.getLocation());
        storageInput.setCompressed(sd.isCompressed());
        String serializationLib = sd.getSerdeInfo().getSerializationLib();
        String sourceName = serializationLibToSourceNameMap.getOrDefault(serializationLib, serializationLib);
        if (newt.getParameters().get("spark.sql.sources.provider") != null) {
            sourceName = newt.getParameters().get("spark.sql.sources.provider");
        }

        storageInput.setSourceShortName(sourceName);
        storageInput.setInputFormat(sd.getInputFormat());
        storageInput.setOutputFormat(sd.getOutputFormat());
        storageInput.setNumberOfBuckets(sd.getNumBuckets());
        storageInput.setBucketColumns(sd.getBucketCols());
        storageInput.setSortColumns(fillOrderList(sd.getSortCols()));
        storageInput.setParameters(sd.getParameters());
        storageInput.setSerdeInfo(fillSerDeInfo(sd.getSerdeInfo()));
        storageInput.setColumns(buildColumnInputs(newt, partitions));
        storageInput.setSkewedInfo(convertToLmsSkewedInfo(sd.getSkewedInfo()));

        return storageInput;
    }

    private io.polycat.catalog.common.model.SerDeInfo fillSerDeInfo(
        SerDeInfo hiveSerDe) {
        if (hiveSerDe != null) {
            io.polycat.catalog.common.model.SerDeInfo serDeInfo = new io.polycat.catalog.common.model.SerDeInfo();
            serDeInfo.setName(hiveSerDe.getName());
            serDeInfo.setSerializationLibrary(hiveSerDe.getSerializationLib());
            serDeInfo.setParameters(hiveSerDe.getParameters());
            return serDeInfo;
        }
        return null;
    }

    private void removePartitionColumns(List<Column> columnList, List<Column> partitions) {
        if (partitions == null || partitions.isEmpty()) {
            return;
        }
        for (int i = partitions.size() - 1; i >= 0; i = i - 1) {
            String partColumnName = partitions.get(i).getColumnName();
            int lastIndex = columnList.size() - 1;
            if (partColumnName.equals(columnList.get(lastIndex).getColumnName())) {
                columnList.remove(lastIndex);
            }
        }
    }

    public List<String> getTables(String catalog, String dbName, String pattern) throws MetaException {
        ListTablesRequest request = new ListTablesRequest();
        request.setCatalogName(catalog);
        request.setDatabaseName(dbName);
        request.setProjectId(getProjectId());
        request.setExpression(pattern);
        List<String> res = new ArrayList<>();
        String[] tableList;
        String pageToken;
        do {
            PagedList<String> tablePagedList = client.listTableNames(request);
            tableList = tablePagedList.getObjects();
            pageToken = tablePagedList.getNextMarker();
            request.setNextToken(pageToken);
            assert tableList != null;
            res.addAll(Arrays.asList(tableList));
        } while (StringUtils.isNotEmpty(pageToken));
        return res;
    }

    @Override
    public List<String> getTables(String dbName, String pattern) throws MetaException {
        return null;
    }

    @Override
    public List<String> getTables(String dbName, String pattern, TableType tableType)
        throws MetaException {

        return null;
    }

    public List<String> getTables(String catalog, String dbName, String pattern, TableType tableType)
        throws MetaException {
        ListTablesRequest request = new ListTablesRequest();
        request.setCatalogName(catalog);
        request.setDatabaseName(dbName);
        request.setProjectId(getProjectId());
        request.setExpression(pattern);
        io.polycat.catalog.common.model.Table[] tablePagedList = client.listTables(request).getObjects();

        List<String> res = new ArrayList<>();
        for (io.polycat.catalog.common.model.Table table : tablePagedList) {
            if (table.getTableType().equalsIgnoreCase(tableType.toString())) {
                res.add(table.getTableName());
            }
        }
        return res;
    }

    @Override
    public List<TableMeta> getTableMeta(String dbNames, String tableNames, List<String> tableTypes)
        throws MetaException {
        return null;
    }

    public List<Table> getTableObjectsByName(String catalogName, String dbname, List<String> tableNames)
        throws MetaException, UnknownDBException {
        List<Table> tables = listTableObjects(catalogName, dbname, tableNames);
        Set<String> set = new HashSet<>(tableNames);
        return tables.stream().filter(t -> set.contains(t.getTableName())).collect(Collectors.toList());
    }

    private List<Table> listTableObjects(String catalogName, String dbname, List<String> tableNames) throws MetaException {
        FilterListInput filterListInput = new FilterListInput();
        if (tableNames != null && !tableNames.isEmpty()) {
            filterListInput.setFilter(tableNames);
        }
        filterListInput.setFilter(tableNames);
        ListTableObjectsRequest request = new ListTableObjectsRequest(getProjectId(), catalogName, dbname, filterListInput);
        List<Table> tables = new ArrayList<>();
        io.polycat.catalog.common.model.Table[] tableList;
        String pageToken;
        do {
            PagedList<io.polycat.catalog.common.model.Table> tablePagedList = client.listTables(request);
            tableList = tablePagedList.getObjects();
            pageToken = tablePagedList.getNextMarker();
            request.setNextToken(pageToken);
            for (io.polycat.catalog.common.model.Table table : tableList) {
                tables.add(mTblTodTbl(table));
            }
        } while (StringUtils.isNotEmpty(pageToken));
        return tables;
    }

    private List<Table> listTables(String catalogName, String dbname, List<String> tableNames) throws MetaException {
        ListTablesRequest request = new ListTablesRequest();
        request.setCatalogName(catalogName);
        request.setDatabaseName(dbname);
        request.setProjectId(getProjectId());
        if (tableNames != null && !tableNames.isEmpty()) {
            request.setExpression(String.join("|", tableNames));
        }
        List<Table> tables = new ArrayList<>();
        io.polycat.catalog.common.model.Table[] tableList;
        String pageToken;
        do {
            PagedList<io.polycat.catalog.common.model.Table> tablePagedList = client.listTables(request);
            tableList = tablePagedList.getObjects();
            pageToken = tablePagedList.getNextMarker();
            request.setNextToken(pageToken);
            for (io.polycat.catalog.common.model.Table table : tableList) {
                tables.add(mTblTodTbl(table));
            }
        } while (StringUtils.isNotEmpty(pageToken));
        return tables;
    }

    @Override
    public List<Table> getTableObjectsByName(String dbname, List<String> tableNames)
        throws MetaException, UnknownDBException {
        return null;
    }

    @Override
    public List<String> getAllTables(String dbName) throws MetaException {
        return null;
    }

    @Override
    public List<String> listTableNamesByFilter(String dbName, String filter, short max_tables)
        throws MetaException, UnknownDBException {
        return null;
    }

    public List<String> listTableNamesByFilter(String catName, String dbName, String filter, short max_tables)
        throws MetaException {
        final GetTableNamesRequest getTableNamesRequest = new GetTableNamesRequest();
        getTableNamesRequest.setFilter(filter);
        getTableNamesRequest.setProjectId(getProjectId());
        getTableNamesRequest.setCatalogName(catName);
        getTableNamesRequest.setDatabaseName(dbName);
        getTableNamesRequest.setMaxResults(String.valueOf(max_tables));
        try {
            return Arrays.asList(client.getTableNames(getTableNamesRequest).getObjects());
        } catch (CatalogException e) {
            throw new MetaException(e.getMessage());
        }
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tableName, short max) {
        return null;
    }

    public List<String> listPartitionNames(String catName, String dbName, String tableName, short max)
        throws MetaException {
        PartitionFilterInput filterInput = new PartitionFilterInput();
        filterInput.setMaxParts(max);
        ListTablePartitionsRequest request = new ListTablePartitionsRequest(getProjectId(), catName, dbName, tableName,
                filterInput);
        List<String> listPartitionNames = client.listPartitionNames(request);
        if (listPartitionNames != null) {
            return listPartitionNames.stream().map(PartitionUtil::escapePartitionName).collect(Collectors.toList());
        }
        return listPartitionNames;
    }

    @Override
    public PartitionValuesResponse listPartitionValues(String db_name, String tbl_name,
            List<FieldSchema> cols, boolean applyDistinct, String filter, boolean ascending,
            List<FieldSchema> order, long maxParts) throws MetaException {
        return null;
    }

    public PartitionValuesResponse listPartitionValues(String ca_name, String db_name, String tbl_name,
            List<FieldSchema> cols, boolean applyDistinct, String filter, boolean ascending,
            List<FieldSchema> order, long maxParts) {
        return null;
    }

    @Override
    public List<String> listPartitionNamesByFilter(String db_name, String tbl_name, String filter,
            short max_parts) throws MetaException {
        return null;
    }

    public List<String> listPartitionNamesByFilter(String ca_name, String db_name, String tbl_name,
            String filter, short max_parts) throws MetaException {
        PartitionFilterInput filterInput = new PartitionFilterInput();
        filterInput.setFilter(filter);
        ListPartitionNamesByFilterRequest request = new ListPartitionNamesByFilterRequest(getProjectId(), ca_name,
                db_name, tbl_name,
                filterInput);
        return client.listPartitionNamesByFilter(request);
    }

    @Override
    public void alterPartition(String db_name, String tbl_name, List<String> part_vals,
            org.apache.hadoop.hive.metastore.api.Partition new_part) throws InvalidObjectException, MetaException {

    }

    public void alterPartition(String caName, String dbName, String tblName, List<String> partValues,
            org.apache.hadoop.hive.metastore.api.Partition newPart) throws InvalidObjectException, MetaException {
        AlterPartitionInput input = new AlterPartitionInput();
        List<PartitionAlterContext> partitionContexts = new ArrayList<>();
        PartitionAlterContext context = convertToContext(newPart, partValues);
        partitionContexts.add(context);
        input.setPartitionContexts(partitionContexts.toArray(new PartitionAlterContext[0]));
        AlterPartitionRequest request = new AlterPartitionRequest(getProjectId(), caName, dbName, tblName,
            input);
        client.alterPartitions(request);
    }

    @Override
    public void alterPartitions(String db_name, String tbl_name, List<List<String>> part_vals_list,
            List<org.apache.hadoop.hive.metastore.api.Partition> new_parts)
            throws InvalidObjectException, MetaException {

    }

    private PartitionAlterContext convertToContext(org.apache.hadoop.hive.metastore.api.Partition partition,
            List<String> oldValues) {
        PartitionAlterContext context = new PartitionAlterContext();
        context.setOldValues(oldValues);
        context.setNewValues(partition.getValues());
        context.setParameters(partition.getParameters());
        context.setInputFormat(partition.getSd().getInputFormat());
        context.setOutputFormat(partition.getSd().getOutputFormat());
        context.setLocation(partition.getSd().getLocation());
        return context;
    }

    public void alterPartitions(String caName, String dbName, String tblName, List<List<String>> partValuesList,
            List<org.apache.hadoop.hive.metastore.api.Partition> newParts)
            throws InvalidObjectException, MetaException {
        AlterPartitionInput input = new AlterPartitionInput();
        List<PartitionAlterContext> partitionContexts = new ArrayList<>(newParts.size());
        Iterator<List<String>> partValItr = partValuesList.iterator();
        for (org.apache.hadoop.hive.metastore.api.Partition partition : newParts) {
            PartitionAlterContext context = convertToContext(partition, partValItr.next());
            partitionContexts.add(context);
        }
        input.setPartitionContexts(partitionContexts.toArray(new PartitionAlterContext[0]));
        AlterPartitionRequest request = new AlterPartitionRequest(getProjectId(), caName, dbName, tblName,
            input);
        client.alterPartitions(request);
    }

    @Override
    public boolean addIndex(Index index) throws InvalidObjectException, MetaException {
        return false;
    }

    @Override
    public Index getIndex(String dbName, String origTableName, String indexName)
        throws MetaException {
        return null;
    }

    @Override
    public boolean dropIndex(String dbName, String origTableName, String indexName)
        throws MetaException {
        return false;
    }

    @Override
    public List<Index> getIndexes(String dbName, String origTableName, int max)
        throws MetaException {
        return null;
    }

    @Override
    public List<String> listIndexNames(String dbName, String origTableName, short max)
        throws MetaException {
        return null;
    }

    @Override
    public void alterIndex(String dbname, String baseTblName, String name, Index newIndex)
        throws InvalidObjectException, MetaException {

    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> getPartitionsByFilter(String dbName, String tblName,
            String filter,
            short maxParts) throws MetaException, NoSuchObjectException {
        return null;
    }

    public List<org.apache.hadoop.hive.metastore.api.Partition> getPartitionsByFilter(String caName, String dbName,
            String tblName,
            String filter, short maxParts) throws MetaException {
        Table table = getTable(caName, dbName, tblName);
        PartitionFilterInput filterInput = new PartitionFilterInput();
        filterInput.setFilter(filter);
        filterInput.setMaxParts(maxParts);
        ListPartitionByFilterRequest request = new ListPartitionByFilterRequest(getProjectId(),
            caName, dbName, tblName, filterInput);

        List<Partition> tablePartitions = client.getPartitionsByFilter(request);
        return tablePartitions.stream().map(partition ->
            convertToPartition(partition, dbName, tblName, table.getSd())).collect(Collectors.toList());
    }

    @Override
    public boolean getPartitionsByExpr(String dbName, String tblName, byte[] expr,
            String defaultPartitionName, short maxParts, List<org.apache.hadoop.hive.metastore.api.Partition> result)
            throws TException {
        return false;
    }

    public boolean getPartitionsByExpr(String caName, String dbName, String tblName, byte[] expr,
            String defaultPartitionName,
            short maxParts, List<org.apache.hadoop.hive.metastore.api.Partition> result) throws TException {
        final PartitionExpressionForMetastore partitionExpressionForMetastore = new PartitionExpressionForMetastore();
        PartFilterExprUtil
                .makeExpressionTree(partitionExpressionForMetastore, expr);

        Table table = getTable(caName, dbName, tblName);
        final GetPartitionsByExprInput getPartitionsByExprInput = new GetPartitionsByExprInput(defaultPartitionName,
                expr, maxParts);
        final ListPartitionsByExprRequest listPartitionsByExprRequest = new ListPartitionsByExprRequest(getProjectId(),
                caName, dbName, tblName, getPartitionsByExprInput);
        try {
            final List<Partition> partitions = client.listPartitionsByExpr(listPartitionsByExprRequest);
            partitions.forEach(partition -> result.add(convertToPartition(partition, dbName, tblName, table.getSd())));
            return false;
        } catch (CatalogException e) {
            LOG.error("Failed get partitions by expr", e);
            throw new MetaException(e.getMessage());
        }

    }

    @Override
    public int getNumPartitionsByFilter(String dbName, String tblName, String filter)
        throws MetaException, NoSuchObjectException {
        return 0;
    }

    @Override
    public int getNumPartitionsByExpr(String dbName, String tblName, byte[] expr)
        throws MetaException, NoSuchObjectException {
        return 0;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> getPartitionsByNames(String dbName, String tblName,
            List<String> partNames) throws MetaException, NoSuchObjectException {
        return null;
    }

    public List<org.apache.hadoop.hive.metastore.api.Partition> getPartitionsByNames(String caName, String dbName,
            String tblName,
            List<String> partNames) throws MetaException, NoSuchObjectException {
        Table table = getTable(caName, dbName, tblName);
        PartitionFilterInput filterInput = new PartitionFilterInput();
        filterInput.setPartNames(partNames.toArray(new String[0]));
        GetTablePartitionsByNamesRequest request = new GetTablePartitionsByNamesRequest(getProjectId(),
            caName, dbName, tblName, filterInput);
        List<Partition> partitions = client.getPartitionsByNames(request);
        return getHivePartitions(partitions, dbName, tblName, table.getSd());
    }

    @Override
    public Table markPartitionForEvent(String dbName, String tblName, Map<String, String> partVals,
            PartitionEventType evtType)
            throws MetaException, UnknownTableException, InvalidPartitionException,
            UnknownPartitionException {
        return null;
    }

    @Override
    public boolean isPartitionMarkedForEvent(String dbName, String tblName,
            Map<String, String> partName, PartitionEventType evtType)
            throws MetaException, UnknownTableException, InvalidPartitionException,
            UnknownPartitionException {
        return false;
    }

    @Override
    public boolean addRole(String rowName, String ownerName)
        throws InvalidObjectException, MetaException, NoSuchObjectException {
        return false;
    }

    @Override
    public boolean removeRole(String roleName) throws MetaException, NoSuchObjectException {
        return false;
    }

    @Override
    public boolean grantRole(Role role, String userName, PrincipalType principalType,
            String grantor, PrincipalType grantorType, boolean grantOption)
            throws MetaException, NoSuchObjectException, InvalidObjectException {
        return false;
    }

    @Override
    public boolean revokeRole(Role role, String userName, PrincipalType principalType,
            boolean grantOption) throws MetaException, NoSuchObjectException {
        return false;
    }

    @Override
    public PrincipalPrivilegeSet getUserPrivilegeSet(String userName, List<String> groupNames)
        throws InvalidObjectException, MetaException {
        return null;
    }

    @Override
    public PrincipalPrivilegeSet getDBPrivilegeSet(String dbName, String userName,
            List<String> groupNames) throws InvalidObjectException, MetaException {
        return null;
    }

    @Override
    public PrincipalPrivilegeSet getTablePrivilegeSet(String dbName, String tableName,
            String userName, List<String> groupNames) throws InvalidObjectException, MetaException {
        return null;
    }

    @Override
    public PrincipalPrivilegeSet getPartitionPrivilegeSet(String dbName, String tableName,
            String partition, String userName, List<String> groupNames)
            throws InvalidObjectException, MetaException {
        return null;
    }

    @Override
    public PrincipalPrivilegeSet getColumnPrivilegeSet(String dbName, String tableName,
            String partitionName, String columnName, String userName, List<String> groupNames)
            throws InvalidObjectException, MetaException {
        return null;
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalGlobalGrants(String principalName,
            PrincipalType principalType) {
        return null;
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalDBGrants(String principalName,
            PrincipalType principalType, String dbName) {
        return null;
    }

    @Override
    public List<HiveObjectPrivilege> listAllTableGrants(String principalName,
            PrincipalType principalType, String dbName, String tableName) {
        return null;
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalPartitionGrants(String principalName,
            PrincipalType principalType, String dbName, String tableName, List<String> partValues,
            String partName) {
        return null;
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalTableColumnGrants(String principalName,
            PrincipalType principalType, String dbName, String tableName, String columnName) {
        return null;
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalPartitionColumnGrants(String principalName,
            PrincipalType principalType, String dbName, String tableName, List<String> partValues,
            String partName, String columnName) {
        return null;
    }

    @Override
    public boolean grantPrivileges(PrivilegeBag privileges)
        throws InvalidObjectException, MetaException, NoSuchObjectException {
        return false;
    }

    @Override
    public boolean revokePrivileges(PrivilegeBag privileges, boolean grantOption)
        throws InvalidObjectException, MetaException, NoSuchObjectException {
        return false;
    }

    @Override
    public Role getRole(String roleName) throws NoSuchObjectException {
        return null;
    }

    @Override
    public List<String> listRoleNames() {
        return null;
    }

    @Override
    public List<Role> listRoles(String principalName, PrincipalType principalType) {
        return null;
    }

    @Override
    public List<RolePrincipalGrant> listRolesWithGrants(String principalName,
            PrincipalType principalType) {
        return null;
    }

    @Override
    public List<RolePrincipalGrant> listRoleMembers(String roleName) {
        return null;
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition getPartitionWithAuth(String dbName, String tblName,
            List<String> partVals,
            String user_name, List<String> group_names)
            throws MetaException, NoSuchObjectException, InvalidObjectException {
        return null;
    }

    public org.apache.hadoop.hive.metastore.api.Partition getPartitionWithAuth(String caName, String dbName,
            String tblName, List<String> partVals,
            String user_name, List<String> group_names) throws NoSuchObjectException, MetaException {
        Table table = getTable(caName, dbName, tblName);
        GetPartitionWithAuthInput filterInput = new GetPartitionWithAuthInput(user_name, group_names, partVals);
        GetPartitionWithAuthRequest request = new GetPartitionWithAuthRequest(getProjectId(), caName, dbName,
                tblName, filterInput);
        Partition partition = client.getPartitionWithAuth(request);
        if (partition == null) {
            throw new NoSuchObjectException("partition values=" + partVals);
        }
        return convertToPartition(partition, dbName, tblName, table.getSd());
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> getPartitionsWithAuth(String dbName, String tblName,
            short maxParts,
            String userName, List<String> groupNames)
            throws MetaException, NoSuchObjectException, InvalidObjectException {
        return null;
    }

    public List<org.apache.hadoop.hive.metastore.api.Partition> getPartitionsWithAuth(String caName, String dbName,
            String tblName, short maxParts,
            String userName, List<String> groupNames) throws NoSuchObjectException, MetaException {
        return getPartitions(caName, dbName, tblName, maxParts);
    }

    @Override
    public List<String> listPartitionNamesPs(String db_name, String tbl_name,
            List<String> part_vals, short max_parts) throws MetaException, NoSuchObjectException {
        return null;
    }

    public List<String> listPartitionNamesPs(String ca_name, String db_name, String tbl_name,
            List<String> part_vals, short max_parts) throws MetaException, NoSuchObjectException {
        PartitionFilterInput filterInput = new PartitionFilterInput();
        filterInput.setValues(part_vals.toArray(new String[0]));
        filterInput.setMaxParts(max_parts);
        ListPartitionNamesPsRequest request = new ListPartitionNamesPsRequest(getProjectId(), ca_name, db_name,
                tbl_name,
                filterInput);
        return client.listPartitionNamesPs(request);
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitionsPsWithAuth(String db_name,
            String tbl_name,
            List<String> part_vals, short max_parts, String userName, List<String> groupNames)
            throws MetaException, InvalidObjectException, NoSuchObjectException {
        return null;
    }

    public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitionsPsWithAuth(String ca_name, String db_name,
            String tbl_name,
            List<String> part_vals, short max_parts, String userName, List<String> groupNames)
            throws MetaException, InvalidObjectException, NoSuchObjectException {
        Table table = getTable(ca_name, db_name, tbl_name);
        GetPartitionsWithAuthInput input = new GetPartitionsWithAuthInput(max_parts, userName, groupNames,
            part_vals);
        ListPartitionsWithAuthRequest request = new ListPartitionsWithAuthRequest(getProjectId(), ca_name, db_name,
            tbl_name, input);
        List<Partition> partitions = client.listPartitionsPsWithAuth(request);
        return getHivePartitions(partitions, db_name, tbl_name, table.getSd());
    }

    private List<org.apache.hadoop.hive.metastore.api.Partition> getHivePartitions(List<Partition> partitions,
            String db_name, String tbl_name, org.apache.hadoop.hive.metastore.api.StorageDescriptor sd) {
        List<org.apache.hadoop.hive.metastore.api.Partition> resList;
        if (CollectionUtils.isNotEmpty(partitions)) {
            resList = new ArrayList<>(partitions.size());
            Partition p;
            for (Partition partition : partitions) {
                p = partition;
                resList.add(convertToPartition(p, db_name, tbl_name, sd));
            }
            return resList;
        }
        return new ArrayList<>();
    }

    public boolean updateTableColumnStatistics(String catalogName, ColumnStatistics colStats)
        throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
        ColumnStatisticsDesc statsDesc = colStats.getStatsDesc();
        try {
            UpdateTableColumnStatisticRequest request = new UpdateTableColumnStatisticRequest(getProjectId(),
                catalogName, statsDesc.getDbName(), statsDesc.getTableName(),
                LsmDataAccessor.toColumnStatistics(colStats, catalogName));
            return client.updateTableColumnStatistics(request);
        } catch (TException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean updateTableColumnStatistics(ColumnStatistics colStats)
        throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
        return false;
    }

    @Override
    public boolean updatePartitionColumnStatistics(ColumnStatistics statsObj, List<String> partVals)
        throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
        return false;
    }

    public boolean updatePartitionColumnStatistics(String catalogName, ColumnStatistics statsObj, List<String> partVals)
        throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
        ColumnStatisticsDesc statsDesc = statsObj.getStatsDesc();
        try {
            UpdatePartitionColumnStatisticRequest request = new UpdatePartitionColumnStatisticRequest(getProjectId(),
                catalogName, statsDesc.getDbName(), statsDesc.getTableName(),
                new ColumnStatisticsInput(LsmDataAccessor.toColumnStatistics(statsObj, catalogName), partVals));
            return client.updatePartitionColumnStatistics(request);
        } catch (TException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public ColumnStatistics getTableColumnStatistics(String dbName, String tableName,
            List<String> colName) throws MetaException, NoSuchObjectException {
        return null;
    }


    public ColumnStatistics getTableColumnStatistics(String catalogName, String dbName, String tableName,
            List<String> colNames) {
        if (CollectionUtils.isEmpty(colNames)) {
            return null;
        }
        try {
            GetTableColumnStatisticRequest request = new GetTableColumnStatisticRequest(getProjectId(), catalogName,
                    dbName, tableName, colNames);
            io.polycat.catalog.common.model.stats.ColumnStatisticsObj[] columnStatisticsObjs = client
                    .getTableColumnsStatistics(
                            request);
            List<ColumnStatisticsObj> statisticsObjs = Arrays.stream(columnStatisticsObjs)
                    .map(HiveDataAccessor::convertToHiveStatsObj).collect(Collectors.toList());
            return new ColumnStatistics(new ColumnStatisticsDesc(true, dbName, tableName), statisticsObjs);
        } catch (Exception e) {
            LOG.warn("Get table column statistics error: {}", e);
            return null;
        }
    }

    @Override
    public List<ColumnStatistics> getPartitionColumnStatistics(String dbName, String tblName,
            List<String> partNames, List<String> colNames) throws MetaException, NoSuchObjectException {
        return null;
    }

    public List<ColumnStatistics> getPartitionColumnStatistics(String catalogName, String dbName, String tblName,
            List<String> partNames, List<String> colNames) throws MetaException, NoSuchObjectException {
        GetPartitionColumnStatisticsRequest request = new GetPartitionColumnStatisticsRequest(getProjectId(),
                catalogName, dbName,
                tblName, new GetPartitionColumnStaticsInput(partNames, colNames));
        PartitionStatisticData result = client.getPartitionColumnStatistics(request);
        return HiveDataAccessor.toColumnStatisticsList(result, dbName, tblName);
    }

    @Override
    public boolean deletePartitionColumnStatistics(String dbName, String tableName, String partName,
            List<String> partVals, String colName)
            throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
        return false;
    }

    public boolean deletePartitionColumnStatistics(String catalogName, String dbName, String tableName, String partName,
            List<String> partVals, String colName)
            throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
        DeletePartitionColumnStatisticsRequest request = new DeletePartitionColumnStatisticsRequest(getProjectId(),
                catalogName,
                dbName, tableName, makePartitionName(getTable(catalogName, dbName, tableName), partVals), colName);
        client.deletePartitionColumnStatistics(request);
        return true;
    }

    @Override
    public boolean deleteTableColumnStatistics(String dbName, String tableName, String colName)
        throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
        return false;
    }

    public boolean deleteTableColumnStatistics(String catalogName, String dbName, String tableName, String colName) {
        DeleteColumnStatisticsRequest request = new DeleteColumnStatisticsRequest(getProjectId(), catalogName,
            dbName, tableName, colName);
        client.deleteTableColumnStatistics(request);
        return true;
    }

    @Override
    public long cleanupEvents() {
        return 0;
    }

    @Override
    public boolean addToken(String tokenIdentifier, String delegationToken) {
        return false;
    }

    @Override
    public boolean removeToken(String tokenIdentifier) {
        return false;
    }

    @Override
    public String getToken(String tokenIdentifier) {
        return client.getContext().getToken();
    }

    @Override
    public List<String> getAllTokenIdentifiers() {
        return null;
    }

    @Override
    public int addMasterKey(String key) throws MetaException {
        return 0;
    }

    @Override
    public void updateMasterKey(Integer seqNo, String key)
        throws NoSuchObjectException, MetaException {

    }

    @Override
    public boolean removeMasterKey(Integer keySeq) {
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
        return null;
    }

    @Override
    public void setMetaStoreSchemaVersion(String version, String comment) throws MetaException {

    }

    @Override
    public void dropPartitions(String dbName, String tblName, List<String> partNames)
        throws MetaException, NoSuchObjectException {

    }

    public void dropPartitions(String caName, String dbName, String tblName, List<String> partNames)
        throws MetaException {
        DropPartitionInput dropPartitionInput = new DropPartitionInput();
        dropPartitionInput.setPartitionNames(partNames);
        DropPartitionRequest request = new DropPartitionRequest(getProjectId(), caName,
            dbName, tblName, dropPartitionInput);
        try {
            client.dropPartition(request);
        } catch (CatalogException e) {
            LOG.warn(e.getMessage() + "\n try to make deprecated partition names");
            final List<String> deprecatedPartNames = partNames.stream().map(partName -> {
                final List<String> partKeys = new ArrayList<>();
                final List<String> partVals = new ArrayList<>();
                Arrays.stream(partName.split("/")).forEach(part -> {
                    final String[] parts = part.split("=");
                    partKeys.add(parts[0]);
                    partVals.add(PartitionUtil.escapePathName(FileUtils.unescapePathName(parts[1])));
                });
                return PartitionUtil.makePartitionName(partKeys, partVals);
            }).collect(Collectors.toList());
            dropPartitionInput.setPartitionNames(deprecatedPartNames);
            client.dropPartition(request);
        }
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalDBGrantsAll(String principalName,
            PrincipalType principalType) {
        return null;
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalTableGrantsAll(String principalName,
            PrincipalType principalType) {
        return null;
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalPartitionGrantsAll(String principalName,
            PrincipalType principalType) {
        return null;
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalTableColumnGrantsAll(String principalName,
            PrincipalType principalType) {
        return null;
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalPartitionColumnGrantsAll(String principalName,
            PrincipalType principalType) {
        return null;
    }

    @Override
    public List<HiveObjectPrivilege> listGlobalGrantsAll() {
        return null;
    }

    @Override
    public List<HiveObjectPrivilege> listDBGrantsAll(String dbName) {
        return null;
    }

    @Override
    public List<HiveObjectPrivilege> listPartitionColumnGrantsAll(String dbName, String tableName,
            String partitionName, String columnName) {
        return null;
    }

    @Override
    public List<HiveObjectPrivilege> listTableGrantsAll(String dbName, String tableName) {
        return null;
    }

    @Override
    public List<HiveObjectPrivilege> listPartitionGrantsAll(String dbName, String tableName,
            String partitionName) {
        return null;
    }

    @Override
    public List<HiveObjectPrivilege> listTableColumnGrantsAll(String dbName, String tableName,
            String columnName) {
        return null;
    }

    @Override
    public void createFunction(Function func) throws InvalidObjectException, MetaException {
        throw new MetaException("Unexpected IO path");
    }

    public void createFunction(String catalogName, Function func) throws InvalidObjectException, MetaException {
        FunctionInput functionInput = convertToFuncInput(func);
        CreateFunctionRequest req = new CreateFunctionRequest(getProjectId(), catalogName, functionInput);
        client.createFunction(req);
    }

    private FunctionInput convertToFuncInput(Function func) {
        FunctionInput fIn = new FunctionInput();
        fIn.setFunctionName(func.getFunctionName());
        fIn.setClassName(func.getClassName());
        if (func.getOwnerName() != null) {
            fIn.setOwner(func.getOwnerName());
        } else {
            fIn.setOwner(getUserId());
        }
        fIn.setOwnerType(func.getOwnerType().name());
        fIn.setFuncType(func.getFunctionType().name());
        fIn.setDatabaseName(func.getDbName());

        if (func.getResourceUris() != null) {
            List<FunctionResourceUri> rUris = new ArrayList<>();
            for (ResourceUri ru : func.getResourceUris()) {
                FunctionResourceUri rUri = new FunctionResourceUri(ru.getResourceType().name(), ru.getUri());
                rUris.add(rUri);
            }
            fIn.setResourceUris(rUris);
        }
        return fIn;
    }

    @Override
    public void alterFunction(String dbName, String funcName, Function newFunction)
        throws InvalidObjectException, MetaException {

    }

    @Override
    public void dropFunction(String dbName, String funcName)
        throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        throw new MetaException("Unexpected IO path");
    }

    public void dropFunction(String catalogName, String dbName, String funcName)
        throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        FunctionRequestBase req = new FunctionRequestBase(getProjectId(), catalogName, dbName, funcName);
        client.dropFunction(req);
    }

    @Override
    public Function getFunction(String dbName, String funcName) throws MetaException {
        throw new MetaException("Unexpected IO path");
    }

    public Function getFunction(String catalogName, String dbName, String funcName) throws MetaException {
        GetFunctionRequest req = new GetFunctionRequest(getProjectId(), catalogName, dbName, funcName);
        FunctionInput funcInput = null;
        try {
            funcInput = client.getFunction(req);
        } catch (CatalogException ignore) {

        }
        return convertToFunction(funcInput);
    }

    private Function convertToFunction(FunctionInput funcInput) {
        if (funcInput == null) {
            return null;
        }

        List<ResourceUri> resourceUriList = new ArrayList<>();
        if (funcInput.getResourceUris() != null) {
            for (FunctionResourceUri fru : funcInput.getResourceUris()) {
                ResourceUri resourceUri = new ResourceUri();
                resourceUri
                    .setResourceType(org.apache.hadoop.hive.metastore.api.ResourceType.valueOf(fru.getType().name()));
                resourceUri.setUri(fru.getUri());
                resourceUriList.add(resourceUri);
            }
        }
        return new Function(funcInput.getFunctionName(),
            funcInput.getDatabaseName(),
            funcInput.getClassName(),
            funcInput.getOwner(),
            getOwnerType(funcInput.getOwnerType()),
                (int)funcInput.getCreateTime(),
            getFunctionType(funcInput.getFuncType()),
            resourceUriList);
    }

    private FunctionType getFunctionType(String funcType) {
        if (StringUtils.isNotEmpty(funcType)) {
            try {
                return FunctionType.valueOf(funcType);
            } catch (Exception e) {
                LOG.warn("Unknown funcType: {}", funcType);
            }
        }
        return FunctionType.JAVA;
    }

    @Override
    public List<Function> getAllFunctions() throws MetaException {
        return null;
    }


    public List<Function> getAllFunctions(String catalogName) throws MetaException {
        final GetAllFunctionRequest getAllFunctionRequest = new GetAllFunctionRequest(getProjectId(), catalogName);
        final FunctionInput[] functionInputs = client.getAllFunctions(getAllFunctionRequest).getObjects();
        return Arrays.stream(functionInputs).map(functionInput -> convertToFunction(functionInput))
            .collect(Collectors.toList());
    }

    @Override
    public List<String> getFunctions(String dbName, String pattern) throws MetaException {
        throw new MetaException("Unexpected IO path");
    }

    public List<String> getFunctions(String catalogName, String dbName, String pattern) throws MetaException {
        ListFunctionRequest req = new ListFunctionRequest(getProjectId(), catalogName, dbName, pattern);
        String[] functionsPagedList = client.listFunctions(req).getObjects();
        return Arrays.stream(functionsPagedList).collect(Collectors.toList());
    }

    @Override
    public AggrStats get_aggr_stats_for(String dbName, String tblName, List<String> partNames,
            List<String> colNames) throws MetaException, NoSuchObjectException {
        return null;
    }

    public AggrStats getAggrColStats(String catalogName, String dbName, String tblName, List<String> partNames,
            List<String> colNames) throws MetaException, NoSuchObjectException {
        GetAggregateColumnStatisticsRequest request = new GetAggregateColumnStatisticsRequest(getProjectId(),
                catalogName, dbName, tblName, new GetPartitionColumnStaticsInput(partNames, colNames));
        return HiveDataAccessor.toAggrStats(client.getAggrColStats(request));
    }

    @Override
    public NotificationEventResponse getNextNotification(NotificationEventRequest rqst) {
        return null;
    }

    @Override
    public void addNotificationEvent(NotificationEvent event) {

    }

    @Override
    public void cleanNotificationEvents(int olderThan) {

    }

    @Override
    public CurrentNotificationEventId getCurrentNotificationEventId() {
        return null;
    }

    @Override
    public void flushCache() {

    }

    @Override
    public ByteBuffer[] getFileMetadata(List<Long> fileIds) throws MetaException {
        return new ByteBuffer[0];
    }

    @Override
    public void putFileMetadata(List<Long> fileIds, List<ByteBuffer> metadata,
            FileMetadataExprType type) throws MetaException {

    }

    @Override
    public boolean isFileMetadataSupported() {
        return false;
    }

    @Override
    public void getFileMetadataByExpr(List<Long> fileIds, FileMetadataExprType type, byte[] expr,
            ByteBuffer[] metadatas, ByteBuffer[] exprResults, boolean[] eliminated)
            throws MetaException {

    }

    @Override
    public FileMetadataHandler getFileMetadataHandler(FileMetadataExprType type) {
        return null;
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
    public List<SQLPrimaryKey> getPrimaryKeys(String db_name, String tbl_name)
        throws MetaException {
        return null;
    }

    @Override
    public List<SQLForeignKey> getForeignKeys(String parent_db_name, String parent_tbl_name,
            String foreign_db_name, String foreign_tbl_name) throws MetaException {
        return null;
    }

    @Override
    public void createTableWithConstraints(Table tbl, List<SQLPrimaryKey> primaryKeys,
            List<SQLForeignKey> foreignKeys) throws InvalidObjectException, MetaException {

    }

    @Override
    public void dropConstraint(String dbName, String tableName, String constraintName)
        throws NoSuchObjectException {

    }

    @Override
    public void addPrimaryKeys(List<SQLPrimaryKey> pks)
        throws InvalidObjectException, MetaException {

    }

    @Override
    public void addForeignKeys(List<SQLForeignKey> fks)
        throws InvalidObjectException, MetaException {

    }

    private void initClientIfNeeded() {
        if (client == null) {
            synchronized (this) {
                if (client == null) {
                    client = PolyCatClient.getInstance(conf);
                }
            }
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
        initClientIfNeeded();
    }

    private String getCurrentCatalog() {
        return getConf().get("hive.hmsbridge.defaultCatalogName", "dash");
    }

    private String getCatName(Table table) {
        return table.getParameters().get(LMS_NAME);
    }

    protected String getProjectId() {
        try {
            String userId = UserGroupInformation.getCurrentUser().getUserName();
            final String[] project2User = userId.split("#");
            if (project2User.length == 2) {
                return project2User[0];
            } else {
                return client.getContext().getProjectId();
            }
        } catch (IOException e) {
            LOG.warn("Cannot get ugi user, return the default projectId", e);
            return client.getContext().getProjectId();
        }
    }

    protected String getUserId(String owner) {
        if (owner == null) {
            return getUserId();
        }
        String[] split = owner.split("#");
        if (split.length == 2) {
            return split[1];
        } else {
            return owner;
        }
    }

    protected String getUserId() {
        try {
            String userId = UserGroupInformation.getCurrentUser().getUserName();
            final String[] project2User = userId.split("#");
            if (project2User.length == 2) {
                return project2User[1];
            } else {
                return userId;
            }
        } catch (IOException e) {
            LOG.warn("Cannot get ugi user, return the polycat user", e);
            return client.getContext().getUserName();
        }
    }

    private String getTenantName() {
        return client.getContext().getTenantName();
    }
}
