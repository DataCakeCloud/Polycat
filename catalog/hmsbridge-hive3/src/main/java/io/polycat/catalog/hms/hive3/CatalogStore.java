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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.polycat.catalog.client.PolyCatClient;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.model.Column;
import io.polycat.catalog.common.model.Database;
import io.polycat.catalog.common.model.PagedList;
import io.polycat.catalog.common.model.PartitionAlterContext;
import io.polycat.catalog.common.model.base.PartitionInput;
import io.polycat.catalog.common.model.stats.PartitionStatisticData;
import io.polycat.catalog.common.plugin.CatalogPlugin;
import io.polycat.catalog.common.plugin.request.CreateCatalogRequest;
import io.polycat.catalog.common.plugin.request.AlterCatalogRequest;
import io.polycat.catalog.common.plugin.request.AlterDatabaseRequest;
import io.polycat.catalog.common.plugin.request.CreateDatabaseRequest;
import io.polycat.catalog.common.plugin.request.CreateTableRequest;
import io.polycat.catalog.common.plugin.request.DeleteDatabaseRequest;
import io.polycat.catalog.common.plugin.request.DeleteTableRequest;
import io.polycat.catalog.common.plugin.request.DropCatalogRequest;
import io.polycat.catalog.common.plugin.request.GetCatalogRequest;
import io.polycat.catalog.common.plugin.request.GetTableRequest;
import io.polycat.catalog.common.plugin.request.*;
import io.polycat.catalog.common.plugin.request.input.*;
import io.polycat.catalog.common.utils.ConfUtil;
import io.polycat.catalog.common.utils.PartitionUtil;
import io.polycat.hivesdk.hive3.tools.HiveDataAccessor;
import io.polycat.hivesdk.hive3.tools.PolyCatDataAccessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import io.polycat.catalog.common.plugin.request.ListDatabasesRequest;
import io.polycat.catalog.common.plugin.request.ListTablesRequest;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;
import io.polycat.catalog.common.plugin.request.input.TableInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.FileMetadataHandler;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.ColStatsObjWithSourceInfo;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.FullTableName;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionExpressionForMetastore;
import org.apache.thrift.TException;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.polycat.hivesdk.hive3.tools.PolyCatDataAccessor.toDatabaseInput;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.polycat.hivesdk.hive3.tools.HiveDataAccessor.*;
import static io.polycat.hivesdk.hive3.tools.PolyCatDataAccessor.*;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.prependCatalogToDbName;
import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;

/**
 * A RawStore implementation that uses PolyCatClient
 */
@Slf4j
public class CatalogStore implements RawStore {

    private CatalogPlugin client;

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

    private Configuration conf;

    public CatalogStore() {
    }

    public CatalogStore(Configuration conf) {
        this.conf = conf;
        initClientIfNeeded();
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
    public void createCatalog(org.apache.hadoop.hive.metastore.api.Catalog mCat) {
        CreateCatalogRequest request = new CreateCatalogRequest();
        final CatalogInput catalogInput = toCatalogInput(mCat);
        catalogInput.setCatalogName(getCatalogMappingName(mCat.getName()));
        request.setInput(catalogInput);
        request.setProjectId(getProjectId());
        client.createCatalog(request);
    }

    @Override
    public void alterCatalog(String catName, Catalog cat) throws MetaException, InvalidOperationException {
        if (!catName.equals(cat.getName()) && ConfUtil.hasMappingName(catName)) {
            throw new InvalidOperationException("Alter mapping catalog name is not allowed! ");
        }
        StoreDataWrapper<Void> wrapper = StoreDataWrapper.getInstance(null);
        wrapper.wrapper(() -> {
            final AlterCatalogRequest alterCatalogRequest = new AlterCatalogRequest(getProjectId(), getCatalogMappingName(catName));
            final CatalogInput catalogInput = toCatalogInput(cat);
            catalogInput.setCatalogName(getCatalogMappingName(cat.getName()));
            alterCatalogRequest.setInput(catalogInput);
            client.alterCatalog(alterCatalogRequest);
            return null;
        });
        wrapper.throwMetaException();
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Catalog getCatalog(String catalogName) throws NoSuchObjectException, MetaException {
        StoreDataWrapper<org.apache.hadoop.hive.metastore.api.Catalog> wrapper = StoreDataWrapper.getInstance(null, ErrorCode.CATALOG_NOT_FOUND, NoSuchObjectException.class);
        wrapper.wrapper(() -> {
            GetCatalogRequest request = new GetCatalogRequest(getProjectId(), getCatalogMappingName(catalogName));
            io.polycat.catalog.common.model.Catalog dCat = client.getCatalog(request);
            dCat.setCatalogName(catalogName);
            return toCatalog(dCat);
        });
        wrapper.throwNoSuchObjectException();
        wrapper.throwMetaException();
        return wrapper.getData();
    }

    @Override
    public List<String> getCatalogs() throws MetaException {
        /*StoreDataWrapper<List<String>> wrapper = StoreDataWrapper.getInstance(null);
        wrapper.wrapper(() -> {
            final ListCatalogsRequest listCatalogsRequest = new ListCatalogsRequest(getProjectId());
            final PagedList<Catalog> catalogs = client.listCatalogs(listCatalogsRequest);
            return Arrays.stream(catalogs.getObjects())
                    .map(catalog -> catalog.getCatalogName())
                    .collect(Collectors.toList());
        });
        wrapper.throwMetaException();
        return wrapper.getData();*/
        //由于catalog mapping的存在以及当前polycat中的某些catalog是作为数据源使用的，所以临时返回hive中的所有mapping的key。
        return new ArrayList<>(ConfUtil.getMappingKeys());
    }

    @Override
    public void dropCatalog(String catalogName) throws NoSuchObjectException, MetaException {
        if (ConfUtil.hasMappingName(catalogName)) {
            throw new MetaException("Drop mapping catalog is not allowed.");
        }
        StoreDataWrapper<Void> wrapper = StoreDataWrapper.getInstance(null, ErrorCode.CATALOG_NOT_FOUND, NoSuchObjectException.class);
        wrapper.wrapper(() -> {
            final DropCatalogRequest dropCatalogRequest = new DropCatalogRequest(getProjectId(), catalogName);
            client.dropCatalog(dropCatalogRequest);
            return null;
        });
        wrapper.throwNoSuchObjectException();
        wrapper.throwMetaException();
    }

    @Override
    public void createDatabase(org.apache.hadoop.hive.metastore.api.Database hiveDatabase) {
        log.info("createDatabase,db name is:" + hiveDatabase.getName());
        CreateDatabaseRequest createDatabaseRequest = new CreateDatabaseRequest();
        createDatabaseRequest.setProjectId(getProjectId());
        createDatabaseRequest.setCatalogName(getCatalogMappingName(hiveDatabase.getCatalogName()));
        final DatabaseInput databaseInput = toDatabaseInput(hiveDatabase, client.getContext().getTenantName(), client.getContext().getAuthSourceType());
        databaseInput.setCreateTime(System.currentTimeMillis());
        databaseInput.setCatalogName(getCatalogMappingName(hiveDatabase.getCatalogName()));
        createDatabaseRequest.setInput(databaseInput);
        client.createDatabase(createDatabaseRequest);
    }

    private String getUserId() {
        return PolyCatDataAccessor.getUserId(getDefaultUser());
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Database getDatabase(String catName, String dbname) throws NoSuchObjectException {
        org.apache.hadoop.hive.metastore.api.Database result = null;
        StoreDataWrapper<org.apache.hadoop.hive.metastore.api.Database> wrapper = StoreDataWrapper.getInstance(result, ErrorCode.DATABASE_NOT_FOUND, NoSuchObjectException.class);
        wrapper.wrapper(() -> {
            final Database polycatDatabase = getPolyCatDatabase(getCatalogMappingName(catName), dbname);
            polycatDatabase.setCatalogName(catName);
            return toDatabase(polycatDatabase);
        });
        wrapper.throwNoSuchObjectException();
        return wrapper.getData();
    }

    @Override
    public boolean dropDatabase(String catalogName, String dbname) throws NoSuchObjectException, MetaException {
        boolean success = false;
        StoreDataWrapper<Boolean> wrapper = StoreDataWrapper.getInstance(success, ErrorCode.DATABASE_NOT_FOUND, NoSuchObjectException.class);
        wrapper.wrapper(() -> {
            DeleteDatabaseRequest deleteDatabaseRequest = new DeleteDatabaseRequest();
            deleteDatabaseRequest.setCatalogName(getCatalogMappingName(catalogName));
            deleteDatabaseRequest.setDatabaseName(dbname);
            deleteDatabaseRequest.setProjectId(getProjectId());
            client.deleteDatabase(deleteDatabaseRequest);
            return true;
        });
        wrapper.throwNoSuchObjectException();
        wrapper.throwMetaException();
        return wrapper.getData();
    }

    private Database getPolyCatDatabase(String catName, String dbname) {
        GetDatabaseRequest request = new GetDatabaseRequest();
        request.setCatalogName(catName);
        request.setDatabaseName(dbname);
        request.setProjectId(getProjectId());
        return client.getDatabase(request);
    }

    @Override
    public boolean alterDatabase(String catName, String dbName, org.apache.hadoop.hive.metastore.api.Database mDb) throws NoSuchObjectException, MetaException {
        StoreDataWrapper<Boolean> wrapper = StoreDataWrapper.getInstance(false, ErrorCode.DATABASE_NOT_FOUND, NoSuchObjectException.class);
        wrapper.wrapper(() -> {
            getPolyCatDatabase(getCatalogMappingName(catName), dbName);
            DatabaseInput databaseInput = toDatabaseInput(mDb, client.getContext().getTenantName(), client.getContext().getAuthSourceType());
            databaseInput.setCatalogName(getCatalogMappingName(catName));
            AlterDatabaseRequest alterDatabaseRequest = new AlterDatabaseRequest();
            alterDatabaseRequest.setInput(databaseInput);
            alterDatabaseRequest.setDatabaseName(dbName);
            alterDatabaseRequest.setProjectId(getProjectId());
            alterDatabaseRequest.setCatalogName(getCatalogMappingName(normalizeIdentifier(catName)));
            client.alterDatabase(alterDatabaseRequest);
            return true;
        });
        wrapper.throwNoSuchObjectException();
        wrapper.throwMetaException();
        return wrapper.getData();
    }

    @Override
    public List<String> getDatabases(String catName, String pattern) throws MetaException {
        StoreDataWrapper<List<String>> wrapper = StoreDataWrapper.getInstance(new ArrayList<>());
        wrapper.wrapper(() ->listDatabases(getCatalogMappingName(catName), pattern));
        wrapper.throwMetaException();
        return wrapper.getData();
    }

    private List<String> listDatabases(String catName, String pattern) {
        ListDatabasesRequest request = new ListDatabasesRequest();
        request.setProjectId(getProjectId());
        request.setCatalogName(catName);
        request.setMaxResults(Integer.MAX_VALUE);
        if (!StringUtils.isEmpty(pattern)) {
            request.setFilter(pattern);
        }
        return Arrays.stream(client.listDatabases(request).getObjects())
                .map(Database::getDatabaseName)
                .collect(Collectors.toList());
    }

    @Override
    public List<String> getAllDatabases(String catName) throws MetaException {
        log.info("show databases,catalog is:" + catName);
        StoreDataWrapper<List<String>> wrapper = StoreDataWrapper.getInstance(new ArrayList<>());
        wrapper.wrapper(() ->listDatabases(getCatalogMappingName(catName), null));
        wrapper.throwMetaException();
        return wrapper.getData();
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
    public void createTable(org.apache.hadoop.hive.metastore.api.Table table) throws InvalidObjectException, MetaException {
        StoreDataWrapper<Void> wrapper = StoreDataWrapper.getInstance(null, ErrorCode.DATABASE_NOT_FOUND, InvalidObjectException.class);
        wrapper.wrapper(() -> {
            TableInput tableInput = PolyCatDataAccessor.toTableInput(table, getDefaultUser());
            tableInput.setCatalogName(getCatalogMappingName(getCatalog(table)));
            CreateTableRequest createTableRequest = new CreateTableRequest();
            createTableRequest.setInput(tableInput);
            createTableRequest.setCatalogName(getCatalogMappingName(getCatalog(table)));
            createTableRequest.setDatabaseName(table.getDbName());
            createTableRequest.setProjectId(getProjectId());
            client.createTable(createTableRequest);
            return null;
        });
        wrapper.throwInvalidObjectException();
        wrapper.throwMetaException();
        log.info("createTable success.");
    }

    private String getCatalog(org.apache.hadoop.hive.metastore.api.Table table) {
        return table.isSetCatName() ? table.getCatName() : getDefaultCatalog(conf);
    }

    @Override
    public boolean dropTable(String catName, String dbName, String tableName)
            throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        if (tableName == null) {
            throw new InvalidInputException("Table name is null.");
        }
        StoreDataWrapper<Boolean> wrapper = StoreDataWrapper.getInstance(false, ErrorCode.TABLE_NOT_FOUND, NoSuchObjectException.class);
        wrapper.wrapper(() -> {
            DeleteTableRequest deleteTableRequest = new DeleteTableRequest();
            deleteTableRequest.setCatalogName(getCatalogMappingName(catName));
            deleteTableRequest.setDatabaseName(dbName);
            deleteTableRequest.setTableName(tableName);
            deleteTableRequest.setProjectId(getProjectId());
            deleteTableRequest.setPurgeFlag(false);
            client.deleteTable(deleteTableRequest);
            return true;
        });
        wrapper.throwNoSuchObjectException();
        wrapper.throwMetaException();
        log.info("dropTable success.");
        return wrapper.getData();
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Table getTable(String catName, String dbName, String tableName) throws MetaException {
        StoreDataWrapper<org.apache.hadoop.hive.metastore.api.Table> wrapper = StoreDataWrapper.getInstance(null);
        wrapper.wrapper(() -> {
            GetTableRequest getTableRequest = new GetTableRequest(getProjectId(), getCatalogMappingName(catName), dbName, tableName);
            io.polycat.catalog.common.model.Table lmsTable = client.getTable(getTableRequest);
            lmsTable.setCatalogName(getCatalogMappingName(catName));
            return toTable(lmsTable);
        });
        wrapper.throwMetaException();
        return wrapper.getData();
    }

    @Override
    public boolean addPartition(Partition partition) throws InvalidObjectException, MetaException {
        StoreDataWrapper<Boolean> wrapper = StoreDataWrapper.getInstance(false);
        wrapper.putCodeException(ErrorCode.DATABASE_NOT_FOUND, InvalidObjectException.class);
        wrapper.putCodeException(ErrorCode.TABLE_NOT_FOUND, InvalidObjectException.class);
        String catNameMapping = getCatalogMappingName(partition.getCatName());
        Table table = getTable(partition.getCatName(), partition.getDbName(), partition.getTableName());
        wrapper.wrapper(() -> {
            PartitionInput partitionBase = toPartitionBaseInput(partition);
            partitionBase.setCatalogName(catNameMapping);

            table.setCatName(catNameMapping);
            if (partitionBase.getStorageDescriptor().getLocation() == null && !isViewTableType(table)) {
                partitionBase.getStorageDescriptor().setLocation(table.getSd().getLocation() + File.separator + makePartitionName(table, partition.getValues()));
            }
            AddPartitionInput partitionInput = new AddPartitionInput();
            partitionInput.setPartitions(new PartitionInput[]{partitionBase});
            AddPartitionRequest request = new AddPartitionRequest(getProjectId(), catNameMapping,
                    table.getDbName(), table.getTableName(), partitionInput);
            client.addPartition(request);
            return true;
        });
        wrapper.throwInvalidObjectException();
        wrapper.throwMetaException();
        return wrapper.getData();
    }

    private boolean isViewTableType(Table table) {
        return TableType.VIRTUAL_VIEW.name().equals(table.getTableType());
    }

    @Override
    public boolean addPartitions(String catName, String dbName, String tblName, List<Partition> parts)
            throws InvalidObjectException, MetaException {
        StoreDataWrapper<Boolean> wrapper = StoreDataWrapper.getInstance(false);
        wrapper.putCodeException(ErrorCode.DATABASE_NOT_FOUND, InvalidObjectException.class);
        wrapper.putCodeException(ErrorCode.TABLE_NOT_FOUND, InvalidObjectException.class);
        String catNameMapping = getCatalogMappingName(catName);
        Table table = getTable(catName, dbName, tblName);
        wrapper.wrapper(() -> {
            List<PartitionInput> partitionBaseList = new ArrayList<>();
            PartitionInput partitionBase;
            for (org.apache.hadoop.hive.metastore.api.Partition part : parts) {
                partitionBase = PolyCatDataAccessor.toPartitionBaseInput(part);
                partitionBase.setCatalogName(catNameMapping);
                if (partitionBase.getStorageDescriptor().getLocation() == null) {
                    partitionBase.getStorageDescriptor().setLocation(table.getSd().getLocation() + File.separator + makePartitionName(table, part.getValues()));
                }
                partitionBaseList.add(partitionBase);
            }
            AddPartitionInput partitionInput = new AddPartitionInput();
            partitionInput.setPartitions(partitionBaseList.toArray(new PartitionInput[0]));
            AddPartitionRequest request = new AddPartitionRequest(getProjectId(), catNameMapping, dbName, tblName,
                    partitionInput);
            client.addPartition(request);
            return true;
        });
        wrapper.throwInvalidObjectException();
        wrapper.throwMetaException();
        return wrapper.getData();
    }

    @Override
    public boolean addPartitions(String catName, String dbName, String tblName,
                                 PartitionSpecProxy partitionSpec, boolean ifNotExists)
            throws InvalidObjectException, MetaException {
        List<Partition> partitions = new ArrayList<>();
        partitionSpec.getPartitionIterator().forEachRemaining(partitions::add);
        List<String> parts = partitions.stream().flatMap(partition -> partition.getValues().stream())
                .collect(Collectors.toList());
        org.apache.hadoop.hive.metastore.api.Partition partition = null;
        try {
            partition = getPartition(catName, dbName, tblName, parts);
            throw new MetaException("Partition already exists: " + partition);
        } catch (NoSuchObjectException e) {
            if (ifNotExists) {
                return true;
            }
        }
        return addPartitions(catName, dbName, tblName, partitions);
    }

    @Override
    public Partition getPartition(String catName, String dbName, String tableName,
                                  List<String> partVals)
            throws MetaException, NoSuchObjectException {
        String catNameMapping = getCatalogMappingName(catName);
        Table table = getTable(catName, dbName, tableName);
        table.setCatName(catName);
        final StoreDataWrapper<Partition> wrapper = StoreDataWrapper.getInstance(null, ErrorCode.PARTITION_VALUES_NOT_MATCH, NoSuchObjectException.class);
        wrapper.wrapper(() -> {
            io.polycat.catalog.common.model.Partition lmsPartition = getLmPartition(catNameMapping, dbName, tableName, partVals, table);
            if (lmsPartition == null) {
                return null;
            }
            lmsPartition.setCatalogName(catName);
            return HiveDataAccessor.toPartition(lmsPartition, table.getSd());
        });
        wrapper.throwNoSuchObjectException();
        wrapper.throwMetaException();
        if (wrapper.getData() == null) {
             throw new NoSuchObjectException("partition values="
                     + partVals.toString());
        }
        return wrapper.getData();
    }

    private io.polycat.catalog.common.model.Partition getLmPartition(String catName, String dbName, String tableName, List<String> partVals,
                                                                      Table table) {
        String partitionName = makePartitionName(table, partVals);
        String partitionNameWithoutEscape = makePartitionNameWithoutEscape(table, partVals);
        GetPartitionRequest request = new GetPartitionRequest(getProjectId(), catName, dbName, tableName,
                partitionName);
        final io.polycat.catalog.common.model.Partition partition = client.getPartition(request);
        if (partition == null && !partitionName.equals(partitionNameWithoutEscape)) {
            log.info("Try to get partition: {} via without escapePath: {}", partitionName, partitionNameWithoutEscape);
            request = new GetPartitionRequest(getProjectId(), catName, dbName, tableName,
                    partitionNameWithoutEscape);
            return client.getPartition(request);
        }
        return partition;
    }

    @Override
    public boolean doesPartitionExist(String catName, String dbName, String tableName,
                                      List<String> partVals)
            throws MetaException {
        final StoreDataWrapper<Boolean> wrapper = StoreDataWrapper.getInstance(false);
        wrapper.wrapper(() -> {
            final PartitionValuesInput partitionValuesInput = new PartitionValuesInput();
            partitionValuesInput.setPartitionValues(partVals);
            final DoesPartitionExistsRequest doesPartitionExistsRequest = new DoesPartitionExistsRequest(getProjectId(),
                    getCatalogMappingName(catName), dbName, tableName, partitionValuesInput);
            return client.doesPartitionExist(doesPartitionExistsRequest);
        });
        wrapper.throwMetaException();
        return wrapper.getData();
    }

    @Override
    public boolean dropPartition(String catName, String dbName, String tblName,
                                 List<String> partVals)
            throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        Table table = getTable(catName, dbName, tblName);
        if (table.getPartitionKeys().size() != partVals.size() || table.getPartitionKeys().size() == 0) {
            throw new MetaException(
                    "The number of input partition column values is not equal to the number of partition table columns");
        }
        if (tblName == null) {
            throw new InvalidInputException("Table name is null.");
        }
        final StoreDataWrapper<Boolean> wrapper = StoreDataWrapper.getInstance(false);
        wrapper.wrapper(() -> {
            List<String> partNames = new ArrayList<>();
            List<String> partitionKeys = table.getPartitionKeys().stream()
                    .map(FieldSchema::getName).collect(Collectors.toList());
            partNames.add(PartitionUtil.makePartitionName(partitionKeys, partVals));
            DropPartitionInput dropPartitionInput = new DropPartitionInput();
            dropPartitionInput.setPartitionNames(partNames);
            DropPartitionRequest request = new DropPartitionRequest(getProjectId(), getCatalogMappingName(catName),
                    dbName, tblName, dropPartitionInput);
            try {
                client.dropPartition(request);
            } catch (CatalogException e) {
                log.warn(e.getMessage() + "\n try to delete partition names");
                final List<String> deprecatedPartVals = partVals.stream()
                        .map(p -> PartitionUtil.escapePathName(FileUtils.unescapePathName(p)))
                        .collect(Collectors.toList());
                final ArrayList<String> deprecatedPartNames = new ArrayList<>();
                deprecatedPartNames.add(PartitionUtil.makePartitionName(partitionKeys, deprecatedPartVals));
                dropPartitionInput.setPartitionNames(deprecatedPartNames);
                client.dropPartition(request);
            }
            return true;
        });
        wrapper.throwMetaException();
        return wrapper.getData();
    }

    @Override
    public List<Partition> getPartitions(String catName, String dbName, String tableName, int max)
            throws MetaException, NoSuchObjectException {
        Table table = getTable(catName, dbName, tableName);
        if (table == null) {
            throw new NoSuchObjectException(String.format("Table does not exist: %s.%s.%s", catName, dbName, tableName));
        }
        final StoreDataWrapper<List<Partition>> wrapper = StoreDataWrapper.getInstance(new ArrayList<>());
        wrapper.wrapper(() -> {
            ListFileRequest listFileRequest = new ListFileRequest(getProjectId(), getCatalogMappingName(catName), dbName, tableName,
                    getFilterInput(max));
            List<io.polycat.catalog.common.model.Partition> partitions = client.listPartitions(listFileRequest);
            partitions.forEach(partition -> partition.setCatalogName(getCatalogMappingName(catName)));
            return HiveDataAccessor.toPartitions(partitions, table.getSd());
        });
        wrapper.throwMetaException();
        return wrapper.getData();
    }

    private FilterInput getFilterInput(Integer max) {
        FilterInput filterInput = new FilterInput();
        if (max != null) {
            filterInput.setLimit(max);
        }
        return filterInput;
    }

    @Override
    public void alterTable(String catName, String dbName, String tblName, org.apache.hadoop.hive.metastore.api.Table newt) throws InvalidObjectException, MetaException {
        if (newt == null) {
            throw new InvalidObjectException("new table is invalid");
        }
        final StoreDataWrapper<Void> wrapper = StoreDataWrapper.getInstance(null);
        wrapper.putCodeException(ErrorCode.TABLE_NOT_FOUND, MetaException.class);
        wrapper.putCodeException(ErrorCode.DATABASE_NOT_FOUND, InvalidObjectException.class);
        AlterColumnContext context = new AlterColumnContext();
        final String catNameMapping = getCatalogMappingName(catName);
        final boolean alterTableColumn = isAlterTableColumn(catNameMapping, dbName, tblName, newt, context);
        wrapper.wrapper(() -> {
            if (alterTableColumn) {
                alterTableColumn(catNameMapping, dbName, tblName, context);
            }
            final TableInput tableInput = toTableInput(newt);
            tableInput.setCatalogName(catNameMapping);
            AlterTableInput alterTableInput = new AlterTableInput(tableInput, null);
            AlterTableRequest alterTableRequest = new AlterTableRequest();
            alterTableRequest.setInput(alterTableInput);
            alterTableRequest.setTableName(tblName);
            alterTableRequest.setCatalogName(catNameMapping);
            alterTableRequest.setDatabaseName(dbName);
            alterTableRequest.setProjectId(getProjectId());
            client.alterTable(alterTableRequest);
            return null;
        });
        wrapper.throwInvalidObjectException();
        wrapper.throwMetaException();
    }

    private void alterTableColumn(String caName, String dbName, String tblName, AlterColumnContext context) {
        ColumnChangeInput colChangeIn = new ColumnChangeInput();
        colChangeIn.setChangeType(context.getColumnOperation());
        if (context.getColumnOperation() == Operation.ADD_COLUMN) {
            List<Column> addColumnList = new ArrayList<>();
            for (FieldSchema fieldSchema : context.getAddColumns()) {
                addColumnList.add(PolyCatDataAccessor.convertToColumnInput(fieldSchema));
            }
            colChangeIn.setColumnList(addColumnList);
        } else if (context.getColumnOperation() == Operation.CHANGE_COLUMN) {
            Map<String, Column> changeColumnMap = new HashMap<>();
            changeColumnMap.put(context.getModifyColumn().getName(), PolyCatDataAccessor.convertToColumnInput(context.getModifyColumn()));
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

    private boolean isAlterTableColumn(String catName, String dbName, String tblName,
                                       org.apache.hadoop.hive.metastore.api.Table newTable, AlterColumnContext context)
            throws MetaException {
        org.apache.hadoop.hive.metastore.api.Table oldTable = getTable(catName, dbName, tblName);
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

    private List<FieldSchema> buildColumnSchema(org.apache.hadoop.hive.metastore.api.Table table) {
        if (table.getParameters().containsKey("spark.sql.sources.provider") &&
                "csv".equalsIgnoreCase(table.getParameters().get("spark.sql.sources.provider"))) {
            int partNum = Integer.parseInt(table.getParameters().get("spark.sql.sources.schema.numParts"));
            List<FieldSchema> columnSchemas = new ArrayList<>();
            for (int i = 0; i < partNum; i++) {
                columnSchemas.addAll(getFieldSchemas(table, i));
            }
            return removePartitionSchemas(columnSchemas, table.getPartitionKeys());
        } else {
            return table.getSd().getCols();
        }
    }

    private List<FieldSchema> removePartitionSchemas(List<FieldSchema> columnSchemas,
                                                     List<FieldSchema> partitionSchemas) {
        Set<String> names = partitionSchemas.stream().map(FieldSchema::getName).collect(Collectors.toSet());
        return columnSchemas.stream().filter(x -> !names.contains(x.getName())).collect(Collectors.toList());
    }

    private FieldSchema convertToFieldSchema(io.polycat.catalog.hms.hive3.Schema schema) {
        String comment = ((JSONObject) schema.getMetadata()).getString("comment");
        return new FieldSchema(schema.getName(), schema.getType(), comment);
    }

    private List<FieldSchema> getFieldSchemas(org.apache.hadoop.hive.metastore.api.Table table, int idx) {
        String struct = table.getParameters().get("spark.sql.sources.schema.part." + idx);
        JSONObject structJson = JSONObject.parseObject(struct);
        JSONArray schemas = structJson.getJSONArray("fields");
        io.polycat.catalog.hms.hive3.Schema[] fieldSchemas = schemas.toJavaObject(Schema[].class);
        return Arrays.stream(fieldSchemas).map(this::convertToFieldSchema).collect(Collectors.toList());
    }

    @Override
    public void updateCreationMetadata(String catName, String dbName, String tableName, CreationMetadata cm)
            throws MetaException {

    }

    @Override
    public List<String> getTables(String catName, String dbName, String pattern) throws MetaException {
        List<String> res = new ArrayList<>();
        final StoreDataWrapper<List<String>> wrapper = StoreDataWrapper.getInstance(new ArrayList<>());
        wrapper.wrapper(() -> {
            ListTablesRequest request = new ListTablesRequest();
            request.setCatalogName(getCatalogMappingName(catName));
            request.setDatabaseName(dbName);
            request.setProjectId(getProjectId());
            request.setExpression(pattern);
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
        });
        wrapper.throwMetaException();
        return wrapper.getData();
    }

    @Override
    public List<String> getTables(String catName, String dbName, String pattern, TableType tableType) throws MetaException {
        List<String> res = new ArrayList<>();
        StoreDataWrapper<List<String>> wrapper = StoreDataWrapper.getInstance(res);
        wrapper.wrapper(() -> {
            ListTablesRequest request = new ListTablesRequest();
            request.setCatalogName(getCatalogMappingName(catName));
            request.setDatabaseName(dbName);
            request.setProjectId(getProjectId());
            request.setExpression(pattern);
            io.polycat.catalog.common.model.Table[] tablePagedList = client.listTables(request).getObjects();
            for (io.polycat.catalog.common.model.Table table : tablePagedList) {
                if (table.getTableType().equalsIgnoreCase(tableType.toString())) {
                    res.add(table.getTableName());
                }
            }
            return res;
        });
        return wrapper.getData();
    }

    @Override
    public List<String> getMaterializedViewsForRewriting(String catName, String dbName)
            throws MetaException, NoSuchObjectException {
        return getTables(catName, dbName, ".*", TableType.MATERIALIZED_VIEW);
    }

    @Override
    public List<TableMeta> getTableMeta(String catName, String dbNames, String tableNames,
                                        List<String> tableTypes) throws MetaException {
        return new ArrayList<>();
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Table> getTableObjectsByName(String catName, String dbName, List<String> tableNames)
            throws MetaException, UnknownDBException {
        List<org.apache.hadoop.hive.metastore.api.Table> tables = listTableObjects(getCatalogMappingName(catName), dbName, tableNames);
        Set<String> set = new HashSet<>(tableNames);
        return tables.stream().filter(t -> set.contains(t.getTableName())).peek(table -> table.setCatName(catName)).collect(Collectors.toList());
    }

    private List<org.apache.hadoop.hive.metastore.api.Table> listTableObjects(String catalogName, String dbname, List<String> tableNames) throws MetaException, UnknownDBException {
        List<org.apache.hadoop.hive.metastore.api.Table> tables = new ArrayList<>();
        StoreDataWrapper<List<Table>> wrapStoreData = StoreDataWrapper.getInstance(tables, ErrorCode.DATABASE_NOT_FOUND, UnknownDBException.class);
        wrapStoreData.wrapper(() -> {
            FilterListInput filterListInput = new FilterListInput();
            if (tableNames != null && !tableNames.isEmpty()) {
                filterListInput.setFilter(tableNames);
            }
            filterListInput.setFilter(tableNames);
            ListTableObjectsRequest request = new ListTableObjectsRequest(getProjectId(), catalogName, dbname, filterListInput);
            io.polycat.catalog.common.model.Table[] tableList;
            String pageToken;
            do {
                PagedList<io.polycat.catalog.common.model.Table> tablePagedList = client.listTables(request);
                tableList = tablePagedList.getObjects();
                pageToken = tablePagedList.getNextMarker();
                request.setNextToken(pageToken);
                for (io.polycat.catalog.common.model.Table table : tableList) {
                    tables.add(toTable(table));
                }
            } while (StringUtils.isNotEmpty(pageToken));
            return tables;
        });
        wrapStoreData.throwUnknownDBException();
        wrapStoreData.throwMetaException();
        return wrapStoreData.getData();
    }

    @Override
    public List<String> getAllTables(String catName, String dbName) throws MetaException {
        return getTables(catName, dbName, "*");
    }

    @Override
    public List<String> listTableNamesByFilter(String catName, String dbName, String filter, short maxTables)
            throws MetaException {
        final StoreDataWrapper<List<String>> wrapper = StoreDataWrapper.getInstance(new ArrayList<>());
        wrapper.wrapper(() -> {
            GetTableNamesRequest getTableNamesRequest = new GetTableNamesRequest();
            getTableNamesRequest.setFilter(filter);
            getTableNamesRequest.setProjectId(getProjectId());
            getTableNamesRequest.setCatalogName(getCatalogMappingName(catName));
            getTableNamesRequest.setDatabaseName(dbName);
            getTableNamesRequest.setMaxResults(String.valueOf(maxTables));
            return Arrays.asList(client.getTableNames(getTableNamesRequest).getObjects());
        });
        wrapper.throwMetaException();
        return wrapper.getData();
    }

    @Override
    public List<String> listPartitionNames(String catName, String dbName,
                                           String tblName, short maxParts) throws MetaException {
        final StoreDataWrapper<List<String>> wrapper = StoreDataWrapper.getInstance(new ArrayList<>());
        wrapper.wrapper(() -> {
            PartitionFilterInput filterInput = new PartitionFilterInput();
            filterInput.setMaxParts(maxParts);
            ListTablePartitionsRequest request = new ListTablePartitionsRequest(getProjectId(), getCatalogMappingName(catName), dbName, tblName,
                    filterInput);
            return client.listPartitionNames(request);
        });
        wrapper.throwMetaException();
        return wrapper.getData();
    }

    @Override
    public PartitionValuesResponse listPartitionValues(String catName, String dbName, String tblName,
                                                       List<FieldSchema> cols, boolean applyDistinct, String filter, boolean ascending,
                                                       List<FieldSchema> order, long maxParts) throws MetaException {
        //return new PartitionValuesResponse();
        return null;
    }

    @Override
    public void alterPartition(String catName, String dbName, String tblName, List<String> partVals,
                               Partition newPart)
            throws InvalidObjectException, MetaException {
        final StoreDataWrapper<Void> wrapper = StoreDataWrapper.getInstance(null);
        wrapper.wrapper(() -> {
            AlterPartitionInput input = new AlterPartitionInput();
            List<PartitionAlterContext> partitionContexts = new ArrayList<>();
            PartitionAlterContext context = convertToContext(newPart, partVals);
            partitionContexts.add(context);
            input.setPartitionContexts(partitionContexts.toArray(new PartitionAlterContext[0]));
            AlterPartitionRequest request = new AlterPartitionRequest(getProjectId(), getCatalogMappingName(catName), dbName, tblName,
                    input);
            client.alterPartitions(request);
            return null;
        });
        wrapper.throwMetaException();
    }

    @Override
    public void alterPartitions(String catName, String dbName, String tblName,
                                List<List<String>> partValsList, List<Partition> newParts)
            throws InvalidObjectException, MetaException {
        final StoreDataWrapper<Void> wrapper = StoreDataWrapper.getInstance(null);
        wrapper.wrapper(() -> {
            AlterPartitionInput input = new AlterPartitionInput();
            List<PartitionAlterContext> partitionContexts = new ArrayList<>(newParts.size());
            Iterator<List<String>> partValItr = partValsList.iterator();
            for (org.apache.hadoop.hive.metastore.api.Partition partition : newParts) {
                PartitionAlterContext context = convertToContext(partition, partValItr.next());
                partitionContexts.add(context);
            }
            input.setPartitionContexts(partitionContexts.toArray(new PartitionAlterContext[0]));
            AlterPartitionRequest request = new AlterPartitionRequest(getProjectId(), getCatalogMappingName(catName), dbName, tblName,
                    input);
            client.alterPartitions(request);
            return null;
        });
        wrapper.throwMetaException();
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

    @Override
    public List<Partition> getPartitionsByFilter(String catName, String dbName, String tblName, String filter, short maxParts)
            throws MetaException, NoSuchObjectException {
        Table table = getTable(catName, dbName, tblName);
        if (table == null) {
            throw new NoSuchObjectException(String.format("Table does not exist: %s.%s.%s", catName, dbName, tblName));
        }
        final StoreDataWrapper<List<Partition>> wrapper = StoreDataWrapper.getInstance(new ArrayList<>());
        wrapper.wrapper(() -> {
            PartitionFilterInput filterInput = new PartitionFilterInput();
            filterInput.setFilter(filter);
            filterInput.setMaxParts(maxParts);
            ListPartitionByFilterRequest request = new ListPartitionByFilterRequest(getProjectId(),
                    getCatalogMappingName(catName), dbName, tblName, filterInput);
            List<io.polycat.catalog.common.model.Partition> tablePartitions = client.getPartitionsByFilter(request);
            tablePartitions.forEach(partition -> partition.setCatalogName(catName));
            return HiveDataAccessor.toPartitions(tablePartitions, table.getSd());
        });
        wrapper.throwMetaException();
        return wrapper.getData();
    }



    @Override
    public boolean getPartitionsByExpr(String catName, String dbName, String tblName,
                                       byte[] expr, String defaultPartitionName, short maxParts, List<Partition> result) throws TException {
        String exprToFilter = new PartitionExpressionForMetastore().convertExprToFilter(expr);
        log.info("getPartitionsByExpr: {}, expr bytes: {}", exprToFilter, expr);
        Table table = getTable(catName, dbName, tblName);
        final StoreDataWrapper<Boolean> wrapper = StoreDataWrapper.getInstance(false);
        wrapper.wrapper(() -> {
            final GetPartitionsByExprInput getPartitionsByExprInput = new GetPartitionsByExprInput(defaultPartitionName,
                    expr, maxParts);
            final ListPartitionsByExprRequest listPartitionsByExprRequest = new ListPartitionsByExprRequest(getProjectId(),
                    getCatalogMappingName(catName), dbName, tblName, getPartitionsByExprInput);
            final List<io.polycat.catalog.common.model.Partition> partitions = client.listPartitionsByExpr(listPartitionsByExprRequest);
            partitions.forEach(partition -> result.add(HiveDataAccessor.toPartition(partition, table.getSd())));
            return false;
        });

        wrapper.throwMetaException();
        return wrapper.getData();
    }

    @Override
    public int getNumPartitionsByFilter(String catName, String dbName, String tblName, String filter)
            throws MetaException, NoSuchObjectException {
        final StoreDataWrapper<Integer> wrapper = StoreDataWrapper.getInstance(0, ErrorCode.TABLE_NOT_FOUND, NoSuchObjectException.class);
        wrapper.wrapper(() -> {
            GetPartitionCountRequest request = new GetPartitionCountRequest();
            request.setProjectId(getProjectId());
            request.setCatalogName(getCatalogMappingName(catName));
            request.setDatabaseName(dbName);
            request.setTableName(tblName);
            return client.getPartitionCount(request);
        });
        wrapper.throwNoSuchObjectException();
        wrapper.throwMetaException();
        return wrapper.getData();
    }

    @Override
    public int getNumPartitionsByExpr(String catName, String dbName, String tableName, byte[] expr)
            throws MetaException, NoSuchObjectException {
        return getNumPartitionsByFilter(catName, dbName, tableName, null);
    }

    @Override
    public List<Partition> getPartitionsByNames(String catName, String dbName, String tblName,
                                                List<String> partNames)
            throws MetaException, NoSuchObjectException {
        Table table = getTable(catName, dbName, tblName);
        final StoreDataWrapper<List<Partition>> wrapper = StoreDataWrapper.getInstance(new ArrayList<>(), ErrorCode.TABLE_NOT_FOUND, NoSuchObjectException.class);
        wrapper.wrapper(() -> {
            PartitionFilterInput filterInput = new PartitionFilterInput();
            filterInput.setPartNames(partNames.toArray(new String[0]));
            GetTablePartitionsByNamesRequest request = new GetTablePartitionsByNamesRequest(getProjectId(),
                    getCatalogMappingName(catName), dbName, tblName, filterInput);
            List<io.polycat.catalog.common.model.Partition> partitions = client.getPartitionsByNames(request);
            partitions.forEach(partition -> partition.setCatalogName(catName));
            return HiveDataAccessor.toPartitions(partitions, table.getSd());
        });
        wrapper.throwNoSuchObjectException();
        wrapper.throwMetaException();
        return wrapper.getData();
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Table markPartitionForEvent(String s, String s1, String s2, Map<String, String> map,
            PartitionEventType partitionEventType)
            throws MetaException, UnknownTableException, InvalidPartitionException, UnknownPartitionException {
        return new org.apache.hadoop.hive.metastore.api.Table();
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
    public Partition getPartitionWithAuth(String catName, String dbName, String tblName, List<String> partVals, String userName,
            List<String> groupNames) throws MetaException, NoSuchObjectException, InvalidObjectException {
        return getPartition(catName, dbName, tblName, partVals);
    }

    @Override
    public List<Partition> getPartitionsWithAuth(String catName, String dbName, String tblName, short maxParts, String userName, List<String> groupNames)
            throws MetaException, NoSuchObjectException, InvalidObjectException {
        return getPartitions(catName, dbName, tblName, maxParts);
    }

    @Override
    public List<String> listPartitionNamesPs(String catName, String dbName, String tblName, List<String> partVals, short maxParts)
            throws MetaException, NoSuchObjectException {
        final StoreDataWrapper<List<String>> wrapper = StoreDataWrapper.getInstance(new ArrayList<>(), ErrorCode.TABLE_NOT_FOUND, NoSuchObjectException.class);
        wrapper.wrapper(() -> {
            PartitionFilterInput filterInput = new PartitionFilterInput();
            filterInput.setMaxParts(maxParts);
            filterInput.setValues(partVals.toArray(new String[]{}));
            ListTablePartitionsRequest request = new ListTablePartitionsRequest(getProjectId(), getCatalogMappingName(catName), dbName, tblName,
                    filterInput);
            return client.listPartitionNames(request);
        });
        wrapper.throwNoSuchObjectException();
        wrapper.throwMetaException();
        return wrapper.getData();
    }

    @Override
    public List<Partition> listPartitionsPsWithAuth(String catName, String dbName, String tblName, List<String> partVals, short maxParts,
            String userName, List<String> groupNames) throws MetaException, InvalidObjectException, NoSuchObjectException {
        Table table = getTable(catName, dbName, tblName);
        final StoreDataWrapper<List<Partition>> wrapper = StoreDataWrapper.getInstance(new ArrayList<>(), ErrorCode.TABLE_NOT_FOUND, NoSuchObjectException.class);
        wrapper.wrapper(() -> {
            final GetPartitionsWithAuthInput getPartitionsWithAuthInput = new GetPartitionsWithAuthInput(maxParts, userName, groupNames, partVals);
            final ListPartitionsWithAuthRequest listPartitionsWithAuthRequest = new ListPartitionsWithAuthRequest(getProjectId(), getCatalogMappingName(catName), dbName, tblName, getPartitionsWithAuthInput);
            final List<io.polycat.catalog.common.model.Partition> partitions = client.listPartitionsPsWithAuth(listPartitionsWithAuthRequest);
            partitions.forEach(partition -> partition.setCatalogName(catName));
            return HiveDataAccessor.toPartitions(partitions, table.getSd());
        });
        wrapper.throwNoSuchObjectException();
        wrapper.throwMetaException();
        return wrapper.getData();
    }

    @Override
    public boolean updateTableColumnStatistics(ColumnStatistics colStats)
            throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
        ColumnStatisticsDesc statsDesc = colStats.getStatsDesc();
        if (statsDesc == null) {
            throw new InvalidObjectException("Invalid column stats object");
        }
        final StoreDataWrapper<Boolean> wrapper = StoreDataWrapper.getInstance(false, ErrorCode.TABLE_NOT_FOUND, NoSuchObjectException.class);
        wrapper.wrapper(() -> {
            UpdateTableColumnStatisticRequest request = null;
            try {
                request = new UpdateTableColumnStatisticRequest(getProjectId(),
                        getCatalogMappingName(colStats.getStatsDesc().getCatName()), statsDesc.getDbName(), statsDesc.getTableName(),
                        PolyCatDataAccessor.toColumnStatistics(colStats));
            } catch (TException e) {
                throw new CatalogException(e);
            }
            return client.updateTableColumnStatistics(request);
        });
        wrapper.throwNoSuchObjectException();
        wrapper.throwMetaException();
        return wrapper.getData();
    }

    @Override
    public boolean updatePartitionColumnStatistics(ColumnStatistics statsObj, List<String> partVals)
            throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
        ColumnStatisticsDesc statsDesc = statsObj.getStatsDesc();
        if (statsDesc == null) {
            throw new InvalidObjectException("Invalid column stats object");
        }
        final StoreDataWrapper<Boolean> wrapper = StoreDataWrapper.getInstance(false, ErrorCode.TABLE_NOT_FOUND, NoSuchObjectException.class);
        wrapper.wrapper(() -> {
            UpdatePartitionColumnStatisticRequest request = null;
            try {
                request = new UpdatePartitionColumnStatisticRequest(getProjectId(),
                        getCatalogMappingName(statsDesc.getCatName()), statsDesc.getDbName(), statsDesc.getTableName(),
                        new ColumnStatisticsInput(PolyCatDataAccessor.toColumnStatistics(statsObj), partVals));
            } catch (TException e) {
                throw new CatalogException(e);
            }
            return client.updatePartitionColumnStatistics(request);
        });
        wrapper.throwNoSuchObjectException();
        wrapper.throwMetaException();
        return wrapper.getData();
    }

    @Override
    public ColumnStatistics getTableColumnStatistics(String catName, String dbName, String tableName, List<String> colNames)
            throws MetaException, NoSuchObjectException {
        final StoreDataWrapper<ColumnStatistics> wrapper = StoreDataWrapper.getInstance(null, ErrorCode.TABLE_NOT_FOUND, NoSuchObjectException.class);
        wrapper.wrapper(() -> {
            GetTableColumnStatisticRequest request = new GetTableColumnStatisticRequest(getProjectId(), getCatalogMappingName(catName),
                    dbName, tableName, colNames);
            io.polycat.catalog.common.model.stats.ColumnStatisticsObj[] columnStatisticsObjs = client.getTableColumnsStatistics(request);
            List<ColumnStatisticsObj> statisticsObjs = Arrays.stream(columnStatisticsObjs)
                    .map(HiveDataAccessor::convertToHiveStatsObj).collect(Collectors.toList());
            return new ColumnStatistics(new ColumnStatisticsDesc(true, dbName, tableName), statisticsObjs);
        });
        wrapper.throwNoSuchObjectException();
        wrapper.throwMetaException();
        return wrapper.getData();
    }

    @Override
    public List<ColumnStatistics> getPartitionColumnStatistics(String catName, String dbName, String tblName,
                                                               List<String> partNames, List<String> colNames) throws MetaException, NoSuchObjectException {
        final StoreDataWrapper<List<ColumnStatistics>> wrapper = StoreDataWrapper.getInstance(new ArrayList<>(), ErrorCode.TABLE_NOT_FOUND, NoSuchObjectException.class);
        wrapper.wrapper(() -> {
            GetPartitionColumnStatisticsRequest request = new GetPartitionColumnStatisticsRequest(getProjectId(),
                    getCatalogMappingName(catName), dbName, tblName, new GetPartitionColumnStaticsInput(partNames, colNames));
            PartitionStatisticData result = client.getPartitionColumnStatistics(request);
            return HiveDataAccessor.toColumnStatisticsList(result, catName, dbName, tblName);
        });
        wrapper.throwNoSuchObjectException();
        wrapper.throwMetaException();
        return wrapper.getData();
    }

    @Override
    public boolean deletePartitionColumnStatistics(String catName, String dbName, String tableName,
                                                   String partName, List<String> partVals, String colName) throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
        final String partitionName = makePartitionName(getTable(catName, dbName, tableName), partVals);
        final StoreDataWrapper<Boolean> wrapper = StoreDataWrapper.getInstance(false, ErrorCode.TABLE_NOT_FOUND, NoSuchObjectException.class);
        wrapper.wrapper(() -> {
            DeletePartitionColumnStatisticsRequest request = new DeletePartitionColumnStatisticsRequest(getProjectId(),
                    getCatalogMappingName(catName), dbName, tableName, partitionName, colName);
            client.deletePartitionColumnStatistics(request);
            return true;
        });
        wrapper.throwNoSuchObjectException();
        wrapper.throwMetaException();
        return wrapper.getData();
    }

    private String makePartitionName(Table table, List<String> partValues) {
        List<String> partitionNames = table.getPartitionKeys().stream().map(FieldSchema::getName)
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
        List<String> partitionNames = table.getPartitionKeys().stream().map(FieldSchema::getName)
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

    @Override
    public boolean deleteTableColumnStatistics(String catName, String dbName, String tableName, String colName)
            throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
        final StoreDataWrapper<Boolean> wrapper = StoreDataWrapper.getInstance(false, ErrorCode.TABLE_NOT_FOUND, NoSuchObjectException.class);
        wrapper.wrapper(() -> {
            DeleteColumnStatisticsRequest request = new DeleteColumnStatisticsRequest(getProjectId(), getCatalogMappingName(catName),
                    dbName, tableName, colName);
            client.deleteTableColumnStatistics(request);
            return true;
        });
        wrapper.throwNoSuchObjectException();
        wrapper.throwMetaException();
        return wrapper.getData();
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
    public void dropPartitions(String catName, String dbName, String tblName, List<String> partNames)
            throws MetaException, NoSuchObjectException {
        final StoreDataWrapper<Void> wrapper = StoreDataWrapper.getInstance(null, ErrorCode.TABLE_NOT_FOUND, NoSuchObjectException.class);
        wrapper.wrapper(() -> {
            DropPartitionInput dropPartitionInput = new DropPartitionInput();
            dropPartitionInput.setPartitionNames(partNames);
            DropPartitionRequest request = new DropPartitionRequest(getProjectId(), getCatalogMappingName(catName),
                    dbName, tblName, dropPartitionInput);
            try {
                client.dropPartition(request);
            } catch (CatalogException e) {
                log.warn(e.getMessage() + "\n try to make deprecated partition names");
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
            return null;
        });
        wrapper.throwNoSuchObjectException();
        wrapper.throwMetaException();
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
        StoreDataWrapper<Boolean> wrapper = StoreDataWrapper.getInstance(false, ErrorCode.DATABASE_NOT_FOUND, InvalidObjectException.class);
        wrapper.wrapper(() -> {
            FunctionInput functionInput = PolyCatDataAccessor.toFunctionInput(function);
            CreateFunctionRequest req = new CreateFunctionRequest(getProjectId(), getCatalogMappingName(function.getCatName()), functionInput);
            client.createFunction(req);
            return true;
        });
        wrapper.throwInvalidObjectException();
        wrapper.throwMetaException();
    }

    @Override
    public void alterFunction(String s, String s1, String s2, Function function)
            throws InvalidObjectException, MetaException {

    }

    @Override
    public void dropFunction(String catName, String dbName, String funcName)
            throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        FunctionRequestBase req = new FunctionRequestBase(getProjectId(), getCatalogMappingName(catName), dbName, funcName);
        client.dropFunction(req);
    }

    @Override
    public Function getFunction(String catName, String dbName, String funcName) throws MetaException {
        GetFunctionRequest req = new GetFunctionRequest(getProjectId(), getCatalogMappingName(catName), dbName, funcName);
        FunctionInput funcInput = null;
        try {
            funcInput = client.getFunction(req);
            return HiveDataAccessor.toFunction(funcInput);
        } catch (CatalogException ignore) {

        }
        return null;
    }

    @Override
    public List<Function> getAllFunctions(String catName) throws MetaException {
        final GetAllFunctionRequest getAllFunctionRequest = new GetAllFunctionRequest(getProjectId(), getCatalogMappingName(catName));
        final FunctionInput[] functionInputs = client.getAllFunctions(getAllFunctionRequest).getObjects();
        return Arrays.stream(functionInputs).map(HiveDataAccessor::toFunction)
                .collect(Collectors.toList());
    }

    @Override
    public List<String> getFunctions(String catName, String dbName, String pattern) throws MetaException {
        ListFunctionRequest req = new ListFunctionRequest(getProjectId(), getCatalogMappingName(catName), dbName, resolveSyntaxSugar(pattern));
        String[] functionsPagedList = client.listFunctions(req).getObjects();
        return Arrays.stream(functionsPagedList).collect(Collectors.toList());
    }

    private String resolveSyntaxSugar(String pattern) {
        // todo more wildcard character to be resolved
        if ("*".equals(pattern)) {
            return "";
        }
        return pattern.replaceAll("\\*", "%");
    }

    @Override
    public AggrStats get_aggr_stats_for(String catName, String dbName, String tblName,
                                        List<String> partNames, List<String> colNames)
            throws MetaException, NoSuchObjectException {
        GetAggregateColumnStatisticsRequest request = new GetAggregateColumnStatisticsRequest(getProjectId(),
                getCatalogMappingName(catName), dbName, tblName, new GetPartitionColumnStaticsInput(partNames, colNames));
        return HiveDataAccessor.toAggrStats(client.getAggrColStats(request));
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
    public List<String> createTableWithConstraints(org.apache.hadoop.hive.metastore.api.Table table, List<SQLPrimaryKey> list, List<SQLForeignKey> list1,
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
        log.info("configuration: {}", this.conf);
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
        return this.conf;
    }

    protected String getProjectId() {
        return PolyCatDataAccessor.getProjectId(getDefaultProjectId());
    }

    protected String getDefaultProjectId() {
        return client.getContext().getProjectId();
    }

    protected String getDefaultUser() {
        return client.getContext().getUserName();
    }

    private String getTenantName() {
        return client.getContext().getTenantName();
    }

    public CatalogPlugin getCatalogPlugin() {
        return client;
    }

    public String getCatalogMappingName(String hiveCatalogName) {
        return ConfUtil.getCatalogMappingName(hiveCatalogName);
    }

    public static <T> T wrapStoreData(T defaultValue, Supplier<T> supplier) {
        try {
            return supplier.get();
        } catch (Exception e) {
            log.warn("From catalog store get data error, {}", e.getMessage());
            return defaultValue;
        }
    }
}
