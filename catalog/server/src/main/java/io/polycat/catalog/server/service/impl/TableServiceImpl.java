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
package io.polycat.catalog.server.service.impl;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import io.polycat.catalog.common.*;
import io.polycat.catalog.common.model.*;

import io.polycat.catalog.common.model.stats.ColumnStatistics;
import io.polycat.catalog.common.model.stats.ColumnStatisticsObj;
import io.polycat.catalog.common.plugin.request.input.ColumnChangeInput;
import io.polycat.catalog.common.plugin.request.input.TableInput;
import io.polycat.catalog.common.plugin.request.input.TableTypeInput;
import io.polycat.catalog.common.types.DataType;
import io.polycat.catalog.common.types.DataTypes;
import io.polycat.catalog.common.types.DecimalType;
import io.polycat.catalog.common.utils.*;

import io.polycat.catalog.server.util.TransactionRunner;
import io.polycat.catalog.server.util.TransactionRunnerUtil;
import io.polycat.catalog.service.api.ObjectNameMapService;
import io.polycat.catalog.service.api.TableService;

import io.polycat.catalog.store.api.*;
import io.polycat.catalog.store.common.StoreConvertor;
import io.polycat.catalog.store.fdb.record.RecordStoreHelper;


import io.polycat.catalog.util.CheckUtil;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import static io.polycat.catalog.common.Operation.ADD_COLUMN;
import static io.polycat.catalog.common.Operation.ALTER_TABLE;
import static io.polycat.catalog.common.Operation.CHANGE_COLUMN;
import static io.polycat.catalog.common.Operation.CREATE_TABLE;
import static io.polycat.catalog.common.Operation.DROP_COLUMN;
import static io.polycat.catalog.common.Operation.DROP_TABLE;
import static io.polycat.catalog.common.Operation.PURGE_TABLE;
import static io.polycat.catalog.common.Operation.RENAME_COLUMN;
import static io.polycat.catalog.common.Operation.RESTORE_TABLE;
import static io.polycat.catalog.common.Operation.SET_PROPERTIES;
import static io.polycat.catalog.common.Operation.UNDROP_TABLE;
import static io.polycat.catalog.common.Operation.UNSET_PROPERTIES;
import static java.util.stream.Collectors.toList;

@Slf4j
@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class TableServiceImpl implements TableService {

    private static int TABLE_STORE_MAX_RETRY_NUM = 256;
    private static final int MAX_BATCH_ROW_NUM = 1024;
    private static int delMaxNumIndexPartitionSetPerTrans = 2048;
    private static final String LMS_KEY = "lms_name";

    private static final String TABLE_STORE_CHECKSUM = "catalogTableStore";

    @Autowired
    private BackendTaskStore backendTaskStore;

    @Autowired
    private CatalogStore catalogStore;

    @Autowired
    private TableMetaStore tableMetaStore;

    @Autowired
    private TableDataStore tableDataStore;

    @Autowired
    private RoleStore roleStore;

    @Autowired
    private UserPrivilegeStore userPrivilegeStore;

    @Autowired
    private ObjectNameMapStore objectNameMapStore;

    @Autowired
    private ObjectNameMapService objectNameMapService;

    @Autowired
    private VersionManager versionManager;

    @Autowired
    private Transaction transaction;

    private static class TableServiceImplHandler {

        private static final TableServiceImpl INSTANCE = new TableServiceImpl();
    }

    public static TableServiceImpl getInstance() {
        return TableServiceImplHandler.INSTANCE;
    }

    private Optional<String> resolveCatalogNameFromInput(Map<String, String> parameters, String key) {
        if (parameters == null) {
            return Optional.empty();
        }

        String layer3CatalogName = parameters.get(key);
        if (layer3CatalogName != null) {
            return Optional.of(layer3CatalogName);
        }

        return Optional.empty();
    }

    private void checkTableParameters(Map<String, String> parameters, TableName tableName)
        throws MetaStoreException {
        Optional<String> catalogName = resolveCatalogNameFromInput(parameters, LMS_KEY);
        if (!catalogName.isPresent()) {
            return;
        }

        if (!tableName.getCatalogName().equals(catalogName.get())) {
            throw new MetaStoreException(ErrorCode.ARGUMENT_ILLEGAL,
                String.format("%s:%s", LMS_KEY, catalogName.get()));
        }
    }

    private void saveObjectNameMapIfNotExist(TransactionContext context, TableName tableName, TableIdent tableIdent,
        Map<String, String> parameters) {
        Optional<String> catalogName = resolveCatalogNameFromInput(parameters, LMS_KEY);
        log.info("Optional catalogName: {}, properties: {}", catalogName.isPresent(), parameters);
        if (!catalogName.isPresent()) {
            return;
        }

        Optional<ObjectNameMap> objectNameMapOptional = objectNameMapStore
            .getObjectNameMap(context, tableName.getProjectId(), ObjectType.TABLE,
                tableName.getDatabaseName(),
                tableName.getTableName());
        // 2 layer object name, which may map to multiple Catalogs
        // If catalog ids are different, the same table object map record exist
        // If the catalog Id is different from the current table,
        // the map is manually modified and the 2Layer object is mapped to another catalog.
        // In this case, the map table is not modified
        if ((objectNameMapOptional.isPresent())
            && (!objectNameMapOptional.get().getTopObjectId().equals(tableIdent.getCatalogId()))) {
            return;
        }

        ObjectNameMap objectNameMap = new ObjectNameMap(tableName, tableIdent);
        objectNameMapStore.insertObjectNameMap(context, objectNameMap);
    }

    private TableHistoryObject buildInitTableHistory() {
        TableHistoryObject tableHistoryObject = new TableHistoryObject();
        tableHistoryObject.setEventId(UuidUtil.generateId());
        tableHistoryObject.setPartitionSetType(TablePartitionSetType.INIT);
        return tableHistoryObject;
    }

    private void checkDecimalParam(ColumnObject columnObject, DecimalType dataType) {
        int precision = dataType.getPrecision();
        int scale = dataType.getScale();

        if (precision == 0 || scale > precision) {
            throw new MetaStoreException(ErrorCode.TABLE_COLUMN_TYPE_INVALID, dataType);
        }
    }

    private ColumnObject convertColumnInput(Column columnInput) {
        CheckUtil.checkStringParameter(columnInput.getColumnName(), columnInput.getColType());
        DataType dataType = DataTypes.valueOf(columnInput.getColType());
        String comment = columnInput.getComment();
        ColumnObject columnObject = new ColumnObject();
        columnObject.setName(columnInput.getColumnName());
        columnObject.setDataType(dataType);
        columnObject.setComment(comment == null ? "" : comment);

        if (DataTypes.isDecimal(dataType)) {
            checkDecimalParam(columnObject, (DecimalType) dataType);
        }
        return columnObject;
    }

    private String tableOperateDetail(TableName tableName) {
        return new StringBuilder()
                .append("database name: ").append(tableName.getDatabaseName()).append(", ")
                .append("table name: ").append(tableName.getTableName())
                .toString();
    }

    private String getTablePath(TransactionContext context, TableIdent tableIdent, TableInput tableInput) {
        String location = tableInput.getStorageDescriptor().getLocation();
        if (location == null || location.isEmpty()) {
            if (!TableTypeInput.VIRTUAL_VIEW.name().equals(tableInput.getTableType())) {
                DatabaseIdent databaseIdent = StoreConvertor.databaseIdent(tableIdent);
                DatabaseObject database = DatabaseObjectHelper.getDatabaseObject(context, databaseIdent);
                String databaseLocation = database.getLocation();
                return PathUtil.tablePath(databaseLocation, tableInput.getTableName(), tableIdent.getTableId());
            }
            return "";
        } else {
            return PathUtil.normalizePath(location);
        }
    }

    private List<ColumnObject> makePartitions(List<Column> partList) {
        if (partList == null || partList.size() == 0) {
            return new ArrayList<>();
        } else {
            return partList.stream().map(this::convertColumnInput).collect(toList());
        }
    }

    private void throwIfColumnNameDuplicate(List<ColumnObject> normalColumns, List<ColumnObject> partitionColumns) {
        HashSet<String> nameSet = new HashSet<>();
        normalColumns.forEach(columnObject -> {
            if (nameSet.contains(columnObject.getName())) {
                throw new CatalogServerException(ErrorCode.COLUMN_ALREADY_EXISTS, columnObject.getName());
            }
            nameSet.add(columnObject.getName());
        });

        partitionColumns.forEach(columnObject -> {
            if (nameSet.contains(columnObject.getName())) {
                throw new CatalogServerException(ErrorCode.COLUMN_ALREADY_EXISTS, columnObject.getName());
            }
            nameSet.add(columnObject.getName());
        });
    }

    private TableIdent createTableInternal(TransactionContext context, DatabaseName databaseName, TableInput tableInput,
        String catalogCommitId) throws CatalogServerException {
        TableName tableName = StoreConvertor.tableName(databaseName.getProjectId(), databaseName.getCatalogName(),
            databaseName.getDatabaseName(), tableInput.getTableName());
        DatabaseIdent databaseIdent = DatabaseObjectHelper.getDatabaseIdent(context, databaseName);
        String tableId = tableMetaStore.generateTableId(context, databaseIdent.getProjectId(), databaseIdent.getCatalogId());
        TableIdent tableIdent = StoreConvertor.tableIdent(databaseIdent, tableId);

        // check whether table already exists
        throwIfTableNameExist(context, databaseIdent, tableName.getTableName());

        //insert table map
        checkTableParameters(tableInput.getParameters(), tableName);
        //saveObjectNameMapIfNotExist(context, tableName, tableIdent, tableInput.getParameters());

        // create subspace
        // tableDataStore.createTableDataPartitionSetSubspace(context, tableIdent);
        // tableDataStore.createTableIndexPartitionSetSubspace(context, tableIdent);
        tableDataStore.createTablePartitionInfo(context, tableIdent);
        int historySubspaceFlag = TableHistorySubspaceHelper.createHistorySubspace(context, tableIdent);

        String version = VersionManagerHelper.getNextVersion(context, databaseIdent.getProjectId(),
            databaseIdent.getRootCatalogId());

        TableBaseObject tableBaseObject = new TableBaseObject(tableInput);

        List<ColumnObject> normalColumns = new ArrayList<>();
        if (tableInput.getStorageDescriptor() != null && tableInput.getStorageDescriptor().getColumns() != null) {
            for (int i=0; i<tableInput.getStorageDescriptor().getColumns().size(); ++i) {
                Column column = tableInput.getStorageDescriptor().getColumns().get(i);
                normalColumns.add(new ColumnObject(i, column.getColumnName(), column.getColType(), column.getComment()));
            }
        }
        List<ColumnObject> partitionColumns = makePartitions(tableInput.getPartitionKeys());
        throwIfColumnNameDuplicate(normalColumns, partitionColumns);
        TableSchemaObject tableSchema = new TableSchemaObject(normalColumns, partitionColumns);

        String tablePath = getTablePath(context, tableIdent, tableInput);
        TableStorageObject tableStorage = new TableStorageObject(tableInput, tablePath);

        tableMetaStore.insertTableReference(context, tableIdent);
        tableMetaStore.insertTable(context, tableIdent, tableName.getTableName(), historySubspaceFlag, tableBaseObject,
                tableSchema, tableStorage);

        // insert the Table properties into TablePropertiesHistory subspace
        tableMetaStore.insertTableBaseHistory(context, tableIdent, version, tableBaseObject);

        // insert the TableSchema into TableSchemaHistory subspace
        tableMetaStore.insertTableSchemaHistory(context, tableIdent, version, tableSchema);

        // insert the table storage info into TableStorageHistory subspace
        tableMetaStore.insertTableStorageHistory(context, tableIdent, version, tableStorage);

        // insert the TableInfo into TableHistory subspace
        TableHistoryObject tableHistory = buildInitTableHistory();
        tableDataStore.insertTableHistory(context, tableIdent, version, tableHistory);

        // insert the TableCommit into TableCommit subspace
        long commitTime = RecordStoreHelper.getCurrentTime();
        OperationObject operation = new OperationObject(TableOperationType.DDL_CREATE_TABLE, 0, 0, 0, 0);
        TableCommitObject tableCommitObject = new TableCommitObject(tableIdent.getProjectId(), tableIdent.getCatalogId(),
            tableIdent.getDatabaseId(), tableIdent.getTableId(), tableName.getTableName(),
            commitTime, commitTime, Collections.singletonList(operation), 0, version);
        tableMetaStore.insertTableCommit(context, tableIdent, tableCommitObject);

        // insert catalog commit
        catalogStore.insertCatalogCommit(context, tableIdent.getProjectId(),
            tableIdent.getCatalogId(), catalogCommitId, commitTime, CREATE_TABLE, tableOperateDetail(tableName));

        // insert user privilege table
        userPrivilegeStore.insertUserPrivilege(context, tableIdent.getProjectId(), tableInput.getOwner(),
            ObjectType.TABLE.name(),
            tableIdent.getTableId(), true, 0);

        return tableIdent;
    }

    private void createTableByInput(DatabaseName databaseName, TableInput tableInput) {
        String catalogCommitId = UuidUtil.generateCatalogCommitId();
        CheckUtil.checkNameLegality("tableName", tableInput.getTableName());

        TableIdent tableIdent = null;
        try (TransactionRunner runner = new TransactionRunner()) {
            runner.setMaxAttempts(TABLE_STORE_MAX_RETRY_NUM);
            tableIdent = runner.run(
                context -> createTableInternal(context, databaseName, tableInput, catalogCommitId));
        } catch (RuntimeException e) {
            // Retry interval, the DB may have been deleted, table exist
            // and the previous transaction that created the table may have succeeded
            CatalogName catalogName = StoreConvertor.catalogName(databaseName.getProjectId(), databaseName.getCatalogName());
            CatalogObject catalogObject = CatalogObjectHelper.getCatalogObject(catalogName);
            CatalogIdent catalogIdent = StoreConvertor
                .catalogIdent(catalogObject.getProjectId(), catalogObject.getCatalogId(), catalogObject.getRootCatalogId());
            if (!CatalogCommitHelper.catalogCommitExist(catalogIdent, catalogCommitId)) {
                throw e;
            }
        }
    }

    @Override
    public void createTable(DatabaseName databaseName, TableInput tableInput) {
        try {
            createTableByInput(databaseName, tableInput);
        } catch (MetaStoreException e) {
            e.printStackTrace();
            throw new CatalogServerException(e.getMessage(), e.getErrorCode());
        }
    }

    @Override
    public Table getTableByName(TableName tableName) {
        try (TransactionRunner runner = new TransactionRunner()) {
            return runner.run(context -> {
                TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);
                //TableObject tableObject = TableObjectHelper.getTableObject(context, tableIdent);
                TableObject tableObject = tableMetaStore.getTable(context, tableIdent, tableName);
                return TableObjectConvertHelper.toTableModel(tableObject);
            });
        } catch (MetaStoreException e) {
            throw new CatalogServerException(e.getMessage(), e.getErrorCode());
        }
    }

    @Override
    public TraverseCursorResult<List<String>> getTableNames(DatabaseName databaseName, String tableType,
        Integer maxResults, String pageToken, String filter) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getTableNames");
    }

    @Override
    public List<Table> getTableObjectsByName(DatabaseName databaseName, List<String> tableNames) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getTableObjectsByName");
    }

    @Override
    public TraverseCursorResult<List<String>> listTableNamesByFilter(DatabaseName databaseName, String filter,
        Integer maxNum, String pageToken) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "listTableNamesByFilter");
    }

    private List<Map<DatabaseIdent, String>> getParentDatabaseIdent(TableIdent tableIdent) {
        List<Map<DatabaseIdent, String>> list = new ArrayList<>();

        try (TransactionRunner runner = new TransactionRunner()) {
            return runner.run(context -> {
                ParentBranchDatabaseIterator parentBranchDatabaseIterator = new ParentBranchDatabaseIterator(context,
                    tableIdent);

                while (parentBranchDatabaseIterator.hasNext()) {
                    Map<DatabaseIdent, String> map = parentBranchDatabaseIterator.next();
                    list.add(map);
                }
                return list;
            });

        }
    }

    private TraverseCursorResult<List<TableCommitObject>> listTableCommits(TableIdent tableIdent, int maxResultNum,
        String basedVersion, CatalogToken catalogToken) {
        try (TransactionRunner runner = new TransactionRunner()) {
            return runner.run(context -> {
                return listTableCommitObject(context, tableIdent, maxResultNum, basedVersion, catalogToken);
            });
        }
    }

    private TraverseCursorResult<List<TableCommitObject>> listTableCommitObject(TransactionContext context,
        TableIdent tableIdent, int maxResultNum, String basedVersion, CatalogToken catalogToken) {

        List<TableCommitObject> tableCommitList = new ArrayList<>();
        int remainNum = maxResultNum;

        byte[] continuation = null;
        String method = Thread.currentThread().getStackTrace()[1].getMethodName();
        if (catalogToken.getContextMapValue(method) != null) {
            continuation = CodecUtil.hex2Bytes(catalogToken.getContextMapValue(method));
        }
        while (true) {
            int batchNum = Math.min(remainNum, MAX_BATCH_ROW_NUM);
            ScanRecordCursorResult<List<TableCommitObject>> batchTableCommitList = tableMetaStore
                .listTableCommit(context, tableIdent, batchNum, continuation, TransactionIsolationLevel.SNAPSHOT,
                    basedVersion);
            tableCommitList.addAll(batchTableCommitList.getResult());

            remainNum = remainNum - batchTableCommitList.getResult().size();
            continuation = batchTableCommitList.getContinuation().orElse(null);
            if ((continuation == null) || (remainNum == 0)) {
                break;
            }
        }

        if (continuation == null) {
            return new TraverseCursorResult<>(tableCommitList, null);
        }

        CatalogToken catalogTokenNew = CatalogToken.buildCatalogToken(TABLE_STORE_CHECKSUM, method,
            CodecUtil.bytes2Hex(continuation), catalogToken.getReadVersion());

        return new TraverseCursorResult<>(tableCommitList, catalogTokenNew);
    }

    private TraverseCursorResult<List<TableCommitObject>> listBranchTableCommitWithToken(TableIdent tableIdent,
        int maxResultNum, CatalogToken catalogToken) {
        int remainResultNum = maxResultNum;
        List<TableCommitObject> listSum = new ArrayList<>();

        List<Map<DatabaseIdent, String>> parentDatabaseList = getParentDatabaseIdent(tableIdent);
        Map<DatabaseIdent, String> subBranchMap = new HashMap<>();
        String readVersion = catalogToken.getReadVersion();
        DatabaseIdent databaseIdent = StoreConvertor.databaseIdent(tableIdent.getProjectId(), tableIdent.getCatalogId(),
            tableIdent.getDatabaseId(), tableIdent.getRootCatalogId());
        subBranchMap.put(databaseIdent, readVersion);
        parentDatabaseList.add(0, subBranchMap);

        CatalogToken nextCatalogToken = catalogToken;
        //find the parent branch breakpoint based on the token information.
        String method = Thread.currentThread().getStackTrace()[1].getMethodName();
        if (catalogToken.getContextMapValue(method) != null) {
            String nextParentCatalogId = catalogToken.getContextMapValue(method);
            parentDatabaseList = cutParentBranchListWithToken(parentDatabaseList, nextParentCatalogId);
        }

        TraverseCursorResult<List<TableCommitObject>> parentBranchTableCommits = null;
        DatabaseIdent parentDatabaseIdent = null;
        for (Map<DatabaseIdent, String> map : parentDatabaseList) {
            Iterator<DatabaseIdent> iterator = map.keySet().iterator();
            parentDatabaseIdent = iterator.next();
            String subBranchVersion = map.get(parentDatabaseIdent);

            TableIdent parentTableIdent = StoreConvertor.tableIdent(parentDatabaseIdent, tableIdent.getTableId());

            String baseVersion = (readVersion.compareTo(subBranchVersion) < 0) ? readVersion : subBranchVersion;
            parentBranchTableCommits = listTableCommits(parentTableIdent, remainResultNum, baseVersion,
                nextCatalogToken);

            List<TableCommitObject> parentBranchTableCommitList = parentBranchTableCommits.getResult().stream()
                .map(tableCommitObject -> {
                    tableCommitObject.setCatalogId(tableIdent.getCatalogId());
                    return tableCommitObject;
                }).collect(toList());

            nextCatalogToken = new CatalogToken(TABLE_STORE_CHECKSUM, catalogToken.getReadVersion());

            listSum.addAll(parentBranchTableCommitList);
            remainResultNum = remainResultNum - parentBranchTableCommitList.size();
            if (remainResultNum == 0) {
                break;
            }
        }

        String stepValue = parentDatabaseIdent.getCatalogId();
        CatalogToken catalogTokenNew = parentBranchTableCommits.getContinuation().map(token -> {
            token.putContextMap(method, stepValue);
            return token;
        }).orElse(null);

        return new TraverseCursorResult<>(listSum, catalogTokenNew);
    }


    private TraverseCursorResult<List<TableCommitObject>> listTableCommitsWithToken(TableIdent tableIdent,
        TableName tableName,
        int maxResults, String pageToken) {
        String latestVersion;
        Optional<CatalogToken> catalogToken = CatalogToken.parseToken(pageToken, TABLE_STORE_CHECKSUM);
        if (!catalogToken.isPresent()) {
            latestVersion = VersionManagerHelper.getLatestVersion(tableIdent);
            catalogToken = Optional.of(new CatalogToken(TABLE_STORE_CHECKSUM, latestVersion));
        }

        TraverseCursorResult<List<TableCommitObject>> tableCommits = listBranchTableCommitWithToken(tableIdent,
            maxResults,
            catalogToken.get());

        return tableCommits;
    }

    private TraverseCursorResult<List<TableCommitObject>> listTableCommitByName(TableName tableName, int maxResults,
        String pageToken) {
        TableIdent tableIdent;
        try (TransactionRunner runner = new TransactionRunner()) {
            tableIdent = runner.run(context -> {
              return TableObjectHelper.getTableIdent(context, tableName);
            });
        }

        TraverseCursorResult<List<TableCommitObject>> traverseCursorResult = listTableCommitsWithToken(tableIdent,
            tableName, maxResults, pageToken);

        return traverseCursorResult;
    }

    @Override
    public TraverseCursorResult<List<io.polycat.catalog.common.model.TableCommit>> listTableCommits(
        TableName tableName, int maxResults, String pageToken) {
        try {
            TraverseCursorResult<List<TableCommitObject>> tableCommitList = listTableCommitByName(tableName, maxResults,
                pageToken);
            List<io.polycat.catalog.common.model.TableCommit> result = new ArrayList<>(
                tableCommitList.getResult().size());

            for (TableCommitObject commit : tableCommitList.getResult()) {
                result.add(convertTableCommit(commit));
            }

            return new TraverseCursorResult(result, tableCommitList.getContinuation().orElse(null));
        } catch (MetaStoreException e) {
            throw new CatalogServerException(e.getMessage(), e.getErrorCode());
        }
    }

    private io.polycat.catalog.common.model.TableCommit convertTableCommit(TableCommitObject commit) {
        if (null == commit) {
            return null;
        }
        io.polycat.catalog.common.model.TableCommit tableCommit = new io.polycat.catalog.common.model.TableCommit();
        tableCommit.setCatalogId(commit.getCatalogId());
        tableCommit.setDatabaseId(commit.getDatabaseId());
        tableCommit.setTableId(commit.getTableId());
        tableCommit.setCommitVersion(commit.getVersion());

        Date date = new Date(commit.getCommitTime());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        tableCommit.setCommitTime(sdf.format(date));

        tableCommit.setTableOperation(
            commit.getOperations().stream().map(operationObject -> {
                return new TableOperation(operationObject.getOperationType().name(),
                    operationObject.getAddedNums(), operationObject.getDeletedNums(), operationObject.getUpdatedNums(),
                    operationObject.getFileCount());
            }).collect(toList()));
        return tableCommit;
    }


    private TableCommitObject getLatestTableCommitByName(TableName tableName) throws MetaStoreException {

        try (TransactionRunner runner = new TransactionRunner()) {
            return runner.run(context -> {
                //convert tableIdent
                TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);
                String latestVersion = VersionManagerHelper.getLatestVersion(context, tableIdent.getProjectId(),
                    tableIdent.getRootCatalogId());
                TableCommitObject tableCommit = TableCommitHelper
                    .getLatestTableCommitOrElseThrow(context, tableIdent, latestVersion);
                return tableCommit;
            });
        }
    }

    @Override
    public io.polycat.catalog.common.model.TableCommit getLatestTableCommit(TableName tableName) {
        try {
            TableCommitObject tableCommit = getLatestTableCommitByName(tableName);
            return convertTableCommit(tableCommit);
        } catch (MetaStoreException e) {
            throw new CatalogServerException(e.getMessage(), e.getErrorCode());
        }
    }


    private List<Map<DatabaseIdent, String>> getParentDatabaseIdent(TransactionContext context, DatabaseIdent databaseIdent) {
        List<Map<DatabaseIdent, String>> list = new ArrayList<>();
        ParentBranchDatabaseIterator parentBranchDatabaseIterator = new ParentBranchDatabaseIterator(context,
                databaseIdent);
        while (parentBranchDatabaseIterator.hasNext()) {
            Map<DatabaseIdent, String> map = parentBranchDatabaseIterator.next();
            list.add(map);
        }
        return list;
    }

    private List<Map<DatabaseIdent, String>> cutParentBranchListWithToken(
        List<Map<DatabaseIdent, String>> parentDatabaseList, String nextCatalogId) {
        int startNum = 0;
        Iterator<Map<DatabaseIdent, String>> mapIterator = parentDatabaseList.iterator();
        while (mapIterator.hasNext()) {
            Map<DatabaseIdent, String> listEntryMap = mapIterator.next();
            Iterator<DatabaseIdent> databaseIdentIterator = listEntryMap.keySet().iterator();
            DatabaseIdent parentDatabaseIdent = databaseIdentIterator.next();
            if (nextCatalogId.equals(parentDatabaseIdent.getCatalogId())) {
                break;
            }

            startNum++;
        }

        return parentDatabaseList.subList(startNum, parentDatabaseList.size());
    }

    private TableObject getTableInBranch(TransactionContext context, TableIdent tableIdent,
        DatabaseName databaseName,
        String subBranchVersion, boolean dropped, long droppedTime) {
        Optional<TableCommitObject> tableCommit = TableCommitHelper
            .getLatestTableCommit(context, tableIdent, subBranchVersion);
        Optional<TableSchemaHistoryObject> tableSchemaHistory = TableSchemaHelper
            .getLatestTableSchema(context, tableIdent, subBranchVersion);
        Optional<TableStorageHistoryObject> tableStorageHistory = TableStorageHelper
            .getLatestTableStorage(context, tableIdent,
                subBranchVersion);
        Optional<TableBaseHistoryObject> tableBaseHistoryObject = TableBaseHelper.getLatestTableBase(context, tableIdent,
            subBranchVersion);

        if ((tableCommit.isPresent()) && (tableSchemaHistory.isPresent()) && (tableStorageHistory.isPresent())
            && tableBaseHistoryObject.isPresent() && (dropped == TableCommitHelper.isDropCommit(tableCommit.get()))) {
            TableName tableName = StoreConvertor.tableName(tableIdent.getProjectId(), databaseName.getCatalogName(),
                databaseName.getDatabaseName(), tableCommit.get().getTableName());
            TableObject table;
            if (!dropped) {
                /*table = TableObjectConvertHelper.buildTable(tableIdent, tableName,
                    tableBaseHistoryObject.get().getTableBaseObject(),
                    new TableStorageObject(tableStorageHistory.get()), tableSchemaHistory.get().getTableSchemaObject(),
                    tableCommit.get().getCreateTime(), false, 0);*/
                table = new TableObject(tableIdent, tableName, 0, tableBaseHistoryObject.get().getTableBaseObject(),
                    tableSchemaHistory.get().getTableSchemaObject(), tableStorageHistory.get().getTableStorageObject(),
                    0);
            } else {
                /*table = TableObjectConvertHelper.buildTable(tableIdent, tableName,
                    tableBaseHistoryObject.get().getTableBaseObject(),
                    new TableStorageObject(tableStorageHistory.get()), tableSchemaHistory.get().getTableSchemaObject(),
                    tableCommit.get().getCreateTime(), true, droppedTime);*/
                table = new TableObject(tableIdent, tableName, 0, tableBaseHistoryObject.get().getTableBaseObject(),
                    tableSchemaHistory.get().getTableSchemaObject(), tableStorageHistory.get().getTableStorageObject(),
                    droppedTime);
            }
            return table;
        }
        return null;
    }

    private ScanRecordCursorResult<List<String>> listInUseTableIds(TransactionContext context, byte[] continuation,
                                                                 DatabaseIdent databaseIdent, int limit) {
        return listInUseTables(context, continuation, databaseIdent,
                limit, true);
    }

    private ScanRecordCursorResult<List<String>> listInUseTableNames(TransactionContext context, byte[] continuation,
                                                                   DatabaseIdent databaseIdent, int limit) {
        return listInUseTables(context, continuation, databaseIdent,
                limit, false);
    }

    private ScanRecordCursorResult<List<String>> listInUseTables(TransactionContext context, byte[] continuation,
        DatabaseIdent databaseIdent, int limit, boolean idFlag) {
        ScanRecordCursorResult<List<TableNameObject>> tableNames = tableMetaStore
            .listTableName(context, databaseIdent,
                limit, continuation, TransactionIsolationLevel.SERIALIZABLE);

        List<String> tableIds = tableNames.getResult().stream().map(s -> {
            if (idFlag) {
                return s.getObjectId();
            }
            return s.getName();
        }).collect(toList());
        if (tableNames.getContinuation().isPresent()) {
            return new ScanRecordCursorResult(tableIds, tableNames.getContinuation().get());
        }

        return new ScanRecordCursorResult(tableIds, null);
    }

    private List<TableObject> getBranchValidTableRecordByTmpTable(TransactionContext context, DatabaseIdent databaseIdent,
                                                        DatabaseName databaseName, String branchVersion, String tmpTable) {
        List<TableObject> tableRecordList = new ArrayList<>();
        Map<String, TableCommitObject> latestTableCommitByIds = tableMetaStore.getLatestTableCommitByTmpTable(context, databaseIdent.getProjectId(), databaseIdent.getCatalogId(), databaseIdent.getDatabaseId(), branchVersion, tmpTable);
        if (latestTableCommitByIds.isEmpty()) {
            return tableRecordList;
        }
        Map<String, TableSchemaHistoryObject> latestTableSchemaByIds = tableMetaStore.getLatestTableSchemaByTmpTable(context, databaseIdent.getProjectId(), databaseIdent.getCatalogId(), databaseIdent.getDatabaseId(), branchVersion, tmpTable);
        Map<String, TableStorageHistoryObject> latestTableStorageByIds = tableMetaStore.getLatestTableStorageByTmpTable(context, databaseIdent.getProjectId(), databaseIdent.getCatalogId(), databaseIdent.getDatabaseId(), branchVersion, tmpTable);
        Map<String, TableBaseHistoryObject> latestTableBaseByIds = tableMetaStore.getLatestTableBaseByTmpTable(context, databaseIdent.getProjectId(), databaseIdent.getCatalogId(), databaseIdent.getDatabaseId(), branchVersion, tmpTable);
        TableIdent tableIdent;
        TableName tableName;
        TableObject table;
        for (String tableId : latestTableSchemaByIds.keySet()) {
            if (latestTableCommitByIds.containsKey(tableId) &&
                    latestTableStorageByIds.containsKey(tableId) &&
                    latestTableBaseByIds.containsKey(tableId)) {
                tableIdent = StoreConvertor.tableIdent(databaseIdent, tableId);
                tableName = StoreConvertor.tableName(tableIdent.getProjectId(), databaseName.getCatalogName(),
                        databaseName.getDatabaseName(), latestTableCommitByIds.get(tableId).getTableName());
                table = new TableObject(tableIdent, tableName, 0, latestTableBaseByIds.get(tableId).getTableBaseObject(),
                        latestTableSchemaByIds.get(tableId).getTableSchemaObject(), latestTableStorageByIds.get(tableId).getTableStorageObject(),
                        0);
                tableRecordList.add(table);
            }
        }
        return tableRecordList;
    }

    private List<TableObject> getBranchValidTableRecord(TransactionContext context, DatabaseIdent databaseIdent,
                                                        DatabaseName databaseName, String branchVersion, List<String> tableIds,
                                                        Boolean dropped) {
        List<TableObject> tableRecordList = new ArrayList<>();
        if (CollectionUtils.isEmpty(tableIds)) {
            return tableRecordList;
        }
        Map<String, TableCommitObject> latestTableCommitByIds = tableMetaStore.getLatestTableCommitByIds(context, databaseIdent.getProjectId(), databaseIdent.getCatalogId(), databaseIdent.getDatabaseId(), branchVersion, tableIds);
        Map<String, TableSchemaHistoryObject> latestTableSchemaByIds = tableMetaStore.getLatestTableSchemaByIds(context, databaseIdent.getProjectId(), databaseIdent.getCatalogId(), databaseIdent.getDatabaseId(), branchVersion, tableIds);
        Map<String, TableStorageHistoryObject> latestTableStorageByIds = tableMetaStore.getLatestTableStorageByIds(context, databaseIdent.getProjectId(), databaseIdent.getCatalogId(), databaseIdent.getDatabaseId(), branchVersion, tableIds);
        Map<String, TableBaseHistoryObject> latestTableBaseByIds = tableMetaStore.getLatestTableBaseByIds(context, databaseIdent.getProjectId(), databaseIdent.getCatalogId(), databaseIdent.getDatabaseId(), branchVersion, tableIds);
        TableIdent tableIdent;
        TableName tableName;
        TableObject table;
        for (String tableId : tableIds) {
            if (latestTableCommitByIds.containsKey(tableId) &&
                    latestTableSchemaByIds.containsKey(tableId) &&
                    latestTableStorageByIds.containsKey(tableId) &&
                    latestTableBaseByIds.containsKey(tableId)) {
                tableIdent = StoreConvertor.tableIdent(databaseIdent, tableId);
                tableName = StoreConvertor.tableName(tableIdent.getProjectId(), databaseName.getCatalogName(),
                        databaseName.getDatabaseName(), latestTableCommitByIds.get(tableId).getTableName());
                table = new TableObject(tableIdent, tableName, 0, latestTableBaseByIds.get(tableId).getTableBaseObject(),
                        latestTableSchemaByIds.get(tableId).getTableSchemaObject(), latestTableStorageByIds.get(tableId).getTableStorageObject(),
                        0);
                tableRecordList.add(table);
            }
        }
        return tableRecordList;
    }

    private TraverseCursorResult<List<TableObject>> listInUseTableWithToken(DatabaseIdent databaseIdent,
        DatabaseName databaseName, String subBranchVersion, int maxRowLimitNum, Boolean dropped,
        CatalogToken catalogToken, boolean listName) throws MetaStoreException {
        try (TransactionRunner runner = new TransactionRunner()) {
            return runner.run(context -> {
                return listInUseTableWithToken(context, databaseIdent, databaseName, subBranchVersion, maxRowLimitNum,
                    dropped, catalogToken, listName);
            });
        }
    }

    private TraverseCursorResult<List<TableObject>> listInUseTableWithToken(TransactionContext context,
        DatabaseIdent databaseIdent, DatabaseName databaseName, String subBranchVersion,
        int maxRowLimitNum, Boolean dropped, CatalogToken catalogToken, boolean listNameFlag) {
        List<TableObject> tables = new ArrayList<>();
        int remainNum = maxRowLimitNum;
        String method = Thread.currentThread().getStackTrace()[1].getMethodName();

        byte[] continuation = (catalogToken.getContextMapValue(method) == null)
            ? null : CodecUtil.hex2Bytes(catalogToken.getContextMapValue(method));

        if (listNameFlag) {
            while (true) {
                int batchNum = Math.min(remainNum, MAX_BATCH_ROW_NUM);

                ScanRecordCursorResult<List<String>> tableNames = listInUseTableNames(context, continuation, databaseIdent,
                        batchNum);
                List<TableObject> tableRecordList = tableNames.getResult().stream().map(TableObject::new).collect(toList());
                tables.addAll(tableRecordList);
                remainNum = remainNum - tableRecordList.size();
                continuation = tableNames.getContinuation().orElse(null);
                if ((continuation == null) || (remainNum == 0)) {
                    break;
                }
            }
        } else {
            String tableObjectNameTmpTable = tableMetaStore.createTableObjectNameTmpTable(context, databaseIdent);
            tables = getBranchValidTableRecordByTmpTable(context, databaseIdent,
                    databaseName,
                    subBranchVersion, tableObjectNameTmpTable);
        /*while (true) {
            int batchNum = Math.min(remainNum, MAX_BATCH_ROW_NUM);

            ScanRecordCursorResult<List<String>> tableIds = listInUseTableIds(context, continuation, databaseIdent,
                batchNum);
            List<TableObject> tableRecordList = getBranchValidTableRecord(context, databaseIdent,
                databaseName,
                subBranchVersion, tableIds.getResult(), dropped);

            tables.addAll(tableRecordList);

            remainNum = remainNum - tableRecordList.size();
            continuation = tableIds.getContinuation().orElse(null);
            if ((continuation == null) || (remainNum == 0)) {
                break;
            }
        }*/
        }


        if (continuation == null) {
            return new TraverseCursorResult<>(tables, null);
        }

        CatalogToken catalogTokenNew = CatalogToken.buildCatalogToken(TABLE_STORE_CHECKSUM, method,
            CodecUtil.bytes2Hex(continuation), catalogToken.getReadVersion());
        return new TraverseCursorResult<>(tables, catalogTokenNew);
    }

    private List<TableObject> getBranchDroppedTableRecord(TransactionContext context, DatabaseIdent databaseIdent,
        DatabaseName databaseName, String branchVersion, List<DroppedTableObject> droppedTableObjectNameList,
        Boolean dropped) {
        List<TableObject> tableRecordList = new ArrayList<>();
        for (DroppedTableObject droppedTableObjectName : droppedTableObjectNameList) {
            TableObject tableRecord = getTableInBranch(context,
                StoreConvertor.tableIdent(databaseIdent, droppedTableObjectName.getObjectId()), databaseName,
                branchVersion, dropped, droppedTableObjectName.getDroppedTime());
            if (tableRecord != null) {
                tableRecordList.add(tableRecord);
            }
        }
        return tableRecordList;
    }

    private TraverseCursorResult<List<TableObject>> listDroppedTablesWithToken(DatabaseIdent databaseIdent,
        DatabaseName databaseName, String subBranchVersion, int maxRowLimitNum, Boolean dropped,
        CatalogToken catalogToken) throws MetaStoreException {
        try (TransactionRunner runner = new TransactionRunner()) {
            return runner.run(context -> {
                return listDroppedTablesWithToken(context, databaseIdent, databaseName, subBranchVersion,
                    maxRowLimitNum, dropped, catalogToken);
            });
        }
    }

    private TraverseCursorResult<List<TableObject>> listDroppedTablesWithToken(TransactionContext context,
        DatabaseIdent databaseIdent, DatabaseName databaseName, String subBranchVersion,
        int maxRowLimitNum, Boolean dropped, CatalogToken catalogToken) {
        List<TableObject> tables = new ArrayList<>();
        int remainNum = maxRowLimitNum;
        String method = Thread.currentThread().getStackTrace()[1].getMethodName();

        byte[] continuation = (catalogToken.getContextMapValue(method) == null)
            ? null : CodecUtil.hex2Bytes(catalogToken.getContextMapValue(method));

        while (true) {
            int batchNum = Math.min(remainNum, MAX_BATCH_ROW_NUM);

            if (dropped) {
                ScanRecordCursorResult<List<DroppedTableObject>> droppedTableObjectNameList = tableMetaStore
                        .listDroppedTable(context, databaseIdent, batchNum, continuation,
                                TransactionIsolationLevel.SNAPSHOT);

                List<TableObject> tableRecordList = getBranchDroppedTableRecord(context, databaseIdent,
                        databaseName,
                        subBranchVersion, droppedTableObjectNameList.getResult(), dropped);
                tables.addAll(tableRecordList);
                remainNum = remainNum - tableRecordList.size();
                continuation = droppedTableObjectNameList.getContinuation().orElse(null);
            }

            if ((continuation == null) || (remainNum == 0)) {
                break;
            }
        }

        if (continuation == null) {
            return new TraverseCursorResult<>(tables, null);
        }

        CatalogToken catalogTokenNew = CatalogToken.buildCatalogToken(TABLE_STORE_CHECKSUM, method,
            CodecUtil.bytes2Hex(continuation), catalogToken.getReadVersion());
        return new TraverseCursorResult<>(tables, catalogTokenNew);
    }

    private interface ListTablesInBranchWithToken {

        TraverseCursorResult<List<TableObject>> list(DatabaseIdent databaseIdent, DatabaseName databaseName,
            String subBranchVersion, int maxRowLimitNum, CatalogToken catalogToken, Boolean dropped);
    }

    private Optional<TableCommitObject> getLatestCurBranchTableCommit(TransactionContext context, TableIdent tableIdent,
        String basedVersion) throws MetaStoreException {
        Optional<TableCommitObject> record = tableMetaStore.getLatestTableCommit(context, tableIdent, basedVersion);
        return record;
    }

    private Boolean checkTableValidInBranch(TransactionContext context, TableIdent tableIdent, String baseVersion) {
        Optional<TableCommitObject> tableCommit = getLatestCurBranchTableCommit(context, tableIdent, baseVersion);
        if (tableCommit.isPresent()) {
            return true;
        }
        return false;
    }

    private Boolean checkTableExistInSubBranch(TransactionContext context, List<Map<DatabaseIdent, String>> parentDatabaseList,
                                               TableObject tableRecord) {
        for (Map<DatabaseIdent, String> map : parentDatabaseList) {
            Iterator<Map.Entry<DatabaseIdent, String>> iterator = map.entrySet().iterator();
            Map.Entry<DatabaseIdent, String> entry = iterator.next();
            DatabaseIdent subBranchDatabaseIdent = entry.getKey();
            String subBranchVersion = entry.getValue();

            if (tableRecord.getCatalogId().equals(subBranchDatabaseIdent.getCatalogId())) {
                return false;
            }

            TableIdent tableIdent = StoreConvertor
                .tableIdent(subBranchDatabaseIdent, tableRecord.getTableId());
            if (checkTableValidInBranch(context, tableIdent, subBranchVersion)) {
                return true;
            }
        }

        return false;
    }

    private TraverseCursorResult<List<TableObject>> listTablesParentBranchDistinct(
            TransactionContext context, DatabaseIdent parentDatabaseIdent,
            DatabaseName databaseName, String parentBranchVersion,
            List<Map<DatabaseIdent, String>> parentDatabaseList,
            int maxRowLimitNum, Boolean dropped, CatalogToken catalogToken, boolean listNameFlag) throws MetaStoreException {
        List<TableObject> parentBranchValidTables = new ArrayList<>();
        int remainResults = maxRowLimitNum;
        CatalogToken nextToken = catalogToken;
        String method = Thread.currentThread().getStackTrace()[1].getMethodName();

        ListTablesInBranchWithToken listTablesValid
            = (databaseId, dbName, branchVersion, maxResult, token, isDrop) -> listInUseTableWithToken(context, databaseId,
            dbName, branchVersion, maxResult, isDrop, token, listNameFlag);

        ListTablesInBranchWithToken listTablesDropped
            = (databaseId, dbName, branchVersion, maxResult, token, isDrop) -> listDroppedTablesWithToken(context, databaseId,
            dbName, branchVersion, maxResult, isDrop, token);

        Map<Integer, ListTablesInBranchWithToken> listStepMap = new HashMap<>();
        listStepMap.put(0, listTablesValid);
        listStepMap.put(1, listTablesDropped);

        Integer step = (catalogToken.getContextMapValue(method) == null)
            ? 0 : Integer.valueOf(catalogToken.getContextMapValue(method));

        TraverseCursorResult<List<TableObject>> parentBranchTables = null;
        while (step < listStepMap.size()) {
            while (true) {
                parentBranchTables = listStepMap.get(step).list(parentDatabaseIdent, databaseName,
                    parentBranchVersion, remainResults, nextToken, dropped);

                //No data is found, Exit.
                if (parentBranchTables.getResult().size() == 0) {
                    break;
                }

                nextToken = parentBranchTables.getContinuation().orElse(null);

                //check whether the table duplicate appears in the subbranch traversal
                List<TableObject> parentBranchTableList = parentBranchTables.getResult().stream()
                    .filter(tableRecord -> !checkTableExistInSubBranch(context, parentDatabaseList, tableRecord))
                    .collect(toList());

                parentBranchValidTables.addAll(parentBranchTableList);
                remainResults = remainResults - parentBranchTableList.size();
                //If the token is null, there is no valid data in the next traversal.
                if ((remainResults == 0) || (nextToken == null)) {
                    break;
                }
            }

            if (remainResults == 0) {
                break;
            }
            nextToken = new CatalogToken(TABLE_STORE_CHECKSUM, catalogToken.getReadVersion());
            step++;
        }

        String stepValue = String.valueOf(step);
        CatalogToken catalogTokenNew = parentBranchTables.getContinuation().map(token -> {
            token.putContextMap(method, stepValue);
            return token;
        }).orElse(null);

        return new TraverseCursorResult<>(parentBranchValidTables, catalogTokenNew);
    }

    private TraverseCursorResult<List<TableObject>> listTablesWithToken(TransactionContext context, DatabaseIdent databaseIdent,
                                                                        DatabaseName databaseName, int maxResultNum, CatalogToken catalogToken,
                                                                        Boolean dropped, boolean listNameFlag) {
        int remainResults = maxResultNum;
        List<TableObject> listSum = new ArrayList<>();
        List<Map<DatabaseIdent, String>> parentDatabaseList = getParentDatabaseIdent(context, databaseIdent);
        Map<DatabaseIdent, String> subBranchMap = new HashMap<>();
        String version = catalogToken.getReadVersion();
        subBranchMap.put(databaseIdent, version);
        parentDatabaseList.add(0, subBranchMap);

        String readVersion = catalogToken.getReadVersion();
        CatalogToken nextToken = catalogToken;
        String method = Thread.currentThread().getStackTrace()[1].getMethodName();
        List<Map<DatabaseIdent, String>> curParentDatabaseList = parentDatabaseList;
        //find the parent branch breakpoint based on the token information.
        if (catalogToken.getContextMapValue(method) != null) {
            String nextParentCatalogId = catalogToken.getContextMapValue(method);
            curParentDatabaseList = cutParentBranchListWithToken(parentDatabaseList, nextParentCatalogId);
        }

        TraverseCursorResult<List<TableObject>> parentBranchValidTables = null;
        DatabaseIdent parentDatabaseIdent = null;
        for (Map<DatabaseIdent, String> map : curParentDatabaseList) {
            Iterator<Map.Entry<DatabaseIdent, String>> iterator = map.entrySet().iterator();
            Map.Entry<DatabaseIdent, String> entry = iterator.next();
            String subBranchVersion = entry.getValue();
            parentDatabaseIdent = entry.getKey();

            parentBranchValidTables = listTablesParentBranchDistinct(
                    context, parentDatabaseIdent, databaseName, subBranchVersion, parentDatabaseList, remainResults, dropped,
                nextToken, listNameFlag);

            List<TableObject> parentBranchValidTableList = parentBranchValidTables.getResult().stream()
                .map(tableRecord -> {
                    tableRecord.setCatalogId(databaseIdent.getCatalogId());
                    tableRecord.setCatalogName(databaseName.getCatalogName());
                    return tableRecord;
                })
                .collect(toList());
            listSum.addAll(parentBranchValidTableList);
            remainResults = remainResults - parentBranchValidTableList.size();
            if (remainResults == 0) {
                break;
            }

            nextToken = new CatalogToken(TABLE_STORE_CHECKSUM, readVersion);
        }

        String stepValue = parentDatabaseIdent.getCatalogId();
        CatalogToken catalogTokenNew = parentBranchValidTables.getContinuation().map(token -> {
            token.putContextMap(method, stepValue);
            return token;
        }).orElse(null);

        return new TraverseCursorResult<>(listSum, catalogTokenNew);
    }

    private TraverseCursorResult<List<TableObject>> listTableByName(DatabaseName databaseName, Boolean includeDeleted,
        Integer maxResults, String pageToken, String filter) {
        //Trade-off between multiple concurrency and Transaction, Without transaction,during the request query,
        // the common inclusion is the result set at this moment
        return TransactionRunnerUtil.transactionRunThrow(context -> {
            return listTableByName(context, databaseName, includeDeleted, maxResults, pageToken, filter, false);
        }).getResult();
    }

    private TraverseCursorResult<List<TableObject>> listTableByName(TransactionContext context,
        DatabaseName databaseName, Boolean includeDeleted, Integer maxResults,
        String pageToken, String filter, boolean listNameFlag) throws MetaStoreException {
        int remainResults = maxResults;

        if (remainResults <= 0) {
            return new TraverseCursorResult(Collections.emptyList(), null);
        }

        DatabaseIdent databaseIdent = DatabaseObjectHelper.getDatabaseIdent(context, databaseName);
        CheckUtil.assertNotNull(databaseIdent, ErrorCode.DATABASE_NOT_FOUND, databaseName.getDatabaseName());

        String version = VersionManagerHelper.getLatestVersion(context, databaseIdent.getProjectId(),
            databaseIdent.getRootCatalogId());

        String method = Thread.currentThread().getStackTrace()[1].getMethodName();

        Optional<CatalogToken> catalogToken = CatalogToken.parseToken(pageToken, TABLE_STORE_CHECKSUM);
        if (!catalogToken.isPresent()) {

            catalogToken = Optional.of(
                CatalogToken.buildCatalogToken(TABLE_STORE_CHECKSUM, method, String.valueOf(false), version));
        }

        List<TableObject> listSum = new ArrayList<>();
        TraverseCursorResult<List<TableObject>> stepResult;

        CatalogToken nextCatalogToken = catalogToken.get();
        boolean dropped = (nextCatalogToken.getContextMapValue(method) == null)
            ? false : Boolean.valueOf(nextCatalogToken.getContextMapValue(method));
        String readVersion = nextCatalogToken.getReadVersion();
        while (true) {
            stepResult = listTablesWithToken(context, databaseIdent, databaseName, remainResults,
                nextCatalogToken, dropped, listNameFlag);

            nextCatalogToken = new CatalogToken(TABLE_STORE_CHECKSUM, readVersion);
            listSum.addAll(filterPattern(stepResult.getResult(), filter));
            remainResults = remainResults - stepResult.getResult().size();
            if (remainResults == 0) {
                break;
            }

            if ((dropped == includeDeleted) && (!stepResult.getContinuation().isPresent())) {
                break;
            }

            dropped = true;
        }

        String stepValue = String.valueOf(dropped);
        CatalogToken catalogTokenNew = stepResult.getContinuation().map(token -> {
            token.putContextMap(method, stepValue);
            return token;
        }).orElse(null);

        return new TraverseCursorResult(listSum, catalogTokenNew);
    }

    private Collection<? extends TableObject> filterPattern(List<TableObject> result, String filter) {
        if (StringUtils.isEmpty(filter)) {
            return result;
        }
        Pattern pattern = TableUtil.getFilterPattern(filter);
        return result.stream().filter(x -> pattern.matcher(x.getName()).matches()).collect(toList());
    }


    @Override
    public TraverseCursorResult<List<Table>> listTable(DatabaseName databaseName, Boolean includeDeleted,
        Integer maxResults, String pageToken, String filter) {

        try {
            TraverseCursorResult<List<TableObject>> tableRecordList = listTableByName(databaseName,
                includeDeleted,
                maxResults,
                pageToken, filter);
            if (tableRecordList.getResult().isEmpty()) {
                return new TraverseCursorResult(Collections.emptyList(), null);
            }

            List<Table> tableList = new ArrayList<>(tableRecordList.getResult().size());
            for (TableObject record : tableRecordList.getResult()) {
                tableList.add(TableObjectConvertHelper.toTableModel(record));
            }
            return new TraverseCursorResult(tableList, tableRecordList.getContinuation().orElse(null));
        } catch (MetaStoreException e) {
            e.printStackTrace();
            throw new CatalogServerException(e.getMessage(), e.getErrorCode());
        }
    }

    @Override
    public PagedList<String> listTableNames(DatabaseName databaseFullName, Boolean includeDrop, Integer maxResults, String pageToken, String filter) {
        PagedList<String> result = new PagedList<>();
        return TransactionRunnerUtil.transactionRunThrow(context -> {
            TraverseCursorResult<List<TableObject>> cursorResult = listTableByName(context, databaseFullName, includeDrop, maxResults, pageToken, filter, true);
            result.setObjects(cursorResult.getResult().stream().map(TableObject::getName).toArray(String[]::new));
            cursorResult.getContinuation().ifPresent(catalogToken -> result.setNextMarker(catalogToken.toString()));
            return result;
        }).getResult();
    }

    @Override
    public void truncateTable(TableName tableName) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "truncateTable");
    }

    private void deleteObjectNameMapIfExist(TransactionContext context, TableName tableName, TableIdent tableIdent) {
        Optional<ObjectNameMap> objectNameMapOptional = objectNameMapStore
            .getObjectNameMap(context, tableName.getProjectId(), ObjectType.TABLE,
                tableName.getDatabaseName(), tableName.getTableName());
        // 2 layer object name, which may map to multiple Catalogs
        // If catalog ids are different, the same table object map record exist
        // If the catalog Id is different from the current table,
        // the map is manually modified and the 2Layer object is mapped to another catalog.
        // In this case, the map table is not modified
        if ((objectNameMapOptional.isPresent())
            && (objectNameMapOptional.get().getTopObjectId().equals(tableIdent.getCatalogId()))) {
            objectNameMapStore.deleteObjectNameMap(context,
                tableName.getProjectId(), ObjectType.TABLE, tableName.getDatabaseName(),
                tableName.getTableName());
        }
    }

    private void deleteTableRecordMetaData(TransactionContext context, TableIdent tableIdent, TableName tableName) {
        /*// delete tableReference
        TableReferenceObject tableReference = tableStore.getTableReference(context, tableIdent);
        if (tableReference != null) {
            tableStore.deleteTableReference(context, tableIdent);
        }

        //delete table schema
        TableSchemaObject tableSchema = tableStore.getTableSchema(context, tableIdent);
        if (tableSchema != null) {
            tableStore.deleteTableSchema(context, tableIdent);
        }

        // delete Table properties
        TableBaseObject tableBase = tableStore.getTableBase(context, tableIdent);
        if (tableBase != null) {
            tableStore.deleteTableBase(context, tableIdent);
        }

        // delete table storage
        TableStorageObject tableStorage = tableStore.getTableStorage(context, tableIdent);
        if (tableStorage != null) {
            tableStore.deleteTableStorage(context, tableIdent);
        }

        //delete table objectName
        DatabaseIdent databaseIdent = StoreConvertor.databaseIdent(tableIdent);
        String tableId = tableStore.getTableId(context, databaseIdent,
            tableName.getTableName());
        if (tableId != null) {
            tableStore.deleteTableName(context, databaseIdent, tableName.getTableName());
        }*/
        tableMetaStore.deleteTableReference(context, tableIdent);
        tableMetaStore.deleteTable(context, tableIdent, tableName.getTableName());
        //delete table map
        deleteObjectNameMapIfExist(context, tableName, tableIdent);
    }

    private void dropTableInternal(TransactionContext context, TableName tableName, String catalogCommitEventId) {
        TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);

        int historySubspaceFlag = tableMetaStore.getTableHistorySubspaceFlag(context, tableIdent);

        deleteTableRecordMetaData(context, tableIdent, tableName);

        String latestVersion = VersionManagerHelper.getLatestVersion(context, tableIdent.getProjectId(), tableIdent.getRootCatalogId());
        TableCommitObject tableCommit = TableCommitHelper.getLatestTableCommit(context, tableIdent, latestVersion)
            .get();
        long time = RecordStoreHelper.getCurrentTime();

        //insert table to dropped object name store
        tableMetaStore.insertDroppedTable(context, tableIdent, tableName, tableCommit.getCreateTime(), time,
            false);


        TableHistorySubspaceHelper.createCommitSubspace(context, tableIdent, historySubspaceFlag);

        // insert the drop table into TableCommit subspace
        tableMetaStore.insertTableCommit(context, tableIdent,
            TableCommitHelper.buildNewTableCommit(tableCommit, latestVersion, time, time, TableOperationType.DDL_DROP_TABLE));

        // insert the drop table into CatalogCommit subspace
        catalogStore.insertCatalogCommit(context, tableIdent.getProjectId(),
            tableIdent.getCatalogId(), catalogCommitEventId, time, DROP_TABLE, tableOperateDetail(tableName));
    }


    private void dropTableByName(TableName tableName) throws MetaStoreException {
        String catalogCommitEventId = UuidUtil.generateCatalogCommitId();

        CatalogIdent catalogIdent;
        try (TransactionRunner runner = new TransactionRunner()) {
            catalogIdent = runner.run(context -> {
                CatalogName catalogName = StoreConvertor.catalogName(tableName.getProjectId(), tableName.getCatalogName());
                return CatalogObjectHelper.getCatalogIdent(context, catalogName);
            });
        }

        try (TransactionRunner runner = new TransactionRunner()) {
            runner.setMaxAttempts(TABLE_STORE_MAX_RETRY_NUM);
            runner.run(context -> {
                dropTableInternal(context, tableName, catalogCommitEventId);
                return null;
            });
        } catch (RuntimeException e) {
            // Retry interval, the table may have been deleted,
            // and the previous transaction that created the table may have succeeded

            // The Event ID for the Catalog commit is assigned before the transaction starts,
            // and the same event ID is used for each retry transaction.
            // the Event ID is used as the catalog commit primary key.
            // key checks whether the previous transaction has been successfully inserted.
            // If the transaction is successfully inserted, reports key existing exception.
            Optional<CatalogCommitObject> catalogCommit = CatalogCommitHelper
                .getCatalogCommit(catalogIdent, catalogCommitEventId);
            if (catalogCommit.isPresent()) {
                return;
            }

            throw e;
        }
    }

    @Override
    public void dropTable(TableName tableName, boolean deleteData, boolean ignoreUnknownTable, boolean ifPurge) {
        try {
            if (ifPurge) {
                dropTablePurgeByName(tableName);
            } else {
                dropTableByName(tableName);
            }
        } catch (MetaStoreException e) {
            throw new CatalogServerException(e.getMessage(), e.getErrorCode());
        }
    }


    private void undropTableInternal(TransactionContext context, TableName tableName, String tableId, String newName,
        String catalogCommitEventId) {
        DatabaseName databaseName = StoreConvertor.databaseName(tableName.getProjectId(),
            tableName.getCatalogName(), tableName.getDatabaseName());

        DatabaseIdent databaseIdent = DatabaseObjectHelper.getDatabaseIdent(context, databaseName);
        if (null == databaseIdent) {
            throw new MetaStoreException(ErrorCode.DATABASE_NOT_FOUND, databaseName.getDatabaseName());
        }
        String versionstamp = VersionManagerHelper.getLatestVersion(context, databaseIdent.getProjectId(),
            databaseIdent.getRootCatalogId());
        TableIdent tableIdent;
        if (tableId == null || StringUtils.isBlank(tableId)) {
            // The tableName must be unique. If there are multiple tables with the same name,
            // getDroppedTableIdent return null.
            tableIdent = TableObjectHelper.getOnlyDroppedTableIdentOrElseThrow(context, tableName);
        } else {
            // The tableId must be exist
            tableIdent = TableObjectHelper.getDroppedTableIdent(context, tableName, tableId);
        }
        // get table latest version
        TableCommitObject tableCommit = TableCommitHelper.getLatestTableCommit(context, tableIdent, versionstamp).get();

        // check whether table already exists
        String tableNameString = (StringUtils.isBlank(newName) ? tableCommit.getTableName() : newName);
        TableName tableNameNew = StoreConvertor
            .tableName(databaseName.getProjectId(), databaseName.getCatalogName(),
                databaseName.getDatabaseName(), tableNameString);
        throwIfTableNameExist(context, databaseIdent, tableNameString);

        //delete dropped table name
        tableMetaStore.deleteDroppedTable(context, tableIdent, tableName);

        TableBaseHistoryObject tableBaseHistoryObject = TableBaseHelper
            .getLatestTableBaseOrElseThrow(context, tableIdent, versionstamp);
        TableBaseObject tableBaseObject = tableBaseHistoryObject.getTableBaseObject();

        TableSchemaHistoryObject tableSchemaHistory = TableSchemaHelper
            .getLatestTableSchemaOrElseThrow(context, tableIdent, versionstamp);
        TableSchemaObject tableSchemaObject = tableSchemaHistory.getTableSchemaObject();

        TableStorageHistoryObject tableStorageHistory = TableStorageHelper
            .getLatestTableStorageOrElseThrow(context, tableIdent, versionstamp);
        TableStorageObject tableStorageObject = tableStorageHistory.getTableStorageObject();

        tableMetaStore.upsertTableReference(context, tableIdent);

        int historySubspaceFlag = tableMetaStore.getTableHistorySubspaceFlag(context, tableIdent);
        tableMetaStore.upsertTable(context, tableIdent, tableNameString, historySubspaceFlag, tableBaseObject,
            tableSchemaObject, tableStorageObject);

        /*// insert table obj name
        tableStore.insertTableName(context, tableIdent, tableNameString);
        // insert table reference
        tableStore.insertTableReference(context, tableIdent, tableNameString);
        // insert table properties
        tableStore.insertTableBase(context, tableIdent, tableBaseObject);
        // insert table schema
        tableStore.insertTableSchema(context, tableIdent, tableSchemaObject);
        // insert table storage
        tableStore.insertTableStorage(context, tableIdent, tableStorageObject);*/

        //insert table map if not exist
        saveObjectNameMapIfNotExist(context, tableNameNew, tableIdent, tableBaseHistoryObject.getTableBaseObject()
            .getParameters());

        // insert the drop table into TableCommit subspace
        long commitTime = RecordStoreHelper.getCurrentTime();
        tableCommit.setTableName(tableNameString);
        tableMetaStore.insertTableCommit(context, tableIdent,
            TableCommitHelper.buildNewTableCommit(tableCommit, versionstamp,
                commitTime, 0, TableOperationType.DDL_UNDROP_TABLE));

        // insert catalog commit.
        catalogStore.insertCatalogCommit(context, tableIdent.getProjectId(), tableIdent.getCatalogId(),
            catalogCommitEventId, commitTime, UNDROP_TABLE, tableOperateDetail(tableNameNew));
    }

    private void undropTableByName(TableName tableName, String tableId, String newName) throws MetaStoreException {
        String catalogCommitEventId = UuidUtil.generateCatalogCommitId();

        CatalogIdent catalogIdent;
        try (TransactionRunner runner = new TransactionRunner()) {
            catalogIdent = runner.run(context -> {
                CatalogName catalogName = StoreConvertor.catalogName(tableName.getProjectId(), tableName.getCatalogName());
                return CatalogObjectHelper.getCatalogIdent(context, catalogName);
            });
        }

        try (TransactionRunner runner = new TransactionRunner()) {
            runner.setMaxAttempts(TABLE_STORE_MAX_RETRY_NUM);
            runner.run(context -> {
                undropTableInternal(context, tableName, tableId, newName, catalogCommitEventId);
                return null;
            });
        } catch (RuntimeException e) {
            // Retry interval, the DB may have been deleted,
            // and the previous transaction that created the table may have succeeded
            Optional<CatalogCommitObject> catalogCommit = CatalogCommitHelper
                .getCatalogCommit(catalogIdent, catalogCommitEventId);
            if (catalogCommit.isPresent()) {
                return;
            }

            throw e;
        }
    }

    @Override
    public void undropTable(TableName tableName, String tableId, String newName) {
        try {
            undropTableByName(tableName, tableId, newName);
        } catch (MetaStoreException e) {
            throw new CatalogServerException(e.getMessage(), e.getErrorCode());
        }
    }

    private void purgeTableInternal(TransactionContext context, TableName tableName, String tableId, String jobId,
        String catalogCommitEventId) {
        TableIdent tableIdent;
        if (tableId == null || StringUtils.isBlank(tableId)) {
            // The tableName must be unique. If there are multiple tables with the same name,
            // getDroppedTableIdent return null.
            tableIdent = TableObjectHelper.getOnlyDroppedTableIdentOrElseThrow(context, tableName);
        } else {
            // The tableId must be exist
            tableIdent = TableObjectHelper.getDroppedTableIdent(context, tableName, tableId);
        }

        String latestVersion = VersionManagerHelper.getLatestVersion(context, tableIdent.getProjectId(), tableIdent.getRootCatalogId());
        comfirmTableCanPurgedOrThrow(context, tableIdent, tableName);
        TableCommitObject tableCommit = TableCommitHelper.getLatestTableCommit(context, tableIdent, latestVersion)
            .get();
        tableMetaStore.insertDroppedTable(context, tableIdent, tableName, tableCommit.getCreateTime(),
            tableCommit.getDroppedTime(), true);
        deleteRolePrivilege(context, tableIdent, tableName);
        backendTaskStore.submitDelHistoryTableJob(context, tableIdent, tableName, latestVersion, jobId);
        // insert catalog commit.
        long commitTime = System.currentTimeMillis();
        catalogStore.insertCatalogCommit(context, tableIdent.getProjectId(), tableIdent.getCatalogId(),
            catalogCommitEventId, commitTime, PURGE_TABLE, tableOperateDetail(tableName));
    }

    private void purgeTableByName(TableName tableName, String tableId) throws MetaStoreException {
        String taskId = UuidUtil.generateId();
        String catalogCommitEventId = UuidUtil.generateCatalogCommitId();

        CatalogIdent catalogIdent;
        try (TransactionRunner runner = new TransactionRunner()) {
            catalogIdent = runner.run(context -> {
                CatalogName catalogName = StoreConvertor.catalogName(tableName.getProjectId(), tableName.getCatalogName());
                return CatalogObjectHelper.getCatalogIdent(context, catalogName);
            });
        }

        try (TransactionRunner runner = new TransactionRunner()) {
            runner.setMaxAttempts(TABLE_STORE_MAX_RETRY_NUM);
            runner.run(context -> {
                purgeTableInternal(context, tableName, tableId, taskId, catalogCommitEventId);
                return null;
            });
        } catch (RuntimeException e) {
            // Retry interval, the table may have been deleted,
            // and the previous transaction that created the table may have succeeded
            Optional<CatalogCommitObject> catalogCommit = CatalogCommitHelper
                .getCatalogCommit(catalogIdent, catalogCommitEventId);
            if (catalogCommit.isPresent()) {
                return;
            }

            if (e instanceof MetaStoreException) {
                MetaStoreException mex = (MetaStoreException) e;
                if (mex.getErrorCode() == ErrorCode.TABLE_NOT_FOUND) {
                    throw new MetaStoreException(ErrorCode.TABLE_PURGE_NOT_FOUND, tableName.getTableName());
                }
            }

            throw e;
        }

        try (TransactionContext context = transaction.openTransaction()) {
            Optional<BackendTaskObject> backendTaskObject = backendTaskStore.getDelHistoryTableJob(context, taskId);
            if (backendTaskObject.isPresent()) {
                dealPurgeTableTask(backendTaskObject.get(), Long.MAX_VALUE);
            }
        }
    }

    @Override
    public void purgeTable(TableName tableName, String tableId) {
        try {
            purgeTableByName(tableName, tableId);
        } catch (MetaStoreException e) {
            throw new CatalogServerException(e.getMessage(), e.getErrorCode());
        }

    }


    private Optional<TableCommitObject> getSubBranchFakeTableCommit(TransactionContext context,
        TableIdent subBranchTableIdent,
        String version) throws MetaStoreException {
        DatabaseIdent parentDatabaseIdent = getParentBranchDatabaseIdent(context, subBranchTableIdent, version);

        TableIdent parentTableIdent = StoreConvertor.tableIdent(parentDatabaseIdent,
            subBranchTableIdent.getTableId());

        Optional<TableCommitObject> tableCommit = tableMetaStore.getTableCommit(context, parentTableIdent, version);

        if (tableCommit.isPresent()) {
            tableCommit.get().setCatalogId(subBranchTableIdent.getCatalogId());
            return tableCommit;
        }

        return Optional.empty();
    }

    private DatabaseIdent getParentBranchDatabaseIdent(TransactionContext context, TableIdent subBranchTableIdent,
        String version) {
        ParentBranchDatabaseIterator parentBranchDatabaseIterator = new ParentBranchDatabaseIterator(context,
            subBranchTableIdent, version);

        if (parentBranchDatabaseIterator.hasNext()) {
            DatabaseIdent parentDatabaseIdent = parentBranchDatabaseIterator.nextDatabase();
            return parentDatabaseIdent;
        }

        return null;
    }

    private Optional<TableCommitObject> getTableCommit(TransactionContext context, TableIdent tableIdent,
        String version) {
        Optional<TableCommitObject> tableCommit = tableMetaStore.getTableCommit(context, tableIdent, version);
        if (!tableCommit.isPresent()) {
            tableCommit = getSubBranchFakeTableCommit(context, tableIdent, version);
        }

        return tableCommit;
    }

    private TableCommitObject getTableCommitOrElseThrow(TransactionContext context, TableIdent tableIdent,
        String version) throws MetaStoreException {
        Optional<TableCommitObject> tableCommit = getTableCommit(context, tableIdent, version);
        if (!tableCommit.isPresent()) {
            throw new MetaStoreException(ErrorCode.TABLE_COMMIT_NOT_FOUND, tableIdent.getTableId());
        }
        return tableCommit.get();
    }

    private void throwIfTableNameExistAndIdDiff(TransactionContext context, DatabaseIdent databaseIdent,
        String tableName, String tableId) {
        String tableIdent = TableObjectHelper.getTableId(context, databaseIdent, tableName);
        if (tableIdent != null) {
            if (!tableIdent.equals(tableId)) {
                throw new MetaStoreException(ErrorCode.TABLE_ALREADY_EXIST, tableName);
            }
        }
    }

    private void throwIfTableNameExist(TransactionContext context, DatabaseIdent databaseIdent, String tableName) {
        String tableId = TableObjectHelper.getTableId(context, databaseIdent, tableName);
        if (tableId != null) {
            throw new MetaStoreException(ErrorCode.TABLE_ALREADY_EXIST, tableName);
        }
    }

    private String tableRestoreDetail(TableName tableName, String version) {
        String commitDetail = tableOperateDetail(tableName);
        return commitDetail + ", version: " + version;
    }

    private void restoreTableInternal(TransactionContext context, TableName tableName, String versionstamp,
        String catalogCommitEventId) {
        // convert TableName to TableIdent
        TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);

        String version = VersionManagerHelper.getLatestVersion(context, tableIdent.getProjectId(),
            tableIdent.getRootCatalogId());

        // get the latest table commit
        TableCommitObject tableCommit = getTableCommitOrElseThrow(context, tableIdent, versionstamp);
        TableName tableNameHistory = StoreConvertor.tableName(tableIdent.getProjectId(),
            tableIdent.getCatalogId(), tableIdent.getDatabaseId(), tableCommit.getTableName());

        //drop current table name
        DatabaseIdent originalDatabaseIdent = StoreConvertor.databaseIdent(tableIdent.getProjectId(),
            tableIdent.getCatalogId(), tableIdent.getDatabaseId(), tableIdent.getRootCatalogId());

        int historySubspaceFlag = tableMetaStore.getTableHistorySubspaceFlag(context, tableIdent);

        tableMetaStore.deleteTableReference(context, tableIdent);
        tableMetaStore.deleteTable(context, tableIdent, tableName.getTableName());
        //DatabaseIdent databaseIdent = StoreConvertor.databaseIdent(tableIdent);
        //tableStore.deleteTableName(context, databaseIdent, tableName.getTableName());
        deleteObjectNameMapIfExist(context, tableName, tableIdent);

        long commitTime = RecordStoreHelper.getCurrentTime();
        if (TableCommitHelper.isDropCommit(tableCommit)) {
            tableMetaStore.insertDroppedTable(context, tableIdent, tableName, tableCommit.getCreateTime(), commitTime,
                false);
            tableCommit = TableCommitHelper
                .buildNewTableCommit(tableCommit, version, commitTime,
                    tableCommit.getDroppedTime(), TableOperationType.DDL_RESTORE_TABLE);
        } else {
            //check whether the current table name is the same, but the table ID is different.
            if (!tableNameHistory.getTableName().equals(tableName.getTableName())) {
                throwIfTableNameExistAndIdDiff(context, originalDatabaseIdent, tableCommit.getTableName(),
                    tableIdent.getTableId());
            }

            tableCommit = TableCommitHelper
                .buildNewTableCommit(tableCommit, version, commitTime, 0, TableOperationType.DDL_RESTORE_TABLE);
            //tableStore.insertTableName(context, tableIdent, tableNameHistory.getTableName());
        }

        TableBaseHistoryObject tableBaseHistoryObject = TableBaseHelper
            .getLatestTableBaseOrElseThrow(context, tableIdent, versionstamp);
        TableBaseObject tableBase = tableBaseHistoryObject.getTableBaseObject();

        TableSchemaHistoryObject tableSchemaHistory = TableSchemaHelper
            .getLatestTableSchemaOrElseThrow(context, tableIdent, versionstamp);
        TableSchemaObject tableSchema = tableSchemaHistory.getTableSchemaObject();

        TableStorageHistoryObject tableStorageHistory = TableStorageHelper
            .getLatestTableStorage(context, tableIdent, versionstamp).get();
        TableStorageObject tableStorage = tableStorageHistory.getTableStorageObject();

        tableMetaStore.upsertTableReference(context, tableIdent);


        historySubspaceFlag = TableHistorySubspaceHelper.createHistorySubspace(context, tableIdent, historySubspaceFlag);

        tableMetaStore.insertTable(context, tableIdent, tableNameHistory.getTableName(), historySubspaceFlag, tableBase,
            tableSchema, tableStorage);


        /*// insert table reference
        TableReferenceObject tableReferenceObject = new TableReferenceObject(tableNameHistory.getTableName(),
            System.currentTimeMillis(), System.currentTimeMillis());
        tableStore.upsertTableReference(context, tableIdent, tableReferenceObject);
        // insert table base
        tableStore.upsertTableBase(context, tableIdent, tableBase);
        // insert table schema
        tableStore.upsertTableSchema(context, tableIdent, tableSchema);
        // insert table storage
        tableStore.upsertTableStorage(context, tableIdent, tableStorage);*/

        // insert table base history
        tableMetaStore.insertTableBaseHistory(context, tableIdent, version, tableBase);
        // insert table schema history
        tableMetaStore.insertTableSchemaHistory(context, tableIdent, version, tableSchema);
        // insert table storage history
        tableMetaStore.insertTableStorageHistory(context, tableIdent, version, tableStorage);

        //insert table obj map
        if (!TableCommitHelper.isDropCommit(tableCommit)) {
            TableName tableNameNew = new TableName(tableName);
            tableNameNew.setTableName(tableNameHistory.getTableName());
            //saveObjectNameMapIfNotExist(context, tableNameNew, tableIdent, tableBase.getParameters());
        }

        // insert table history
        // get the latest table history
        Optional<TableHistoryObject> tableHistory = TableHistoryHelper
            .getLatestTableHistory(context, tableIdent, versionstamp);
        tableDataStore.insertTableHistory(context, tableIdent, version, tableHistory.get());

        // insert table indexes if present
        Optional<TableIndexesHistoryObject> optionalTableIndexesHistory =
            TableIndexHelper.getLatestTableIndexes(context, tableIdent, versionstamp);
        if (optionalTableIndexesHistory.isPresent()) {
            TableIndexesObject tableIndexes = tableDataStore.insertTableIndexes(context, tableIdent,
                optionalTableIndexesHistory.get().getTableIndexInfoObjectList());

            // insert table indexes history
            tableDataStore.insertTableIndexesHistory(context, tableIdent, version, tableIndexes);
        }

        // insert the drop table into TableCommit subspace
        tableMetaStore.insertTableCommit(context, tableIdent, tableCommit);

        // insert catalog commit
        catalogStore.insertCatalogCommit(context, tableIdent.getProjectId(), tableIdent.getCatalogId(),
            catalogCommitEventId, commitTime, RESTORE_TABLE,
            tableRestoreDetail(tableNameHistory, CodecUtil.bytes2Hex(versionstamp.getBytes()))
        );
    }


    private void restoreTableByName(TableName tableName, String version) throws MetaStoreException {
        String versionstamp = version;
        String catalogCommitEventId = UuidUtil.generateCatalogCommitId();

        CatalogIdent catalogIdent;
        try (TransactionRunner runner = new TransactionRunner()) {
            catalogIdent = runner.run(context -> {
                CatalogName catalogName = StoreConvertor.catalogName(tableName.getProjectId(), tableName.getCatalogName());
                return CatalogObjectHelper.getCatalogIdent(context, catalogName);
            });
        }

        try (TransactionRunner runner = new TransactionRunner()) {
            runner.setMaxAttempts(TABLE_STORE_MAX_RETRY_NUM);
            runner.run(context -> {
                restoreTableInternal(context, tableName, versionstamp, catalogCommitEventId);
                return null;
            });
        } catch (RuntimeException e) {
            // Retry interval, the table may have been deleted,
            // and the previous transaction that created the table may have succeeded
            Optional<CatalogCommitObject> catalogCommit = CatalogCommitHelper
                .getCatalogCommit(catalogIdent, catalogCommitEventId);
            if (catalogCommit.isPresent()) {
                return;
            }

            throw e;
        }
    }

    @Override
    public void restoreTable(TableName tableName, String version) {
        try {
            restoreTableByName(tableName, version);
        } catch (MetaStoreException e) {
            throw new CatalogServerException(e.getMessage(), e.getErrorCode());
        }
    }

    private boolean isCheckoutBranch(TableName tableName, TableInput tableInput) {
        return false;
        /*
        if (tableInput.getParameters() == null || !tableInput.getParameters().containsKey(LMS_KEY)) {
            return false;
        }

        Optional<MetaObjectName> tblInNameMap = ObjectNameMapHelper.getObjectFromNameMap(tableName.getProjectId(),
            ObjectType.TABLE.name(), tableName.getDatabaseName(), tableName.getTableName());
        String lmsName = tableInput.getParameters().get(LMS_KEY);
        if (tblInNameMap.isPresent() && tblInNameMap.get().getCatalogName().equals(lmsName)) {
            return false;
        }
        return true;
         */
    }

    private void checkLmsNameValid(TransactionContext context, TableName tableName, String lmsName) {
        CatalogId catalogId = catalogStore
            .getCatalogId(context, tableName.getProjectId(), tableName.getCatalogName());
        if (catalogId == null) {
            throw new MetaStoreException(ErrorCode.CATALOG_NOT_FOUND, lmsName);
        }

        TableName tableNameCheck = StoreConvertor.tableName(tableName.getProjectId(), lmsName,
            tableName.getDatabaseName(), tableName.getTableName());
        TableObjectHelper.getTableIdent(context, tableNameCheck);
        return;
    }

    private void updateObjectNameMap(TransactionContext context, TableName tableName, TableIdent tableIdent,
        TableInput tableInput) {
        objectNameMapStore.deleteObjectNameMap(context, tableName.getProjectId(), ObjectType.TABLE,
            tableName.getDatabaseName(), tableName.getTableName());

        String versionstamp = VersionManagerHelper.getLatestVersion(context, tableIdent.getProjectId(), tableIdent.getRootCatalogId());
        TableBaseHistoryObject tableBaseHistoryObject = TableBaseHelper.getLatestTableBaseOrElseThrow(context, tableIdent,
            versionstamp);

        Optional<String> catalogName = resolveCatalogNameFromInput(tableInput.getParameters(), LMS_KEY);
        //It's either a three Layer format or an invalid value
        TableIdent tableIdentNew;
        if (catalogName.isPresent()) {
            checkLmsNameValid(context, tableName, catalogName.get());
            CatalogId catalogId = catalogStore
                .getCatalogId(context, tableName.getProjectId(), catalogName.get());
            if (catalogId == null) {
                throw new MetaStoreException(ErrorCode.CATALOG_NOT_FOUND, catalogName.get());
            }

            tableIdentNew = new TableIdent(tableIdent.getProjectId(), catalogId.getCatalogId(),
                tableIdent.getDatabaseId(), tableIdent.getTableId(), tableIdent.getRootCatalogId());

            tableMetaStore.upsertTableReference(context, tableIdentNew);

            TableObject tableObject = tableMetaStore.getTable(context, tableIdent, tableName);
            tableObject.getTableBaseObject().getParameters().put(LMS_KEY, tableInput.getParameters().get(LMS_KEY));
            int historySubspaceFlag = tableMetaStore.getTableHistorySubspaceFlag(context, tableIdentNew);
            historySubspaceFlag = TableHistorySubspaceHelper.createBaseHistorySubspace(context, tableIdentNew, historySubspaceFlag);
            tableMetaStore.upsertTable(context, tableIdentNew, tableObject.getName(), historySubspaceFlag,
                tableObject.getTableBaseObject(), tableObject.getTableSchemaObject(),
                tableObject.getTableStorageObject());
            tableMetaStore.insertTableBaseHistory(context, tableIdentNew, versionstamp, tableObject.getTableBaseObject());

            // insert table properties
            /*TableBaseObject tableBaseObject = tableBaseHistoryObject.getTableBaseObject();
            tableBaseObject.getParameters().put(LMS_KEY, tableInput.getParameters().get(LMS_KEY));
            tableMetaStore.upsertTableBase(context, tableIdent, tableBaseObject);
            tableMetaStore.insertTableBaseHistory(context, tableIdent, versionstamp, tableBaseObject);*/

            TableName tableNameNew = new TableName(tableName);
            tableNameNew.setTableName(tableInput.getTableName());
            //ObjectNameMap objectNameMap = buildObjectNameMap(tableNameNew, tableIdentNew);
            ObjectNameMap objectNameMap = new ObjectNameMap(tableNameNew, tableIdentNew);
            objectNameMapStore.insertObjectNameMap(context, objectNameMap);
        } else {
            throw new MetaStoreException(ErrorCode.ARGUMENT_ILLEGAL, "lms_name empty");
        }
    }

    private void insertTableNameIntoSubBranch(TransactionContext context, TableIdent parentBranchTableIdent,
        String tableName) {
        CatalogIdent parentBranchCatalogIdent = StoreConvertor.catalogIdent(parentBranchTableIdent.getProjectId(),
            parentBranchTableIdent.getCatalogId(), parentBranchTableIdent.getRootCatalogId());

        List<CatalogObject> subBranchCatalogList = catalogStore
            .getNextLevelSubBranchCatalogs(context, parentBranchCatalogIdent);

        for (CatalogObject catalogRecord : subBranchCatalogList) {
            CatalogIdent subBranchCatalogIdent = StoreConvertor.catalogIdent(catalogRecord.getProjectId(),
                catalogRecord.getCatalogId(), catalogRecord.getRootCatalogId());
            TableIdent subBranchTableIdent = StoreConvertor.tableIdent(catalogRecord.getProjectId(),
                catalogRecord.getCatalogId(), parentBranchTableIdent.getDatabaseId(),
                parentBranchTableIdent.getTableId(), catalogRecord.getRootCatalogId());

            //Check whether the sub-branch table has a name.
            DatabaseIdent subBranchDatabaseIdent = StoreConvertor.databaseIdent(subBranchCatalogIdent,
                subBranchTableIdent.getDatabaseId());

            List<TableIdent> tableIdentList = TableObjectHelper.getTablesIdent(context, subBranchDatabaseIdent, true);
            Map<String, String> subBranchTableIds = tableIdentList.stream()
                .collect(Collectors.toMap(TableIdent::getTableId,
                    TableIdent::getCatalogId));
            if (subBranchTableIds.containsKey(subBranchTableIdent.getTableId())) {
                continue;
            }

            String latestVersion = VersionManagerHelper.getLatestVersion(context, subBranchTableIdent.getProjectId(),
                subBranchTableIdent.getRootCatalogId());
            /*TableReferenceObject tableReferenceObject = TableReferenceHelper
                .getTableReferenceOrElseThrow(context, parentBranchTableIdent);*/
            TableBaseObject tableBaseObject = TableBaseHelper
                .getLatestTableBaseOrElseThrow(context, parentBranchTableIdent, latestVersion).getTableBaseObject();
            TableSchemaObject tableSchemaObject = TableSchemaHelper
                .getLatestTableSchemaOrElseThrow(context, parentBranchTableIdent, latestVersion).getTableSchemaObject();
            TableStorageObject tableStorageObject = TableStorageHelper
                .getLatestTableStorageOrElseThrow(context, parentBranchTableIdent, latestVersion).getTableStorageObject();

            tableMetaStore.upsertTableReference(context, subBranchTableIdent);

            //int historySubspaceFlag = tableMetaStore.getTableHistorySubspaceFlag(context, parentBranchTableIdent);
            tableMetaStore.upsertTable(context, subBranchTableIdent, tableName, 0,
                tableBaseObject, tableSchemaObject, tableStorageObject);
            // insert an object into ObjectName subspace
            //tableStore.insertTableName(context, subBranchTableIdent, tableName);
        }
    }

    private void updateObjectNameMapIfExist(TransactionContext context, TableName tableName, TableIdent tableIdent,
        String tableNameNew) {
        String versionstamp = VersionManagerHelper.getLatestVersion(context, tableIdent.getProjectId(), tableIdent.getRootCatalogId());
        TableBaseHistoryObject tableBaseHistoryObject = TableBaseHelper.getLatestTableBaseOrElseThrow(context, tableIdent,
            versionstamp);

        Optional<String> catalogName = resolveCatalogNameFromInput(tableBaseHistoryObject.getTableBaseObject()
            .getParameters(), LMS_KEY);
        if (!catalogName.isPresent()) {
            return;
        }

        Optional<ObjectNameMap> objectNameMapOptional = objectNameMapStore
            .getObjectNameMap(context, tableName.getProjectId(), ObjectType.TABLE,
                tableName.getDatabaseName(),
                tableName.getTableName());
        // 2 layer object name, which may map to multiple Catalogs
        // If catalog ids are different, the same table object map record exist
        // If the catalog Id is different from the current table,
        // the map is manually modified and the 2Layer object is mapped to another catalog.
        // In this case, the map table is not modified
        if ((objectNameMapOptional.isPresent())
            && (!objectNameMapOptional.get().getTopObjectId().equals(tableIdent.getCatalogId()))) {
            return;
        }

        objectNameMapStore.deleteObjectNameMap(context, tableName.getProjectId(), ObjectType.TABLE,
            tableName.getDatabaseName(), tableName.getTableName());
        TableName tablePathNameNew = new TableName(tableName);
        tablePathNameNew.setTableName(tableNameNew);
        //ObjectNameMap objectNameMap = buildObjectNameMap(tablePathNameNew, tableIdent);
        ObjectNameMap objectNameMap = new ObjectNameMap(tablePathNameNew, tableIdent);
        objectNameMapStore.insertObjectNameMap(context, objectNameMap);
    }

    private void alterTableInternal(TransactionContext context, TableName tableName, TableInput tableInput,
        String catalogCommitEventId) {
        TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);

        String tableNameNew = tableInput.getTableName();
        String latestVersion = VersionManagerHelper.getLatestVersion(context, tableIdent.getProjectId(), tableIdent.getRootCatalogId());

        // update tableReference. This step is required for each branch process, in order to provide concurrency mutex
        /*TableReferenceObject tableReference = TableReferenceHelper.getTableReferenceOrElseThrow(context, tableIdent);
        tableReference.setMetaUpdateTime(System.currentTimeMillis());*/

        /*if (null != tableStore.getTableReference(context, tableIdent)) {
            tableStore.updateTableReference(context, tableIdent, tableReference);
        } else {
            tableStore.insertTableReference(context, tableIdent, tableReference.getName());
        }*/

        int historySubspaceFlag = tableMetaStore.getTableHistorySubspaceFlag(context, tableIdent);
        historySubspaceFlag = TableHistorySubspaceHelper.createCommitSubspace(context, tableIdent, historySubspaceFlag);
        if (isCheckoutBranch(tableName, tableInput)) {
            /******* alter object map *******/
            updateObjectNameMap(context, tableName, tableIdent, tableInput);
        } else {
            /******* alter table *******/
            // rename table
            if (!tableNameNew.equals(tableName.getTableName())) {
                CheckUtil.checkNameLegality("tableName", tableInput.getTableName());
                // update table object
                DatabaseIdent databaseIdent = StoreConvertor.databaseIdent(tableIdent);

                throwIfTableNameExist(context, databaseIdent, tableNameNew);

                updateObjectNameMapIfExist(context, tableName, tableIdent, tableNameNew);

                // If the parent branch has sub-branches, you need to add the old name to the sub-branches
                insertTableNameIntoSubBranch(context, tableIdent, tableName.getTableName());

                tableMetaStore.deleteTableReference(context, tableIdent);
                tableMetaStore.deleteTable(context, tableIdent, tableName.getTableName());

            }

            TableBaseHistoryObject tableBaseHistoryObject = TableBaseHelper.getLatestTableBaseOrElseThrow(context, tableIdent,
                latestVersion);
            TableBaseObject tableBaseObject = tableBaseHistoryObject.getTableBaseObject();
            // update table base
            updateTableBaseObject(context, tableInput, tableIdent, latestVersion, tableBaseObject);

            TableSchemaObject tableSchemaObject = TableSchemaHelper.getLatestTableSchemaOrElseThrow(context, tableIdent,
                latestVersion).getTableSchemaObject();
            TableStorageObject tableStorageObject = TableStorageHelper
                .getLatestTableStorageOrElseThrow(context, tableIdent, latestVersion).getTableStorageObject();
            TableStorageHelper.updateTableStorageObj(tableStorageObject, tableInput.getStorageDescriptor());
            tableMetaStore.upsertTableReference(context, tableIdent);

            tableMetaStore.upsertTable(context, tableIdent, tableNameNew, historySubspaceFlag,
                tableBaseObject, tableSchemaObject, tableStorageObject);
        }

        long commitTime = RecordStoreHelper.getCurrentTime();
        TableCommitObject tableCommit = TableCommitHelper
            .getLatestTableCommitOrElseThrow(context, tableIdent, latestVersion);
        tableCommit.setTableName(tableNameNew);
        // insert the alter into TableCommit subspac
        tableMetaStore.insertTableCommit(context, tableIdent,
            TableCommitHelper.buildNewTableCommit(tableCommit, latestVersion, commitTime,
                0, TableOperationType.DDL_DELETE_COLUMN));

        // insert the alter into CatalogCommit subspace
        catalogStore.insertCatalogCommit(context, tableIdent.getProjectId(),
            tableIdent.getCatalogId(), catalogCommitEventId, commitTime, ALTER_TABLE, tableOperateDetail(tableName)
        );
    }

    private void updateTableBaseObject(TransactionContext context, TableInput tableInput, TableIdent tableIdent, String latestVersion, TableBaseObject tableBaseObject) {
        boolean updateFlag = updateTableParams(tableInput, tableBaseObject)
                | updateTableCreateTime(tableInput, tableBaseObject)
                | updateTableTypeParams(tableInput, tableBaseObject);
        boolean alterOwnerFlag = updateTableOwner(tableInput, tableBaseObject);
        if (updateFlag || alterOwnerFlag) {
            tableMetaStore.insertTableBaseHistory(context, tableIdent, latestVersion, tableBaseObject);
            if(alterOwnerFlag) {
                // delete old owner privilege
                userPrivilegeStore.deleteUserPrivilege(context, tableIdent.getProjectId(), tableBaseObject.getOwner(),
                        ObjectType.TABLE.name(), tableIdent.getTableId());
                // insert user privilege table
                userPrivilegeStore.insertUserPrivilege(context, tableIdent.getProjectId(), tableInput.getOwner(),
                        ObjectType.TABLE.name(),
                        tableIdent.getTableId(), true, 0);
            }
        }
    }

    private boolean updateTableOwner(TableInput tableInput, TableBaseObject tableBaseObject) {
        if (tableInput.getOwner() != null && !tableInput.getOwner().equals(tableBaseObject.getOwner())) {
            tableBaseObject.setOwner(tableInput.getOwner());
            if (TableUtil.isIcebergTableByParams(tableBaseObject.getParameters())) {
                tableBaseObject.getParameters().put(Constants.OWNER_PARAM, tableInput.getOwner());
            }
            return true;
        }
        return false;
    }

    private boolean updateTableCreateTime(TableInput tableInput, TableBaseObject tableBaseObject) {
        if (tableInput.getCreateTime() != null && tableInput.getCreateTime() > 0) {
            tableBaseObject.setCreateTime(tableInput.getCreateTime());
            return true;
        }
        return false;
    }

    private boolean updateTableTypeParams(TableInput tableInput, TableBaseObject tableBaseObject) {
        boolean update = false;
        String tableType = tableInput.getTableType();
        try {
            if (StringUtils.isNotEmpty(tableType)) {
                switch (TableTypeInput.valueOf(tableType)) {
                    case VIRTUAL_VIEW:
                        if (StringUtils.isNotEmpty(tableInput.getViewExpandedText())) {
                            tableBaseObject.setViewExpandedText(tableInput.getViewExpandedText());
                            update = true;
                        }
                        if (StringUtils.isNotEmpty(tableInput.getViewOriginalText())) {
                            tableBaseObject.setViewOriginalText(tableInput.getViewOriginalText());
                            update = true;
                        }
                        break;
                    case MANAGED_TABLE:
                        break;
                    case EXTERNAL_TABLE:
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
            log.warn("Table: {}.{} type={} unresolved", tableInput.getDatabaseName(), tableInput.getTableName(), tableType, e);
        }
        return update;
    }

    private boolean updateTableParams(TableInput tableInput, TableBaseObject tableBaseObject) {
        if (tableInput.getParameters() != null && !tableInput.getParameters().isEmpty()) {
            tableBaseObject.getParameters().putAll(tableInput.getParameters());
            return true;
        }
        return false;
    }

    private void alterTableByName(TableName tableName, TableInput tableInput) throws MetaStoreException {
        String catalogCommitEventId = UuidUtil.generateCatalogCommitId();
        TransactionRunnerUtil.transactionRunThrow(context -> {
            CatalogName catalogName = StoreConvertor.catalogName(tableName.getProjectId(), tableName.getCatalogName());
            CatalogIdent catalogIdent = CatalogObjectHelper.getCatalogIdent(context, catalogName);
            try {
                alterTableInternal(context, tableName, tableInput, catalogCommitEventId);
            } catch (RuntimeException e) {
                // Retry interval, the table may have been deleted,
                // and the previous transaction that created the table may have succeeded
                Optional<CatalogCommitObject> catalogCommit = CatalogCommitHelper
                        .getCatalogCommit(catalogIdent, catalogCommitEventId);
                if (catalogCommit.isPresent()) {
                    return null;
                }
                throw e;
            }
            return null;
        }).getResult();

    }


    @Override
    public void alterTable(TableName tableName, TableInput tableInput,
        Map<String, String> alterTableParams) {
        try {
            alterTableByName(tableName, tableInput);
        } catch (MetaStoreException e) {
            e.printStackTrace();
            throw new CatalogServerException(e.getMessage(), e.getErrorCode());
        }
    }


    private boolean columnListHasName(String name, List<ColumnObject> columnList) {
        for (ColumnObject col : columnList) {
            if (col.getName().equals(name)) {
                return true;
            }
        }
        return false;
    }

    private int getBiggestOrdinal(List<ColumnObject> columnList) {
        int biggestOrdinal = 0;
        for (ColumnObject col : columnList) {
            biggestOrdinal = Math.max(biggestOrdinal, col.getOrdinal());
        }
        return biggestOrdinal;
    }

    private void fillColumnsWithStartOrdinal(List<ColumnObject> columnList, int startOrdinal) {
        int ord = startOrdinal;
        for (int i = 0; i < columnList.size(); ++i) {
            ColumnObject col = columnList.get(i);
            if (col.getOrdinal() == -1) {
                col.setOrdinal(ord);
                ++ord;
                --i; // revert index in case skipping unresolved element
            }
        }
    }

    private List<ColumnObject> buildAdditionalColumns(List<ColumnObject> oldColumnList,
        List<Column> addColumnList) {
        List<ColumnObject> newColumnList = new ArrayList<>(oldColumnList);
        addColumnList.stream().map(this::convertColumnInput).forEach(column -> {
            boolean dup = columnListHasName(column.getName(), newColumnList);
            if (dup) {
                throw new MetaStoreException(ErrorCode.COLUMN_ALREADY_EXISTS, column.getName());
            }
            newColumnList.add(column);
        });

        int startOrdinal = getBiggestOrdinal(oldColumnList);
        fillColumnsWithStartOrdinal(newColumnList, startOrdinal + 1);
        return newColumnList;
    }


    private void addColumnInternal(TransactionContext context, TableName tableName, List<Column> addColumnList,
        String catalogCommitEventId) {
        TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);
        String latestVersion = VersionManagerHelper.getLatestVersion(context, tableIdent.getProjectId(), tableIdent.getRootCatalogId());
        TableSchemaObject tableSchemaObject = TableSchemaHelper
            .getLatestTableSchemaOrElseThrow(context, tableIdent, latestVersion).getTableSchemaObject();
        List<ColumnObject> oldColumnList = tableSchemaObject.getColumns();
        List<ColumnObject> newColumnList = buildAdditionalColumns(oldColumnList, addColumnList);
        tableSchemaObject.setColumns(newColumnList);

        /*TableReferenceObject tableReference = TableReferenceHelper.getTableReferenceOrElseThrow(context, tableIdent);
        tableReference.setMetaUpdateTime(System.currentTimeMillis());*/

        TableBaseObject tableBaseObject = TableBaseHelper
            .getLatestTableBaseOrElseThrow(context, tableIdent, latestVersion).getTableBaseObject();
        TableStorageObject tableStorageObject = TableStorageHelper
            .getLatestTableStorageOrElseThrow(context, tableIdent, latestVersion).getTableStorageObject();

        TableCommitObject tableCommit = TableCommitHelper.getLatestTableCommit(context, tableIdent, latestVersion)
            .get();

        tableMetaStore.upsertTableReference(context, tableIdent);

        int historySubspaceFlag = tableMetaStore.getTableHistorySubspaceFlag(context, tableIdent);
        tableMetaStore.upsertTable(context, tableIdent, tableCommit.getTableName(), historySubspaceFlag,
            tableBaseObject, tableSchemaObject, tableStorageObject);

        /*// 1. update table schema subspace
        tableStore.upsertTableSchema(context, tableIdent, tableSchemaObject);

        // 3. update table reference subspace
        tableStore.upsertTableReference(context, tableIdent, tableReference);*/

        // 2. insert table schema history subspace
        tableMetaStore.insertTableSchemaHistory(context, tableIdent, latestVersion, tableSchemaObject);
        // 4. insert table commit subspace
        long commitTime = RecordStoreHelper.getCurrentTime();
        tableMetaStore.insertTableCommit(context, tableIdent,
            TableCommitHelper.buildNewTableCommit(tableCommit, latestVersion, commitTime,
                0, TableOperationType.DDL_ADD_COLUMN));

        // 5. insert catalog commit subspace
        catalogStore.insertCatalogCommit(context, tableIdent.getProjectId(),
            tableIdent.getCatalogId(), catalogCommitEventId, commitTime, ADD_COLUMN, tableOperateDetail(tableName));
    }

    private void addColumn(TableName tableName, List<Column> addColumnList) throws MetaStoreException {
        String catalogCommitEventId = UuidUtil.generateCatalogCommitId();

        CatalogIdent catalogIdent = null;
        try (TransactionRunner runner = new TransactionRunner()) {
            catalogIdent = runner.run(context -> {
                CatalogName catalogName = StoreConvertor.catalogName(tableName.getProjectId(), tableName.getCatalogName());
                return CatalogObjectHelper.getCatalogIdent(context, catalogName);
            });
        }

        try (TransactionRunner runner = new TransactionRunner()) {
            runner.setMaxAttempts(TABLE_STORE_MAX_RETRY_NUM);
            runner.run(context -> {
                addColumnInternal(context, tableName, addColumnList, catalogCommitEventId);
                return null;
            });
        } catch (RuntimeException e) {
            // Retry interval, the table may have been deleted,
            // and the previous transaction that created the table may have succeeded
            Optional<CatalogCommitObject> catalogCommit = CatalogCommitHelper
                .getCatalogCommit(catalogIdent, catalogCommitEventId);
            if (catalogCommit.isPresent()) {
                return;
            }

            throw e;
        }

    }

    private void replaceColumn(TableName tableName, List<Column> replaceColumnList) throws MetaStoreException {
        throw new UnsupportedOperationException();
    }

    private List<ColumnObject> buildColumnList(List<ColumnObject> oldColumnList,
        Map<String, Column> changeColumnMap) {
        Map<String, ColumnObject> columnsMap = oldColumnList.stream()
            .collect(Collectors.toMap(ColumnObject::getName, col -> col));

        for (Map.Entry<String, Column> entry : changeColumnMap.entrySet()) {
            if (!columnsMap.containsKey(entry.getKey())) {
                throw new MetaStoreException(ErrorCode.COLUMN_NOT_FOUND, entry.getKey());
            }

            ColumnObject column = columnsMap.get(entry.getKey());
            column.updateColumnObject(column, entry.getValue().getColumnName(), entry.getValue().getColType(),
                entry.getValue().getComment());

        }
        List<ColumnObject> newColumnList = new ArrayList<>(columnsMap.values());
        newColumnList.sort(Comparator.comparingInt(ColumnObject::getOrdinal));
        return newColumnList;
    }


    private void changeColumnInternal(TransactionContext context, TableName tableName,
        Map<String, Column> changeColumnMap, String catalogCommitEventId) {
        // insert table schema
        TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);
        String latestVersion = VersionManagerHelper.getLatestVersion(context, tableIdent.getProjectId(), tableIdent.getRootCatalogId());

        /*TableReferenceObject tableReference = TableReferenceHelper
            .getTableReferenceOrElseThrow(context, tableIdent);
        tableReference.setMetaUpdateTime(System.currentTimeMillis());*/

        TableBaseObject tableBaseObject = TableBaseHelper
            .getLatestTableBaseOrElseThrow(context, tableIdent, latestVersion).getTableBaseObject();
        TableStorageObject tableStorageObject = TableStorageHelper
            .getLatestTableStorageOrElseThrow(context, tableIdent, latestVersion).getTableStorageObject();

        TableSchemaObject tableSchemaObject = TableSchemaHelper
            .getLatestTableSchemaOrElseThrow(context, tableIdent, latestVersion).getTableSchemaObject();
        List<ColumnObject> columnList = buildColumnList(tableSchemaObject.getColumns(), changeColumnMap);
        tableSchemaObject.setColumns(columnList);

        tableMetaStore.upsertTableReference(context, tableIdent);

        int historySubspaceFlag = tableMetaStore.getTableHistorySubspaceFlag(context, tableIdent);
        tableMetaStore.upsertTable(context, tableIdent, tableName.getTableName(), historySubspaceFlag,
            tableBaseObject, tableSchemaObject, tableStorageObject);
        /*tableStore.upsertTableSchema(context, tableIdent, tableSchemaObject);
        // insert table reference
        tableStore.upsertTableReference(context, tableIdent, tableReference);*/

        // insert table schema history
        tableMetaStore.insertTableSchemaHistory(context, tableIdent, latestVersion, tableSchemaObject);

        // insert table commit
        TableCommitObject tableCommit = TableCommitHelper.getLatestTableCommit(context, tableIdent, latestVersion)
            .get();
        long commitTime = RecordStoreHelper.getCurrentTime();
        tableMetaStore.insertTableCommit(context, tableIdent,
            TableCommitHelper.buildNewTableCommit(tableCommit, latestVersion, commitTime,
                0, TableOperationType.DDL_MODIFY_DATA_TYPE));

        // insert catalog commit
        catalogStore.insertCatalogCommit(context, tableIdent.getProjectId(),
            tableIdent.getCatalogId(), catalogCommitEventId, commitTime, CHANGE_COLUMN, tableOperateDetail(tableName));
    }

    private void changeColumn(TableName tableName, Map<String, Column> changeColumns) throws MetaStoreException {
        String catalogCommitEventId = UuidUtil.generateCatalogCommitId();

        CatalogIdent catalogIdent;
        try (TransactionRunner runner = new TransactionRunner()) {
            catalogIdent = runner.run(context -> {
                CatalogName catalogName = StoreConvertor.catalogName(tableName.getProjectId(), tableName.getCatalogName());
                return CatalogObjectHelper.getCatalogIdent(context, catalogName);
            });
        }

        try (TransactionRunner runner = new TransactionRunner()) {
            runner.setMaxAttempts(TABLE_STORE_MAX_RETRY_NUM);
            runner.run(context -> {
                changeColumnInternal(context, tableName, changeColumns, catalogCommitEventId);
                return null;
            });
        } catch (RuntimeException e) {
            // Retry interval, the table may have been deleted, col operation may have been success,
            // and the previous transaction that created the table may have succeeded
            Optional<CatalogCommitObject> catalogCommit = CatalogCommitHelper
                .getCatalogCommit(catalogIdent, catalogCommitEventId);
            if (catalogCommit.isPresent()) {
                return;
            }

            throw e;
        }
    }

    private List<ColumnObject> buildRenameColumnList(List<ColumnObject> oldColumnList,
        Map<String, String> renameColumnMap) {
        Map<String, ColumnObject> columnsMap = oldColumnList.stream()
            .collect(Collectors.toMap(ColumnObject::getName, col -> col));

        for (Map.Entry<String, String> entry : renameColumnMap.entrySet()) {
            CheckUtil.checkStringParameter(entry.getValue());
            if (!columnsMap.containsKey(entry.getKey())) {
                throw new MetaStoreException(ErrorCode.COLUMN_NOT_FOUND, entry.getKey());
            }
            if (columnsMap.containsKey(entry.getValue())) {
                throw new MetaStoreException(ErrorCode.COLUMN_ALREADY_EXISTS, entry.getValue());
            }

            ColumnObject newColumn = columnsMap.get(entry.getKey());
            newColumn.setName(entry.getValue());
            columnsMap.put(entry.getValue(), newColumn);
            columnsMap.remove(entry.getKey());

        }
        List<ColumnObject> newColumnList = new ArrayList<>(columnsMap.values());
        newColumnList.sort(Comparator.comparingInt(ColumnObject::getOrdinal));
        return newColumnList;
    }

    private void renameColumnInternal(TransactionContext context, TableName tableName,
        Map<String, String> renameColumns,
        String catalogCommitEventId) {
        TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);
        String latestVersion = VersionManagerHelper.getLatestVersion(context, tableIdent.getProjectId(), tableIdent.getRootCatalogId());
        if (renameColumns.size() != 0) {

            /*TableReferenceObject tableReference = TableReferenceHelper
                .getTableReferenceOrElseThrow(context, tableIdent);
            tableReference.setMetaUpdateTime(System.currentTimeMillis());*/

            TableBaseObject tableBaseObject = TableBaseHelper
                .getLatestTableBaseOrElseThrow(context, tableIdent, latestVersion).getTableBaseObject();
            TableSchemaObject tableSchemaObject = TableSchemaHelper
                .getLatestTableSchemaOrElseThrow(context, tableIdent, latestVersion).getTableSchemaObject();
            TableStorageObject tableStorageObject = TableStorageHelper
                .getLatestTableStorageOrElseThrow(context, tableIdent, latestVersion).getTableStorageObject();

            List<ColumnObject> oldColumnList = tableSchemaObject.getColumns();

            tableMetaStore.upsertTableReference(context, tableIdent);

            // 1. update table schema subspace
            List<ColumnObject> newColumnList = buildRenameColumnList(oldColumnList, renameColumns);
            tableSchemaObject.setColumns(newColumnList);
            //tableStore.upsertTableSchema(context, tableIdent, tableSchemaObject);
            int historySubspaceFlag = tableMetaStore.getTableHistorySubspaceFlag(context, tableIdent);
            tableMetaStore.upsertTable(context, tableIdent, tableName.getTableName(), historySubspaceFlag,
                tableBaseObject, tableSchemaObject, tableStorageObject);

            // 2. insert table schema history subspace
            tableMetaStore.insertTableSchemaHistory(context, tableIdent, latestVersion, tableSchemaObject);

            // 3. update table reference subspace
            //tableStore.updateTableReference(context, tableIdent, tableReference);

        }

        // 4. insert table commit subspace
        TableCommitObject tableCommit = TableCommitHelper.getLatestTableCommit(context, tableIdent, latestVersion)
            .get();
        long commitTime = RecordStoreHelper.getCurrentTime();
        tableMetaStore.insertTableCommit(context, tableIdent,
            TableCommitHelper.buildNewTableCommit(tableCommit, latestVersion, commitTime,
                0, TableOperationType.DDL_RENAME_COLUMN));

        // 5. insert catalog commit subspace
        catalogStore.insertCatalogCommit(context, tableIdent.getProjectId(),
            tableIdent.getCatalogId(), catalogCommitEventId, commitTime, RENAME_COLUMN, tableOperateDetail(tableName));
    }

    private void renameColumn(TableName tableName, Map<String, String> renameColumns)
        throws MetaStoreException {
        String catalogCommitEventId = UuidUtil.generateCatalogCommitId();

        CatalogIdent catalogIdent;
        try (TransactionRunner runner = new TransactionRunner()) {
            catalogIdent = runner.run(context -> {
                CatalogName catalogName = StoreConvertor.catalogName(tableName.getProjectId(), tableName.getCatalogName());
                return CatalogObjectHelper.getCatalogIdent(context, catalogName);
            });
        }

        try (TransactionRunner runner = new TransactionRunner()) {
            runner.setMaxAttempts(TABLE_STORE_MAX_RETRY_NUM);
            runner.run(context -> {
                renameColumnInternal(context, tableName, renameColumns, catalogCommitEventId);
                return null;
            });
        } catch (RuntimeException e) {
            // Retry interval, the table may have been deleted, col operation may have been success,
            // and the previous transaction that created the table may have succeeded
            Optional<CatalogCommitObject> catalogCommit = CatalogCommitHelper
                .getCatalogCommit(catalogIdent, catalogCommitEventId);
            if (catalogCommit.isPresent()) {
                return;
            }

            throw e;
        }

    }

    private List<ColumnObject> buildRemainColumns(List<ColumnObject> oldColumnList, List<String> dropColumnList,
        List<ColumnObject> partitionsColumnList) {
        List<ColumnObject> newColumnList = new ArrayList<>(oldColumnList);
        Map<String, ColumnObject> columnsMap = oldColumnList.stream()
            .collect(Collectors.toMap(ColumnObject::getName, col -> col));
        Map<String, ColumnObject> partitionColumnsMap = partitionsColumnList.stream()
            .collect(Collectors.toMap(ColumnObject::getName, col -> col));
        for (String entry : dropColumnList) {
            if (!columnsMap.containsKey(entry)) {
                // partition column can not be dropped
                if (partitionColumnsMap.containsKey(entry)) {
                    throw new MetaStoreException(ErrorCode.COLUMN_CAN_NOT_BE_DROPPED, entry);
                }
                throw new MetaStoreException(ErrorCode.COLUMN_NOT_FOUND, entry);
            }
            ColumnObject column = columnsMap.get(entry);
            newColumnList.remove(column);
        }

        // is need change Column ordinal
        newColumnList.sort(Comparator.comparingInt(ColumnObject::getOrdinal));
        List<ColumnObject> filledColumnList = new ArrayList<>(newColumnList.size());
        for (int i = 0; i < newColumnList.size(); ++i) {
            ColumnObject columnObject = new ColumnObject(newColumnList.get(i));
            columnObject.setOrdinal(i);
            filledColumnList.add(columnObject);
        }
        return filledColumnList;
    }

    private void dropColumnInternal(TransactionContext context, TableName tableName, List<String> dropColumnList,
        String catalogCommitEventId) {
        TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);
        String latestVersion = VersionManagerHelper.getLatestVersion(context, tableIdent.getProjectId(), tableIdent.getRootCatalogId());

        /*TableReferenceObject tableReference = TableReferenceHelper
            .getTableReferenceOrElseThrow(context, tableIdent);
        tableReference.setMetaUpdateTime(System.currentTimeMillis());*/

        TableBaseObject tableBaseObject = TableBaseHelper
            .getLatestTableBaseOrElseThrow(context, tableIdent, latestVersion).getTableBaseObject();
        TableStorageObject tableStorageObject = TableStorageHelper
            .getLatestTableStorageOrElseThrow(context, tableIdent, latestVersion).getTableStorageObject();

        TableSchemaObject tableSchemaObject = TableSchemaHelper
            .getLatestTableSchemaOrElseThrow(context, tableIdent, latestVersion).getTableSchemaObject();
        List<ColumnObject> oldColumnList = tableSchemaObject.getColumns();
        List<ColumnObject> partitionsColumnList = tableSchemaObject.getPartitionKeys();
        List<ColumnObject> newColumnList = buildRemainColumns(oldColumnList, dropColumnList, partitionsColumnList);
        tableSchemaObject.setColumns(newColumnList);

        tableMetaStore.upsertTableReference(context, tableIdent);

        int historySubspaceFlag = tableMetaStore.getTableHistorySubspaceFlag(context, tableIdent);
        tableMetaStore.upsertTable(context, tableIdent, tableName.getTableName(), historySubspaceFlag,
            tableBaseObject, tableSchemaObject, tableStorageObject);
        /*// 1. update table schema subspace
        tableStore.upsertTableSchema(context, tableIdent, tableSchemaObject);
        // 3. update table reference subspace
        tableStore.upsertTableReference(context, tableIdent, tableReference);*/

        // 2. insert table schema history subspace
        tableMetaStore.insertTableSchemaHistory(context, tableIdent, latestVersion, tableSchemaObject);

        // 4. insert table commit subspace
        TableCommitObject tableCommit = TableCommitHelper.getLatestTableCommit(context, tableIdent, latestVersion)
            .get();
        long commitTime = RecordStoreHelper.getCurrentTime();
        tableMetaStore.insertTableCommit(context, tableIdent, TableCommitHelper.
            buildNewTableCommit(tableCommit, latestVersion, commitTime,
                0, TableOperationType.DDL_DELETE_COLUMN));

        // 5. insert catalog commit subspace
        catalogStore.insertCatalogCommit(context, tableIdent.getProjectId(),
            tableIdent.getCatalogId(), catalogCommitEventId, commitTime, DROP_COLUMN, tableOperateDetail(tableName));
    }


    private void dropColumn(TableName tableName, List<String> dropColumnList) throws MetaStoreException {
        String catalogCommitEventId = UuidUtil.generateCatalogCommitId();

        CatalogIdent catalogIdent;
        try (TransactionRunner runner = new TransactionRunner()) {
            catalogIdent = runner.run(context -> {
                CatalogName catalogName = StoreConvertor.catalogName(tableName.getProjectId(), tableName.getCatalogName());
                return CatalogObjectHelper.getCatalogIdent(context, catalogName);
            });
        }

        try (TransactionRunner runner = new TransactionRunner()) {
            runner.setMaxAttempts(TABLE_STORE_MAX_RETRY_NUM);
            runner.run(context -> {
                dropColumnInternal(context, tableName, dropColumnList, catalogCommitEventId);
                return null;
            });
        } catch (RuntimeException e) {
            // Retry interval, the table may have been deleted,
            // and the previous transaction that created the table may have succeeded
            Optional<CatalogCommitObject> catalogCommit = CatalogCommitHelper
                .getCatalogCommit(catalogIdent, catalogCommitEventId);
            if (catalogCommit.isPresent()) {
                return;
            }

            throw e;
        }
    }

    @Override
    public void alterColumn(TableName tableNameParam, ColumnChangeInput columnChangeInput) {
        io.polycat.catalog.common.Operation type = columnChangeInput.getChangeType();

        try {
            switch (type) {
                case ADD_COLUMN:
                    List<Column> addColumnList = columnChangeInput.getColumnList();
                    addColumn(tableNameParam, addColumnList);
                    break;
                case REPLACE_COLUMN:
                    List<Column> replaceColumnList = columnChangeInput.getColumnList();
                    replaceColumn(tableNameParam, replaceColumnList);
                    break;
                case CHANGE_COLUMN:
                    Map<String, Column> changeColumnMap = columnChangeInput.getChangeColumnMap();
                    changeColumn(tableNameParam, changeColumnMap);
                    break;
                case RENAME_COLUMN:
                    Map<String, String> renameColumnMap = columnChangeInput.getRenameColumnMap();
                    renameColumn(tableNameParam, renameColumnMap);
                    break;
                case DROP_COLUMN:
                    List<String> dropColumnList = columnChangeInput.getDropColumnList();
                    dropColumn(tableNameParam, dropColumnList);
                    break;
                default:
                    throw new CatalogServerException(ErrorCode.ARGUMENT_ILLEGAL, type.getPrintName());
            }
        } catch (MetaStoreException e) {
            throw new CatalogServerException(e.getMessage(), e.getErrorCode());
        }
    }

    private void comfirmTableCanPurgedOrThrow(TransactionContext context, TableIdent tableIdent, TableName tableName) {
        CatalogIdent parentBranchCatalogIdent = StoreConvertor.catalogIdent(tableIdent.getProjectId(),
            tableIdent.getCatalogId(), tableIdent.getRootCatalogId());

        if (catalogStore.hasSubBranchCatalog(context, parentBranchCatalogIdent)) {
            throw new MetaStoreException(ErrorCode.SHARED_OBJECT_PURGE_ILLEGAL,
                "SubBranch", String.format("[Table]%s", tableName.getTableName()));
        }
    }

    private void deleteRolePrivilege(TransactionContext context, TableIdent tableIdent, TableName tableName) {
        String rolePrivilegeObjectId = RolePrivilegeHelper.getRolePrivilegeObjectIdByTable(tableIdent, tableName);
        roleStore
            .removeAllPrivilegeOnObject(context, tableIdent.getProjectId(), ObjectType.TABLE.name(),
                    rolePrivilegeObjectId);
    }

    private long getEndTime(long secondsLimit) {
        BigDecimal l1 = BigDecimal.valueOf(System.currentTimeMillis());
        BigDecimal l2 = BigDecimal.valueOf(secondsLimit);
        BigDecimal result = l1.add(l2.multiply(BigDecimal.valueOf(1000)));
        return 0 < result.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) ? Long.MAX_VALUE
            : System.currentTimeMillis() + secondsLimit * 1000;
    }

    private TableIdent getTaskTableIdent(BackendTaskObject taskObject)
        throws MetaStoreException {
        //get projectId
        String projectId;
        try {
            projectId = taskObject.getProjectId();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            log.warn("[backendTask] Id:" + taskObject.getTaskId() + " taskType:" + taskObject.getTaskType()
                    .name() + " params.projectId is empty");
            return null;
        }

        return StoreConvertor
            .tableIdent(projectId, taskObject.getParamsOrThrow("catalog_id"),
                taskObject.getParamsOrThrow("database_id"), taskObject.getParamsOrThrow("table_id"), null);
    }

    private void purgeTableCommitWithTrans(BackendTaskObject taskObject, long endTimeMill) {
        TableIdent tableIdent = getTaskTableIdent(taskObject);
        byte[] continuation = null;
        do {
            if (System.currentTimeMillis() > endTimeMill) {
                throw new MetaStoreException(ErrorCode.TASK_EXECUTE_TIMEOUT, taskObject.getTaskName());
            }
            // deal N record per trans, prevent trans TIMEOUT
            try (TransactionContext context = transaction.openTransaction()) {
                continuation = tableMetaStore
                    .deleteTableCommit(context, tableIdent, taskObject.getParamsOrThrow("begVersion"),
                        taskObject.getParamsOrThrow("endVersion"), continuation);
                context.commit();
            }
        } while (continuation != null);
    }

    private void purgeTableSchemaHistoryWithTrans(BackendTaskObject taskObject, long endTimeMill) {
        TableIdent tableIdent = getTaskTableIdent(taskObject);
        byte[] continuation = null;
        do {
            if (System.currentTimeMillis() > endTimeMill) {
                throw new MetaStoreException(ErrorCode.TASK_EXECUTE_TIMEOUT, taskObject.getTaskName());
            }
            // deal N record per trans, prevent trans TIMEOUT
            try (TransactionContext context = transaction.openTransaction()) {
                continuation = tableMetaStore
                    .deleteTableSchemaHistory(context, tableIdent, taskObject.getParamsOrThrow("begVersion"),
                        taskObject.getParamsOrThrow("endVersion"), continuation);
                context.commit();
            }
        } while (continuation != null);
    }

    private void purgeDataPartitionSetRecordsWithTrans(BackendTaskObject taskObject, long endTimeMill) {
        byte[] continuation = null;
        TableIdent tableIdent = getTaskTableIdent(taskObject);
        do {
            if (endTimeMill < System.currentTimeMillis()) {
                throw new MetaStoreException(ErrorCode.TASK_EXECUTE_TIMEOUT, taskObject.getTaskName());
            }

            try (TransactionContext context = transaction.openTransaction()) {
                continuation = tableDataStore.deleteDataPartition(context, tableIdent, continuation);
                context.commit();
            }
        } while (continuation != null);
    }

    private void purgeTableHistoryWithTrans(BackendTaskObject taskObject, long endTimeMill) {
        purgeDataPartitionSetRecordsWithTrans(taskObject, endTimeMill);
        purgeIndexPartitionSetRecordsWithTrans(taskObject, endTimeMill);
        purgeTableHistoryRecordWithTrans(taskObject, endTimeMill);
    }

    private void purgeTableHistoryRecordWithTrans(BackendTaskObject taskObject, long endTimeMill) {
        TableIdent tableIdent = getTaskTableIdent(taskObject);
        byte[] continuation = null;
        do {
            if (System.currentTimeMillis() > endTimeMill) {
                throw new MetaStoreException(ErrorCode.TASK_EXECUTE_TIMEOUT, taskObject.getTaskName());
            }
            // deal N record per trans, prevent trans TIMEOUT
            try (TransactionContext context = transaction.openTransaction()) {
                continuation = tableDataStore
                    .deleteTableHistory(context, tableIdent, taskObject.getParamsOrThrow("begVersion"),
                        taskObject.getParamsOrThrow("endVersion"), continuation);
                context.commit();
            }
        } while (continuation != null);
    }

    private void purgeIndexPartitionSetRecordsWithTrans(BackendTaskObject taskObject, long endTimeMill) {
        byte[] continuation = null;
        TableIdent tableIdent = getTaskTableIdent(taskObject);
        do {
            if (endTimeMill < System.currentTimeMillis()) {
                throw new MetaStoreException(ErrorCode.TASK_EXECUTE_TIMEOUT, taskObject.getTaskName());
            }

            try (TransactionContext context = transaction.openTransaction()) {
                continuation = tableDataStore.deleteIndexPartition(context, tableIdent, continuation);
                context.commit();
            }
        } while (continuation != null);
    }

    private void purgeTableStorageHistoryWithTrans(BackendTaskObject taskObject, long endTimeMill) {
        TableIdent tableIdent = getTaskTableIdent(taskObject);
        byte[] continuation = null;
        do {
            if (System.currentTimeMillis() > endTimeMill) {
                throw new MetaStoreException(ErrorCode.TASK_EXECUTE_TIMEOUT, taskObject.getTaskName());
            }
            // deal N record per trans, prevent trans TIMEOUT
            try (TransactionContext context = transaction.openTransaction()) {
                continuation = tableMetaStore
                    .deleteTableStorageHistory(context, tableIdent, taskObject.getParamsOrThrow("begVersion"),
                        taskObject.getParamsOrThrow("endVersion"), continuation);
                context.commit();
            }
        } while (continuation != null);
    }

    private void purgeTablePropertiesHistoryWithTrans(BackendTaskObject taskObject, long endTimeMill) {
        TableIdent tableIdent = getTaskTableIdent(taskObject);
        byte[] continuation = null;
        do {
            if (System.currentTimeMillis() > endTimeMill) {
                throw new MetaStoreException(ErrorCode.TASK_EXECUTE_TIMEOUT, taskObject.getTaskName());
            }
            // deal N record per trans, prevent trans TIMEOUT
            try (TransactionContext context = transaction.openTransaction()) {
                continuation = tableMetaStore
                    .deleteTableBaseHistory(context, tableIdent, taskObject.getParamsOrThrow("begVersion"),
                        taskObject.getParamsOrThrow("endVersion"), continuation);
                context.commit();
            }
        } while (continuation != null);
    }

    private void deleteUserPrivilegeWithTrans(BackendTaskObject taskObject, long endTimeMill) {
        TableIdent tableIdent = getTaskTableIdent(taskObject);
        byte[] continuation = null;
        do {
            if (System.currentTimeMillis() > endTimeMill) {
                throw new MetaStoreException(ErrorCode.TASK_EXECUTE_TIMEOUT, taskObject.getTaskName());
            }
            try (TransactionContext context = transaction.openTransaction()) {
                continuation = userPrivilegeStore
                    .deleteUserPrivilegeByToken(context, tableIdent.getProjectId(),
                        ObjectType.TABLE.name(),
                        tableIdent.getTableId(), continuation);
                context.commit();
            }
        } while (continuation != null);
    }

    private void dealPurgeTableTask(BackendTaskObject backendTaskObject, long secondsLimit) {
        long endTimeMill = getEndTime(secondsLimit);
        purgeTableCommitWithTrans(backendTaskObject, endTimeMill);
        purgeTableSchemaHistoryWithTrans(backendTaskObject, endTimeMill);
        purgeTableHistoryWithTrans(backendTaskObject, endTimeMill);
        purgeTableStorageHistoryWithTrans(backendTaskObject, endTimeMill);
        purgeTablePropertiesHistoryWithTrans(backendTaskObject, endTimeMill);
        deleteUserPrivilegeWithTrans(backendTaskObject, endTimeMill);
        // if History table's record is clear, delete backend task object
        purgeTableInfosWithTrans(backendTaskObject);
    }

    private void purgeTableInfosWithTrans(BackendTaskObject taskObject) {
        try (TransactionRunner runner = new TransactionRunner()) {
            runner.run(context -> {
                backendTaskStore.deleteBackendTask(context, taskObject.getTaskId());
                return null;
            });
        }
    }

    private void dropTablePurgeInternal(TransactionContext context, TableName tableName, String jobId,
        String catalogCommitEventId) {
        TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);

        comfirmTableCanPurgedOrThrow(context, tableIdent, tableName);

        String latestVersion = VersionManagerHelper.getLatestVersion(context, tableIdent.getProjectId(), tableIdent.getRootCatalogId());
        deleteTableRecordMetaData(context, tableIdent, tableName);
        deleteRolePrivilege(context, tableIdent, tableName);
        backendTaskStore.submitDelHistoryTableJob(context, tableIdent, tableName, latestVersion, jobId);

        TableCommitObject tableCommit = TableCommitHelper.getLatestTableCommit(context, tableIdent, latestVersion)
            .get();
        long time = RecordStoreHelper.getCurrentTime();
        tableMetaStore.insertDroppedTable(context, tableIdent, tableName, tableCommit.getCreateTime(), time,
           true);
        catalogStore.insertCatalogCommit(context, tableIdent.getProjectId(),
            tableIdent.getCatalogId(), catalogCommitEventId, time, PURGE_TABLE, tableOperateDetail(tableName));
        tableDataStore.dropTablePartitionInfo(context, tableIdent);
    }

    private void dropTablePurgeByName(TableName tableName) throws MetaStoreException {
        //todo validator tableIdent
        String catalogCommitEventId = UuidUtil.generateCatalogCommitId();
        String taskId = UuidUtil.generateId();
        CatalogIdent catalogIdent;
        try (TransactionRunner runner = new TransactionRunner()) {
            catalogIdent = runner.run(context -> {
                CatalogName catalogName = StoreConvertor.catalogName(tableName.getProjectId(), tableName.getCatalogName());
                return CatalogObjectHelper.getCatalogIdent(context, catalogName);
            });
        }

        try (TransactionRunner runner = new TransactionRunner()) {
            runner.setMaxAttempts(TABLE_STORE_MAX_RETRY_NUM);
            runner.run(context -> {
                dropTablePurgeInternal(context, tableName, taskId, catalogCommitEventId);
                return null;
            });
        } catch (RuntimeException e) {
            // Retry interval, the table may have been deleted,
            // and the previous transaction that created the table may have succeeded
            Optional<CatalogCommitObject> catalogCommit = CatalogCommitHelper
                .getCatalogCommit(catalogIdent, catalogCommitEventId);
            if (catalogCommit.isPresent()) {
                return;
            }

            throw e;
        }

        try (TransactionRunner runner = new TransactionRunner()) {
            runner.run(context -> {
                Optional<BackendTaskObject> backendTaskObject = backendTaskStore.getDelHistoryTableJob(context, taskId);
                if (backendTaskObject.isPresent()) {
                    dealPurgeTableTask(backendTaskObject.get(), 10);
                }
                return null;
            });
        }
    }


    private void removeMapKeys(Map<String, String> properties, List<String> keys) {
        for (String key : keys) {
            properties.remove(key);
        }
    }

    public enum ChangeType {
        SET_PROPERTIES,
        UNSET_PROPERTIES,
    }

    private void alterPropertiesInternal(TransactionContext context, TableName tableName,
        Map<String, String> setProperties,
        List<String> unsetPropertyKeys, ChangeType changeType, String catalogCommitEventId) {

        // 1.
        TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);
        if (null == tableIdent) {
            throw new MetaStoreException(ErrorCode.TABLE_NOT_FOUND, tableName);
        }
        String latestVersion = VersionManagerHelper.getLatestVersion(context, tableIdent.getProjectId(), tableIdent.getRootCatalogId());

        /*TableReferenceObject tableReference = TableReferenceHelper
            .getTableReferenceOrElseThrow(context, tableIdent);
        tableReference.setMetaUpdateTime(System.currentTimeMillis());*/

        TableBaseObject tableBaseObject = TableBaseHelper
            .getLatestTableBaseOrElseThrow(context, tableIdent, latestVersion).getTableBaseObject();
        TableSchemaObject tableSchemaObject = TableSchemaHelper
            .getLatestTableSchemaOrElseThrow(context, tableIdent, latestVersion).getTableSchemaObject();
        TableStorageObject tableStorageObject = TableStorageHelper
            .getLatestTableStorageOrElseThrow(context, tableIdent, latestVersion).getTableStorageObject();

        // 3.
        /*Optional<TableBaseHistoryObject> optionalTableBaseHistoryObject = TableBaseHelper.getLatestTableBase(context,
            tableIdent, latestVersion);*/

        Map<String, String> parameters = new HashMap<>(tableBaseObject.getParameters());
            /*optionalTableBaseHistoryObject
            .map((t) -> new HashMap<>(t.getTableBaseObject().getParameters()))
            .orElseGet(HashMap::new);*/

        TableOperationType operationType = TableOperationType.DDL_SET_PROPERTIES;
        if (changeType == ChangeType.SET_PROPERTIES) {
            // adjust operation if needed for iceberg
            String operationName = setProperties.remove("polycat_operation");
            if (!StringUtils.isEmpty(operationName)) {
                operationType = TableOperationType.valueOf(operationName);
            }
            parameters.putAll(setProperties);
        } else {
            operationType = TableOperationType.DDL_UNSET_PROPERTIES;
            removeMapKeys(parameters, unsetPropertyKeys);
        }
        tableBaseObject.setParameters(parameters);
        int historySubspaceFlag = tableMetaStore.getTableHistorySubspaceFlag(context, tableIdent);
        tableMetaStore.upsertTable(context, tableIdent, tableName.getTableName(), historySubspaceFlag,
            tableBaseObject, tableSchemaObject, tableStorageObject);

        tableMetaStore.upsertTableReference(context, tableIdent);
        // 2.
        /*tableStore.upsertTableReference(context, tableIdent, tableReference);
        tableStore.upsertTableBase(context, tableIdent, tableBaseObject);*/

        tableMetaStore.insertTableBaseHistory(context, tableIdent, latestVersion, tableBaseObject);

        // 4.
        long commitTime = RecordStoreHelper.getCurrentTime();
        TableCommitObject tableCommit = TableCommitHelper.getLatestTableCommit(context, tableIdent, latestVersion)
            .get();
        tableMetaStore.insertTableCommit(context, tableIdent,
            TableCommitHelper.buildNewTableCommit(tableCommit, latestVersion, commitTime, 0, operationType));

        // 5.
        io.polycat.catalog.common.Operation op = changeType == ChangeType.SET_PROPERTIES ?
            SET_PROPERTIES : UNSET_PROPERTIES;
        catalogStore.insertCatalogCommit(context, tableIdent.getProjectId(), tableIdent.getCatalogId(),
            catalogCommitEventId, commitTime, op, tableOperateDetail(tableName));
    }

    /**
     * Set or Unset Table Properties
     * <p>
     * 1. 将名字转换为TableIdentity(该函数已经处理了Branch的情况)
     * <p>
     * 2. 读取TableReference 2.1 如果当前Branch读到，则[更新]TableReference的latest_meta 2.2 否则从历史Branch中读取，则在当前Branch下[插入]一个TableReference记录，更新latest_meta。
     * <p>
     * 3. 读取TableProperties 3.1 如果当前Branch读到，则[更新]最新TableProperties，且插入一个TableProperties历史 3.2
     * 否则从历史Branch中读取，则在当前Branch下[插入]一个TableProperties记录，且插入一个TableProperties历史
     * <p>
     * 4. 插入Table commit记录 5. 插入Catalog commit记录
     */
    private void alterProperties(TableName tableName, Map<String, String> setProperties, List<String> unsetPropertyKeys,
        ChangeType changeType) {
        String catalogCommitEventId = UuidUtil.generateCatalogCommitId();

        CatalogIdent catalogIdent;
        try (TransactionRunner runner = new TransactionRunner()) {
            catalogIdent = runner.run(context -> {
                CatalogName catalogName = StoreConvertor.catalogName(tableName.getProjectId(), tableName.getCatalogName());
                return CatalogObjectHelper.getCatalogIdent(context, catalogName);
            });
        }

        try (TransactionRunner runner = new TransactionRunner()) {
            runner.setMaxAttempts(TABLE_STORE_MAX_RETRY_NUM);
            runner.run(context -> {
                alterPropertiesInternal(context, tableName, setProperties, unsetPropertyKeys, changeType,
                    catalogCommitEventId);
                return null;
            });
        } catch (RuntimeException e) {
            // Retry interval, the table may have been deleted,
            // and the previous transaction that created the table may have succeeded
            Optional<CatalogCommitObject> catalogCommit = CatalogCommitHelper
                .getCatalogCommit(catalogIdent, catalogCommitEventId);
            if (catalogCommit.isPresent()) {
                return;
            }

            throw e;
        }
    }

    private void setPropertiesByName(TableName tableName, Map<String, String> properties) {
        alterProperties(tableName, properties, null, ChangeType.SET_PROPERTIES);
    }

    @Override
    public void setProperties(TableName tableName, Map<String, String> properties) {
        try {
            setPropertiesByName(tableName, properties);
        } catch (MetaStoreException e) {
            throw new CatalogServerException(e.getMessage(), e.getErrorCode());
        }
    }

    private void unsetPropertiesByName(TableName tableName, List<String> propertyKeys) {
        try {
            alterProperties(tableName, null, propertyKeys, ChangeType.UNSET_PROPERTIES);
        } catch (MetaStoreException e) {
            throw new CatalogServerException(e.getMessage(), e.getErrorCode());
        }
    }

    @Override
    public void unsetProperties(TableName tableName, List<String> propertyKeys) {
        try {
            unsetPropertiesByName(tableName, propertyKeys);
        } catch (MetaStoreException e) {
            throw new CatalogServerException(e.getMessage(), e.getErrorCode());
        }
    }

    @Override
    public ColumnStatisticsObj[] getTableColumnStatistics(TableName tableName, List<String> colNames) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getTableColumnStatistics");
    }

    @Override
    public void updateTableColumnStatistics(String projectId, ColumnStatistics stats) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "updateTableColumnStatistics");
    }

    @Override
    public boolean deleteTableColumnStatistics(TableName tableName, String colName) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "deleteTableColumnStatistics");
    }

    @Override
    public void addConstraint(String projectId, List<Constraint> constraints) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "addConstraint");
    }

    @Override
    public void addForeignKey(String projectId, List<ForeignKey> foreignKeys) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "addForeignKey");
    }

    @Override
    public void addPrimaryKey(String projectId, List<PrimaryKey> primaryKeys) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "addPrimaryKey");
    }

    @Override
    public void dropConstraint(TableName tableName, String cstrName) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "dropConstraint");
    }

    @Override
    public List<Constraint> getConstraints(TableName tableName, ConstraintType type) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getConstraints");
    }

    @Override
    public List<ForeignKey> getForeignKeys(TableName parentTableName, TableName foreignTableName) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getForeignKeys");
    }

    @Override
    public List<PrimaryKey> getPrimaryKeys(TableName tableName) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getPrimaryKeys");
    }











}
