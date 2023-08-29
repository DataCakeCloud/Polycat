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
package io.polycat.catalog.store.fdb.record.impl;

import java.util.*;

import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.CatalogIdent;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.DroppedTableObject;
import io.polycat.catalog.common.model.OperationObject;
import io.polycat.catalog.common.model.ScanRecordCursorResult;
import io.polycat.catalog.common.model.TableBaseHistoryObject;
import io.polycat.catalog.common.model.TableBaseObject;
import io.polycat.catalog.common.model.TableCommitObject;
import io.polycat.catalog.common.model.TableIdent;
import io.polycat.catalog.common.model.TableName;
import io.polycat.catalog.common.model.TableNameObject;
import io.polycat.catalog.common.model.TableObject;
import io.polycat.catalog.common.model.TableOperationType;
import io.polycat.catalog.common.model.TableReferenceObject;
import io.polycat.catalog.common.model.TableSchemaHistoryObject;
import io.polycat.catalog.common.model.TableSchemaObject;
import io.polycat.catalog.common.model.TableStorageHistoryObject;
import io.polycat.catalog.common.model.TableStorageObject;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.TransactionIsolationLevel;
import io.polycat.catalog.common.utils.CodecUtil;
import io.polycat.catalog.common.utils.UuidUtil;
import io.polycat.catalog.store.api.TableMetaStore;
import io.polycat.catalog.store.common.StoreConvertor;
import io.polycat.catalog.store.common.StoreMetadata;
import io.polycat.catalog.store.common.TableStoreConvertor;
import io.polycat.catalog.store.fdb.record.DirectoryStoreHelper;
import io.polycat.catalog.store.fdb.record.RecordStoreHelper;
import io.polycat.catalog.store.fdb.record.StoreTypeUtil;
import io.polycat.catalog.store.fdb.record.TransactionContextUtil;
import io.polycat.catalog.store.protos.DroppedTableObjectName;
import io.polycat.catalog.store.protos.Operation;
import io.polycat.catalog.store.protos.Table;
import io.polycat.catalog.store.protos.TableBaseHistory;
import io.polycat.catalog.store.protos.TableCommit;
import io.polycat.catalog.store.protos.TableReference;
import io.polycat.catalog.store.protos.TableSchemaHistory;
import io.polycat.catalog.store.protos.TableStorageHistory;

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordVersion;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import static java.util.stream.Collectors.toList;

@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "fdb")
public class TableMetaStoreImpl implements TableMetaStore {

    private static final Logger logger = Logger.getLogger(TableMetaStoreImpl.class);

    private static int dealMaxNumRecordPerTrans = 2048;

    private static class TableStoreImplHandler {

        private static final TableMetaStoreImpl INSTANCE = new TableMetaStoreImpl();
    }

    public static TableMetaStoreImpl getInstance() {
        return TableMetaStoreImpl.TableStoreImplHandler.INSTANCE;
    }

    private Optional<FDBQueriedRecord<Message>> queryTableStoreRecord(FDBRecordStore recordStore,
        StoreMetadata storeMetadata, Versionstamp basedVersion) {
        FDBRecordVersion fdbEndRecordVersion = FDBRecordVersion
            .lastInGlobalVersion(basedVersion.getTransactionVersion());
        FDBRecordVersion fdbStartRecordVersion = FDBRecordVersion
            .firstInGlobalVersion(basedVersion.getTransactionVersion());

        QueryComponent filter = Query.and(Query.version().lessThanOrEquals(fdbEndRecordVersion),
            Query.version().greaterThanOrEquals(fdbStartRecordVersion));

        return RecordStoreHelper.getLatestHistoryRecord(recordStore, storeMetadata, filter);
    }

    private Optional<FDBQueriedRecord<Message>> queryLatestTableStoreRecord(FDBRecordStore recordStore,
        StoreMetadata storeMetadata, String basedVersion) {
        Versionstamp baseVersionstamp = Versionstamp.fromBytes(CodecUtil.hex2Bytes(basedVersion));
        FDBRecordVersion baseFdbRecordVersion = FDBRecordVersion
            .lastInGlobalVersion(baseVersionstamp.getTransactionVersion());
        QueryComponent filter = Query.version().lessThanOrEquals(baseFdbRecordVersion);
        return RecordStoreHelper.getLatestHistoryRecord(recordStore, storeMetadata, filter);
    }

    /*
    table subspace
     */

    private Tuple buildTableKey(String tableId) {
        return Tuple.from(tableId);
    }

    private TableObject trans2TableObject(TableIdent tableIdent, TableName tableName, Table table) {
        return new TableObject(tableIdent, tableName, table.getHistorySubspaceFlag(),
            new TableBaseObject(table.getBaseInfo()),
            new TableSchemaObject(table.getSchemaInfo()),
            new TableStorageObject(table.getStorageInfo()),
            0);
    }

    @Override
    public void createTableSubspace(TransactionContext context, CatalogIdent catalogIdent) throws MetaStoreException {

    }

    @Override
    public void dropTableSubspace(TransactionContext context, CatalogIdent catalogIdent) throws MetaStoreException {

    }

    @Override
    public String generateTableId(TransactionContext context, String projectId, String catalogId) throws MetaStoreException {
        return UuidUtil.generateUUID32();
    }

    @Override
    public void insertTable(TransactionContext context, TableIdent tableIdent, String tableName,
        int historySubspaceFlag, TableBaseObject tableBaseObject, TableSchemaObject tableSchemaObject,
        TableStorageObject tableStorageObject)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore tableStore = DirectoryStoreHelper.getTableStore(fdbRecordContext, tableIdent);
        Table tableReference = Table.newBuilder()
            .setTableId(tableIdent.getTableId())
            .setName(tableName)
            .setHistorySubspaceFlag(historySubspaceFlag)
            .setBaseInfo(TableStoreConvertor.getTableBaseInfo(tableBaseObject))
            .setSchemaInfo(TableStoreConvertor.getSchemaInfo(tableSchemaObject))
            .setStorageInfo(TableStoreConvertor.getTableStorageInfo(tableStorageObject))
            .build();
        tableStore.insertRecord(tableReference);
    }

    @Override
    public void upsertTable(TransactionContext context, TableIdent tableIdent, String tableName,
        int historySubspaceFlag, TableBaseObject tableBaseObject, TableSchemaObject tableSchemaObject,
        TableStorageObject tableStorageObject)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore tableStore = DirectoryStoreHelper.getTableStore(fdbRecordContext, tableIdent);
        Table table = Table.newBuilder()
            .setTableId(tableIdent.getTableId())
            .setName(tableName)
            .setHistorySubspaceFlag(historySubspaceFlag)
            .setBaseInfo(TableStoreConvertor.getTableBaseInfo(tableBaseObject))
            .setSchemaInfo(TableStoreConvertor.getSchemaInfo(tableSchemaObject))
            .setStorageInfo(TableStoreConvertor.getTableStorageInfo(tableStorageObject))
            .build();
        tableStore.saveRecord(table);
    }

    @Override
    public TableObject getTable(TransactionContext context, TableIdent tableIdent,
        TableName tableName) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore tableStore = DirectoryStoreHelper.getTableStore(fdbRecordContext, tableIdent);
        FDBStoredRecord<Message> storedRecord = tableStore.loadRecord(buildTableKey(tableIdent.getTableId()));
        if (storedRecord == null) {
            return null;
        }
        Table table = Table.newBuilder().mergeFrom(storedRecord.getRecord()).build();
        return trans2TableObject(tableIdent, tableName, table);
    }

    @Override
    public int getTableHistorySubspaceFlag(TransactionContext context, TableIdent tableIdent)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore tableStore = DirectoryStoreHelper.getTableStore(fdbRecordContext, tableIdent);
        FDBStoredRecord<Message> storedRecord = tableStore.loadRecord(buildTableKey(tableIdent.getTableId()));
        if (storedRecord == null) {
            return 0;
        }
        Table.Builder table = Table.newBuilder().mergeFrom(storedRecord.getRecord());
        return table.getHistorySubspaceFlag();
    }

    @Override
    public void deleteTable(TransactionContext context, TableIdent tableIdent, String tableName)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore tableStore = DirectoryStoreHelper.getTableStore(fdbRecordContext, tableIdent);
        tableStore.deleteRecord(buildTableKey(tableIdent.getTableId()));
    }

    /**
     * table object name
     */

    @Override
    public String getTableId(TransactionContext context, DatabaseIdent databaseIdent,
        String tableName) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore tableStore = DirectoryStoreHelper.getTableStore(fdbRecordContext, databaseIdent);

        QueryComponent filter = Query.field("name").equalsValue(tableName);

        List<FDBQueriedRecord<Message>> recordList = RecordStoreHelper.getRecords(tableStore,
            StoreMetadata.TABLE, filter);

        if (recordList.size() != 1) {
            return null;
        }
        Table.Builder builder = Table.newBuilder().mergeFrom(recordList.get(0).getRecord());
        return builder.getTableId();
    }

    @Override
    public ScanRecordCursorResult<List<TableNameObject>> listTableName(TransactionContext context,
        DatabaseIdent databaseIdent, int maxNum, byte[] continuation, TransactionIsolationLevel isolationLevel) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore tableStore = DirectoryStoreHelper.getTableStore(fdbRecordContext,
            databaseIdent);
        TupleRange tupleRange = TupleRange.ALL;

        // get lasted tableId, contains in-use and dropped
        ScanRecordCursorResult<List<FDBStoredRecord<Message>>> scannedRecords = RecordStoreHelper
            .listStoreRecordsWithToken(tupleRange, tableStore, 0, maxNum, continuation,
                StoreTypeUtil.trans2IsolationLevel(isolationLevel));

        List<TableNameObject> tableNameObjectList = new ArrayList<>(scannedRecords.getResult().size());

        for (FDBStoredRecord<Message> i : scannedRecords.getResult()) {
            Table.Builder builder = Table.newBuilder().mergeFrom(i.getRecord());
            tableNameObjectList.add(new TableNameObject(builder.getName(), builder.getTableId()));
        }

        return new ScanRecordCursorResult<>(tableNameObjectList, scannedRecords.getContinuation().orElse(null));
    }

    @Override
    public String createTableObjectNameTmpTable(TransactionContext context, DatabaseIdent databaseIdent) throws MetaStoreException {
        return null;
    }

    /**
     * table reference
     */

    private Tuple buildTableReferenceKey(String tableId) {
        return Tuple.from(tableId);
    }

    private TableReferenceObject trans2TableReferenceObject(TableReference tableReference) {
        return new TableReferenceObject(0, 0);
    }

    @Override
    public void createTableReferenceSubspace(TransactionContext context, CatalogIdent catalogIdent)
        throws MetaStoreException {

    }

    @Override
    public void dropTableReferenceSubspace(TransactionContext context, CatalogIdent catalogIdent)
        throws MetaStoreException {

    }

    @Override
    public void insertTableReference(TransactionContext context, TableIdent tableIdent) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore tableReferenceStore = DirectoryStoreHelper.getTableReferenceStore(fdbRecordContext, tableIdent);
        TableReference tableReference = TableReference.newBuilder()
            .setTableId(tableIdent.getTableId())
            .setUpdateTime(System.currentTimeMillis())
            .build();
        tableReferenceStore.insertRecord(tableReference);
    }

    @Override
    public void upsertTableReference(TransactionContext context, TableIdent tableIdent) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore tableRefStore = DirectoryStoreHelper.getTableReferenceStore(fdbRecordContext, tableIdent);
        TableReference tableReferenceNew = TableReference.newBuilder()
            .setTableId(tableIdent.getTableId())
            .setUpdateTime(System.currentTimeMillis())
            .build();
        tableRefStore.saveRecord(tableReferenceNew);
    }

    @Override
    public TableReferenceObject getTableReference(TransactionContext context, TableIdent tableIdent) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore tableReferenceStore = DirectoryStoreHelper.getTableReferenceStore(fdbRecordContext, tableIdent);
        FDBStoredRecord<Message> storedRecord = tableReferenceStore.loadRecord(
            buildTableReferenceKey(tableIdent.getTableId()));
        if (storedRecord == null) {
            return null;
        }
        TableReference tableReference = TableReference.newBuilder().mergeFrom(storedRecord.getRecord()).build();
        return trans2TableReferenceObject(tableReference);
    }

    @Override
    public void deleteTableReference(TransactionContext context, TableIdent tableIdent) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore tableReferenceStore = DirectoryStoreHelper.getTableReferenceStore(fdbRecordContext, tableIdent);
        tableReferenceStore.deleteRecord(buildTableReferenceKey(tableIdent.getTableId()));
    }

    /**
     * table base
     */

    @Override
    public TableBaseObject getTableBase(TransactionContext context, TableIdent tableIdent) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore tableStore = DirectoryStoreHelper.getTableStore(fdbRecordContext, tableIdent);
        FDBStoredRecord<Message> storedRecord = tableStore.loadRecord(
            buildTableKey(tableIdent.getTableId()));
        if (storedRecord == null) {
            return null;
        }
        Table table = Table.newBuilder().mergeFrom(storedRecord.getRecord()).build();
        return new TableBaseObject(table.getBaseInfo());
    }

    /**
     * table schema
     */

    @Override
    public TableSchemaObject getTableSchema(TransactionContext context, TableIdent tableIdent) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore tableStore = DirectoryStoreHelper.getTableStore(fdbRecordContext, tableIdent);
        FDBStoredRecord<Message> storedRecord = tableStore.loadRecord(
            buildTableKey(tableIdent.getTableId()));
        if (storedRecord == null) {
            return null;
        }
        Table table = Table.newBuilder().mergeFrom(storedRecord.getRecord()).build();
        return new TableSchemaObject(table.getSchemaInfo());
    }

    /**
     * table storage
     */
    @Override
    public TableStorageObject getTableStorage(TransactionContext context, TableIdent tableIdent)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore tableStore = DirectoryStoreHelper.getTableStore(fdbRecordContext, tableIdent);
        FDBStoredRecord<Message> storedRecord = tableStore.loadRecord(
            buildTableKey(tableIdent.getTableId()));
        if (storedRecord == null) {
            return null;
        }
        Table table = Table.newBuilder().mergeFrom(storedRecord.getRecord()).build();
        return new TableStorageObject(table.getStorageInfo());
    }

    /**
     * table base history subspace
     */

    private TableBaseHistoryObject trans2TableBaseHistoryObject(
        TableBaseHistory tableBaseHistory) {
        return new TableBaseHistoryObject(
            tableBaseHistory.getEventId(),
            CodecUtil.byteString2Hex(tableBaseHistory.getVersion()),
            new TableBaseObject(tableBaseHistory.getTableBaseInfo()));
    }

    @Override
    public void createTableBaseHistorySubspace(TransactionContext context, String projectId)
        throws MetaStoreException {

    }

    @Override
    public void dropTableBaseHistorySubspace(TransactionContext context, TableIdent tableIdent)
        throws MetaStoreException {

    }

    @Override
    public void insertTableBaseHistory(TransactionContext context, TableIdent tableIdent,
        String version, TableBaseObject tableBaseObject) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore tablePropertiesHistoryStore = DirectoryStoreHelper.getTableBaseHistoryStore(fdbRecordContext,
            tableIdent);
        TableBaseHistory tableBaseHistory = TableBaseHistory.newBuilder()
            .setEventId(UuidUtil.generateId())
            .setTableBaseInfo(TableStoreConvertor.getTableBaseInfo(tableBaseObject))
            .build();

        tablePropertiesHistoryStore.insertRecord(tableBaseHistory);
    }

    @Override
    public Optional<TableBaseHistoryObject> getLatestTableBase(TransactionContext context,
        TableIdent tableIdent, String basedVersion) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore recordStore = DirectoryStoreHelper.getTableBaseHistoryStore(fdbRecordContext, tableIdent);
        Optional<FDBQueriedRecord<Message>> record = queryLatestTableStoreRecord(recordStore,
            StoreMetadata.TABLE_BASE_HISTORY, basedVersion);
        if (!record.isPresent()) {
            return Optional.empty();
        }

        Versionstamp version = record.get().getVersion().toVersionstamp();
        TableBaseHistory tableBaseHistory = TableBaseHistory.newBuilder()
            .mergeFrom(record.get().getRecord())
            .setVersion(ByteString.copyFrom(version.getBytes())).build();
        return Optional.of(trans2TableBaseHistoryObject(tableBaseHistory));
    }

    @Override
    public Map<String, TableBaseHistoryObject> getLatestTableBaseByIds(TransactionContext context, String projectId, String catalogId, String databaseId, String basedVersion, List<String> tableIds) throws MetaStoreException {
        return null;
    }

    @Override
    public TableBaseHistoryObject getLatestTableBaseOrElseThrow(TransactionContext context, TableIdent tableIdent,
        String basedVersion) throws MetaStoreException {
        Optional<TableBaseHistoryObject> tableBaseHistoryObjectOptional = getLatestTableBase(context, tableIdent,
            basedVersion);
        if (!tableBaseHistoryObjectOptional.isPresent()) {
            throw new MetaStoreException(ErrorCode.TABLE_BASE_NOT_FOUND, tableIdent.getTableId());
        }
        return tableBaseHistoryObjectOptional.get();
    }

    @Override
    public ScanRecordCursorResult<List<TableBaseHistoryObject>> listTableBaseHistory(
        TransactionContext context, TableIdent tableIdent, int maxNum, byte[] continuation,
        TransactionIsolationLevel isolationLevel, String baseVersion) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore tableBaseHistoryStore = DirectoryStoreHelper
            .getTableBaseHistoryStore(fdbRecordContext, tableIdent);
        FDBRecordVersion baseFdbRecordVersion = FDBRecordVersion.lastInGlobalVersion(
            Versionstamp.fromBytes(CodecUtil.hex2Bytes(baseVersion)).getTransactionVersion());
        QueryComponent filter = Query.version().lessThanOrEquals(baseFdbRecordVersion);
        RecordQueryPlan plan = RecordStoreHelper
            .buildVersionRecordQueryPlan(tableBaseHistoryStore, StoreMetadata.TABLE_BASE_HISTORY,
                filter, true);

        RecordCursor<TableBaseHistory> cursor = tableBaseHistoryStore.executeQuery(plan, continuation,
                ExecuteProperties
                    .newBuilder().setReturnedRowLimit(maxNum)
                    .setIsolationLevel(StoreTypeUtil.trans2IsolationLevel(isolationLevel))
                    .build())
            .map(rec -> TableBaseHistory.newBuilder()
                .mergeFrom(rec.getRecord())
                .setVersion(ByteString.copyFrom(rec.getVersion().toVersionstamp().getBytes()))
                .build());

        List<TableBaseHistory> tableBaseHistoryList = cursor.asList().join();
        byte[] continuationNew = cursor.getNext().getContinuation().toBytes();
        if (continuation == null) {
            List<TableBaseHistoryObject> tablePropertiesHistoryObjectList = tableBaseHistoryList.stream()
                .map(tablePropertiesHistory -> trans2TableBaseHistoryObject(tablePropertiesHistory)).collect(toList());
            return new ScanRecordCursorResult<>(tablePropertiesHistoryObjectList, null);
        }
        List<TableBaseHistoryObject> tablePropertiesHistoryObjectList = tableBaseHistoryList.stream()
            .map(tablePropertiesHistory -> trans2TableBaseHistoryObject(tablePropertiesHistory)).collect(toList());
        return new ScanRecordCursorResult<>(tablePropertiesHistoryObjectList, continuationNew);
    }

    @Override
    public byte[] deleteTableBaseHistory(TransactionContext context, TableIdent tableIdent, String startVersion,
        String endVersion, byte[] continuation) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore store = DirectoryStoreHelper.getTableBaseHistoryStore(fdbRecordContext, tableIdent);
        FDBRecordVersion start = FDBRecordVersion
            .fromVersionstamp(Versionstamp.fromBytes(CodecUtil.hex2Bytes(startVersion)));
        FDBRecordVersion end = FDBRecordVersion
            .fromVersionstamp(Versionstamp.fromBytes(CodecUtil.hex2Bytes(endVersion)));
        QueryComponent filter = Query.and(Query.version().lessThan(end), Query.version().greaterThanOrEquals(start));
        RecordQueryPlan plan = RecordStoreHelper
            .buildVersionRecordQueryPlan(store, StoreMetadata.TABLE_BASE_HISTORY, filter, true);
        return RecordStoreHelper
            .deleteStoreRecord(store, plan, continuation, dealMaxNumRecordPerTrans, IsolationLevel.SNAPSHOT);
    }

    /**
     * table schema history subspace
     */



    private TableSchemaHistoryObject trans2TableSchemaHistoryObject(TableSchemaHistory tableSchemaHistory) {
        TableSchemaObject tableSchemaObject = new TableSchemaObject(tableSchemaHistory.getSchema());
        return new TableSchemaHistoryObject(tableSchemaHistory.getEventId(),
            CodecUtil.byteString2Hex(tableSchemaHistory.getVersion()), tableSchemaObject);
    }

    @Override
    public void createTableSchemaHistorySubspace(TransactionContext context, String projectId)
        throws MetaStoreException {

    }

    @Override
    public void dropTableSchemaHistorySubspace(TransactionContext context, String projectId)
        throws MetaStoreException {

    }

    @Override
    public void insertTableSchemaHistory(TransactionContext context, TableIdent tableIdent, String version,
        TableSchemaObject tableSchemaObject) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore tableSchemaHistoryStore = DirectoryStoreHelper.getTableSchemaHistoryStore(fdbRecordContext,
            tableIdent);
        TableSchemaHistory tableSchemaHistory = TableSchemaHistory.newBuilder()
            .setEventId(UuidUtil.generateId())
            .setSchema(TableStoreConvertor.getSchemaInfo(tableSchemaObject))
            .build();
        tableSchemaHistoryStore.insertRecord(tableSchemaHistory);
    }

    @Override
    public void upsertTableSchemaHistory(TransactionContext context, TableIdent tableIdent, String version,
        TableSchemaObject tableSchemaObject) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore tableSchemaHistoryStore = DirectoryStoreHelper.getTableSchemaHistoryStore(fdbRecordContext,
            tableIdent);
        TableSchemaHistory tableSchemaHistory = TableSchemaHistory.newBuilder()
            .setEventId(UuidUtil.generateId())
            .setVersion(ByteString.copyFrom(version.getBytes()))
            .setSchema(TableStoreConvertor.getSchemaInfo(tableSchemaObject))
            .build();
        tableSchemaHistoryStore.saveRecord(tableSchemaHistory,
            FDBRecordVersion.fromVersionstamp(Versionstamp.fromBytes(CodecUtil.hex2Bytes(version))));
    }

    @Override
    public Optional<TableSchemaHistoryObject> getLatestTableSchema(TransactionContext context, TableIdent tableIdent,
        String basedVersion) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore recordStore = DirectoryStoreHelper.getTableSchemaHistoryStore(fdbRecordContext, tableIdent);

        Optional<FDBQueriedRecord<Message>> record = queryLatestTableStoreRecord(recordStore,
            StoreMetadata.TABLE_SCHEMA_HISTORY, basedVersion);
        if (!record.isPresent()) {
            return Optional.empty();
        }

        Versionstamp version = record.get().getVersion().toVersionstamp();
        TableSchemaHistory tableSchemaHistory = TableSchemaHistory.newBuilder()
            .mergeFrom(record.get().getRecord()).setVersion(ByteString.copyFrom(version.getBytes())).build();
        return Optional.of(trans2TableSchemaHistoryObject(tableSchemaHistory));
    }

    @Override
    public TableSchemaHistoryObject getLatestTableSchemaOrElseThrow(TransactionContext context, TableIdent tableIdent,
        String basedVersion) throws MetaStoreException {
        Optional<TableSchemaHistoryObject> tableSchemaHistory = getLatestTableSchema(context, tableIdent, basedVersion);
        if (!tableSchemaHistory.isPresent()) {
            throw new MetaStoreException(ErrorCode.TABLE_SCHEMA_NOT_FOUND, tableIdent.getTableId());
        }
        return tableSchemaHistory.get();
    }

    @Override
    public ScanRecordCursorResult<List<TableSchemaHistoryObject>> listTableSchemaHistory(TransactionContext context,
        TableIdent tableIdent, int maxNum, byte[] continuation, TransactionIsolationLevel isolationLevel,
        String baseVersion) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore tableSchemaHistoryStore = DirectoryStoreHelper.getTableSchemaHistoryStore(fdbRecordContext,
            tableIdent);
        FDBRecordVersion baseFdbRecordVersion = FDBRecordVersion.lastInGlobalVersion(
            Versionstamp.fromBytes(CodecUtil.hex2Bytes(baseVersion)).getTransactionVersion());
        QueryComponent filter = Query.version().lessThanOrEquals(baseFdbRecordVersion);
        RecordQueryPlan plan = RecordStoreHelper
            .buildVersionRecordQueryPlan(tableSchemaHistoryStore, StoreMetadata.TABLE_SCHEMA_HISTORY,
                filter, true);

        RecordCursor<TableSchemaHistory> cursor = tableSchemaHistoryStore.executeQuery(plan, continuation,
                ExecuteProperties
                    .newBuilder().setReturnedRowLimit(maxNum)
                    .setIsolationLevel(StoreTypeUtil.trans2IsolationLevel(isolationLevel))
                    .build())
            .map(rec -> TableSchemaHistory.newBuilder()
                .mergeFrom(rec.getRecord())
                .setVersion(ByteString.copyFrom(rec.getVersion().toVersionstamp().getBytes()))
                .build());

        List<TableSchemaHistory> tableSchemaHistoryList = cursor.asList().join();
        byte[] continuationNew = cursor.getNext().getContinuation().toBytes();
        if (continuation == null) {
            List<TableSchemaHistoryObject> tableSchemaHistoryObjectList = tableSchemaHistoryList.stream()
                .map(tableSchemaHistory -> trans2TableSchemaHistoryObject(tableSchemaHistory)).collect(toList());
            return new ScanRecordCursorResult<>(tableSchemaHistoryObjectList, null);
        }

        List<TableSchemaHistoryObject> tableSchemaHistoryObjectList = tableSchemaHistoryList.stream()
            .map(tableSchemaHistory -> trans2TableSchemaHistoryObject(tableSchemaHistory)).collect(toList());
        return new ScanRecordCursorResult<>(tableSchemaHistoryObjectList, continuationNew);
    }

    // get table schema history records from the specific time point (fromTime), each table only returns the newest one
    @Override
    public List<TableSchemaHistoryObject> listSchemaHistoryFromTimePoint(TransactionContext context,
        String fromTime, TableIdent tableIdent) throws MetaStoreException {
        List<TableSchemaHistoryObject> schemaHistoryList = new ArrayList<>();
        FDBRecordVersion fromTimeVersion = FDBRecordVersion
            .lastInGlobalVersion(Versionstamp.fromBytes(CodecUtil.hex2Bytes(fromTime)).getTransactionVersion());
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore store = DirectoryStoreHelper.getTableSchemaHistoryStore(fdbRecordContext, tableIdent);
        QueryComponent filter = Query.version().greaterThanOrEquals(fromTimeVersion);

        List<FDBQueriedRecord<Message>> recordList = RecordStoreHelper
            .getRecords(store, StoreMetadata.TABLE_SCHEMA_HISTORY, filter);

        for (FDBQueriedRecord<Message> record : recordList) {
            TableSchemaHistory tsh = TableSchemaHistory.newBuilder().mergeFrom(record.getRecord()).build();
            schemaHistoryList.add(trans2TableSchemaHistoryObject(tsh));
        }

        return schemaHistoryList;
    }

    @Override
    public byte[] deleteTableSchemaHistory(TransactionContext context, TableIdent tableIdent, String startVersion,
        String endVersion, byte[] continuation) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore store = DirectoryStoreHelper.getTableSchemaHistoryStore(fdbRecordContext, tableIdent);
        FDBRecordVersion start = FDBRecordVersion
            .fromVersionstamp(Versionstamp.fromBytes(CodecUtil.hex2Bytes(startVersion)));
        FDBRecordVersion end = FDBRecordVersion
            .fromVersionstamp(Versionstamp.fromBytes(CodecUtil.hex2Bytes(endVersion)));
        QueryComponent filter = Query.and(Query.version().lessThan(end), Query.version().greaterThanOrEquals(start));
        RecordQueryPlan plan = RecordStoreHelper
            .buildVersionRecordQueryPlan(store, StoreMetadata.TABLE_SCHEMA_HISTORY, filter, true);
        return RecordStoreHelper
            .deleteStoreRecord(store, plan, continuation, dealMaxNumRecordPerTrans, IsolationLevel.SNAPSHOT);
    }

    /**
     * table storage history subspace
     */

    private TableStorageHistoryObject trans2TableStorageHistoryObject(TableStorageHistory tableStorageHistory) {
        TableStorageObject tableStorageObject = new TableStorageObject(tableStorageHistory.getStorageInfo());
        return new TableStorageHistoryObject(tableStorageHistory.getEventId(),
            CodecUtil.byteString2Hex(tableStorageHistory.getVersion()),
            tableStorageObject);
    }

    @Override
    public void createTableStorageHistorySubspace(TransactionContext context, String projectId)
        throws MetaStoreException {

    }

    @Override
    public void dropTableStorageHistorySubspace(TransactionContext context, String projectId)
        throws MetaStoreException {

    }

    @Override
    public void insertTableStorageHistory(TransactionContext context, TableIdent tableIdent,
        String version, TableStorageObject tableStorageObject) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore tableStorageHistoryStore = DirectoryStoreHelper.getTableStorageHistoryStore(fdbRecordContext,
            tableIdent);
        TableStorageHistory.Builder builder = TableStorageHistory.newBuilder()
            .setEventId(UuidUtil.generateId())
            .setStorageInfo(TableStoreConvertor.getTableStorageInfo(tableStorageObject));
        tableStorageHistoryStore.insertRecord(builder.build());
    }

    @Override
    public Optional<TableStorageHistoryObject> getLatestTableStorage(TransactionContext context,
        TableIdent tableIdent, String basedVersion) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore recordStore = DirectoryStoreHelper.getTableStorageHistoryStore(fdbRecordContext, tableIdent);
        Optional<FDBQueriedRecord<Message>> record = queryLatestTableStoreRecord(recordStore,
            StoreMetadata.TABLE_STORAGE_HISTORY, basedVersion);
        if (!record.isPresent()) {
            return Optional.empty();
        }

        Versionstamp version = record.get().getVersion().toVersionstamp();
        TableStorageHistory tableStorageHistory = TableStorageHistory.newBuilder().mergeFrom(record.get().getRecord())
            .setVersion(ByteString.copyFrom(version.getBytes())).build();
        return Optional.of(trans2TableStorageHistoryObject(tableStorageHistory));
    }

    @Override
    public TableStorageHistoryObject getLatestTableStorageOrElseThrow(TransactionContext context, TableIdent tableIdent,
        String basedVersion) throws MetaStoreException {
        Optional<TableStorageHistoryObject> tableStorageHistory = getLatestTableStorage(context, tableIdent,
            basedVersion);
        if (!tableStorageHistory.isPresent()) {
            throw new MetaStoreException(ErrorCode.TABLE_STORAGE_NOT_FOUND, tableIdent.getTableId());
        }
        return tableStorageHistory.get();
    }

    @Override
    public ScanRecordCursorResult<List<TableStorageHistoryObject>> listTableStorageHistory(TransactionContext context,
        TableIdent tableIdent, int maxNum, byte[] continuation, TransactionIsolationLevel isolationLevel,
        String baseVersion) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore tableStorageHistoryStore = DirectoryStoreHelper.getTableStorageHistoryStore(fdbRecordContext,
            tableIdent);
        FDBRecordVersion baseFdbRecordVersion = FDBRecordVersion.lastInGlobalVersion(
            Versionstamp.fromBytes(CodecUtil.hex2Bytes(baseVersion)).getTransactionVersion());
        QueryComponent filter = Query.version().lessThanOrEquals(baseFdbRecordVersion);
        RecordQueryPlan plan = RecordStoreHelper
            .buildVersionRecordQueryPlan(tableStorageHistoryStore, StoreMetadata.TABLE_STORAGE_HISTORY,
                filter, true);

        RecordCursor<TableStorageHistory> cursor = tableStorageHistoryStore.executeQuery(plan, continuation,
                ExecuteProperties
                    .newBuilder().setReturnedRowLimit(maxNum)
                    .setIsolationLevel(StoreTypeUtil.trans2IsolationLevel(isolationLevel))
                    .build())
            .map(rec -> TableStorageHistory.newBuilder()
                .mergeFrom(rec.getRecord())
                .setVersion(ByteString.copyFrom(rec.getVersion().toVersionstamp().getBytes()))
                .build());

        List<TableStorageHistory> tableStorageHistoryList = cursor.asList().join();
        byte[] continuationNew = cursor.getNext().getContinuation().toBytes();
        if (continuation == null) {
            List<TableStorageHistoryObject> tableStorageHistoryObjectList = tableStorageHistoryList.stream()
                .map(tableStorageHistory -> trans2TableStorageHistoryObject(tableStorageHistory)).collect(toList());

            return new ScanRecordCursorResult<>(tableStorageHistoryObjectList, null);
        }

        List<TableStorageHistoryObject> tableStorageHistoryObjectList = tableStorageHistoryList.stream()
            .map(tableStorageHistory -> trans2TableStorageHistoryObject(tableStorageHistory)).collect(toList());
        return new ScanRecordCursorResult<>(tableStorageHistoryObjectList, continuationNew);
    }

    @Override
    public byte[] deleteTableStorageHistory(TransactionContext context, TableIdent tableIdent, String startVersion,
        String endVersion, byte[] continuation) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore store = DirectoryStoreHelper.getTableStorageHistoryStore(fdbRecordContext, tableIdent);
        FDBRecordVersion start = FDBRecordVersion
            .fromVersionstamp(Versionstamp.fromBytes(CodecUtil.hex2Bytes(startVersion)));
        FDBRecordVersion end = FDBRecordVersion
            .fromVersionstamp(Versionstamp.fromBytes(CodecUtil.hex2Bytes(endVersion)));
        QueryComponent filter = Query.and(Query.version().lessThan(end), Query.version().greaterThanOrEquals(start));
        RecordQueryPlan plan = RecordStoreHelper
            .buildVersionRecordQueryPlan(store, StoreMetadata.TABLE_STORAGE_HISTORY, filter, true);
        return RecordStoreHelper
            .deleteStoreRecord(store, plan, continuation, dealMaxNumRecordPerTrans, IsolationLevel.SNAPSHOT);
    }

    /**
     * table commit subspace
     */

    private Operation trans2Operation(OperationObject tableOperation) {
        Operation operation = Operation.newBuilder()
            .setAddedNums(tableOperation.getAddedNums())
            .setDeletedNums(tableOperation.getDeletedNums())
            .setUpdatedNums(tableOperation.getUpdatedNums())
            .setFileCount(tableOperation.getFileCount())
            .setOperation(tableOperation.getOperationType().name())
            .build();
        return operation;
    }

    private TableCommit trans2TableCommit(TableCommitObject tableCommitObject) {
        List<Operation> operationList = tableCommitObject.getOperations().stream()
            .map(tableOperation -> trans2Operation(tableOperation)).collect(toList());
        TableCommit tableCommit = TableCommit.newBuilder()
            .setEventId(UuidUtil.generateId())
            .setTableName(tableCommitObject.getTableName())
            .setCreateTime(tableCommitObject.getCreateTime())
            .setCommitTime(tableCommitObject.getCommitTime())
            .addAllOperation(operationList)
            .setDroppedTime(tableCommitObject.getDroppedTime())
            .setVersion(CodecUtil.hex2ByteString(tableCommitObject.getVersion()))
            .build();
        return tableCommit;
    }

    private Optional<TableCommit> getLatestCurBranchTableCommit(TransactionContext context, TableIdent tableIdent,
        String basedVersion) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore recordStore = DirectoryStoreHelper.getTableCommitStore(fdbRecordContext, tableIdent);
        Optional<FDBQueriedRecord<Message>> record = queryLatestTableStoreRecord(recordStore,
            StoreMetadata.TABLE_COMMIT, basedVersion);
        if (!record.isPresent()) {
            return Optional.empty();
        }

        Versionstamp version = record.get().getVersion().toVersionstamp();
        return Optional.of(TableCommit.newBuilder().mergeFrom(record.get().getRecord())
            .setVersion(ByteString.copyFrom(version.getBytes())).build());
    }

    private OperationObject trans2TableOperation(Operation operation) {
        TableOperationType operationType = TableOperationType.valueOf(operation.getOperation());
        OperationObject result = new OperationObject(operationType, operation.getAddedNums(),
            operation.getDeletedNums(), operation.getUpdatedNums(), operation.getFileCount());
        return result;
    }

    private TableCommitObject trans2TableCommitObject(TableCommit tableCommit, TableIdent tableIdent) {
        List<OperationObject> tableOperations = tableCommit.getOperationList().stream()
            .map(operation -> trans2TableOperation(operation)).collect(toList());
        TableCommitObject tableCommitObject = new TableCommitObject(
            tableIdent.getProjectId(),
            tableIdent.getCatalogId(),
            tableIdent.getDatabaseId(),
            tableIdent.getTableId(),
            tableCommit.getTableName(),
            (long) tableCommit.getCreateTime(),
            (long) tableCommit.getCommitTime(),
            tableOperations, 0,
            CodecUtil.byteString2Hex(tableCommit.getVersion()));

        if (tableCommit.hasDroppedTime()) {
            tableCommitObject.setDroppedTime(tableCommit.getDroppedTime());
        }

        return tableCommitObject;
    }

    //If a table is modified in the parent branch, the table is considered valid in the parent branch.
    //The service process needs to further check whether the corresponding metadata table is modified.
    private List<FDBQueriedRecord<Message>> listTableStoreRecord(FDBRecordStore recordStore,
        StoreMetadata storeMetadata, Versionstamp startVersion, Versionstamp endVersion, boolean sortReverse) {
        QueryComponent filter = Query.and(Query.version().lessThanOrEquals(endVersion),
            Query.version().greaterThan(startVersion));

        return RecordStoreHelper.getHistoryRecords(recordStore, storeMetadata, filter, sortReverse);
    }

    private List<TableCommit> getTableCommitsFromRecordList(List<FDBQueriedRecord<Message>> recordList) {
        List<TableCommit> tableCommitList = new ArrayList<>();
        if (recordList.isEmpty()) {
            return Collections.emptyList();
        }

        for (FDBQueriedRecord<Message> record : recordList) {
            Versionstamp version = record.getVersion().toVersionstamp();
            TableCommit tableCommit = TableCommit.newBuilder()
                .mergeFrom(record.getRecord()).setVersion(ByteString.copyFrom(version.getBytes())).build();

            tableCommitList.add(tableCommit);
        }
        return tableCommitList;
    }

    @Override
    public void createTableCommitSubspace(TransactionContext context, String projectId) throws MetaStoreException {

    }

    @Override
    public void dropTableCommitSubspace(TransactionContext context, String projectId) throws MetaStoreException {

    }

    @Override
    public void insertTableCommit(TransactionContext context, TableIdent tableIdent,
        TableCommitObject tableCommitObject) throws MetaStoreException {
        TableCommit tableCommitNew = trans2TableCommit(tableCommitObject);
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore tableCommitStore = DirectoryStoreHelper.getTableCommitStore(fdbRecordContext, tableIdent);
        tableCommitStore.insertRecord(tableCommitNew);
    }

    @Override
    public Optional<TableCommitObject> getTableCommit(TransactionContext context, TableIdent tableIdent,
        String version) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore recordStore = DirectoryStoreHelper.getTableCommitStore(fdbRecordContext, tableIdent);
        Optional<FDBQueriedRecord<Message>> record = queryTableStoreRecord(recordStore, StoreMetadata.TABLE_COMMIT,
            Versionstamp.fromBytes(CodecUtil.hex2Bytes(version)));
        if (!record.isPresent()) {
            return Optional.empty();
        }

        Versionstamp versionstamp = record.get().getVersion().toVersionstamp();
        TableCommit tableCommit = TableCommit.newBuilder()
            .mergeFrom(record.get().getRecord()).setVersion(ByteString.copyFrom(versionstamp.getBytes())).build();
        return Optional.of(trans2TableCommitObject(tableCommit, tableIdent));
    }

    @Override
    public Optional<TableCommitObject> getLatestTableCommit(TransactionContext context, TableIdent tableIdent,
        String basedVersion) throws MetaStoreException {
        Optional<TableCommit> tableCommit = getLatestCurBranchTableCommit(context, tableIdent, basedVersion);
        return tableCommit.map(commit -> trans2TableCommitObject(commit, tableIdent));
    }

    @Override
    public List<TableCommitObject> listTableCommit(TransactionContext context, TableIdent tableIdent,
        String startVersion, String endVersion) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore recordStore = DirectoryStoreHelper.getTableCommitStore(fdbRecordContext, tableIdent);

        List<FDBQueriedRecord<Message>> recordList = listTableStoreRecord(recordStore,
            StoreMetadata.TABLE_COMMIT, Versionstamp.fromBytes(CodecUtil.hex2Bytes(startVersion)),
            Versionstamp.fromBytes(CodecUtil.hex2Bytes(endVersion)), true);

        return getTableCommitsFromRecordList(recordList).stream()
            .map(tableCommit -> trans2TableCommitObject(tableCommit, tableIdent)).collect(toList());
    }

    @Override
    public ScanRecordCursorResult<List<TableCommitObject>> listTableCommit(TransactionContext context,
        TableIdent tableIdent, int maxNum, byte[] continuation, TransactionIsolationLevel isolationLevel,
        String baseVersion) throws MetaStoreException {
        List<TableCommitObject> tableCommitObjectList;
        FDBRecordVersion baseFdbRecordVersion = FDBRecordVersion.lastInGlobalVersion(
            Versionstamp.fromBytes(CodecUtil.hex2Bytes(baseVersion)).getTransactionVersion());

        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore tableCommitStore = DirectoryStoreHelper.getTableCommitStore(fdbRecordContext, tableIdent);
        QueryComponent filter = Query.version().lessThanOrEquals(baseFdbRecordVersion);
        RecordQueryPlan plan = RecordStoreHelper
            .buildVersionRecordQueryPlan(tableCommitStore, StoreMetadata.TABLE_COMMIT,
                filter, true);

        RecordCursor<TableCommit> cursor = tableCommitStore.executeQuery(plan, continuation,
                ExecuteProperties
                    .newBuilder().setReturnedRowLimit(maxNum)
                    .setIsolationLevel(StoreTypeUtil.trans2IsolationLevel(isolationLevel))
                    .build())
            .map(rec -> TableCommit.newBuilder()
                .mergeFrom(rec.getRecord())
                .setVersion(ByteString.copyFrom(rec.getVersion().toVersionstamp().getBytes()))
                .build());

        List<TableCommit> tableCommitList = cursor.asList().join();
        byte[] continuationNew = cursor.getNext().getContinuation().toBytes();
        if (continuationNew == null) {
            tableCommitObjectList = tableCommitList.stream()
                .map(tableCommit -> trans2TableCommitObject(tableCommit, tableIdent)).collect(toList());
            return new ScanRecordCursorResult<>(tableCommitObjectList, null);
        }

        tableCommitObjectList = tableCommitList.stream()
            .map(tableCommit -> trans2TableCommitObject(tableCommit, tableIdent)).collect(toList());
        return new ScanRecordCursorResult<>(tableCommitObjectList, continuationNew);
    }

    @Override
    public byte[] deleteTableCommit(TransactionContext context, TableIdent tableIdent, String startVersion,
        String endVersion, byte[] continuation) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore store = DirectoryStoreHelper.getTableCommitStore(fdbRecordContext, tableIdent);
        FDBRecordVersion start = FDBRecordVersion
            .fromVersionstamp(Versionstamp.fromBytes(CodecUtil.hex2Bytes(startVersion)));
        FDBRecordVersion end = FDBRecordVersion
            .fromVersionstamp(Versionstamp.fromBytes(CodecUtil.hex2Bytes(endVersion)));

        QueryComponent filter = Query.and(Query.version().lessThan(end), Query.version().greaterThanOrEquals(start));
        RecordQueryPlan plan = RecordStoreHelper
            .buildVersionRecordQueryPlan(store, StoreMetadata.TABLE_COMMIT, filter, true);
        return RecordStoreHelper
            .deleteStoreRecord(store, plan, continuation, dealMaxNumRecordPerTrans, IsolationLevel.SNAPSHOT);
    }

    /**
     * table dropped object
     */

    private Tuple buildDroppedTableObjectNameKey(String tableName, String tableId) {
        return Tuple.from(tableName, tableId);
    }

    private DroppedTableObject trans2DroppedTableNameObject(DroppedTableObjectName droppedTableObjectName) {
        return new DroppedTableObject(droppedTableObjectName.getName(), droppedTableObjectName.getObjectId(),
            droppedTableObjectName.getCreateTime(), droppedTableObjectName.getDroppedTime(),
            droppedTableObjectName.getIsPurge() == true);
    }

    private boolean isTableUndropAvail(FDBStoredRecord record) {
        // PURGE record only in current DroppedTableObjectNameStore, record is null means not PURGE type
        if (record == null) {
            return false;
        }
        DroppedTableObjectName droppedTableObjectName = DroppedTableObjectName.newBuilder()
            .mergeFrom(record.getRecord()).build();

        if (droppedTableObjectName.getIsPurge() == true) {
            return false;
        }
        return true;
    }

    @Override
    public void createDroppedTableSubspace(TransactionContext context, CatalogIdent catalogIdent)
        throws MetaStoreException {

    }

    @Override
    public void dropDroppedTableSubspace(TransactionContext context, CatalogIdent catalogIdent)
        throws MetaStoreException {

    }


    @Override
    public void insertDroppedTable(TransactionContext context, TableIdent tableIdent, TableName tableName,
        long createTime, long droppedTime, boolean isPurge) {
        DroppedTableObjectName droppedObjectName = DroppedTableObjectName.newBuilder()
            .setName(tableName.getTableName())
            .setObjectId(tableIdent.getTableId())
            .setCreateTime(createTime)
            .setDroppedTime(droppedTime)
            .setIsPurge(isPurge)
            .build();
        DatabaseIdent databaseIdent = StoreConvertor.databaseIdent(tableIdent);
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore droppedObjectNameStore = DirectoryStoreHelper.getDroppedTableObjectNameStore(fdbRecordContext,
            databaseIdent);
        droppedObjectNameStore.saveRecord(droppedObjectName);
    }

    @Override
    public void deleteDroppedTable(TransactionContext context, TableIdent tableIdent, TableName tableName) {
        Tuple droppedTableObjectNameKey = buildDroppedTableObjectNameKey(tableName.getTableName(),
            tableIdent.getTableId());
        DatabaseIdent databaseIdent = StoreConvertor.databaseIdent(tableIdent);
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore droppedObjectNameStore = DirectoryStoreHelper.getDroppedTableObjectNameStore(fdbRecordContext,
            databaseIdent);
        droppedObjectNameStore.deleteRecord(droppedTableObjectNameKey);
    }

    @Override
    public Map<String, TableCommitObject> getLatestTableCommitByIds(TransactionContext context, String projectId, String catalogId, String databaseId, String branchVersion, List<String> tableIds) {
        return null;
    }

    @Override
    public Map<String, TableSchemaHistoryObject> getLatestTableSchemaByIds(TransactionContext context, String projectId, String catalogId, String databaseId, String branchVersion, List<String> tableIds) {
        return null;
    }

    @Override
    public Map<String, TableStorageHistoryObject> getLatestTableStorageByIds(TransactionContext context, String projectId, String catalogId, String databaseId, String branchVersion, List<String> tableIds) {
        return null;
    }

    @Override
    public Map<String, TableCommitObject> getLatestTableCommitByTmpTable(TransactionContext context, String projectId, String catalogId, String databaseId, String branchVersion, String tmpTable) {
        return null;
    }

    @Override
    public Map<String, TableSchemaHistoryObject> getLatestTableSchemaByTmpTable(TransactionContext context, String projectId, String catalogId, String databaseId, String branchVersion, String tmpTable) {
        return null;
    }

    @Override
    public Map<String, TableStorageHistoryObject> getLatestTableStorageByTmpTable(TransactionContext context, String projectId, String catalogId, String databaseId, String branchVersion, String tmpTable) {
        return null;
    }

    @Override
    public Map<String, TableBaseHistoryObject> getLatestTableBaseByTmpTable(TransactionContext context, String projectId, String catalogId, String databaseId, String branchVersion, String tmpTable) {
        return null;
    }

    @Override
    public DroppedTableObject getDroppedTable(TransactionContext context, TableIdent tableIdent,
        String tableName) {
        Tuple droppedTableObjectNameKey = buildDroppedTableObjectNameKey(tableName, tableIdent.getTableId());
        DatabaseIdent databaseIdent = StoreConvertor.databaseIdent(tableIdent);
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore droppedObjectNameStore = DirectoryStoreHelper
            .getDroppedTableObjectNameStore(fdbRecordContext, databaseIdent);

        FDBStoredRecord<Message> storedRecord = droppedObjectNameStore.loadRecord(droppedTableObjectNameKey);
        if (storedRecord == null) {
            return null;
        }
        DroppedTableObjectName droppedTableObjectName = DroppedTableObjectName.newBuilder()
            .mergeFrom(storedRecord.getRecord()).build();
        return trans2DroppedTableNameObject(droppedTableObjectName);
    }

    @Override
    public ScanRecordCursorResult<List<DroppedTableObject>> listDroppedTable(TransactionContext context,
        DatabaseIdent databaseIdent, int maxNum, byte[] continuation, TransactionIsolationLevel isolationLevel) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore DroppedtableObjectNameStore = DirectoryStoreHelper.getDroppedTableObjectNameStore(
            fdbRecordContext, databaseIdent);
        TupleRange tupleRange = TupleRange.ALL;

        List<DroppedTableObjectName> droppedTableObjectNameList = new ArrayList<>();

        // get lasted tableId, contains in-use and dropped
        ScanRecordCursorResult<List<FDBStoredRecord<Message>>> scannedRecords = RecordStoreHelper
            .listStoreRecordsWithToken(tupleRange, DroppedtableObjectNameStore, 0, maxNum, continuation,
                StoreTypeUtil.trans2IsolationLevel(isolationLevel));

        for (FDBStoredRecord<Message> i : scannedRecords.getResult()) {
            DroppedTableObjectName objectName = DroppedTableObjectName
                .newBuilder()
                .mergeFrom(i.getRecord()).build();
            droppedTableObjectNameList.add(objectName);
        }

        List<DroppedTableObject> droppedTableObjectList = droppedTableObjectNameList.stream()
            .map(droppedTableObjectName -> trans2DroppedTableNameObject(droppedTableObjectName)).collect(toList());
        return new ScanRecordCursorResult<>(droppedTableObjectList, scannedRecords.getContinuation().orElse(null));
    }

    @Override
    public List<DroppedTableObject> getDroppedTablesByName(TransactionContext context,
        DatabaseIdent databaseIdent, String tableName) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore droppedTableObjectNameStore = DirectoryStoreHelper
            .getDroppedTableObjectNameStore(fdbRecordContext, databaseIdent);

        QueryComponent filter = Query.field("name").equalsValue(tableName);

        List<FDBQueriedRecord<Message>> recordList = RecordStoreHelper
            .getRecords(droppedTableObjectNameStore, StoreMetadata.TABLE_DROPPED_OBJECT_NAME, filter);

        List<DroppedTableObjectName> droppedTableIdList = new ArrayList<>();
        for (FDBQueriedRecord<Message> record : recordList) {
            if (isTableUndropAvail(record.getStoredRecord())) {
                DroppedTableObjectName droppedTableObjectName = DroppedTableObjectName.newBuilder()
                    .mergeFrom(record.getRecord()).build();
                droppedTableIdList.add(droppedTableObjectName);
            }
        }

        return droppedTableIdList.stream()
            .map(droppedTableObjectName -> trans2DroppedTableNameObject(droppedTableObjectName)).collect(toList());
    }


}
