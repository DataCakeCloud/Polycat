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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;


import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.model.CatalogIdent;
import io.polycat.catalog.common.model.DatabaseHistoryObject;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.DatabaseNameObject;
import io.polycat.catalog.common.model.DatabaseObject;
import io.polycat.catalog.common.model.DroppedDatabaseNameObject;

import io.polycat.catalog.common.model.ScanRecordCursorResult;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.TransactionIsolationLevel;
import io.polycat.catalog.common.utils.CodecUtil;
import io.polycat.catalog.common.utils.UuidUtil;
import io.polycat.catalog.store.api.DatabaseStore;
import io.polycat.catalog.store.common.StoreConvertor;
import io.polycat.catalog.store.common.StoreMetadata;

import io.polycat.catalog.store.fdb.record.DirectoryStoreHelper;
import io.polycat.catalog.store.fdb.record.RecordStoreHelper;
import io.polycat.catalog.store.fdb.record.StoreTypeUtil;
import io.polycat.catalog.store.fdb.record.TransactionContextUtil;
import io.polycat.catalog.store.protos.DatabaseHistory;
import io.polycat.catalog.store.protos.Database;
import io.polycat.catalog.store.protos.DroppedObjectName;
import io.polycat.catalog.store.protos.ObjectName;

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
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

@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "fdb")
public class DatabaseStoreImpl implements DatabaseStore {
    private static final Logger logger = Logger.getLogger(DatabaseStoreImpl.class);

    private static class DatabaseStoreImplHandler {
        private static final DatabaseStoreImpl INSTANCE = new DatabaseStoreImpl();
    }

    public static DatabaseStoreImpl getInstance() {
        return DatabaseStoreImpl.DatabaseStoreImplHandler.INSTANCE;
    }

    private DatabaseNameObject trans2DatabaseNameObject(ObjectName objectName) {
        return new DatabaseNameObject(objectName.getParentId(), objectName.getName(), objectName.getObjectId());
    }

    private ObjectName trans2NameObject(DatabaseNameObject databaseNameObject) {
        return ObjectName.newBuilder()
            .setType(ObjectType.DATABASE.name())
            .setParentId(databaseNameObject.getParentId())
            .setName(databaseNameObject.getName())
            .setObjectId(databaseNameObject.getObjectId())
            .build();
    }

    private DroppedDatabaseNameObject trans2DroppedDatabaseNameObject(DroppedObjectName droppedObjectName) {
        return new DroppedDatabaseNameObject(droppedObjectName.getParentId(), droppedObjectName.getName(),
            droppedObjectName.getObjectId(), droppedObjectName.getDroppedTime());

    }

    private DroppedObjectName trans2DroppedObjectName(DroppedDatabaseNameObject droppedDatabaseNameObject) {
        return DroppedObjectName.newBuilder()
            .setType(ObjectType.DATABASE.name())
            .setParentId(droppedDatabaseNameObject.getParentId())
            .setName(droppedDatabaseNameObject.getName())
            .setObjectId(droppedDatabaseNameObject.getObjectId())
            .setDroppedTime(droppedDatabaseNameObject.getDroppedTime())
            .build();

    }

    @Override
    public void createDatabaseSubspace(TransactionContext context, String projectId) {

    }

    @Override
    public void createDatabaseHistorySubspace(TransactionContext context, String projectId) {

    }

    @Override
    public void createDroppedDatabaseNameSubspace(TransactionContext context, String projectId) {

    }

    @Override
    public void dropDatabaseSubspace(TransactionContext context, String projectId) {

    }

    @Override
    public String generateDatabaseId(TransactionContext context, String projectId, String catalogId) throws MetaStoreException {
        return UuidUtil.generateUUID32();
    }

    @Override
    public void dropDatabaseHistorySubspace(TransactionContext context, String projectId) {

    }

    @Override
    public void dropDroppedDatabaseNameSubspace(TransactionContext context, String projectId) {

    }

    @Override
    public ScanRecordCursorResult<List<String>> listDatabaseId(TransactionContext context,
        CatalogIdent catalogIdent, int maxNum, byte[] continuation, TransactionIsolationLevel isolationLevel) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore databaseObjectNameStore = DirectoryStoreHelper.getDatabaseObjectNameStore(fdbRecordContext, catalogIdent);
        TupleRange tupleRange = TupleRange.ALL;

        List<ObjectName> databaseObjectList = new ArrayList<>();

        // get lasted tableId, contains in-use and dropped
        ScanRecordCursorResult<List<FDBStoredRecord<Message>>> scannedRecords = RecordStoreHelper
            .listStoreRecordsWithToken(tupleRange, databaseObjectNameStore, 0, maxNum, continuation,
                StoreTypeUtil.trans2IsolationLevel(isolationLevel));

        for (FDBStoredRecord<Message> i : scannedRecords.getResult()) {
            ObjectName databaseObjectName  = ObjectName
                .newBuilder()
                .mergeFrom(i.getRecord()).build();

            databaseObjectList.add(databaseObjectName);
        }

        List<String> databaseIdList = databaseObjectList.stream()
            .map(objectName ->objectName.getObjectId()).collect(
                Collectors.toList());
        return new ScanRecordCursorResult<>(databaseIdList, scannedRecords.getContinuation().orElse(null));
    }


    @Override
    public ScanRecordCursorResult<List<DroppedDatabaseNameObject>> listDroppedDatabaseObjectName(TransactionContext context,
        CatalogIdent catalogIdent, int maxNum, byte[] continuation, TransactionIsolationLevel isolationLevel) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore databaseObjectNameStore = DirectoryStoreHelper.getDroppedDatabaseObjectNameStore(fdbRecordContext, catalogIdent);
        TupleRange tupleRange = TupleRange.ALL;

        List<DroppedObjectName> databaseObjectList = new ArrayList<>();

        // get lasted tableId, contains in-use and dropped
        ScanRecordCursorResult<List<FDBStoredRecord<Message>>> scannedRecords = RecordStoreHelper
            .listStoreRecordsWithToken(tupleRange, databaseObjectNameStore, 0, maxNum, continuation,
                StoreTypeUtil.trans2IsolationLevel(isolationLevel));

        for (FDBStoredRecord<Message> i : scannedRecords.getResult()) {
            DroppedObjectName droppedObjectName  = DroppedObjectName
                .newBuilder()
                .mergeFrom(i.getRecord()).build();

            databaseObjectList.add(droppedObjectName);
        }

        List<DroppedDatabaseNameObject> droppedDatabaseNameObjectList = databaseObjectList.stream()
            .map(droppedObjectName -> trans2DroppedDatabaseNameObject(droppedObjectName)).collect(
                Collectors.toList());
        return new ScanRecordCursorResult<>(droppedDatabaseNameObjectList, scannedRecords.getContinuation().orElse(null));
    }



    private Optional<FDBQueriedRecord<Message>> getDatabaseStoreRecord(FDBRecordStore recordStore,
        StoreMetadata storeMetadata, Versionstamp basedVersion) {
        FDBRecordVersion fdbEndRecordVersion = FDBRecordVersion.lastInGlobalVersion(
            basedVersion.getTransactionVersion());
        FDBRecordVersion fdbStartRecordVersion = FDBRecordVersion.fromVersionstamp(basedVersion);

        QueryComponent filter = Query.and(Query.version().lessThanOrEquals(fdbEndRecordVersion),
            Query.version().greaterThanOrEquals(fdbStartRecordVersion));

        return RecordStoreHelper.getLatestHistoryRecord(recordStore, storeMetadata, filter);
    }

    private DatabaseHistoryObject trans2DatabaseHistoryObject(DatabaseHistory databaseHistory) {
        DatabaseHistoryObject databaseHistoryObject = new DatabaseHistoryObject();
        databaseHistoryObject.setEventId(databaseHistory.getEventId());
        databaseHistoryObject.setName(databaseHistory.getName());
        databaseHistoryObject.setCreateTime(databaseHistory.getCreateTime());
        databaseHistoryObject.getProperties().putAll(databaseHistory.getPropertiesMap());
        databaseHistoryObject.setLocation(databaseHistory.getLocation());
        databaseHistoryObject.setDescription(databaseHistory.getDescription());
        databaseHistoryObject.setUserId(databaseHistory.getUserId());
        databaseHistoryObject.setVersion(CodecUtil.byteString2Hex(databaseHistory.getVersion()));
        databaseHistoryObject.setDroppedTime(databaseHistory.getDroppedTime());
        return databaseHistoryObject;

    }

    @Override
    public Optional<DatabaseHistoryObject> getDatabaseHistory(TransactionContext context, DatabaseIdent databaseIdent,
       String baseVersion) {
        Versionstamp targetVersion = Versionstamp.fromBytes(CodecUtil.hex2Bytes(baseVersion));
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore recordStore = DirectoryStoreHelper.getDatabaseHistoryStore(fdbRecordContext, databaseIdent);

        Optional<FDBQueriedRecord<Message>> optional = getDatabaseStoreRecord(recordStore,
            StoreMetadata.DATABASE_HISTORY_RECORD, targetVersion);

        if (!optional.isPresent()) {
            return Optional.empty();
        }
        FDBQueriedRecord<Message> record = optional.get();
        Versionstamp version = record.getVersion().toVersionstamp();
        DatabaseHistory databaseHistory = DatabaseHistory.newBuilder()
            .mergeFrom(record.getRecord()).setVersion(ByteString.copyFrom(version.getBytes())).build();

        return Optional.of(trans2DatabaseHistoryObject(databaseHistory));
    }

    private Optional<DatabaseHistory> getDatabaseHistory(Optional<FDBQueriedRecord<Message>> optional) {
        if (!optional.isPresent()) {
            return Optional.empty();
        }
        FDBQueriedRecord<Message> record = optional.get();
        Versionstamp version = record.getVersion().toVersionstamp();
        DatabaseHistory databaseHistory = DatabaseHistory.newBuilder()
            .mergeFrom(record.getRecord()).setVersion(ByteString.copyFrom(version.getBytes())).build();
        return Optional.of(databaseHistory);
    }


    private void insertDatabaseObjectName(TransactionContext context, DatabaseIdent databaseIdent, String databaseName) {
        CatalogIdent catalogIdent = StoreConvertor.catalogIdent(databaseIdent.getProjectId(),
            databaseIdent.getCatalogId(), databaseIdent.getRootCatalogId());
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore objectNameStore = DirectoryStoreHelper.getDatabaseObjectNameStore(fdbRecordContext, catalogIdent);
        ObjectName databaseObjectName = ObjectName.newBuilder()
            .setType(ObjectType.DATABASE.name())
            .setParentId(databaseIdent.getCatalogId())
            .setName(databaseName)
            .setObjectId(databaseIdent.getDatabaseId())
            .build();
        objectNameStore.insertRecord(databaseObjectName);
    }


    public static Tuple buildDatabaseObjectNameKey(DatabaseIdent databaseIdent, String databaseName) {
        return Tuple.from(ObjectType.DATABASE.name(), databaseIdent.getCatalogId(), databaseName);
    }


    private void deleteDatabaseObjectName(TransactionContext context, DatabaseIdent databaseIdent,
        String databaseName) {
        CatalogIdent catalogIdent = StoreConvertor
            .catalogIdent(databaseIdent.getProjectId(), databaseIdent.getCatalogId(), databaseIdent.getRootCatalogId());
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore objectNameStore = DirectoryStoreHelper.getDatabaseObjectNameStore(fdbRecordContext, catalogIdent);

        FDBStoredRecord<Message> storedRecord = objectNameStore
            .loadRecord(buildDatabaseObjectNameKey(databaseIdent, databaseName));
        if (storedRecord == null) {
            return;
        }

        objectNameStore.deleteRecord(buildDatabaseObjectNameKey(databaseIdent, databaseName));
    }

    public static DroppedObjectName getDroppedObjectName(FDBRecordStore store, Tuple primaryKey) {
        FDBStoredRecord<Message> storedRecord = store.loadRecord(primaryKey);
        if (storedRecord == null) {
            return null;
        }
        return DroppedObjectName.newBuilder().mergeFrom(storedRecord.getRecord()).build();
    }

    private Tuple buildDatabaseObjNameKey(DatabaseIdent databaseIdent, String databaseName) {
        return Tuple.from(ObjectType.DATABASE.name(), databaseIdent.getCatalogId(), databaseName, databaseIdent.getDatabaseId());
    }

    @Override
    public void deleteDroppedDatabaseObjectName(TransactionContext context, DatabaseIdent databaseIdent,
            String databaseName) {
        CatalogIdent catalogIdent = StoreConvertor.catalogIdent(databaseIdent.getProjectId(),
            databaseIdent.getCatalogId(), databaseIdent.getRootCatalogId());
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore droppedObjectNameStore = DirectoryStoreHelper
            .getDroppedDatabaseObjectNameStore(fdbRecordContext, catalogIdent);
        Tuple droppedObjectNameKey = buildDatabaseObjNameKey(databaseIdent, databaseName);

        FDBStoredRecord<Message> storedRecord = droppedObjectNameStore.loadRecord(droppedObjectNameKey);
        if (storedRecord != null) {
            droppedObjectNameStore.deleteRecord(droppedObjectNameKey);
        }
    }

    @Override
    public void insertDroppedDatabaseObjectName(TransactionContext context, DatabaseIdent databaseIdent,
        String databaseName) {
        CatalogIdent catalogIdent = StoreConvertor.catalogIdent(databaseIdent.getProjectId(),
            databaseIdent.getCatalogId(), databaseIdent.getRootCatalogId());
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore droppedObjectNameStore = DirectoryStoreHelper
            .getDroppedDatabaseObjectNameStore(fdbRecordContext, catalogIdent);
        DroppedObjectName droppedObjectName = DroppedObjectName.newBuilder()
            .setType(ObjectType.DATABASE.name())
            .setParentId(databaseIdent.getCatalogId())
            .setName(databaseName)
            .setObjectId(databaseIdent.getDatabaseId())
            .setDroppedTime(RecordStoreHelper.getCurrentTime())
            .build();
        droppedObjectNameStore.insertRecord(droppedObjectName);
    }


    private Database trans2Database(DatabaseObject databaseObject) {
        return Database.newBuilder()
            .setPrimaryKey(0)
            .setName(databaseObject.getName())
            .putAllProperties(databaseObject.getProperties())
            .setLocation(databaseObject.getLocation())
            .setDescription(databaseObject.getDescription())
            .setUserId(databaseObject.getUserId())
            .setCreateTime(databaseObject.getCreateTime()).build();
    }

    private DatabaseObject trans2DatabaseObject(DatabaseIdent databaseIdent, Database database) {
       DatabaseObject databaseObject = new DatabaseObject();

        databaseObject.setName(database.getName());
        databaseObject.setProjectId(databaseIdent.getProjectId());
        databaseObject.setCatalogId(databaseIdent.getCatalogId());
        databaseObject.setDatabaseId(databaseIdent.getDatabaseId());
        databaseObject.getProperties().putAll(database.getPropertiesMap());
        databaseObject.setLocation(database.getLocation());
        databaseObject.setCreateTime(database.getCreateTime());
        databaseObject.setDescription(database.getDescription());
        databaseObject.setUserId(database.getUserId());

        return databaseObject;

    }


    @Override
    public void insertDatabase(TransactionContext context, DatabaseIdent databaseIdent, DatabaseObject databaseObject) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore databaseStore = DirectoryStoreHelper.getDatabaseStore(fdbRecordContext, databaseIdent);
        databaseStore.insertRecord(trans2Database(databaseObject));
        insertDatabaseObjectName(context, databaseIdent, databaseObject.getName());
    }

    @Override
    public void upsertDatabase(TransactionContext context, DatabaseIdent databaseIdent, DatabaseObject databaseObject) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore databaseStore = DirectoryStoreHelper.getDatabaseStore(fdbRecordContext, databaseIdent);
        FDBStoredRecord<Message> storedRecord = databaseStore.loadRecord(buildDatabaseKey());
        if (storedRecord != null) {
            Database database = Database.newBuilder().mergeFrom(storedRecord.getRecord()).build();
            deleteDatabaseObjectName(context, databaseIdent, database.getName());
        }

        databaseStore.saveRecord(trans2Database(databaseObject));
        insertDatabaseObjectName(context, databaseIdent, databaseObject.getName());
    }

    @Override
    public void updateDatabase(TransactionContext context, DatabaseIdent databaseIdent, DatabaseObject newDatabaseObject) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore databaseStore = DirectoryStoreHelper.getDatabaseStore(fdbRecordContext, databaseIdent);
        FDBStoredRecord<Message> storedRecord = databaseStore.loadRecord(buildDatabaseKey());
        if (storedRecord == null) {
            throw new MetaStoreException(ErrorCode.DATABASE_NOT_FOUND, newDatabaseObject.getName());
        }
        Database database = Database.newBuilder().mergeFrom(storedRecord.getRecord()).build();
        deleteDatabaseObjectName(context, databaseIdent, database.getName());
        insertDatabaseObjectName(context, databaseIdent, newDatabaseObject.getName());
        databaseStore.updateRecord(trans2Database(newDatabaseObject));
    }


    @Override
    public void deleteDatabase(TransactionContext context, DatabaseIdent databaseIdent) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore databaseStore = DirectoryStoreHelper.getDatabaseStore(fdbRecordContext, databaseIdent);
        Tuple pkey = buildDatabaseKey();
        FDBStoredRecord<Message> dbRecord = databaseStore.loadRecord(pkey);
        if (dbRecord != null) {
            Database database = Database.newBuilder().mergeFrom(dbRecord.getRecord()).build();
            deleteDatabaseObjectName(context, databaseIdent, database.getName());
            databaseStore.deleteRecord(pkey);
        }
    }

    @Override
    public void insertDatabaseHistory(TransactionContext context, DatabaseHistoryObject databaseHistoryObject,
        DatabaseIdent databaseIdent, boolean dropped, String version) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore databaseHistoryStore = DirectoryStoreHelper.getDatabaseHistoryStore(fdbRecordContext, databaseIdent);
        DatabaseHistory databaseHistory = trans2DatabaseHistory(databaseHistoryObject);
        if (dropped) {
            databaseHistory = databaseHistory.toBuilder().setDroppedTime(RecordStoreHelper.getCurrentTime()).build();
        }
        databaseHistoryStore.saveRecord(databaseHistory);
    }


    private DatabaseHistory trans2DatabaseHistory(DatabaseHistoryObject databaseHistoryObject) {
        return DatabaseHistory.newBuilder()
            .setEventId(UuidUtil.generateId())
            .setName(databaseHistoryObject.getName())
            .putAllProperties(databaseHistoryObject.getProperties())
            .setCreateTime(databaseHistoryObject.getCreateTime())
            .setLocation(databaseHistoryObject.getLocation())
            .setDescription(databaseHistoryObject.getDescription())
            .setUserId(databaseHistoryObject.getUserId())
            .build();
    }

    private Optional<FDBQueriedRecord<Message>> getLatestDatabaseStoreRecord(FDBRecordStore recordStore,
        StoreMetadata storeMetadata, Versionstamp basedVersion) {
        FDBRecordVersion baseFdbRecordVersion = FDBRecordVersion.lastInGlobalVersion(
            basedVersion.getTransactionVersion());
        QueryComponent filter = Query.version().lessThanOrEquals(baseFdbRecordVersion);
        return RecordStoreHelper.getLatestHistoryRecord(recordStore, storeMetadata, filter);
    }

    @Override
    public Optional<DatabaseHistoryObject> getLatestDatabaseHistory(TransactionContext context, DatabaseIdent databaseIdent,
            String basedVersion) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore recordStore = DirectoryStoreHelper.getDatabaseHistoryStore(fdbRecordContext, databaseIdent);
        Versionstamp versionstamp = Versionstamp.fromBytes(CodecUtil.hex2Bytes(basedVersion));
        Optional<FDBQueriedRecord<Message>> optional = getLatestDatabaseStoreRecord(recordStore,
            StoreMetadata.DATABASE_HISTORY_RECORD, versionstamp);

        if (!optional.isPresent()) {
            return Optional.empty();
        }
        FDBQueriedRecord<Message> record = optional.get();
        Versionstamp version = record.getVersion().toVersionstamp();
        DatabaseHistory databaseHistory = DatabaseHistory.newBuilder()
            .mergeFrom(record.getRecord()).setVersion(ByteString.copyFrom(version.getBytes())).build();

        return Optional.of(trans2DatabaseHistoryObject(databaseHistory));
    }

    @Override
    public Map<String, DatabaseHistoryObject> getLatestDatabaseHistoryByIds(TransactionContext context, String projectId, String catalogId, String basedVersion, List<String> databaseIds) {
        return null;
    }


    @Override
    public Optional<String> getDatabaseId(TransactionContext context, CatalogIdent catalogIdent,
        String databaseName) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore objNameStore = DirectoryStoreHelper.getDatabaseObjectNameStore(fdbRecordContext, catalogIdent);
        FDBStoredRecord<Message> record = objNameStore.loadRecord(Tuple.from(ObjectType.DATABASE.name(),
            catalogIdent.getCatalogId(), databaseName));
        if (null == record) {
            return Optional.empty();
        }

        ObjectName objectName = ObjectName.newBuilder().mergeFrom(record.getRecord()).build();
        return Optional.of(objectName.getObjectId());
    }

    @Override
    public DroppedDatabaseNameObject getDroppedDatabaseObjectName(TransactionContext context, DatabaseIdent databaseIdent,
        String databaseName) {
        CatalogIdent catalogIdent = StoreConvertor.catalogIdent(databaseIdent.getProjectId(),
            databaseIdent.getCatalogId(), databaseIdent.getRootCatalogId());
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore droppedObjectNameStore = DirectoryStoreHelper.getDroppedDatabaseObjectNameStore(fdbRecordContext, catalogIdent);

        FDBStoredRecord<Message> storedRecord = droppedObjectNameStore
            .loadRecord(buildDatabaseObjNameKey(databaseIdent, databaseName));
        if (storedRecord == null) {
            return null;
        }

        DroppedObjectName droppedObjectName = DroppedObjectName.newBuilder().mergeFrom(storedRecord.getRecord()).build();
        return trans2DroppedDatabaseNameObject(droppedObjectName);
    }


    @Override
    public ScanRecordCursorResult<List<DatabaseHistoryObject>> listDatabaseHistory(TransactionContext context, DatabaseIdent databaseIdent,
        int maxBatchRowNum, byte[] continuation, String baseVersion) {
        Versionstamp versionstamp = Versionstamp.fromBytes(CodecUtil.hex2Bytes(baseVersion));
        FDBRecordVersion baseFdbRecordVersion = FDBRecordVersion.lastInGlobalVersion(
            versionstamp.getTransactionVersion());
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore databaseHistoryStore = DirectoryStoreHelper.getDatabaseHistoryStore(fdbRecordContext, databaseIdent);
        QueryComponent filter = Query.version().lessThanOrEquals(baseFdbRecordVersion);
        RecordQueryPlan plan = RecordStoreHelper.buildVersionRecordQueryPlan(databaseHistoryStore,
            StoreMetadata.DATABASE_HISTORY_RECORD, filter, true);

        RecordCursor<DatabaseHistory> cursor = databaseHistoryStore.executeQuery(plan, continuation,
            ExecuteProperties
                .newBuilder().setReturnedRowLimit(maxBatchRowNum)
                .setIsolationLevel(IsolationLevel.SNAPSHOT)
                .build())
            .map(rec -> DatabaseHistory.newBuilder()
                .mergeFrom(rec.getRecord())
                .setVersion(ByteString.copyFrom(rec.getVersion().toVersionstamp().getBytes()))
                .build());

        List<DatabaseHistory> batchDatabaseHistoryList = cursor.asList().join();
        continuation = cursor.getNext().getContinuation().toBytes();

        List<DatabaseHistoryObject> databaseHistoryObjectList = batchDatabaseHistoryList.stream()
            .map(databaseHistory -> trans2DatabaseHistoryObject(databaseHistory)).collect(
                Collectors.toList());

        return new ScanRecordCursorResult<>(databaseHistoryObjectList, continuation);

    }


    @Override
    public DatabaseObject getDatabase(TransactionContext context, DatabaseIdent databaseIdent) {
        // get Database instance by database id
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore databaseStore = DirectoryStoreHelper.getDatabaseStore(fdbRecordContext, databaseIdent);
        FDBStoredRecord<Message> storedRecord = databaseStore.loadRecord(buildDatabaseKey());
        if (storedRecord != null) {
            Database database = Database.newBuilder().mergeFrom(storedRecord.getRecord()).build();
            return trans2DatabaseObject(databaseIdent, database);
        }

        return null;
    }

    @Override
    public List<DatabaseObject> getDatabaseByIds(TransactionContext ctx, String projectId, String catalogId, List<String> databaseIds) {
        return null;
    }

    private Tuple buildDatabaseKey() {
        return Tuple.from(0);
    }

}
