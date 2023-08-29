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
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.tuple.Tuple;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.model.CatalogCommitObject;
import io.polycat.catalog.common.model.CatalogHistoryObject;
import io.polycat.catalog.common.model.CatalogId;
import io.polycat.catalog.common.model.CatalogIdent;
import io.polycat.catalog.common.model.CatalogName;
import io.polycat.catalog.common.model.CatalogObject;
import io.polycat.catalog.common.model.ScanRecordCursorResult;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.TransactionIsolationLevel;
import io.polycat.catalog.common.utils.CodecUtil;
import io.polycat.catalog.common.utils.UuidUtil;
import io.polycat.catalog.store.api.CatalogStore;
import io.polycat.catalog.store.common.CatalogStoreConvertor;
import io.polycat.catalog.store.common.StoreConvertor;
import io.polycat.catalog.store.common.StoreMetadata;
import io.polycat.catalog.store.fdb.record.DirectoryStoreHelper;
import io.polycat.catalog.store.fdb.record.RecordStoreHelper;
import io.polycat.catalog.store.fdb.record.StoreTypeUtil;
import io.polycat.catalog.store.fdb.record.TransactionContextUtil;
import io.polycat.catalog.store.protos.CatalogCommit;
import io.polycat.catalog.store.protos.CatalogHistory;
import io.polycat.catalog.store.protos.Catalog;
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
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import io.polycat.catalog.store.protos.SubBranchRecord;
import io.polycat.catalog.store.protos.common.CatalogInfo;

@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "fdb")
public class CatalogStoreImpl implements CatalogStore {
    private static final Logger logger = Logger.getLogger(CatalogStoreImpl.class);

    private static class CatalogStoreImplHandler {
        private static final CatalogStoreImpl INSTANCE = new CatalogStoreImpl();
    }

    public static CatalogStoreImpl getInstance() {
        return CatalogStoreImpl.CatalogStoreImplHandler.INSTANCE;
    }


    private Optional<FDBQueriedRecord<Message>> getLatestCatalogStoreRecord(FDBRecordStore recordStore,
        StoreMetadata storeMetadata, String basedVersion) {
        FDBRecordVersion baseFdbRecordVersion = FDBRecordVersion
            .lastInGlobalVersion(Versionstamp.fromBytes(CodecUtil.hex2Bytes(basedVersion)).getTransactionVersion());
        QueryComponent filter = Query.version().lessThanOrEquals(baseFdbRecordVersion);
        return RecordStoreHelper.getLatestHistoryRecord(recordStore, storeMetadata, filter);
    }

    private Optional<FDBQueriedRecord<Message>> getCatalogStoreRecord(FDBRecordStore recordStore,
        StoreMetadata storeMetadata, Versionstamp basedVersion) {
        FDBRecordVersion fdbEndRecordVersion = FDBRecordVersion
            .lastInGlobalVersion(basedVersion.getTransactionVersion());
        FDBRecordVersion fdbStartRecordVersion = FDBRecordVersion.fromVersionstamp(basedVersion);
        QueryComponent filter = Query.and(Query.version().lessThanOrEquals(fdbEndRecordVersion),
            Query.version().greaterThanOrEquals(fdbStartRecordVersion));

        return RecordStoreHelper.getLatestHistoryRecord(recordStore, storeMetadata, filter);
    }


    /**
     * catalog
     */
    private Tuple buildCatalogKey(String catalogId) {
        return Tuple.from(catalogId);
    }

    private Catalog trans2Catalog(CatalogObject catalogObject) {
        CatalogInfo catalogInfo = CatalogStoreConvertor.getCatalogInfo(catalogObject);
        Catalog catalog = Catalog.newBuilder()
            .setCatalogId(catalogObject.getCatalogId())
            .setName(catalogObject.getName())
            .setRootCatalogId(catalogObject.getRootCatalogId())
            .setCatalogInfo(catalogInfo)
            .build();

        return catalog;
    }

    @Override
    public void createCatalogSubspace(TransactionContext context, String projectId) throws MetaStoreException {

    }

    @Override
    public void dropCatalogSubspace(TransactionContext context, String projectId) throws MetaStoreException {

    }

    @Override
    public String generateCatalogId(TransactionContext context, String projectId) throws MetaStoreException {
        return UuidUtil.generateUUID32();
    }

    @Override
    public void insertCatalog(TransactionContext context, CatalogIdent catalogIdent, CatalogObject catalogObject) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore catalogStore = DirectoryStoreHelper.getCatalogStore(fdbRecordContext, catalogIdent.getProjectId());
        catalogStore.insertRecord(trans2Catalog(catalogObject));
    }

    @Override
    public void updateCatalog(TransactionContext context, CatalogObject catalogObject, CatalogObject newCatalogObject) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        CatalogIdent catalogIdent = StoreConvertor.catalogIdent(catalogObject);
        FDBRecordStore catalogStore = DirectoryStoreHelper.getCatalogStore(fdbRecordContext, catalogIdent.getProjectId());
        catalogStore.saveRecord(trans2Catalog(newCatalogObject));
    }

    @Override
    public void deleteCatalog(TransactionContext context, CatalogName catalogName, CatalogIdent catalogIdent) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore catalogStore = DirectoryStoreHelper.getCatalogStore(fdbRecordContext, catalogIdent.getProjectId());
        catalogStore.deleteRecord(buildCatalogKey(catalogIdent.getCatalogId()));
    }

    @Override
    public CatalogObject getCatalogById(TransactionContext context, CatalogIdent catalogIdent) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore catalogStore = DirectoryStoreHelper.getCatalogStore(fdbRecordContext, catalogIdent.getProjectId());
        FDBStoredRecord<Message> storedRecord = catalogStore.loadRecord(buildCatalogKey(catalogIdent.getCatalogId()));
        if (storedRecord == null) {
            return null;
        }
        Catalog.Builder catalog = Catalog.newBuilder().mergeFrom(storedRecord.getRecord());
        return new CatalogObject(catalogIdent.getProjectId(), catalog.getCatalogId(), catalog.getName(),
            catalog.getRootCatalogId(), catalog.getCatalogInfo());
    }

    @Override
    public CatalogId getCatalogId(TransactionContext context, String projectId, String catalogName) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore catalogStore = DirectoryStoreHelper.getCatalogStore(fdbRecordContext, projectId);
        QueryComponent filter = Query.field("name").equalsValue(catalogName);

        List<FDBQueriedRecord<Message>> recordList = RecordStoreHelper.getRecords(catalogStore,
            StoreMetadata.CATALOG, filter);

        if (recordList.size() != 1) {
            return null;
        }

        Catalog.Builder builder = Catalog.newBuilder().mergeFrom(recordList.get(0).getRecord());
        return new CatalogId(builder.getCatalogId(), builder.getRootCatalogId());
    }

    @Override
    public CatalogObject getCatalogByName(TransactionContext context, CatalogName catalogName)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore catalogStore = DirectoryStoreHelper.getCatalogStore(fdbRecordContext, catalogName.getProjectId());
        QueryComponent filter = Query.field("name").equalsValue(catalogName.getCatalogName());

        List<FDBQueriedRecord<Message>> recordList = RecordStoreHelper.getRecords(catalogStore,
            StoreMetadata.CATALOG, filter);

        if (recordList.size() != 1) {
            return null;
        }

        Catalog.Builder builder = Catalog.newBuilder().mergeFrom(recordList.get(0).getRecord());
        return new CatalogObject(catalogName.getProjectId(), builder.getCatalogId(), builder.getName(),
            builder.getRootCatalogId(), builder.getCatalogInfo());
    }

    @Override
    public ScanRecordCursorResult<List<CatalogObject>> listCatalog(TransactionContext context,
        String projectId, int maxNum, byte[] continuation, TransactionIsolationLevel isolationLevel) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore catalogStore = DirectoryStoreHelper.getCatalogStore(fdbRecordContext, projectId);
        TupleRange tupleRange = TupleRange.ALL;

        // get lasted tableId, contains in-use and dropped
        ScanRecordCursorResult<List<FDBStoredRecord<Message>>> scannedRecords = RecordStoreHelper
            .listStoreRecordsWithToken(tupleRange, catalogStore, 0, maxNum, continuation,
                StoreTypeUtil.trans2IsolationLevel(isolationLevel));

        List<CatalogObject> catalogObjectList = new ArrayList<>(scannedRecords.getResult().size());
        for (FDBStoredRecord<Message> i : scannedRecords.getResult()) {
            Catalog.Builder builder  = Catalog
                .newBuilder()
                .mergeFrom(i.getRecord());
            CatalogObject catalogObject = new CatalogObject(projectId, builder.getCatalogId(), builder.getName(),
                builder.getRootCatalogId(), builder.getCatalogInfo());
            catalogObjectList.add(catalogObject);
        }

        return new ScanRecordCursorResult<>(catalogObjectList, scannedRecords.getContinuation().orElse(null));
    }

    /**
     * catalog history
     */
    @Override
    public void createCatalogHistorySubspace(TransactionContext context, String projectId) throws MetaStoreException {

    }

    @Override
    public void dropCatalogHistorySubspace(TransactionContext context, String projectId)
        throws MetaStoreException {

    }

    @Override
    public void deleteCatalogHistory(TransactionContext context, CatalogIdent catalogIdent) throws MetaStoreException {

    }

    @Override
    public void insertCatalogHistory(TransactionContext context, CatalogIdent catalogIdent, CatalogObject catalogObject,
        String version) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore catalogHistoryStore = DirectoryStoreHelper.getCatalogHistoryStore(fdbRecordContext, catalogIdent);
        CatalogInfo catalogInfo = CatalogStoreConvertor.getCatalogInfo(catalogObject);
        CatalogHistory catalogHistory = CatalogHistory.newBuilder()
            .setEventId(UuidUtil.generateId())
            .setCatalogId(catalogObject.getCatalogId())
            .setName(catalogObject.getName())
            .setRootCatalogId(catalogObject.getRootCatalogId())
            .setCatalogInfo(catalogInfo)
            .build();
        catalogHistoryStore.insertRecord(catalogHistory);
    }

    @Override
    public CatalogHistoryObject getLatestCatalogHistory(TransactionContext context, CatalogIdent catalogIdent,
        String latestVersion) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore recordStore = DirectoryStoreHelper.getCatalogHistoryStore(fdbRecordContext, catalogIdent);
        FDBQueriedRecord<Message> record = getLatestCatalogStoreRecord(recordStore,
            StoreMetadata.CATALOG_HISTORY, latestVersion).get();

        Versionstamp version = record.getVersion().toVersionstamp();
        CatalogHistory catalogHistory = CatalogHistory.newBuilder()
            .mergeFrom(record.getRecord()).setVersion(ByteString.copyFrom(version.getBytes())).build();

        return CatalogStoreConvertor.trans2CatalogHistoryObject(catalogIdent, catalogHistory.getEventId(),
            CodecUtil.byteString2Hex(catalogHistory.getVersion()), catalogHistory.getName(), catalogHistory.getRootCatalogId(),
            catalogHistory.getCatalogInfo());
    }

    @Override
    public CatalogHistoryObject getCatalogHistoryByVersion(TransactionContext context, CatalogIdent catalogIdent,
        String version) throws MetaStoreException {
        Versionstamp baseVersion = Versionstamp.fromBytes(CodecUtil.hex2Bytes(version));
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore recordStore = DirectoryStoreHelper.getCatalogHistoryStore(fdbRecordContext, catalogIdent);
        FDBQueriedRecord<Message> record = getCatalogStoreRecord(recordStore,
            StoreMetadata.CATALOG_HISTORY, baseVersion).get();

        Versionstamp recordVersion = record.getVersion().toVersionstamp();
        CatalogHistory catalogHistory = CatalogHistory.newBuilder()
            .mergeFrom(record.getRecord()).setVersion(ByteString.copyFrom(recordVersion.getBytes())).build();

        return CatalogStoreConvertor.trans2CatalogHistoryObject(catalogIdent, catalogHistory.getEventId(),
            CodecUtil.byteString2Hex(catalogHistory.getVersion()), catalogHistory.getName(), catalogHistory.getRootCatalogId(),
            catalogHistory.getCatalogInfo());
    }

    /**
     * catalog commit
     */
    private CatalogCommitObject trans2CatalogCommitObject(CatalogIdent catalogIdent, CatalogCommit catalogCommit) {
        CatalogCommitObject catalogCommitObject = new CatalogCommitObject();
        catalogCommitObject.setProjectId(catalogIdent.getProjectId());
        catalogCommitObject.setCatalogId(catalogIdent.getCatalogId());
        catalogCommitObject.setCommitId(catalogCommit.getCommitId());
        catalogCommitObject.setCommitTime(catalogCommit.getCommitTime());
        catalogCommitObject.setOperation(catalogCommit.getOperation());
        catalogCommitObject.setDetail(catalogCommit.getDetail());
        catalogCommitObject.setVersion(CodecUtil.byteString2Hex(catalogCommit.getVersion()));

        return catalogCommitObject;

    }

    @Override
    public void createCatalogCommitSubspace(TransactionContext context, String projectId) throws MetaStoreException {

    }

    @Override
    public void dropCatalogCommitSubspace(TransactionContext context, String projectId)
        throws MetaStoreException {

    }

    @Override
    public void deleteCatalogCommit(TransactionContext context, CatalogIdent catalogIdent) throws MetaStoreException {

    }

    @Override
    public void insertCatalogCommit(TransactionContext context, String projectId, String catalogId, String commitId,
        long commitTime, Operation operation, String detail, String version) {
        CatalogIdent catalogIdent = new CatalogIdent(projectId, catalogId);
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore catalogCommitStore = DirectoryStoreHelper.getCatalogCommitStore(fdbRecordContext, catalogIdent);
        CatalogCommit catalogCommit = CatalogCommit.newBuilder()
            .setCommitId(commitId)
            .setCommitTime(commitTime)
            .setOperation(operation.getPrintName())
            .setDetail(detail)
            .build();
        catalogCommitStore.insertRecord(catalogCommit);
    }

    //// todo : after all user insertCatalogCommit with version, this interface will delete
    @Override
    public void insertCatalogCommit(TransactionContext context, String projectId, String catalogId, String commitId,
        long commitTime, Operation operation, String detail) throws MetaStoreException {
        insertCatalogCommit(context, projectId, catalogId, commitId, commitTime, operation, detail, "");
    }

    @Override
    public Boolean catalogCommitExist(TransactionContext context, CatalogIdent catalogIdent, String commitId)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore catalogCommitStore = DirectoryStoreHelper.getCatalogCommitStore(fdbRecordContext, catalogIdent);
        FDBStoredRecord<Message> record = catalogCommitStore.loadRecord(Tuple.from(commitId));
        return (record != null);
    }

    // todo : after all user catalogCommitExist, this interface will delete
    @Override
    public Optional<CatalogCommitObject> getCatalogCommit(TransactionContext context, CatalogIdent catalogIdent,
        String commitId) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore catalogCommitStore = DirectoryStoreHelper.getCatalogCommitStore(fdbRecordContext, catalogIdent);
        FDBStoredRecord<Message> record = catalogCommitStore.loadRecord(Tuple.from(commitId));
        if (record == null) {
            return Optional.empty();
        }

        CatalogCommit catalogCommit = CatalogCommit.newBuilder().mergeFrom(record.getRecord()).build();

        return Optional.of(trans2CatalogCommitObject(catalogIdent, catalogCommit));
    }

    @Override
    public CatalogCommitObject getLatestCatalogCommit(TransactionContext context, CatalogIdent catalogIdent,
        String baseVersion) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore recordStore = DirectoryStoreHelper.getCatalogCommitStore(fdbRecordContext, catalogIdent);
        FDBQueriedRecord<Message> record = getLatestCatalogStoreRecord(recordStore,
            StoreMetadata.CATALOG_COMMIT, baseVersion).get();

        Versionstamp version = record.getVersion().toVersionstamp();
        CatalogCommit catalogCommit = CatalogCommit.newBuilder()
            .mergeFrom(record.getRecord()).setVersion(ByteString.copyFrom(version.getBytes())).build();

        return trans2CatalogCommitObject(catalogIdent, catalogCommit);
    }

    @Override
    public ScanRecordCursorResult<List<CatalogCommitObject>> listCatalogCommit(TransactionContext context,
        CatalogIdent catalogIdent, int maxNum, byte[] continuation, byte[] version) {
        Versionstamp readVersion = Versionstamp.fromBytes(version);
        FDBRecordVersion baseFdbRecordVersion = FDBRecordVersion.lastInGlobalVersion(readVersion.getTransactionVersion());
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore catalogCommitStore = DirectoryStoreHelper.getCatalogCommitStore(fdbRecordContext, catalogIdent);
        QueryComponent filter = Query.version().lessThanOrEquals(baseFdbRecordVersion);
        RecordQueryPlan plan = RecordStoreHelper.buildVersionRecordQueryPlan(catalogCommitStore,
            StoreMetadata.CATALOG_COMMIT,
            filter, true);

        RecordCursor<CatalogCommit> cursor = catalogCommitStore.executeQuery(plan, continuation,
                ExecuteProperties
                    .newBuilder().setReturnedRowLimit(maxNum)
                    .setIsolationLevel(IsolationLevel.SNAPSHOT)
                    .build())
            .map(rec -> CatalogCommit.newBuilder()
                .mergeFrom(rec.getRecord())
                .setVersion(ByteString.copyFrom(rec.getVersion().toVersionstamp().getBytes()))
                .build());

        List<CatalogCommit> batchCatalogCommitList = cursor.asList().join();
        continuation = cursor.getNext().getContinuation().toBytes();

        List<CatalogCommitObject> catalogCommitObjectList = batchCatalogCommitList.stream()
            .map(catalogCommit -> trans2CatalogCommitObject(catalogIdent, catalogCommit)).collect(
                Collectors.toList());

        return new ScanRecordCursorResult<>(catalogCommitObjectList, continuation);
    }

    /**
     * branch
     */
    private Tuple buildBranchKey(String catalogId) {
        return Tuple.from(catalogId);
    }

    private List<CatalogObject> getSubBranchCatalogs(TransactionContext context, CatalogIdent parentBranchCatalogIdent) {
        List<CatalogObject> subBranchCatalogList = new ArrayList<>();
        ScanRecordCursorResult<List<CatalogObject>> catalogList = listCatalog(context,
            parentBranchCatalogIdent.getProjectId(),
            Integer.MAX_VALUE, null, TransactionIsolationLevel.SERIALIZABLE);

        for (CatalogObject catalog : catalogList.getResult()) {
            if (catalog.hasParentId()) {
                if (catalog.getParentId().equals(parentBranchCatalogIdent.getCatalogId())) {
                    subBranchCatalogList.add(catalog);
                    CatalogIdent catalogIdent = StoreConvertor.catalogIdent(catalog);
                    subBranchCatalogList.addAll(getSubBranchCatalogs(context, catalogIdent));
                }
            }
        }

        return subBranchCatalogList;
    }

    @Override
    public void createBranchSubspace(TransactionContext context, String projectId) {

    }

    @Override
    public void dropBranchSubspace(TransactionContext context, String projectId) {

    }

    @Override
    public void deleteBranch(TransactionContext context, CatalogIdent catalogIdent) {

    }

    @Override
    public boolean hasSubBranchCatalog(TransactionContext context, CatalogIdent parentBranchCatalogIdent) {
        List<CatalogObject> catalogRecordList = getSubBranchCatalogs(context, parentBranchCatalogIdent);
        return !catalogRecordList.isEmpty();
    }

    @Override
    public void insertCatalogSubBranch(TransactionContext context, CatalogIdent catalogIdent, String subBranchCatalogId,
        String parentVersion) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore subBranchStore = DirectoryStoreHelper.getCatalogSubBranchStore(fdbRecordContext, catalogIdent);
        SubBranchRecord subBranchRecord = SubBranchRecord.newBuilder()
            .setCatalogId(subBranchCatalogId)
            .setParentVersion(CodecUtil.hex2ByteString(parentVersion))
            .build();
        subBranchStore.saveRecord(subBranchRecord);
    }

    @Override
    public List<CatalogObject> getParentBranchCatalog(TransactionContext context, CatalogIdent subbranchCatalogIdent) {
        List<CatalogObject> catalogList = new ArrayList<>();
        CatalogObject currentCatalog = getCatalogById(context, subbranchCatalogIdent);
        if (currentCatalog == null) {
            return catalogList;
        }

        while (currentCatalog.hasParentId()) {
            CatalogIdent parentCatalogIdent = StoreConvertor.catalogIdent(currentCatalog.getProjectId(),
                currentCatalog.getParentId(), currentCatalog.getRootCatalogId());
            CatalogObject parentCatalog = getCatalogById(context, parentCatalogIdent);
            if (parentCatalog == null) {
                return catalogList;
            }

            catalogList.add(parentCatalog);

            currentCatalog = parentCatalog;
        }

        return catalogList;
    }

    @Override
    public List<CatalogObject> getNextLevelSubBranchCatalogs(TransactionContext context,
        CatalogIdent parentBranchCatalogIdent) {
        //Obtain tableId include dropped and in-use
        List<CatalogObject> catalogRecordList = new ArrayList<>();
        TupleRange tupleRange = TupleRange.ALL;
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore tableStore = DirectoryStoreHelper.getCatalogSubBranchStore(fdbRecordContext, parentBranchCatalogIdent);
        // get lasted tableId, contains in-use and dropped
        List<FDBStoredRecord<Message>> scannedRecords = RecordStoreHelper
            .listStoreRecords(tupleRange, tableStore, Integer.MAX_VALUE, null, IsolationLevel.SERIALIZABLE);

        for (FDBStoredRecord<Message> i : scannedRecords) {
            SubBranchRecord subBranchRecord = SubBranchRecord.newBuilder().mergeFrom(i.getRecord()).build();
            CatalogIdent subBranchCatalogIdent = StoreConvertor.catalogIdent(parentBranchCatalogIdent.getProjectId(),
                subBranchRecord.getCatalogId(), parentBranchCatalogIdent.getRootCatalogId());
            FDBRecordStore CatalogStore = DirectoryStoreHelper.getCatalogStore(fdbRecordContext, subBranchCatalogIdent.getProjectId());
            FDBStoredRecord<Message> storedRecord = CatalogStore.loadRecord(buildBranchKey(subBranchCatalogIdent.getCatalogId()));
            if (storedRecord == null) {
                return Collections.emptyList();
            }
            Catalog catalog = Catalog.newBuilder().mergeFrom(storedRecord.getRecord()).build();
            catalogRecordList.add(new CatalogObject(subBranchCatalogIdent.getProjectId(), catalog.getCatalogId(), catalog.getName(),
                catalog.getRootCatalogId(), catalog.getCatalogInfo()));
        }

        return catalogRecordList;
    }

}
