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

import io.polycat.catalog.common.model.ColumnStatisticsAggrObject;
import io.polycat.catalog.common.model.ColumnStatisticsObject;
import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.TableName;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.model.DataPartitionSetObject;
import io.polycat.catalog.common.model.IndexPartitionSetObject;
import io.polycat.catalog.common.model.PartitionObject;
import io.polycat.catalog.common.model.TableHistoryObject;
import io.polycat.catalog.common.model.TableIdent;
import io.polycat.catalog.common.model.TableIndexInfoObject;
import io.polycat.catalog.common.model.TableIndexesHistoryObject;
import io.polycat.catalog.common.model.TableIndexesObject;
import io.polycat.catalog.common.model.TablePartitionSetType;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.plugin.request.input.PartitionFilterInput;
import io.polycat.catalog.common.utils.CodecUtil;
import io.polycat.catalog.common.utils.UuidUtil;
import io.polycat.catalog.store.api.TableDataStore;
import io.polycat.catalog.store.common.StoreMetadata;
import io.polycat.catalog.store.fdb.record.DirectoryStoreHelper;
import io.polycat.catalog.store.fdb.record.RecordStoreHelper;
import io.polycat.catalog.store.fdb.record.TransactionContextUtil;
import io.polycat.catalog.store.protos.DataPartitionSet;
import io.polycat.catalog.store.protos.IndexPartitionSet;
import io.polycat.catalog.store.protos.TableHistory;
import io.polycat.catalog.store.protos.TableIndexes;
import io.polycat.catalog.store.protos.TableIndexesHistory;

import com.apple.foundationdb.record.IsolationLevel;
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

import io.polycat.catalog.store.common.TableStoreConvertor;
import io.polycat.catalog.store.protos.common.Partition;
import io.polycat.catalog.store.protos.common.TableIndexInfo;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import static java.util.stream.Collectors.toList;

@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "fdb")
public class TableDataStoreImpl implements TableDataStore {

    private static final Logger logger = Logger.getLogger(TableDataStoreImpl.class);

    // a maximum of 2048 data partition set records can be stored in a index partition set.
    // the value is not final because the test case needs to modify it.
    private static int indexStoredMaxNumber = 2048;

    // 97280 = 95 x 1024, a data partition set can store up to 97280 bytes.
    // the value is not final because the test case needs to modify it.
    private static int dataStoredMaxSize = 97280;

    private static int dealMaxNumRecordPerTrans = 2048;

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

    /**
     * table history subspace
     */

    private Optional<TableHistoryObject> getCurBranchTableHistory(TransactionContext context, TableIdent tableIdent,
        Versionstamp versionstamp) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore recordStore = DirectoryStoreHelper.getTableHistoryStore(fdbRecordContext, tableIdent);
        Optional<FDBQueriedRecord<Message>> record = queryTableStoreRecord(recordStore,
            StoreMetadata.TABLE_HISTORY, versionstamp);
        if (!record.isPresent()) {
            return Optional.empty();
        }

        Versionstamp version = record.get().getVersion().toVersionstamp();
        TableHistory tableHistory = TableHistory.newBuilder()
            .mergeFrom(record.get().getRecord()).setVersion(ByteString.copyFrom(version.getBytes())).build();
        return Optional.of(new TableHistoryObject(tableHistory.getTableDataInfo(), tableHistory.getEventId(),
            CodecUtil.byteString2Hex(tableHistory.getVersion())));
    }

    @Override
    public void createTableDataPartitionSetSubspace(TransactionContext context, TableIdent tableIdent) {
    }

    @Override
    public void createTableIndexPartitionSetSubspace(TransactionContext context, TableIdent tableIdent) {

    }

    @Override
    public void dropTableDataPartitionSetSubspace(TransactionContext context, TableIdent tableIdent) {

    }

    @Override
    public void dropTableIndexPartitionSetSubspace(TransactionContext context, TableIdent tableIdent) {

    }

    @Override
    public void createTableHistorySubspace(TransactionContext context, String projectId)
        throws MetaStoreException {

    }

    @Override
    public void createTableDataPartitionSet(TransactionContext context, String projectId) {

    }

    @Override
    public void dropTableHistorySubspace(TransactionContext context, TableIdent tableIdent) throws MetaStoreException {

    }

    @Override
    public TableHistoryObject getLatestTableHistoryOrElseThrow(TransactionContext context, TableIdent tableIdent,
        String basedVersion) throws MetaStoreException {
        Optional<TableHistoryObject> tableHistory = getLatestTableHistory(context, tableIdent, basedVersion);

        if (!tableHistory.isPresent()) {
            throw new MetaStoreException(ErrorCode.TABLE_DATA_HISTORY_NOT_FOUND, tableIdent.getTableId());
        }
        return tableHistory.get();
    }


    @Override
    public void insertTableHistory(TransactionContext context, TableIdent tableIdent,
        String version, TableHistoryObject tableHistoryObject) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore tableHistoryStore = DirectoryStoreHelper.getTableHistoryStore(fdbRecordContext, tableIdent);

        TableHistory tableHistory = TableHistory.newBuilder()
            .setEventId(UuidUtil.generateId())
            .setTableDataInfo(TableStoreConvertor.getTableDataInfo(tableHistoryObject))
            .build();
        tableHistoryStore.insertRecord(tableHistory);
    }

    @Override
    public byte[] deleteTableHistory(TransactionContext context, TableIdent tableIdent, String startVersion,
        String endVersion, byte[] continuation) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore store = DirectoryStoreHelper.getTableHistoryStore(fdbRecordContext, tableIdent);
        FDBRecordVersion start = FDBRecordVersion
            .fromVersionstamp(Versionstamp.fromBytes(CodecUtil.hex2Bytes(startVersion)));
        FDBRecordVersion end = FDBRecordVersion
            .fromVersionstamp(Versionstamp.fromBytes(CodecUtil.hex2Bytes(endVersion)));
        QueryComponent filter = Query.and(Query.version().lessThan(end), Query.version().greaterThanOrEquals(start));
        RecordQueryPlan plan = RecordStoreHelper
            .buildVersionRecordQueryPlan(store, StoreMetadata.TABLE_HISTORY, filter, true);
        return RecordStoreHelper
            .deleteStoreRecord(store, plan, continuation, dealMaxNumRecordPerTrans, IsolationLevel.SNAPSHOT);
    }

    @Override
    public Optional<TableHistoryObject> getLatestTableHistory(TransactionContext context, TableIdent tableIdent,
        String basedVersion) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore recordStore = DirectoryStoreHelper.getTableHistoryStore(fdbRecordContext, tableIdent);

        Optional<FDBQueriedRecord<Message>> record = queryLatestTableStoreRecord(recordStore,
            StoreMetadata.TABLE_HISTORY, basedVersion);
        if (!record.isPresent()) {
            return Optional.empty();
        }

        Versionstamp version = record.get().getVersion().toVersionstamp();
        TableHistory tableHistory = TableHistory.newBuilder()
            .mergeFrom(record.get().getRecord()).setVersion(ByteString.copyFrom(version.getBytes())).build();
        return Optional.of(new TableHistoryObject(tableHistory.getTableDataInfo(), tableHistory.getEventId(),
            CodecUtil.byteString2Hex(tableHistory.getVersion())));
    }

    @Override
    public Optional<TableHistoryObject> getTableHistory(TransactionContext context, TableIdent tableIdent,
        String version) {
        Optional<TableHistoryObject> tableHistory = getCurBranchTableHistory(context, tableIdent,
            Versionstamp.fromBytes(CodecUtil.hex2Bytes(version)));
        return tableHistory;
    }

    @Override
    public void createTableIndexHistorySubspace(TransactionContext context, String projectId)
            throws MetaStoreException {

    }

    @Override
    public void dropTableIndexHistorySubspace(TransactionContext context, TableIdent tableIdent) throws MetaStoreException {

    }

    @Override
    public void createTableIndexSubspace(TransactionContext context, String projectId)
            throws MetaStoreException {

    }

    @Override
    public void dropTableIndexSubspace(TransactionContext context, TableIdent tableIdent) throws MetaStoreException {

    }

    @Override
    public Optional<TableIndexesHistoryObject> getLatestTableIndexes(TransactionContext context,
        TableIdent tableIdent, String basedVersion) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore recordStore =
            DirectoryStoreHelper.getTableIndexesHistoryStore(fdbRecordContext, tableIdent);
        Optional<FDBQueriedRecord<Message>> record =
            queryLatestTableStoreRecord(recordStore, StoreMetadata.TABLE_INDEXES_HISTORY,
                basedVersion);
        if (!record.isPresent()) {
            return Optional.empty();
        }

        Versionstamp version = record.get().getVersion().toVersionstamp();
        TableIndexesHistory tableIndexesHistory =
            TableIndexesHistory.newBuilder().mergeFrom(record.get().getRecord())
                .setVersion(ByteString.copyFrom(version.getBytes())).build();
        return Optional.of(trans2TableIndexesHistoryObject(tableIndexesHistory));
    }

    @Override
    public TableIndexesObject getTableIndexes(TransactionContext context, TableIdent tableIdent,
        String tableName) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore tableIndexesStore =
            DirectoryStoreHelper.getTableIndexesStore(fdbRecordContext, tableIdent);
        FDBStoredRecord<Message> storedRecord =
            tableIndexesStore.loadRecord(tableIndexes());
        if (storedRecord == null) {
            return null;
        }
        TableIndexes tableIndexes =
            TableIndexes.newBuilder().mergeFrom(storedRecord.getRecord()).build();
        return new TableIndexesObject(
            tableIndexes.getTableIndexList().stream().map(tableIndexInfo -> new TableIndexInfoObject(tableIndexInfo))
                .collect(toList()));
    }

    @Override
    public TableIndexesObject insertTableIndexes(TransactionContext context, TableIdent tableIdent,
        List<TableIndexInfoObject> tableIndexInfoObjectList) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        List<TableIndexInfo> tableIndexInfoList =
            tableIndexInfoObjectList.stream()
                .map(tableIndexInfoObject -> TableStoreConvertor.getTableIndexInfo(tableIndexInfoObject))
                .collect(toList());
        // get table index subspace
        FDBRecordStore tableIndexesStore =
            DirectoryStoreHelper.getTableIndexesStore(fdbRecordContext, tableIdent);
        // create new tableIndexes
        TableIndexes tableIndexes =
            TableIndexes.newBuilder().setPrimaryKey(0).addAllTableIndex(tableIndexInfoList).build();

        tableIndexesStore.saveRecord(tableIndexes);
        List<TableIndexInfoObject> tableIndexInfoObjects =
            tableIndexes.getTableIndexList().stream().map(tableIndexInfo -> new TableIndexInfoObject(tableIndexInfo))
                .collect(toList());
        return new TableIndexesObject(tableIndexInfoObjects);
    }

    @Override
    public void insertTableIndexesHistory(TransactionContext context, TableIdent tableIdent, String version,
        TableIndexesObject tableIndexes) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        // get table index history subspace
        FDBRecordStore tableIndexesHistoryStore =
            DirectoryStoreHelper.getTableIndexesHistoryStore(fdbRecordContext, tableIdent);
        List<TableIndexInfo> tableIndexInfoList =
            tableIndexes.getTableIndexInfoObjectList().stream()
                .map(tableIndexInfoObject -> TableStoreConvertor.getTableIndexInfo(tableIndexInfoObject))
                .collect(toList());
        TableIndexesHistory tableIndexesHistory =
            TableIndexesHistory.newBuilder().setEventId(UuidUtil.generateId())
                .addAllTableIndex(tableIndexInfoList).build();

        tableIndexesHistoryStore.insertRecord(tableIndexesHistory);
    }

    @Override
    public List<PartitionObject> getPartitionsByPartitionNames(TransactionContext context, TableIdent tableIdent,
        List<String> setIds, String curSetId, List<String> partitionNames, int maxParts) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getPartitionsByPartitionName");
    }

    @Override
    public Integer getTablePartitionCountByFilter(TransactionContext context, TableIdent tableIdent, String filter) {
        return null;
    }

    @Override
    public Integer getTablePartitionCountByKeyValues(TransactionContext context, TableIdent tableIdent, List<String> partitionKeys, List<String> values) {
        return null;
    }

    @Override
    public String getLatestPartitionName(TransactionContext context, TableIdent tableIdent) {
        return null;
    }

    @Override
    public List<PartitionObject> getPartitionsByPartitionNamesWithColumnInfo(TransactionContext context,
        TableIdent tableIdent, List<String> setIds, String curSetId, List<String> partitionNames, int maxParts) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getPartitionsByPartitionNamesWithColumnInfo");
    }

    @Override
    public List<PartitionObject> getPartitionsByFilter(TransactionContext context, TableIdent tableIdent,
        String curSetId, String filter, int maxParts) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getPartitionsByFilter");
    }

    @Override
    public List<PartitionObject> getPartitionsByKeyValues(TransactionContext context, TableIdent tableIdent,
        String curSetId, List<String> partitionKeys, List<String> values, int maxParts) {
        return null;
    }

    /**
     * table partition
     */

    @Override
    public List<PartitionObject> getAllPartitionsFromDataNode(TransactionContext context, TableIdent tableIdent,
        List<String> setIds, String curSetId) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore dataPartitionSetStore = DirectoryStoreHelper.getDataPartitionSetStore(fdbRecordContext,
            tableIdent);
        List<Partition> partitionList = new ArrayList<>();
        for (String setId : setIds) {
            FDBStoredRecord<Message> record = dataPartitionSetStore.loadRecord(buildDataPartitionSetKey(setId));
            DataPartitionSet dataPartitionSet = DataPartitionSet.newBuilder().mergeFrom(record.getRecord()).build();
            partitionList.addAll(dataPartitionSet.getDataPartitionSetInfo().getDataPartitionsList());
        }
        FDBStoredRecord<Message> record = dataPartitionSetStore.loadRecord(buildDataPartitionSetKey(curSetId));
        if (record == null) {
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
        DataPartitionSet dataPartitionSet = DataPartitionSet.newBuilder().mergeFrom(record.getRecord()).build();
        partitionList.addAll(dataPartitionSet.getDataPartitionSetInfo().getDataPartitionsList());
        return partitionList.stream().map(partition -> new PartitionObject(partition)).collect(toList());
    }

    @Override
    public List<PartitionObject> listTablePartitions(TransactionContext context, TableIdent tableIdent,
        List<String> setIds, String curSetId, Integer maxParts) {
        return null;
    }

    @Override
    public List<String> listTablePartitionNames(TransactionContext context, TableIdent tableIdent, PartitionFilterInput filterInput, List<String> partitionKeys) {
        return null;
    }

    @Override
    public List<PartitionObject> getAllPartitionsFromTableHistory(TransactionContext context, TableIdent tableIdent,
        TableHistoryObject latestTableHistory) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore indexPartitionSetStore = DirectoryStoreHelper.getIndexPartitionSetStore(fdbRecordContext,
            tableIdent);
        List<PartitionObject> partitionList = new ArrayList<>();
        if (latestTableHistory.getPartitionSetType() == TablePartitionSetType.DATA) {
            List<PartitionObject> partitionList1 = getAllPartitionsFromDataNode(context, tableIdent,
                latestTableHistory.getSetIds(),
                latestTableHistory.getCurSetId());
            partitionList.addAll(partitionList1);
        } else if (latestTableHistory.getPartitionSetType() == TablePartitionSetType.INDEX) {
            for (String setId : latestTableHistory.getSetIds()) {
                FDBStoredRecord<Message> indexRecord = indexPartitionSetStore
                    .loadRecord(buildIndexPartitionSetKey(setId));
                if (indexRecord == null) {
                    throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
                }
                IndexPartitionSet indexPartitionSet = IndexPartitionSet.newBuilder()
                    .mergeFrom(indexRecord.getRecord()).build();
                List<PartitionObject> partitionList1 = getAllPartitionsFromDataNode(context, tableIdent,
                    indexPartitionSet.getIndexPartitionSetInfo().getSetIdsList(),
                    indexPartitionSet.getIndexPartitionSetInfo().getCurSetId());
                partitionList.addAll(partitionList1);
            }
            FDBStoredRecord<Message> indexRecord = indexPartitionSetStore
                .loadRecord(buildIndexPartitionSetKey(latestTableHistory.getCurSetId()));
            if (indexRecord == null) {
                throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
            }
            IndexPartitionSet indexPartitionSet = IndexPartitionSet.newBuilder().mergeFrom(indexRecord.getRecord())
                .build();
            List<PartitionObject> partitionList1 = getAllPartitionsFromDataNode(context, tableIdent,
                indexPartitionSet.getIndexPartitionSetInfo().getSetIdsList(),
                indexPartitionSet.getIndexPartitionSetInfo().getCurSetId());
            partitionList.addAll(partitionList1);
        }
        return partitionList;
    }

    @Override
    public byte[] deleteDataPartition(TransactionContext context, TableIdent tableIdent, byte[] continuation) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore store = DirectoryStoreHelper.getDataPartitionSetStore(fdbRecordContext, tableIdent);
        QueryComponent filter = Query.and(Query.field("catalog_id").equalsValue(tableIdent.getCatalogId()),
            Query.field("database_id").equalsValue(tableIdent.getDatabaseId()),
            Query.field("table_id").equalsValue(tableIdent.getTableId()));
        RecordQueryPlan plan = RecordStoreHelper.buildRecordQueryPlan(store, StoreMetadata.DATA_PARTITION_SET, filter);
        return RecordStoreHelper
            .deleteStoreRecord(store, plan, continuation, dealMaxNumRecordPerTrans, IsolationLevel.SNAPSHOT);
    }

    @Override
    public byte[] deleteIndexPartition(TransactionContext context, TableIdent tableIdent, byte[] continuation) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore store = DirectoryStoreHelper.getDataPartitionSetStore(fdbRecordContext, tableIdent);
        QueryComponent filter = Query.and(Query.field("catalog_id").equalsValue(tableIdent.getCatalogId()),
            Query.field("database_id").equalsValue(tableIdent.getDatabaseId()),
            Query.field("table_id").equalsValue(tableIdent.getTableId()));
        RecordQueryPlan plan = RecordStoreHelper.buildRecordQueryPlan(store, StoreMetadata.INDEX_PARTITION_SET, filter);
        return RecordStoreHelper
            .deleteStoreRecord(store, plan, continuation, dealMaxNumRecordPerTrans, IsolationLevel.SNAPSHOT);
    }

    @Override
    public void deletePartitionInfoByNames(TransactionContext context, TableIdent tableIdent,
        String setId, List<String> partitionNames) {

    }

    @Override
    public void insertDataPartitionSet(TransactionContext context, TableIdent tableIdent,
        DataPartitionSetObject dataPartitionSetObject) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore dataPartitionSetStore = DirectoryStoreHelper.getDataPartitionSetStore(fdbRecordContext,
            tableIdent);
        dataPartitionSetStore.insertRecord(TableStoreConvertor.getDataPartitionSet(dataPartitionSetObject));
    }

    @Override
    public void createTablePartitionInfo(TransactionContext context,  TableIdent tableIdent) {

    }

    @Override
    public void dropTablePartitionInfo(TransactionContext context, TableIdent tableIdent) {

    }

    @Override
    public void insertPartitionInfo(TransactionContext context, TableIdent tableIdent,
        DataPartitionSetObject dataPartitionSetObject) {

    }

    @Override
    public DataPartitionSetObject getDataPartitionSet(TransactionContext context, TableIdent tableIdent, String setId) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore dataPartitionSetStore = DirectoryStoreHelper.getDataPartitionSetStore(fdbRecordContext,
            tableIdent);
        FDBStoredRecord<Message> record = dataPartitionSetStore
            .loadRecord(buildDataPartitionSetKey(setId));
        if (record == null) {
            return null;
        }

        DataPartitionSet dataPartitionSet = DataPartitionSet.newBuilder().mergeFrom(record.getRecord()).build();
        return new DataPartitionSetObject(dataPartitionSet.getSetId(), dataPartitionSet.getCatalogId(),
            dataPartitionSet.getDatabaseId(), dataPartitionSet.getTableId(), dataPartitionSet.getDataPartitionSetInfo());
    }

    @Override
    public void updateDataPartitionSet(TransactionContext context, TableIdent tableIdent,
        DataPartitionSetObject dataPartitionSetObject) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore dataPartitionSetStore = DirectoryStoreHelper.getDataPartitionSetStore(fdbRecordContext,
            tableIdent);
        dataPartitionSetStore.updateRecord(TableStoreConvertor.getDataPartitionSet(dataPartitionSetObject));
    }


    @Override
    public void insertIndexPartitionSet(TransactionContext context, TableIdent tableIdent,
        IndexPartitionSetObject indexPartitionSetObject) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore indexPartitionSetStore = DirectoryStoreHelper.getIndexPartitionSetStore(fdbRecordContext,
            tableIdent);
        indexPartitionSetStore.insertRecord(TableStoreConvertor.getIndexPartitionSet(indexPartitionSetObject));
    }

    @Override
    public IndexPartitionSetObject getIndexPartitionSet(TransactionContext context, TableIdent tableIdent,
        String setId) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore indexPartitionSetStore = DirectoryStoreHelper.getIndexPartitionSetStore(fdbRecordContext,
            tableIdent);
        FDBStoredRecord<Message> record = indexPartitionSetStore
            .loadRecord(buildDataPartitionSetKey(setId));
        if (record == null) {
            return null;
        }
        IndexPartitionSet indexPartitionSet = IndexPartitionSet.newBuilder().mergeFrom(record.getRecord()).build();
        return new IndexPartitionSetObject(indexPartitionSet.getSetId(), indexPartitionSet.getCatalogId(),
                indexPartitionSet.getDatabaseId(), indexPartitionSet.getTableId(), indexPartitionSet.getIndexPartitionSetInfo());
    }

    @Override
    public void updateIndexPartitionSet(TransactionContext context, TableIdent tableIdent,
        IndexPartitionSetObject indexPartitionSetObject) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore indexPartitionSetStore = DirectoryStoreHelper.getIndexPartitionSetStore(fdbRecordContext,
            tableIdent);
        indexPartitionSetStore.updateRecord(TableStoreConvertor.getIndexPartitionSet(indexPartitionSetObject));
    }


    @Override
    public int getParitionSerializedSize(PartitionObject partitionObject) {
        Partition partition = TableStoreConvertor.getPartition(partitionObject);
        return partition.getSerializedSize();
    }

    @Override
    public boolean doesPartitionExists(TransactionContext context, TableIdent tableIdent, String partitionName) {
        return false;
    }

    @Override
    public void createColumnStatisticsSubspace(TransactionContext context, String projectId) {

    }

    @Override
    public List<ColumnStatisticsObject> getTableColumnStatistics(TransactionContext context, String projectId,
            TableName tableName, List<String> colNames) {
        return null;
    }

    @Override
    public void updateTableColumnStatistics(TransactionContext context, String projectId,
            List<ColumnStatisticsObject> columnStatisticsObjects) {

    }

    @Override
    public void deleteTableColumnStatistics(TransactionContext context, TableName tableName, String colName) {

    }

    @Override
    public List<ColumnStatisticsObject> getPartitionColumnStatistics(TransactionContext context,
            TableName tableName, List<String> partNames, List<String> colNames) {
        return null;
    }

    @Override
    public void updatePartitionColumnStatistics(TransactionContext context, TableName tableName,
            List<ColumnStatisticsObject> columnStatisticsObjects) {

    }

    @Override
    public void deletePartitionColumnStatistics(TransactionContext context, TableName tableName,
            String partName, String columnName) {

    }

    @Override
    public long getFoundPartNums(TransactionContext context, TableName tableName, List<String> partNames,
            List<String> colNames) {
        return 0;
    }

    @Override
    public List<ColumnStatisticsAggrObject> getAggrColStatsFor(TransactionContext context, TableName tableName,
            List<String> partNames, List<String> colNames) {
        return null;
    }

    private TableIndexesHistoryObject trans2TableIndexesHistoryObject(TableIndexesHistory tableIndexesHistory) {
        List<TableIndexInfoObject> indexInfoObjects =
            tableIndexesHistory.getTableIndexList().stream().map(tableIndexInfo -> new TableIndexInfoObject(tableIndexInfo))
                .collect(toList());
        return new TableIndexesHistoryObject(tableIndexesHistory.getEventId(), indexInfoObjects,
           CodecUtil.byteString2Hex(tableIndexesHistory.getVersion()));
    }


    private Tuple buildDataPartitionSetKey(String setId) {
        return Tuple.from(setId);
    }

    private Tuple buildIndexPartitionSetKey(String setId) {
        return Tuple.from(setId);
    }

    public Tuple tableIndexes() {
        return Tuple.from(0);
    }


}
