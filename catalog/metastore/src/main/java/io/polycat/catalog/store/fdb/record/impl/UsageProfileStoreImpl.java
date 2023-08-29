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

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.*;
import io.polycat.catalog.store.api.UsageProfileStore;
import io.polycat.catalog.store.common.StoreMetadata;
import io.polycat.catalog.store.fdb.record.DirectoryStoreHelper;
import io.polycat.catalog.store.fdb.record.TransactionContextUtil;
import io.polycat.catalog.store.protos.UsageProfileRecord;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import java.util.*;

@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "fdb")
public class UsageProfileStoreImpl implements UsageProfileStore {

    private final long timeLimit = 4000;

    private final int maxBatchRowNum = 1024;

    public static UsageProfileStoreImpl getInstance() {
        return UsageProfileStoreImpl.UsageProfileStoreImplHandler.INSTANCE;
    }

    private Tuple buildUsageProfilePrimaryKey(String catalogId, String databaseId, String tableId, long startTime,
                                              String opType) {
        return Tuple.from(catalogId, databaseId, tableId, startTime, opType);
    }

    @Override
    public void createUsageProfileSubspace(TransactionContext context, String projectId) {

    }

    @Override
    public void dropUsageProfileSubspace(TransactionContext context, String projectId) {

    }

    @Override
    public void updateUsageProfile(TransactionContext context, String projectId, UsageProfileObject usageProfileObject)
            throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore usageProfileStore = DirectoryStoreHelper.getUsageProfileStore(fdbRecordContext, projectId);
        UsageProfileRecord usageProfileRecord = UsageProfileRecord.newBuilder()
                .setCatalogName(usageProfileObject.getCatalogName())
                .setDatabaseName(usageProfileObject.getDatabaseName())
                .setTableId(usageProfileObject.getTableId())
                .setCreateTime(usageProfileObject.getCreateTime())
                .setCreateDay(usageProfileObject.getCreateDayTime())
                .setOpType(usageProfileObject.getOpType())
                .setCount(usageProfileObject.getCount())
                .setTaskId(usageProfileObject.getTaskId())
                .setUserId(usageProfileObject.getUserId())
                .build();
        usageProfileStore.updateRecord(usageProfileRecord);
    }

    @Override
    public Optional<UsageProfileObject> getUsageProfile(TransactionContext context, String projectId, String catalogId,
                                                        String databaseId, String tableId, long startTime, String opType) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore usageProfileStore = DirectoryStoreHelper.getUsageProfileStore(fdbRecordContext, projectId);
        FDBStoredRecord<Message> storedRecord = usageProfileStore
                .loadRecord(buildUsageProfilePrimaryKey(catalogId, databaseId, tableId, startTime, opType));
        if (storedRecord == null) {
            return Optional.empty();
        }
        UsageProfileRecord.Builder builder = UsageProfileRecord.newBuilder().mergeFrom(storedRecord.getRecord());
        return Optional.of(new UsageProfileObject(projectId, catalogId, databaseId, tableId, startTime, opType,
                builder.getCount()));
    }

    @Override
    public void deleteUsageProfile(TransactionContext context, String projectId, long startTime, long endTime)
            throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore usageProfileStore = DirectoryStoreHelper.getUsageProfileStore(fdbRecordContext, projectId);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType(StoreMetadata.USAGE_PROFILE_RECORD.getRecordTypeName())
                .setFilter(Query.and(Query.field("start_time").greaterThanOrEquals(startTime),
                        Query.field("start_time").lessThanOrEquals(endTime)))
                .build();
        RecordCursorIterator<FDBQueriedRecord<Message>> cursor = usageProfileStore.executeQuery(query)
                .asIterator();
        while (cursor.hasNext()) {
            UsageProfileRecord.Builder builder = UsageProfileRecord.newBuilder()
                    .mergeFrom(Objects.requireNonNull(cursor.next()).getRecord());
            Tuple primaryKey = buildUsageProfilePrimaryKey(builder.getCatalogName(), builder.getDatabaseName(),
                    builder.getTableId(), builder.getCreateTime(), builder.getOpType());
            usageProfileStore.deleteRecord(primaryKey);
        }
    }

    @Override
    public ScanRecordCursorResult<List<UsageProfileObject>> listUsageProfile(TransactionContext context, String projectId, String tableId, String catalogName, String databaseName, List<String> opTypes, long startTime, long endTime, byte[] continuation) throws MetaStoreException {
        List<QueryComponent> filters = new ArrayList<>();
        filters.add(Query.field("catalog_name").equalsValue(catalogName));
        if (databaseName != null) {
            filters.add(Query.field("database_name").equalsValue(databaseName));
        }
        if (tableId != null) {
            filters.add(Query.field("table_id").equalsValue(tableId));
        }
        if (CollectionUtils.isNotEmpty(opTypes)) {
            filters.add(Query.field("op_type").in(opTypes));
        }
        if (startTime != 0) {
            filters.add(Query.field("create_time").greaterThanOrEquals(startTime));
        }
        if (endTime != 0) {
            filters.add(Query.field("create_time").lessThanOrEquals(endTime));
        }
        QueryComponent filter = Query.and(filters);
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore usageProfileStore = DirectoryStoreHelper.getUsageProfileStore(fdbRecordContext, projectId);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType(StoreMetadata.USAGE_PROFILE_RECORD.getRecordTypeName())
                .setFilter(filter)
                .build();
        RecordCursor<UsageProfileObject> cursor = usageProfileStore.executeQuery(query,
                        continuation,
                        ExecuteProperties
                                .newBuilder()
                                .setTimeLimit(timeLimit)
                                .setReturnedRowLimit(maxBatchRowNum)
                                .setIsolationLevel(IsolationLevel.SNAPSHOT)
                                .build())
                .map(rec -> UsageProfileRecord.newBuilder().mergeFrom(rec.getRecord()))
                .map(builder -> new UsageProfileObject(projectId, builder.getCatalogName(), builder.getDatabaseName(),
                        builder.getTableId(), builder.getCreateTime(), builder.getOpType(), builder.getCount()));
        List<UsageProfileObject> usageProfileObjectList = cursor.asList().join();

        return new ScanRecordCursorResult<>(usageProfileObjectList, cursor.getNext().getContinuation().toBytes());
    }

    @Override
    public void recordUsageProfile(TransactionContext context, UsageProfileObject usageProfileObject) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore usageProfileStore = DirectoryStoreHelper.getUsageProfileStore(fdbRecordContext, usageProfileObject.getProjectId());
        UsageProfileRecord usageProfileRecord = UsageProfileRecord.newBuilder()
                .setCatalogName(usageProfileObject.getCatalogName())
                .setDatabaseName(usageProfileObject.getDatabaseName())
                .setTableId(usageProfileObject.getTableId())
                .setCreateTime(usageProfileObject.getCreateTime())
                .setCreateDay(usageProfileObject.getCreateDayTime())
                .setOpType(usageProfileObject.getOpType())
                .setCount(usageProfileObject.getCount())
                .setTaskId(usageProfileObject.getTaskId())
                .setUserId(usageProfileObject.getUserId())
                .build();
        usageProfileStore.insertRecord(usageProfileRecord);
    }

    @Override
    public ScanRecordCursorResult<List<UsageProfileObject>> listUsageProfilePreStatByFilter(TransactionContext context, String projectId, String catalogName, String databaseName, String tableName, List<String> opTypes, long startTime, long endTime, int maxBatchRowNum, byte[] continuation) {
        List<QueryComponent> filters = new ArrayList<>();
        filters.add(Query.field("catalog_name").equalsValue(catalogName));
        if (databaseName != null) {
            filters.add(Query.field("database_name").equalsValue(databaseName));
        }
        if (tableName != null) {
            filters.add(Query.field("table_name").equalsValue(tableName));
        }
        if (CollectionUtils.isNotEmpty(opTypes)) {
            filters.add(Query.field("op_type").in(opTypes));
        }
        if (startTime != 0) {
            filters.add(Query.field("create_time").greaterThanOrEquals(startTime));
        }
        if (endTime != 0) {
            filters.add(Query.field("create_time").lessThanOrEquals(endTime));
        }
        QueryComponent filter = Query.and(filters);
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore usageProfileStore = DirectoryStoreHelper.getUsageProfileStore(fdbRecordContext, projectId);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType(StoreMetadata.USAGE_PROFILE_RECORD.getRecordTypeName())
                .setFilter(filter)
                .build();
        RecordCursor<UsageProfileObject> cursor = usageProfileStore.executeQuery(query,
                        continuation,
                        ExecuteProperties
                                .newBuilder()
                                .setTimeLimit(timeLimit)
                                .setReturnedRowLimit(maxBatchRowNum)
                                .setIsolationLevel(IsolationLevel.SNAPSHOT)
                                .build())
                .map(rec -> UsageProfileRecord.newBuilder().mergeFrom(rec.getRecord()))
                .map(builder -> new UsageProfileObject(projectId, builder.getCatalogName(), builder.getDatabaseName(),
                        builder.getTableId(), builder.getCreateTime(), builder.getOpType(), builder.getCount()));
        List<UsageProfileObject> usageProfileObjectList = cursor.asList().join();

        return new ScanRecordCursorResult<>(usageProfileObjectList, cursor.getNext().getContinuation().toBytes());
    }

    @Override
    public List<UsageProfileAccessStatObject> getUsageProfileAccessStatList(String projectId, UsageProfileAccessStatObject accessStatObject, Set<String> opTypesSet, boolean sortAsc) {
        return null;
    }

    @Override
    public List<String> getTableAccessUsers(TransactionContext context, String projectId, String catalogName, String databaseName, String tableName) {
        return null;
    }

    @Override
    public List<UsageProfileObject> getUsageProfileDetailsByCondition(TransactionContext context, String projectId, UsageProfileObject upo, long startTime, long endTime, int rowCount) {
        return null;
    }

    private static class UsageProfileStoreImplHandler {
        private static final UsageProfileStoreImpl INSTANCE = new UsageProfileStoreImpl();
    }

}
