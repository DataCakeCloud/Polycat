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
import java.util.Objects;

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.store.api.DataLineageStore;
import io.polycat.catalog.common.lineage.ELineageDirection;
import io.polycat.catalog.common.lineage.ELineageType;
import io.polycat.catalog.common.model.*;
import io.polycat.catalog.store.common.StoreMetadata;
import io.polycat.catalog.store.fdb.record.DirectoryStoreHelper;
import io.polycat.catalog.store.fdb.record.TransactionContextUtil;
import io.polycat.catalog.store.protos.DataLineageRecord;

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.google.protobuf.Message;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "fdb")
public class DataLineageStoreImpl implements DataLineageStore {

    private final long timeLimit = 40000;
    private final int maxBatchRowNum = 1024;
    @Override
    public void createSubspace(TransactionContext context, String projectId) {

    }

    private static class DataLineageStoreImplHandler {
        private static final DataLineageStoreImpl INSTANCE = new DataLineageStoreImpl();
    }

    public static DataLineageStoreImpl getInstance() {
        return DataLineageStoreImpl.DataLineageStoreImplHandler.INSTANCE;
    }

    @Override
    public void upsertDataLineage(TransactionContext context, DataLineageObject dataLineageObject) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore dataLineageStore = DirectoryStoreHelper.getDataLineageStore(fdbRecordContext, dataLineageObject.getProjectId());
        DataLineageRecord dataLineageRecord = DataLineageRecord.newBuilder()
            .setCatalogId(dataLineageObject.getCatalogId())
            .setDatabaseId(dataLineageObject.getDatabaseId())
            .setTableId(dataLineageObject.getTableId())
            .setDataSourceType(dataLineageObject.getDataSourceType().name())
            .setDataSourceContent(dataLineageObject.getDataSourceContent())
            .setOperation(dataLineageObject.getOperation())
            .build();
        dataLineageStore.saveRecord(dataLineageRecord);
    }

    @Override
    public List<DataLineageObject> listDataLineageByTableId(TransactionContext ctx, String projectId, String catalogId,
        String databaseId, String tableId) throws MetaStoreException {
        RecordQuery query = RecordQuery.newBuilder()
            .setRecordType(StoreMetadata.DATA_LINEAGE_RECORD.getRecordTypeName())
            .setFilter(Query.and(Query.field("catalog_id").equalsValue(catalogId),
                Query.field("database_id").equalsValue(databaseId),
                Query.field("table_id").equalsValue(tableId)))
            .build();

        return listDataLineageRecords(ctx, projectId, query);
    }

    @Override
    public List<DataLineageObject> listDataLineageByDataSource(TransactionContext ctx, String projectId,
        DataSourceType dataSourceType, String dataSourceContent) throws MetaStoreException {
        RecordQuery query = RecordQuery.newBuilder()
            .setRecordType(StoreMetadata.DATA_LINEAGE_RECORD.getRecordTypeName())
            .setFilter(Query.and(Query.field("data_source_type").equalsValue(dataSourceType.name()),
                Query.field("data_source_content").equalsValue(dataSourceContent)))
            .setAllowedIndex(StoreMetadata.DATA_LINEAGE_RECORD.getName() + "-secondary-index")
            .build();
        return listDataLineageRecords(ctx, projectId, query);
    }

    @Override
    public List<Integer> upsertLineageVertexAndGet(TransactionContext context, String projectId, List<LineageVertex> vertex) {
        return null;
    }

    @Override
    public void insertLineageEdgeFact(TransactionContext context, String projectId, LineageEdgeFact edgeFact) {

    }

    @Override
    public void upsertLineageEdge(TransactionContext context, String projectId, List<LineageEdge> list) {

    }

    @Override
    public LineageEdgeFact getLineageEdgeFact(TransactionContext context, String projectId, String jobFactId) {
        return null;
    }

    @Override
    public LineageVertex getLineageVertex(TransactionContext context, String projectId, int dbType, int objectType, String qualifiedName) {
        return null;
    }

    @Override
    public List<LineageEdge> getLineageGraph(TransactionContext context, String projectId, Integer nodeId, int depth, ELineageDirection lineageDirection, ELineageType lineageType, Long startTime) {
        return null;
    }

    private List<DataLineageObject> listDataLineageRecords(TransactionContext context, String projectId, RecordQuery query) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore dataLineageStore = DirectoryStoreHelper.getDataLineageStore(fdbRecordContext, projectId);
        byte[] continuation = null;
        List<DataLineageObject> dataLineageObjects = new ArrayList<>();
        while (true) {
            RecordCursorIterator<FDBQueriedRecord<Message>> cursor = dataLineageStore.executeQuery(query,
                    continuation,
                    ExecuteProperties
                        .newBuilder().setTimeLimit(timeLimit)
                        .setReturnedRowLimit(maxBatchRowNum)
                        .setIsolationLevel(IsolationLevel.SNAPSHOT)
                        .build())
                .asIterator();
            while (cursor.hasNext()) {
                DataLineageRecord.Builder builder = DataLineageRecord.newBuilder()
                    .mergeFrom(Objects.requireNonNull(cursor.next()).getRecord());
                DataSourceType dataSourceType = DataSourceType.valueOf(builder.getDataSourceType());
                DataLineageObject dataLineageObject = new DataLineageObject(projectId, builder.getCatalogId(),
                    builder.getDatabaseId(), builder.getTableId(), dataSourceType, builder.getDataSourceContent(),
                    builder.getOperation());
                dataLineageObjects.add(dataLineageObject);
            }
            continuation = cursor.getContinuation();
            if (continuation == null) {
                break;
            }
        }
        return dataLineageObjects;
    }

}

