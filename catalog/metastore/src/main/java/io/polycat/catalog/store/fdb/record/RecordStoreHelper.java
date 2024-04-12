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
package io.polycat.catalog.store.fdb.record;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import java.util.Optional;
import java.util.function.Supplier;


import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.model.ScanRecordCursorResult;
import io.polycat.catalog.store.common.StoreMetadata;

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.expressions.VersionKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.protobuf.Message;


/**
 * rules: 1. rules of defining get record store Methods: should begin with "get" and end with "Store" (case sensitive),
 * i.e., getRecordStore, getTableObjectNameStore, ... Otherwise, record store could not be created in advance, further
 * leading to parallel access failure.
 */
public class RecordStoreHelper {

    public static final Logger logger = Logger.getLogger(RecordStoreHelper.class);

    public static long getCurrentTime() {
        return System.currentTimeMillis();
    }

    public static RecordQueryPlan buildVersionRecordQueryPlan(FDBRecordStore recordStore, StoreMetadata storeMetadata,
        QueryComponent filter, Boolean sortReverse) {
        RecordQuery query = RecordQuery.newBuilder()
            .setRecordType(storeMetadata.getRecordTypeName())
            .setFilter(filter)
            .setSort(VersionKeyExpression.VERSION, sortReverse)
            .build();

        RecordQueryPlanner planner = new RecordQueryPlanner(storeMetadata.getRecordMetaData(),
            recordStore.getRecordStoreState());
        return planner.plan(query);
    }

    public static RecordQueryPlan buildRecordQueryPlan(FDBRecordStore recordStore, StoreMetadata storeMetadata,
        QueryComponent filter) {
        RecordQuery query = RecordQuery.newBuilder()
            .setRecordType(storeMetadata.getRecordTypeName())
            .setFilter(filter)
            .build();

        RecordQueryPlanner planner = new RecordQueryPlanner(storeMetadata.getRecordMetaData(),
            recordStore.getRecordStoreState());
        return planner.plan(query);
    }

    public static List<FDBQueriedRecord<Message>> getRecords(FDBRecordStore recordStore, StoreMetadata recordMetaData,
        QueryComponent filter) {
        RecordQueryPlan plan = RecordStoreHelper
            .buildRecordQueryPlan(recordStore, recordMetaData, filter);

        RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, null,
            ExecuteProperties.newBuilder().build()).asIterator();

        List<FDBQueriedRecord<Message>> recordList = new LinkedList<>();
        while (cursor.hasNext()) {
            FDBQueriedRecord<Message> record = cursor.next();
            recordList.add(record);
        }

        return recordList;
    }

    public static List<FDBStoredRecord<Message>> listStoreRecords(TupleRange tupleRange, FDBRecordStore tableStore,
        int maxScannedRecords, byte[] continuation, IsolationLevel isolationLevel) {
        List<FDBStoredRecord<Message>> scannedRecords = new ArrayList<>();
        Supplier<ScanProperties> props = () -> new ScanProperties(ExecuteProperties.newBuilder()
            .setReturnedRowLimit(maxScannedRecords)
            .setIsolationLevel(isolationLevel)
            .build());

        RecordCursorIterator<FDBStoredRecord<Message>> messageCursor;

        do {
            messageCursor = tableStore.scanRecords(tupleRange, continuation, props.get()).asIterator();
            while (messageCursor.hasNext()) {
                FDBStoredRecord<Message> record = messageCursor.next();
                scannedRecords.add(record);
            }
            continuation = messageCursor.getContinuation();
        } while (null != continuation);

        return scannedRecords;
    }

    public static ScanRecordCursorResult<List<FDBStoredRecord<Message>>> listStoreRecordsWithToken(
        TupleRange tupleRange, FDBRecordStore tableStore, long timeLimit, int maxScannedRecords, byte[] continuation,
        IsolationLevel isolationLevel) {
        List<FDBStoredRecord<Message>> scannedRecords = new ArrayList<>();

        Supplier<ScanProperties> props = () -> new ScanProperties(ExecuteProperties.newBuilder()
            .setTimeLimit(timeLimit)
            .setReturnedRowLimit(maxScannedRecords)
            .setIsolationLevel(isolationLevel)
            .build());

        RecordCursorIterator<FDBStoredRecord<Message>> messageCursor;

        messageCursor = tableStore.scanRecords(tupleRange, continuation, props.get()).asIterator();
        while (messageCursor.hasNext()) {
            FDBStoredRecord<Message> record = messageCursor.next();
            scannedRecords.add(record);
        }
        byte[] continuationNew = messageCursor.getContinuation();

        return new ScanRecordCursorResult<>(scannedRecords, continuationNew);
    }


    public static long convertVersion(byte[] versionStamp) {
        byte[] temp = new byte[8];
        System.arraycopy(versionStamp, 0, temp, 0, 8);
        return ByteBuffer.wrap(temp).getLong();
    }

    public static Versionstamp getCurContextVersion(FDBRecordContext context) {
        return Versionstamp.fromBytes(ByteBuffer.allocate(Versionstamp.LENGTH)
            .order(ByteOrder.BIG_ENDIAN)
            .putLong(context.getReadVersion())
            .putInt(0xffffffff)
            .array());
    }

    public static List<FDBQueriedRecord<Message>> getHistoryRecords(FDBRecordStore recordStore, StoreMetadata storeMetadata,
        QueryComponent filter, Boolean sortReverse) {
        RecordQueryPlan plan = RecordStoreHelper
            .buildVersionRecordQueryPlan(recordStore, storeMetadata, filter, sortReverse);

        RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, null,
            ExecuteProperties.newBuilder().build()).asIterator();

        List<FDBQueriedRecord<Message>> recordList = new LinkedList<>();
        while (cursor.hasNext()) {
            FDBQueriedRecord<Message> record = cursor.next();
            recordList.add(record);
        }

        return recordList;
    }


    public static Optional<FDBQueriedRecord<Message>> getLatestHistoryRecord(FDBRecordStore recordStore,
        StoreMetadata storeMetadata, QueryComponent filter) {
        RecordQueryPlan plan = RecordStoreHelper.buildVersionRecordQueryPlan(recordStore, storeMetadata, filter, true);

        RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, null,
            ExecuteProperties.newBuilder().setReturnedRowLimit(1).build()).asIterator();

        if (cursor.hasNext()) {
            return Optional.of(cursor.next());
        }

        return Optional.empty();
    }


    private static int deleteStoreRecordByCursor(FDBRecordStore store,
        RecordCursorIterator<FDBQueriedRecord<Message>> cursor) {
        int cnt = 0;
        while (cursor.hasNext()) {
            store.deleteRecord(cursor.next().getPrimaryKey());
            cnt++;
        }
        return cnt;
    }

    public static byte[] deleteStoreRecord(FDBRecordStore store, RecordQueryPlan plan, byte[] continuation,
        Integer maxNumRecordPerTrans, IsolationLevel isolationLevel) {
        RecordCursorIterator<FDBQueriedRecord<Message>> cursor = store
            .executeQuery(plan, continuation,
                ExecuteProperties.newBuilder().setReturnedRowLimit(maxNumRecordPerTrans)
                    .setIsolationLevel(isolationLevel).build()).asIterator();

        deleteStoreRecordByCursor(store, cursor);
        return cursor.getContinuation();
    }

}
