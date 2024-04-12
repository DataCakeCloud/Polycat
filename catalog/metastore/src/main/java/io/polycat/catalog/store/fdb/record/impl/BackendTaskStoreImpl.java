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

import java.util.Optional;

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordVersion;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;
import io.polycat.catalog.store.api.BackendTaskStore;
import io.polycat.catalog.common.model.BackendTaskObject;
import io.polycat.catalog.common.model.BackendTaskType;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.utils.CodecUtil;
import io.polycat.catalog.store.fdb.record.DirectoryStoreHelper;
import io.polycat.catalog.store.fdb.record.TransactionContextUtil;
import io.polycat.catalog.store.protos.BackendTaskRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import io.polycat.catalog.common.model.TableIdent;
import io.polycat.catalog.common.model.TableName;
import io.polycat.catalog.store.protos.TaskType;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "fdb")
public class BackendTaskStoreImpl implements BackendTaskStore {

    private BackendTaskType trans2BackendTaskType (TaskType taskType) {
        switch (taskType) {
            case DROP_TABLE_PURGE:
                return BackendTaskType.DROP_TABLE_PURGE;
            default:
                throw new UnsupportedOperationException("failed to convert " + taskType.name());
        }
    }

    private BackendTaskObject trans2BackendTaskObject(BackendTaskRecord backendTaskRecord) {
        BackendTaskObject backendTaskObject = new BackendTaskObject();
        backendTaskObject.setTaskId(backendTaskRecord.getTaskId());
        backendTaskObject.setTaskName(backendTaskRecord.getTaskName());
        backendTaskObject.setTaskType(trans2BackendTaskType(backendTaskRecord.getTaskType()));
        backendTaskObject.setProjectId(backendTaskRecord.getProjectId());
        backendTaskObject.getParams().putAll(backendTaskRecord.getParams());

        return backendTaskObject;

    }

    @Override
    public BackendTaskObject submitDelHistoryTableJob(TransactionContext context, TableIdent tableIdent,
        TableName tableName, String latestVersion, String taskId) {
        BackendTaskRecord backendTaskRecord = BackendTaskRecord.newBuilder()
            .setTaskId(taskId)
            .setTaskName("PURGE_TABLE")
            .setTaskType(TaskType.DROP_TABLE_PURGE)
            .setProjectId(tableIdent.getProjectId())
            .putParams("catalog_id", tableIdent.getCatalogId())
            .putParams("database_id", tableIdent.getDatabaseId())
            .putParams("table_id", tableIdent.getTableId())
            .putParams("tableName", tableName.getTableName())
            .putParams("begVersion", CodecUtil.bytes2Hex(FDBRecordVersion.MIN_VERSION.toBytes()))
            .putParams("endVersion", latestVersion)
            .build();
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore backendTaskObjectStore = DirectoryStoreHelper.getBackendTaskObjectStore(fdbRecordContext);
        backendTaskObjectStore.insertRecord(backendTaskRecord);
        return trans2BackendTaskObject(backendTaskRecord);
    }

    @Override
    public Optional<BackendTaskObject> getDelHistoryTableJob(TransactionContext context, String taskId) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore backendTaskObjectStore = DirectoryStoreHelper.getBackendTaskObjectStore(fdbRecordContext);
        FDBStoredRecord<Message> record = backendTaskObjectStore.loadRecord(Tuple.from(taskId));
        if (record == null) {
            return Optional.empty();
        }

        BackendTaskRecord backendTaskRecord = BackendTaskRecord.newBuilder().mergeFrom(record.getRecord()).build();
        return Optional.of(trans2BackendTaskObject(backendTaskRecord));
    }

    @Override
    public void deleteBackendTask(TransactionContext context, String taskId) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore backendTaskObjectStore = DirectoryStoreHelper.getBackendTaskObjectStore(fdbRecordContext);
        backendTaskObjectStore.deleteRecord(Tuple.from(taskId));
    }



}