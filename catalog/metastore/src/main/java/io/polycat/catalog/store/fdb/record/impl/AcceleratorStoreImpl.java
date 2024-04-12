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
import io.polycat.catalog.store.api.AcceleratorStore;
import io.polycat.catalog.common.model.AcceleratorPropertiesObject;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.SqlTemplate;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.plugin.request.input.AcceleratorInput;
import io.polycat.catalog.common.plugin.request.input.SqlTemplateInput;

import io.polycat.catalog.store.common.StoreMetadata;

import io.polycat.catalog.store.fdb.record.DirectoryStoreHelper;
import io.polycat.catalog.store.fdb.record.RecordStoreHelper;
import io.polycat.catalog.store.fdb.record.TransactionContextUtil;
import io.polycat.catalog.store.protos.AcceleratorObjectName;
import io.polycat.catalog.store.protos.AcceleratorProperties;
import io.polycat.catalog.store.protos.AcceleratorTemplate;


import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;


@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "fdb")
public class AcceleratorStoreImpl implements AcceleratorStore {

    private static class AcceleratorStoreImplHandler {
        private static final AcceleratorStoreImpl INSTANCE = new AcceleratorStoreImpl();
    }

    public static AcceleratorStoreImpl getInstance() {
        return AcceleratorStoreImpl.AcceleratorStoreImplHandler.INSTANCE;
    }

    private Tuple acceleratorObjectNamePrimaryKey(DatabaseIdent databaseIdent, String acceleratorName) {
        return Tuple.from(databaseIdent.getCatalogId(), databaseIdent.getDatabaseId(), acceleratorName);
    }

    private Tuple acceleratorPropertiesPrimaryKey(String acceleratorId) {
        return Tuple.from(acceleratorId);
    }

    private Tuple acceleratorTemplatePrimaryKey(String acceleratorId, String catalogId, String databaseId,
        String tableId, int hashCode) {
        return Tuple.from(acceleratorId, catalogId, databaseId, tableId, hashCode);
    }

    public Boolean acceleratorObjectNameExist(TransactionContext context, DatabaseIdent databaseIdent,
        String acceleratorName) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore acceleratorObjectNameStore = DirectoryStoreHelper
            .getAcceleratorObjectNameStore(fdbRecordContext, databaseIdent.getProjectId());
        FDBStoredRecord<Message> record = acceleratorObjectNameStore.loadRecord(
            acceleratorObjectNamePrimaryKey(databaseIdent, acceleratorName));
        return (record != null);
    }

    public String getAcceleratorId(TransactionContext context, DatabaseIdent databaseIdent,
        String acceleratorName) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore acceleratorObjectNameStore = DirectoryStoreHelper
            .getAcceleratorObjectNameStore(fdbRecordContext, databaseIdent.getProjectId());
        FDBStoredRecord<Message> record = acceleratorObjectNameStore.loadRecord(
            acceleratorObjectNamePrimaryKey(databaseIdent, acceleratorName));
        if (record == null) {
            return null;
        }
        AcceleratorObjectName.Builder builder = AcceleratorObjectName.newBuilder().mergeFrom(record.getRecord());
        return builder.getAcceleratorId();
    }

    public void insertAcceleratorObjectName(TransactionContext context, DatabaseIdent databaseIdent,
        String acceleratorName, String acceleratorId) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore acceleratorObjectNameStore = DirectoryStoreHelper
            .getAcceleratorObjectNameStore(fdbRecordContext, databaseIdent.getProjectId());
        AcceleratorObjectName objectName = AcceleratorObjectName.newBuilder()
                .setCatalogId(databaseIdent.getCatalogId())
                .setDatabaseId(databaseIdent.getDatabaseId())
                .setName(acceleratorName)
                .setAcceleratorId(acceleratorId)
                .build();
        acceleratorObjectNameStore.insertRecord(objectName);
    }

    public void deleteAcceleratorObjectName(TransactionContext context, DatabaseIdent databaseIdent,
        String acceleratorName) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore acceleratorObjectNameStore = DirectoryStoreHelper
            .getAcceleratorObjectNameStore(fdbRecordContext, databaseIdent.getProjectId());
        acceleratorObjectNameStore.deleteRecord(acceleratorObjectNamePrimaryKey(databaseIdent, acceleratorName));
    }

    public void insertAcceleratorProperties(TransactionContext context, String projectId, String acceleratorId,
        AcceleratorInput acceleratorInput, String location) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore acceleratorPropertiesStore = DirectoryStoreHelper
            .getAcceleratorPropertiesStore(fdbRecordContext, projectId);
        AcceleratorProperties acceleratorProperties = AcceleratorProperties.newBuilder()
            .setAcceleratorId(acceleratorId)
            .setName(acceleratorInput.getName())
            .setLib(acceleratorInput.getLib())
            .setSqlStatement(acceleratorInput.getSqlStatement())
            .setLocation(location)
            .setCompiled(false)
            .build();

        acceleratorPropertiesStore.insertRecord(acceleratorProperties);
    }

    public void deleteAcceleratorProperties(TransactionContext context, String projectId, String acceleratorId)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore acceleratorPropertiesStore = DirectoryStoreHelper.getAcceleratorPropertiesStore(fdbRecordContext, projectId);
        acceleratorPropertiesStore.deleteRecord(acceleratorPropertiesPrimaryKey(acceleratorId));
    }

    public void updateAcceleratorPropertiesCompiled(TransactionContext context, String projectId,
        String acceleratorId, boolean compiled)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore acceleratorPropertiesStore = DirectoryStoreHelper.getAcceleratorPropertiesStore(fdbRecordContext, projectId);
        FDBStoredRecord<Message> record = acceleratorPropertiesStore
            .loadRecord(acceleratorPropertiesPrimaryKey(acceleratorId));
        AcceleratorProperties acceleratorProperties = AcceleratorProperties.newBuilder()
            .mergeFrom(record.getRecord())
            .setCompiled(compiled)
            .build();
        acceleratorPropertiesStore.updateRecord(acceleratorProperties);
    }

    public List<AcceleratorPropertiesObject> listAcceleratorProperties(TransactionContext context,
        DatabaseIdent databaseIdent) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        TupleRange tupleRange = TupleRange
            .allOf(Tuple.from(databaseIdent.getCatalogId(), databaseIdent.getDatabaseId()));
        int maxResults = Integer.MAX_VALUE;
        FDBRecordStore acceleratorRecordStore = DirectoryStoreHelper
            .getAcceleratorPropertiesStore(fdbRecordContext, databaseIdent.getProjectId());
        List<FDBStoredRecord<Message>> acceleratorRecords = RecordStoreHelper.listStoreRecords(tupleRange,
            acceleratorRecordStore, maxResults, null, IsolationLevel.SERIALIZABLE);
        List<AcceleratorPropertiesObject> acceleratorPropertiesObjectList = new ArrayList<>(acceleratorRecords.size());
        for (FDBStoredRecord<Message> storeRecord : acceleratorRecords) {
            AcceleratorProperties.Builder builder = AcceleratorProperties.newBuilder()
                .mergeFrom(storeRecord.getRecord());
            acceleratorPropertiesObjectList.add(new AcceleratorPropertiesObject(builder.getAcceleratorId(),
                builder.getName(), builder.getLib(), builder.getSqlStatement(), builder.getLocation(),
                builder.getCompiled(), builder.getPropertiesMap()));
        }
        return acceleratorPropertiesObjectList;
    }

    public void insertTemplates(TransactionContext context, String projectId, String acceleratorId,
        List<SqlTemplateInput> sqlTemplateInputList) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore templateStore = DirectoryStoreHelper.getAcceleratorTemplateStore(fdbRecordContext, projectId);
        for (SqlTemplateInput sqlTemplateInput : sqlTemplateInputList) {
            AcceleratorTemplate template = AcceleratorTemplate.newBuilder()
                .setAcceleratorId(acceleratorId)
                .setCatalogId(sqlTemplateInput.getCatalogId())
                .setDatabaseId(sqlTemplateInput.getDatabaseId())
                .setTableId(sqlTemplateInput.getTableId())
                .setHashCode(sqlTemplateInput.getHashCode())
                .setSqlTemplate(sqlTemplateInput.getSqlTemplate())
                .setCompiled(false)
                .setBinFilePath(sqlTemplateInput.getBinFilePath())
                .build();
            templateStore.insertRecord(template);
        }
    }

    public List<SqlTemplate> listAcceleratorTemplate(TransactionContext context, String projectId,
        String acceleratorId) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore templateStore = DirectoryStoreHelper.getAcceleratorTemplateStore(fdbRecordContext, projectId);
        List<SqlTemplate> sqlTemplateList = new ArrayList<>();
        QueryComponent filter = Query.field("accelerator_id").equalsValue(acceleratorId);
        RecordQuery query = RecordQuery.newBuilder()
            .setRecordType(StoreMetadata.ACCELERATOR_TEMPLATE.getRecordTypeName()).setFilter(filter).build();
        try (RecordCursor<FDBQueriedRecord<Message>> cursor = templateStore.executeQuery(query)) {
            RecordCursorIterator<FDBQueriedRecord<Message>> iterator = cursor.asIterator();
            while (iterator.hasNext()) {
                AcceleratorTemplate.Builder builder = AcceleratorTemplate.newBuilder()
                    .mergeFrom(Objects.requireNonNull(iterator.next()).getRecord());
                sqlTemplateList.add(new SqlTemplate(builder.getCatalogId(), builder.getDatabaseId(), builder.getTableId(),
                    builder.getHashCode(), builder.getSqlTemplate(), builder.getCompiled(), builder.getBinFilePath()));
            }
        }
        return sqlTemplateList;
    }

    public void updateTemplateStatusCompiled(TransactionContext context, String projectId, String acceleratorId,
        SqlTemplateInput templateInput) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore templateStore = DirectoryStoreHelper.getAcceleratorTemplateStore(fdbRecordContext, projectId);
        FDBStoredRecord<Message> record = templateStore
            .loadRecord(acceleratorTemplatePrimaryKey(acceleratorId, templateInput.getCatalogId(),
                templateInput.getDatabaseId(), templateInput.getTableId(), templateInput.getHashCode()));
        AcceleratorTemplate acceleratorTemplate = AcceleratorTemplate.newBuilder().mergeFrom(record.getRecord())
            .setBinFilePath(templateInput.getBinFilePath())
            .setCompiled(true)
            .build();
        templateStore.updateRecord(acceleratorTemplate);
    }

    public void deleteAcceleratorTemplateAll(TransactionContext context, String projectId, String acceleratorId)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore templateStore = DirectoryStoreHelper.getAcceleratorTemplateStore(fdbRecordContext, projectId);
        QueryComponent filter = Query.field("accelerator_id").equalsValue(acceleratorId);
        RecordQuery query = RecordQuery.newBuilder()
            .setRecordType(StoreMetadata.ACCELERATOR_TEMPLATE.getRecordTypeName())
            .setFilter(filter).build();
        try (RecordCursor<FDBQueriedRecord<Message>> cursor = templateStore.executeQuery(query)) {
            RecordCursorIterator<FDBQueriedRecord<Message>> iterator = cursor.asIterator();
            while (iterator.hasNext()) {
                AcceleratorTemplate.Builder builder = AcceleratorTemplate.newBuilder()
                    .mergeFrom(Objects.requireNonNull(iterator.next()).getRecord());
                templateStore.deleteRecord(acceleratorTemplatePrimaryKey(acceleratorId, builder.getCatalogId(),
                    builder.getDatabaseId(), builder.getTableId(), builder.getHashCode()));
            }
        }
    }

}
