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

import java.util.List;
import java.util.Optional;

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.expressions.Query;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.model.ObjectNameMap;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.store.api.ObjectNameMapStore;
import io.polycat.catalog.store.common.StoreMetadata;
import io.polycat.catalog.store.fdb.record.DirectoryStoreHelper;
import io.polycat.catalog.store.fdb.record.RecordStoreHelper;
import io.polycat.catalog.store.fdb.record.StoreTypeUtil;
import io.polycat.catalog.store.fdb.record.TransactionContextUtil;
import io.polycat.catalog.store.protos.ObjectNameMapRecord;

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "fdb")
public class ObjectNameMapStoreImpl implements ObjectNameMapStore {
    private static class ObjectNameMapStoreImplHandler {
        private static final ObjectNameMapStoreImpl INSTANCE = new ObjectNameMapStoreImpl();
    }

    public static ObjectNameMapStoreImpl getInstance() {
        return ObjectNameMapStoreImplHandler.INSTANCE;
    }

    private Tuple buildObjectNameMapPrimaryKey(ObjectType objectType, String upperObjectName, String objectName) {
        return Tuple.from(objectType.name(), upperObjectName, objectName);
    }

    private void checkObjectType(io.polycat.catalog.common.ObjectType objectType) {
        if (objectType != io.polycat.catalog.common.ObjectType.DATABASE
            && objectType != io.polycat.catalog.common.ObjectType.TABLE) {
            throw new MetaStoreException(ErrorCode.OBJECT_NAME_MAP_TYPE_ERROR, objectType);
        }
    }

    @Override
    public void createObjectNameMapSubspace(TransactionContext context, String projectId) {
        return;
    }

    @Override
    public void dropObjectNameMapSubspace(TransactionContext context, String projectId) {
        return;
    }

    @Override
    public void insertObjectNameMap(TransactionContext context, ObjectNameMap objectNameMap) throws MetaStoreException {
        checkObjectType(objectNameMap.getObjectType());
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore objectNameMapStore = DirectoryStoreHelper.getObjectNameMapStore(fdbRecordContext, objectNameMap.getProjectId());
        ObjectNameMapRecord objectNameMapSpace = ObjectNameMapRecord.newBuilder()
            .setObjectType(objectNameMap.getObjectType().name())
            .setUpperObjectName(objectNameMap.getUpperObjectName())
            .setObjectName(objectNameMap.getObjectName())
            .setTopObjectId(objectNameMap.getTopObjectId())
            .setUpperObjectId(objectNameMap.getUpperObjectId())
            .setObjectId(objectNameMap.getObjectId())
            .build();
        objectNameMapStore.insertRecord(objectNameMapSpace);
    }

    @Override
    public Optional<ObjectNameMap> getObjectNameMap(TransactionContext context, String projectId,
        ObjectType objectType, String upperObjectName, String objectName)
        throws MetaStoreException {
        checkObjectType(objectType);
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore objectNameMapStore = DirectoryStoreHelper.getObjectNameMapStore(fdbRecordContext, projectId);
        FDBStoredRecord<Message> storedRecord = objectNameMapStore
            .loadRecord(buildObjectNameMapPrimaryKey(objectType, upperObjectName, objectName));
        if (storedRecord == null) {
            return Optional.empty();
        }

        ObjectNameMapRecord.Builder builder = ObjectNameMapRecord.newBuilder().mergeFrom(storedRecord.getRecord());
        return Optional.of(new ObjectNameMap(projectId, objectType, upperObjectName, objectName,
            builder.getTopObjectId(), builder.getUpperObjectId(), builder.getObjectId()));
    }

    @Override
    public void deleteObjectNameMap(TransactionContext context, String projectId,
        io.polycat.catalog.common.ObjectType objectType,
        String upperObjectName, String objectName) throws MetaStoreException {
        checkObjectType(objectType);
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore objectNameMapStore = DirectoryStoreHelper.getObjectNameMapStore(fdbRecordContext, projectId);
        Tuple key = buildObjectNameMapPrimaryKey(objectType, upperObjectName, objectName);
        objectNameMapStore.deleteRecord(key);
    }

    @Override
    public List<ObjectNameMap> listObjectNameMap(TransactionContext context, String projectId,
        io.polycat.catalog.common.ObjectType objectType, String upperObjectName, String topObjectId) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore objectNameMapStore = DirectoryStoreHelper.getObjectNameMapStore(fdbRecordContext, projectId);
        QueryComponent filter = Query
            .and(Query.field("object_type").equalsValue(objectType.name()),
                Query.field("upper_object_name").equalsValue(upperObjectName),
                Query.field("top_object_id").equalsValue(topObjectId));

        RecordQueryPlan plan = RecordStoreHelper.buildRecordQueryPlan(objectNameMapStore,
            StoreMetadata.OBJECT_NAME_MAP_RECORD, filter);

        RecordCursor<ObjectNameMap> cursor = objectNameMapStore
            .executeQuery(plan, null,
                ExecuteProperties.newBuilder().setIsolationLevel(IsolationLevel.SERIALIZABLE).build())
            .map(rec -> ObjectNameMapRecord.newBuilder().mergeFrom(rec.getRecord()))
            .map(builder -> new ObjectNameMap(projectId,
                ObjectType.valueOf(builder.getObjectType()),
                builder.getUpperObjectName(), builder.getObjectName(), builder.getTopObjectId(),
                builder.getUpperObjectId(), builder.getObjectId()));

        List<ObjectNameMap> objectNameMapList = cursor.asList().join();
        return objectNameMapList;
    }
    

}
