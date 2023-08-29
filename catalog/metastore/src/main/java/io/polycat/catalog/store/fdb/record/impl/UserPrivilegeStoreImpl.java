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

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;

import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.UserPrivilege;
import io.polycat.catalog.store.api.UserPrivilegeStore;
import io.polycat.catalog.store.common.StoreMetadata;
import io.polycat.catalog.store.fdb.record.DirectoryStoreHelper;
import io.polycat.catalog.store.fdb.record.RecordStoreHelper;
import io.polycat.catalog.store.fdb.record.TransactionContextUtil;
import io.polycat.catalog.store.protos.UserPrivilegeRecord;

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.util.List;

@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "fdb")
public class UserPrivilegeStoreImpl implements UserPrivilegeStore {

    private static int dealMaxNumRecordPerTrans = 2048;

    private static class UserPrivilegeStoreImplHandler {
        private static final UserPrivilegeStoreImpl INSTANCE = new UserPrivilegeStoreImpl();
    }

    public static UserPrivilegeStoreImpl getInstance() {
        return UserPrivilegeStoreImpl.UserPrivilegeStoreImplHandler.INSTANCE;
    }

    private Tuple userPrivilegePrimaryKey(String userId, String objectType, String objectId) {
        return Tuple.from(userId, objectType, objectId);
    }

    @Override
    public UserPrivilege getUserPrivilege(TransactionContext context, String projectId, String userId,
        String objectType, String objectId) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore userPrivilegeStore = DirectoryStoreHelper.getUserPrivilegeStore(fdbRecordContext, projectId);
        FDBStoredRecord<Message> storedRecord =
                userPrivilegeStore.loadRecord(userPrivilegePrimaryKey(userId, objectType, objectId));
        if (storedRecord == null) {
            return null;
        }
        UserPrivilegeRecord.Builder record = UserPrivilegeRecord.newBuilder().mergeFrom(storedRecord.getRecord());
        return new UserPrivilege(userId, objectType, objectId, record.getIsOwner(),
                record.getPrivilege());
    }

    @Override
    public List<UserPrivilege> getUserPrivileges(TransactionContext context, String projectId, String userId, String objectType) throws MetaStoreException {
        return null;
    }

    @Override
    public void createUserPrivilegeSubspace(TransactionContext context, String projectId) throws MetaStoreException {

    }

    @Override
    public void dropUserPrivilegeSubspace(TransactionContext context, String projectId) throws MetaStoreException {

    }

    @Override
    public void insertUserPrivilege(TransactionContext context, String projectId, String userId, String objectType,
        String objectId, Boolean isOwner, long privilege) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore userPrivilegeStore = DirectoryStoreHelper.getUserPrivilegeStore(fdbRecordContext, projectId);
        UserPrivilegeRecord userPrivilege = UserPrivilegeRecord.newBuilder()
            .setUserId(userId)
            .setObjectType(objectType)
            .setObjectId(objectId)
            .setIsOwner(isOwner)
            .setPrivilege(privilege).build();
        userPrivilegeStore.saveRecord(userPrivilege);
    }


    @Override
    public void deleteUserPrivilege(TransactionContext context, String projectId, String userId,
        String objectType, String objectId) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore userPrivilegeStore = DirectoryStoreHelper.getUserPrivilegeStore(fdbRecordContext, projectId);
        userPrivilegeStore.deleteRecord(userPrivilegePrimaryKey(userId, objectType, objectId));
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

    @Override
    public byte[] deleteUserPrivilegeByToken(TransactionContext context, String projectId, String objectType,
        String objectId, byte[] continuation) throws MetaStoreException {
        QueryComponent filter = Query.and(Query.field("object_type").equalsValue(objectType),
            Query.field("object_id").equalsValue(objectId));
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore store = DirectoryStoreHelper.getUserPrivilegeStore(fdbRecordContext, projectId);
        RecordQueryPlan plan = RecordStoreHelper.buildRecordQueryPlan(store, StoreMetadata.USER_PRIVILEGE_RECORD,
            filter);

        RecordCursorIterator<FDBQueriedRecord<Message>> cursor = store
            .executeQuery(plan, continuation,
                ExecuteProperties.newBuilder().setReturnedRowLimit(dealMaxNumRecordPerTrans)
                    .setIsolationLevel(IsolationLevel.SNAPSHOT).build()).asIterator();

        deleteStoreRecordByCursor(store, cursor);
        return cursor.getContinuation();
    }

}
