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

import java.util.HashSet;
import java.util.Objects;

import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.store.api.UserGroupStore;
import io.polycat.catalog.store.common.StoreMetadata;
import io.polycat.catalog.store.fdb.record.DirectoryStoreHelper;
import io.polycat.catalog.store.fdb.record.TransactionContextUtil;
import io.polycat.catalog.store.protos.GroupProperties;
import io.polycat.catalog.store.protos.GroupUser;
import io.polycat.catalog.store.protos.UserProperties;

import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "fdb")
public class UserGroupStoreImpl implements UserGroupStore {

    private static final Logger logger = Logger.getLogger(UserGroupStoreImpl.class);

    private static class UserGroupStoreImplHandler {
        private static final UserGroupStoreImpl INSTANCE = new UserGroupStoreImpl();
    }

    public static UserGroupStoreImpl getInstance() {
        return UserGroupStoreImpl.UserGroupStoreImplHandler.INSTANCE;
    }


    private Tuple UserPropertiesPrimaryKey(String userId) {
        return Tuple.from(userId);
    }

    private Tuple GroupPropertiesPrimaryKey(String groupId) {
        return Tuple.from(groupId);
    }

    private Tuple GroupUserPrimaryKey(String groupId, String userId) {
        return Tuple.from(groupId, userId);
    }


    // user
    @Override
    public String  getUserIdByName(TransactionContext context, String projectId, int userSource, String userName)
        throws MetaStoreException {

        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore userPropertiesStore = DirectoryStoreHelper.getUserPropertiesStore(fdbRecordContext, projectId);
        QueryComponent filter = Query.and (Query.field("user_name").equalsValue(userName),
                Query.field("user_source").equalsValue(userSource));
        RecordQuery query = RecordQuery.newBuilder().setRecordType(StoreMetadata.USER_PROPERTIES.getRecordTypeName())
            .setFilter(filter).build();
        try (RecordCursor<FDBQueriedRecord<Message>> cursor = userPropertiesStore.executeQuery(query)) {
            RecordCursorIterator<FDBQueriedRecord<Message>> iterator = cursor.asIterator();
            while (iterator.hasNext()) {
                UserProperties.Builder builder = UserProperties.newBuilder()
                    .mergeFrom(Objects.requireNonNull(iterator.next()).getRecord());
                return builder.getUserId();
            }
        }
        return null;
    }

    @Override
    public void insertUserObject(TransactionContext context, String projectId, int userSource,
        String userName, String userId)
        throws MetaStoreException {

        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore userPropertiesStore = DirectoryStoreHelper.getUserPropertiesStore(fdbRecordContext, projectId);
        UserProperties userProperties = UserProperties.newBuilder()
            .setUserId(userId)
            .setUserSource(userSource)
            .setUserName(userName)
            .build();
        userPropertiesStore.insertRecord(userProperties);
    }


    @Override
    public void deleteUserObjectById(TransactionContext context, String projectId, String userId)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore userPropertiesStore = DirectoryStoreHelper.getUserPropertiesStore(fdbRecordContext, projectId);
        userPropertiesStore.deleteRecord(UserPropertiesPrimaryKey(userId));
    }

    @Override
    public HashSet<String> getAllUserIdByDomain(TransactionContext context, String projectId, String domainId)
        throws MetaStoreException {

        HashSet<String> userIdSet = new HashSet<>();
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore userPropertiesStore = DirectoryStoreHelper.getUserPropertiesStore(fdbRecordContext, projectId);
        RecordQuery query = RecordQuery.newBuilder().setRecordType(StoreMetadata.USER_PROPERTIES.getRecordTypeName())
            .build();
        try (RecordCursor<FDBQueriedRecord<Message>> cursor = userPropertiesStore.executeQuery(query)) {
            RecordCursorIterator<FDBQueriedRecord<Message>> iterator = cursor.asIterator();
            while (iterator.hasNext()) {
                UserProperties.Builder builder = UserProperties.newBuilder()
                    .mergeFrom(Objects.requireNonNull(iterator.next()).getRecord());
                userIdSet.add(builder.getUserId());
            }
        }
        return userIdSet;
    }

    //group
    @Override
    public String  getGroupIdByName(TransactionContext context, String projectId, String groupName)
        throws MetaStoreException {

        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore groupPropertiesStore = DirectoryStoreHelper.getGroupPropertiesStore(fdbRecordContext, projectId);
        QueryComponent filter = Query.field("group_name").equalsValue(groupName);
        RecordQuery query = RecordQuery.newBuilder().setRecordType(StoreMetadata.GROUP_PROPERTIES.getRecordTypeName())
            .setFilter(filter).build();
        try (RecordCursor<FDBQueriedRecord<Message>> cursor = groupPropertiesStore.executeQuery(query)) {
            RecordCursorIterator<FDBQueriedRecord<Message>> iterator = cursor.asIterator();
            while (iterator.hasNext()) {
                GroupProperties.Builder builder = GroupProperties.newBuilder()
                    .mergeFrom(Objects.requireNonNull(iterator.next()).getRecord());
                return builder.getGroupId();
            }
        }
        return null;
    }

    @Override
    public void insertGroupObject(TransactionContext context, String projectId, String groupName, String groupId)
        throws MetaStoreException {

        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore groupPropertiesStore = DirectoryStoreHelper.getGroupPropertiesStore(fdbRecordContext, projectId);
        GroupProperties groupProperties = GroupProperties.newBuilder()
            .setGroupId(groupId)
            .setGroupName(groupName)
            .build();
        groupPropertiesStore.insertRecord(groupProperties);
    }


    @Override
    public void deleteGroupObjectById(TransactionContext context, String projectId, String groupId)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore groupPropertiesStore = DirectoryStoreHelper.getGroupPropertiesStore(fdbRecordContext, projectId);
        groupPropertiesStore.deleteRecord(GroupPropertiesPrimaryKey(groupId));
    }


    @Override
    public HashSet<String> getAllGroupIdByDomain(TransactionContext context, String projectId, String domainId)
        throws MetaStoreException {

        HashSet<String> userIdSet = new HashSet<>();
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore groupPropertiesStore = DirectoryStoreHelper.getGroupPropertiesStore(fdbRecordContext, projectId);
        RecordQuery query = RecordQuery.newBuilder().setRecordType(StoreMetadata.GROUP_PROPERTIES.getRecordTypeName())
            .build();
        try (RecordCursor<FDBQueriedRecord<Message>> cursor = groupPropertiesStore.executeQuery(query)) {
            RecordCursorIterator<FDBQueriedRecord<Message>> iterator = cursor.asIterator();
            while (iterator.hasNext()) {
                GroupProperties.Builder builder = GroupProperties.newBuilder()
                    .mergeFrom(Objects.requireNonNull(iterator.next()).getRecord());
                userIdSet.add(builder.getGroupId());
            }
        }
        return userIdSet;
    }


    // group-user
    @Override
    public void insertGroupUser(TransactionContext context, String projectId, String groupId, String userId)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore groupUserStore = DirectoryStoreHelper.getGroupUserStore(fdbRecordContext, projectId);
        GroupUser groupUser = GroupUser.newBuilder()
            .setGroupId(groupId)
            .setUserId(userId)
            .build();
        groupUserStore.insertRecord(groupUser);
    }

    @Override
    public HashSet<String> getUserIdSetByGroupId(TransactionContext context, String projectId, String groupId)
        throws MetaStoreException {

        HashSet<String> userIdSet = new HashSet<>();
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore groupUserStore = DirectoryStoreHelper.getGroupUserStore(fdbRecordContext, projectId);
        QueryComponent filter = Query.field("group_id").equalsValue(groupId);
        RecordQuery query = RecordQuery.newBuilder().setRecordType(StoreMetadata.GROUP_USER.getRecordTypeName())
            .setFilter(filter).build();
        try (RecordCursor<FDBQueriedRecord<Message>> cursor = groupUserStore.executeQuery(query)) {
            RecordCursorIterator<FDBQueriedRecord<Message>> iterator = cursor.asIterator();
            while (iterator.hasNext()) {
                GroupUser.Builder builder = GroupUser.newBuilder()
                    .mergeFrom(Objects.requireNonNull(iterator.next()).getRecord());
                userIdSet.add(builder.getUserId());
            }
        }
        return userIdSet;
    }

    @Override
    public HashSet<String> getGroupIdSetByUserId(TransactionContext context, String projectId, String userId)
        throws MetaStoreException {

        HashSet<String> groupIdSet = new HashSet<>();
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore groupUserStore = DirectoryStoreHelper.getGroupUserStore(fdbRecordContext, projectId);
        QueryComponent filter = Query.field("user_id").equalsValue(userId);
        RecordQuery query = RecordQuery.newBuilder().setRecordType(StoreMetadata.GROUP_USER.getRecordTypeName())
            .setFilter(filter).build();
        try (RecordCursor<FDBQueriedRecord<Message>> cursor = groupUserStore.executeQuery(query)) {
            RecordCursorIterator<FDBQueriedRecord<Message>> iterator = cursor.asIterator();
            while (iterator.hasNext()) {
                GroupUser.Builder builder = GroupUser.newBuilder()
                    .mergeFrom(Objects.requireNonNull(iterator.next()).getRecord());
                groupIdSet.add(builder.getGroupId());
            }
        }
        return groupIdSet;
    }

    @Override
    public void deleteGroupUser(TransactionContext context, String projectId, String groupId, String userId)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore groupUserStore = DirectoryStoreHelper.getGroupUserStore(fdbRecordContext, projectId);
        groupUserStore.deleteRecord(GroupUserPrimaryKey(groupId, userId));
    }

    @Override
    public void delAllGroupUserByGroup(TransactionContext context, String projectId, String groupId)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore groupUserStore = DirectoryStoreHelper.getGroupUserStore(fdbRecordContext, projectId);
        QueryComponent filter = Query.field("group_id").equalsValue(groupId);
        RecordQuery query = RecordQuery.newBuilder().setRecordType(StoreMetadata.GROUP_USER.getRecordTypeName())
            .setFilter(filter).build();
        try (RecordCursor<FDBQueriedRecord<Message>> cursor = groupUserStore.executeQuery(query)) {
            RecordCursorIterator<FDBQueriedRecord<Message>> iterator = cursor.asIterator();
            while (iterator.hasNext()) {
                GroupUser.Builder groupUser = GroupUser.newBuilder()
                    .mergeFrom(Objects.requireNonNull(iterator.next()).getRecord());
                groupUserStore.deleteRecord(GroupUserPrimaryKey(groupId, groupUser.getUserId()));
            }
        }
    }

    @Override
    public void delAllGroupUserByUser(TransactionContext context, String projectId, String userId)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore groupUserStore = DirectoryStoreHelper.getGroupUserStore(fdbRecordContext, projectId);
        QueryComponent filter = Query.field("user_id").equalsValue(userId);
        RecordQuery query = RecordQuery.newBuilder().setRecordType(StoreMetadata.GROUP_USER.getRecordTypeName())
            .setFilter(filter).build();
        try (RecordCursor<FDBQueriedRecord<Message>> cursor = groupUserStore.executeQuery(query)) {
            RecordCursorIterator<FDBQueriedRecord<Message>> iterator = cursor.asIterator();
            while (iterator.hasNext()) {
                GroupUser.Builder groupUser = GroupUser.newBuilder()
                    .mergeFrom(Objects.requireNonNull(iterator.next()).getRecord());
                groupUserStore.deleteRecord(GroupUserPrimaryKey(groupUser.getGroupId(), userId));
            }
        }
    }

}
