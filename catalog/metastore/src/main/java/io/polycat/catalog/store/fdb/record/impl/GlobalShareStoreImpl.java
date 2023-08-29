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

import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.RoleObject;
import io.polycat.catalog.common.model.RoleUserObject;
import io.polycat.catalog.common.model.ScanRecordCursorResult;
import io.polycat.catalog.common.model.ShareConsumerObject;
import io.polycat.catalog.common.model.ShareNameObject;
import io.polycat.catalog.common.model.ShareObject;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.TransactionIsolationLevel;
import io.polycat.catalog.store.api.GlobalShareStore;
import io.polycat.catalog.store.common.StoreMetadata;
import io.polycat.catalog.store.fdb.record.DirectoryStoreHelper;
import io.polycat.catalog.store.fdb.record.RecordStoreHelper;
import io.polycat.catalog.store.fdb.record.StoreTypeUtil;
import io.polycat.catalog.store.fdb.record.TransactionContextUtil;
import io.polycat.catalog.store.protos.GlobalShareConsumer;
import io.polycat.catalog.store.protos.GlobalShareProperties;
import io.polycat.catalog.store.protos.RoleProperties;
import io.polycat.catalog.store.protos.RoleUser;
import io.polycat.catalog.store.protos.ShareConsumer;
import io.polycat.catalog.store.protos.ShareObjectName;
import io.polycat.catalog.store.protos.ShareProperties;

import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
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
public class GlobalShareStoreImpl implements GlobalShareStore {

    private static class GlobalShareStoreImplHandler {
        private static final GlobalShareStoreImpl INSTANCE = new GlobalShareStoreImpl();
    }

    public static GlobalShareStoreImpl getInstance() {
        return GlobalShareStoreImpl.GlobalShareStoreImplHandler.INSTANCE;
    }

    private Tuple GlobalSharePropertiesPrimaryKey(String projectId, String shareId) {
        return Tuple.from(projectId, shareId);
    }

    private Tuple GlobalShareConsumerPrimaryKey(String projectId, String shareId, String consumerId) {
        return Tuple.from(projectId, shareId, consumerId);
    }


    @Override
    public Boolean shareObjectNameExist(TransactionContext context, String projectId, String shareName)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore sharePropertiesStore = DirectoryStoreHelper.getGlobalSharePropertiesStore(fdbRecordContext);
        QueryComponent filter = Query.and(Query.field("project_id").equalsValue(projectId),
            Query.field("share_name").equalsValue(shareName));
        RecordQuery query = RecordQuery.newBuilder()
            .setRecordType(StoreMetadata.GLOBAL_SHARE_PROPERTIES.getRecordTypeName())
            .setFilter(filter).build();
        try (RecordCursor<FDBQueriedRecord<Message>> cursor = sharePropertiesStore.executeQuery(query)) {
            RecordCursorIterator<FDBQueriedRecord<Message>> iterator = cursor.asIterator();
            while (iterator.hasNext()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void insertShareProperties(TransactionContext context, String projectId, String shareId,
        String shareName, String catalogId,
        String ownerAccount, String ownerUser) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore sharePropertiesStore = DirectoryStoreHelper.getGlobalSharePropertiesStore(fdbRecordContext);
        GlobalShareProperties shareProperties = GlobalShareProperties.newBuilder()
            .setProjectId(projectId)
            .setShareId(shareId)
            .setShareName(shareName)
            .setCatalogId(catalogId)
            .setOwnerAccount(ownerAccount)
            .setOwnerUserId(ownerUser)
            .setCreateTime(RecordStoreHelper.getCurrentTime())
            .build();
        sharePropertiesStore.insertRecord(shareProperties);
    }

    @Override
    public void updateShareProperties(TransactionContext context, String projectId, ShareObject shareObject)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore sharePropertiesStore = DirectoryStoreHelper.getGlobalSharePropertiesStore(fdbRecordContext);
        GlobalShareProperties shareProperties = GlobalShareProperties.newBuilder()
            .setProjectId(projectId)
            .setShareId(shareObject.getShareId())
            .setShareName(shareObject.getShareName())
            .setOwnerAccount(shareObject.getOwnerAccount())
            .setOwnerUserId(shareObject.getOwnerUser())
            .setCreateTime(shareObject.getCreateTime())
            .setCatalogId(shareObject.getCatalogId())
            .build();
        sharePropertiesStore.updateRecord(shareProperties);
    }

    @Override
    public ShareObject getSharePropertiesById(TransactionContext context, String projectId, String shareId)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore sharePropertiesStore = DirectoryStoreHelper.getGlobalSharePropertiesStore(fdbRecordContext);
        FDBStoredRecord<Message> storedRecord = sharePropertiesStore.loadRecord(
            GlobalSharePropertiesPrimaryKey(projectId, shareId));
        if (storedRecord == null) {
            throw new MetaStoreException(ErrorCode.SHARE_ID_NOT_FOUND, shareId);
        }
        GlobalShareProperties.Builder builder = GlobalShareProperties.newBuilder().mergeFrom(storedRecord.getRecord());
        ShareObject shareObject = new ShareObject(projectId, shareId,
            builder.getShareName(), builder.getOwnerAccount(),
            builder.getOwnerUserId(), builder.getCreateTime(), builder.getCatalogId());
        if (builder.hasCatalogId()) {
            shareObject.setCatalogId(builder.getCatalogId());
        }
        return shareObject;
    }

    @Override
    public ShareObject getSharePropertiesByName(TransactionContext context, String projectId, String shareName)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore sharePropertiesStore = DirectoryStoreHelper.getGlobalSharePropertiesStore(fdbRecordContext);
        QueryComponent filter = Query.and(Query.field("project_id").equalsValue(projectId),
            Query.field("share_name").equalsValue(shareName));
        RecordQuery query = RecordQuery.newBuilder()
            .setRecordType(StoreMetadata.GLOBAL_SHARE_PROPERTIES.getRecordTypeName())
            .setFilter(filter).build();
        try (RecordCursor<FDBQueriedRecord<Message>> cursor = sharePropertiesStore.executeQuery(query)) {
            RecordCursorIterator<FDBQueriedRecord<Message>> iterator = cursor.asIterator();
            while (iterator.hasNext()) {
                GlobalShareProperties.Builder builder = GlobalShareProperties.newBuilder().
                    mergeFrom(Objects.requireNonNull(iterator.next()).getRecord());
                return new ShareObject(projectId, builder.getShareId(), shareName,
                    builder.getOwnerAccount(),  builder.getOwnerUserId(), builder.getCreateTime(),
                    builder.getCatalogId());
            }
        }
        return null;
    }

    @Override
    public String getShareId(TransactionContext context, String projectId, String shareName)
        throws MetaStoreException {
        ShareObject shareObject = getSharePropertiesByName(context, projectId, shareName);

        if (shareObject == null) {
            throw new MetaStoreException(ErrorCode.SHARE_NOT_FOUND, shareName);
        }

        return shareObject.getShareId();
    }


    @Override
    public void deleteGlobalShareProperties(TransactionContext context, String projectId, String shareId)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore sharePropertiesStore = DirectoryStoreHelper.getGlobalSharePropertiesStore(fdbRecordContext);
        sharePropertiesStore.deleteRecord(GlobalSharePropertiesPrimaryKey(projectId, shareId));
    }


    @Override
    public List<ShareObject> listShareObject(TransactionContext context,
        String projectId, String namePattern, int maxNum, byte[] continuation, TransactionIsolationLevel isolationLevel)
        throws MetaStoreException {
        List<ShareObject> shareObjectList = new ArrayList<>();

        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore sharePropertiesStore = DirectoryStoreHelper.getGlobalSharePropertiesStore(fdbRecordContext);
        QueryComponent filter = Query.field("project_id").equalsValue(projectId);
        RecordQuery query = RecordQuery.newBuilder()
            .setRecordType(StoreMetadata.GLOBAL_SHARE_PROPERTIES.getRecordTypeName())
            .setFilter(filter).build();
        try (RecordCursor<FDBQueriedRecord<Message>> cursor = sharePropertiesStore.executeQuery(query)) {
            RecordCursorIterator<FDBQueriedRecord<Message>> iterator = cursor.asIterator();
            while (iterator.hasNext()) {
                GlobalShareProperties.Builder builder = GlobalShareProperties.newBuilder()
                    .mergeFrom(Objects.requireNonNull(iterator.next()).getRecord());
                ShareObject shareObject =  new ShareObject(projectId, builder.getShareId(), builder.getShareName(),
                    builder.getOwnerAccount(),  builder.getOwnerUserId(), builder.getCreateTime(),
                    builder.getCatalogId());
                if (!isNamePatternValid(shareObject.getShareName() ,namePattern)) {
                    continue;
                }
                shareObjectList.add(shareObject);
            }
        }
        return shareObjectList;
    }


    @Override
    public void insertShareConsumer(TransactionContext context, String projectId, String shareId, String accountId,
        String managerUser) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore shareConsumerStore = DirectoryStoreHelper.getGlobalShareConsumerStore(fdbRecordContext);
        GlobalShareConsumer shareConsumer = GlobalShareConsumer.newBuilder()
            .setProjectId(projectId)
            .setShareId(shareId)
            .setConsumerId(accountId)
            .setManagerUser(managerUser)
            .build();
        shareConsumerStore.insertRecord(shareConsumer);
    }

    @Override
    public void addUsersToShareConsumer(TransactionContext context, String projectId, String shareId, String accountId,
        String[] users) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore shareConsumerStore = DirectoryStoreHelper.getGlobalShareConsumerStore(fdbRecordContext);
        FDBStoredRecord<Message> storedRecord = shareConsumerStore.loadRecord(
            GlobalShareConsumerPrimaryKey(projectId, shareId, accountId));
        if (storedRecord == null) {
            throw new MetaStoreException(ErrorCode.SHARE_CONSUMER_NOT_FOUND, shareId, accountId);
        }

        GlobalShareConsumer.Builder builder = GlobalShareConsumer.newBuilder().mergeFrom(storedRecord.getRecord());
        for (String user : users) {
            String[] userSrc = user.split(":");
            if (userSrc.length > 1) {
                builder.putUsers(userSrc[2], userSrc[0] + ":" + userSrc[1]);
            } else {
                builder.putUsers(user, "USER:IAM");
            }
        }
        shareConsumerStore.updateRecord(builder.build());
    }

    public Boolean shareConsumerExist(TransactionContext context, String projectId, String shareId,
        String accountId) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore shareConsumerStore = DirectoryStoreHelper.getGlobalShareConsumerStore(fdbRecordContext);
        FDBStoredRecord<Message> storedRecord = shareConsumerStore.loadRecord(
            GlobalShareConsumerPrimaryKey(projectId, shareId, accountId));
        return (storedRecord != null);
    }

    public ShareConsumerObject getGlobalShareConsumers(TransactionContext context, String projectId, String shareId,
        String accountId) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore shareConsumerStore = DirectoryStoreHelper.getGlobalShareConsumerStore(fdbRecordContext);
        FDBStoredRecord<Message> storedRecord = shareConsumerStore.loadRecord(
            GlobalShareConsumerPrimaryKey(projectId, shareId, accountId));
        if (storedRecord == null) {
            throw new MetaStoreException(ErrorCode.SHARE_CONSUMER_NOT_FOUND, shareId, accountId);
        }
        ShareConsumer.Builder builder = ShareConsumer.newBuilder().mergeFrom(storedRecord.getRecord());
        return new ShareConsumerObject(projectId, shareId, accountId, builder.getManagerUser(), builder.getUsersMap());
    }

    @Override
    public List<ShareConsumerObject> getShareAllConsumers(TransactionContext context, String projectId, String shareId)
        throws MetaStoreException {
        List<ShareConsumerObject> shareConsumerObjectList = new ArrayList<>();
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore shareConsumerStore = DirectoryStoreHelper.getGlobalShareConsumerStore(fdbRecordContext);
        QueryComponent filter = Query.and(Query.field("project_id").equalsValue(projectId),
            Query.field("share_id").equalsValue(shareId));
        RecordQuery query =  RecordQuery.newBuilder()
            .setRecordType(StoreMetadata.GLOBAL_SHARE_CONSUMER.getRecordTypeName()).setFilter(filter).build();
        try (RecordCursor<FDBQueriedRecord<Message>> cursor = shareConsumerStore.executeQuery(query)) {
            RecordCursorIterator<FDBQueriedRecord<Message>> iterator = cursor.asIterator();
            while (iterator.hasNext()) {
                GlobalShareConsumer shareConsumer = GlobalShareConsumer.newBuilder()
                    .mergeFrom(Objects.requireNonNull(iterator.next()).getRecord()).build();
                shareConsumerObjectList.add(new ShareConsumerObject(projectId, shareId, shareConsumer.getConsumerId(),
                    shareConsumer.getManagerUser(), shareConsumer.getUsersMap()));
            }
        }
        return shareConsumerObjectList;
    }

    public List<ShareConsumerObject> getShareAllConsumersWithAccount(TransactionContext context, String accountId)
        throws MetaStoreException {
        List<ShareConsumerObject> shareConsumerObjectList = new ArrayList<>();
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore shareConsumerStore = DirectoryStoreHelper.getGlobalShareConsumerStore(fdbRecordContext);
        QueryComponent filter = Query.field("consumer_id").equalsValue(accountId);
        RecordQuery query =  RecordQuery.newBuilder()
            .setRecordType(StoreMetadata.GLOBAL_SHARE_CONSUMER.getRecordTypeName()).setFilter(filter).build();
        try (RecordCursor<FDBQueriedRecord<Message>> cursor = shareConsumerStore.executeQuery(query)) {
            RecordCursorIterator<FDBQueriedRecord<Message>> iterator = cursor.asIterator();
            while (iterator.hasNext()) {
                GlobalShareConsumer shareConsumer = GlobalShareConsumer.newBuilder()
                    .mergeFrom(Objects.requireNonNull(iterator.next()).getRecord()).build();
                shareConsumerObjectList.add(new ShareConsumerObject(shareConsumer.getProjectId(),
                    shareConsumer.getShareId(), shareConsumer.getConsumerId(),
                    shareConsumer.getManagerUser(), shareConsumer.getUsersMap()));
            }
        }
        return shareConsumerObjectList;
    }


    @Override
    public void removeUsersFromShareConsumer(TransactionContext context, String projectId, String shareId, String accountId,
        String[] users) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore shareConsumerStore = DirectoryStoreHelper.getGlobalShareConsumerStore(fdbRecordContext);
        FDBStoredRecord<Message> storedRecord = shareConsumerStore.loadRecord(
            GlobalShareConsumerPrimaryKey(projectId, shareId, accountId));
        if (storedRecord == null) {
            throw new MetaStoreException(ErrorCode.SHARE_CONSUMER_NOT_FOUND, shareId, accountId);
        }
        GlobalShareConsumer.Builder builder = GlobalShareConsumer.newBuilder().mergeFrom(storedRecord.getRecord());
        for (String user : users) {
            String[] userSrc = user.split(":");
            if (userSrc.length > 1) {
                builder.putUsers(userSrc[2], userSrc[0] + ":" + userSrc[1]);
            } else {
                builder.putUsers(user, "USER:IAM");
            }
        }
        shareConsumerStore.updateRecord(builder.build());
    }


    @Override
    public void deleteShareConsumer(TransactionContext context, String projectId, String shareId, String accountId)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore shareConsumerStore = DirectoryStoreHelper.getGlobalShareConsumerStore(fdbRecordContext);
        shareConsumerStore.deleteRecord(GlobalShareConsumerPrimaryKey(projectId, shareId, accountId));
    }

    @Override
    public void delAllShareConsumer(TransactionContext context, String projectId, String shareId)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore shareConsumerStore = DirectoryStoreHelper.getGlobalShareConsumerStore(fdbRecordContext);
        QueryComponent filter = Query.and(Query.field("project_id").equalsValue(projectId),
            Query.field("share_id").equalsValue(shareId));
        RecordQuery query =  RecordQuery.newBuilder()
            .setRecordType(StoreMetadata.GLOBAL_SHARE_CONSUMER.getRecordTypeName()).setFilter(filter).build();
        try (RecordCursor<FDBQueriedRecord<Message>> cursor = shareConsumerStore.executeQuery(query)) {
            RecordCursorIterator<FDBQueriedRecord<Message>> iterator = cursor.asIterator();
            while (iterator.hasNext()) {
                GlobalShareConsumer shareConsumer = GlobalShareConsumer.newBuilder()
                    .mergeFrom(Objects.requireNonNull(iterator.next()).getRecord()).build();
                shareConsumerStore.deleteRecord(
                    GlobalShareConsumerPrimaryKey(projectId,shareId,shareConsumer.getConsumerId()));
            }
        }
    }


    private Boolean isNamePatternValid(String name, String pattern) {
        if (pattern == null) {
            return true;
        }
        if (pattern.isEmpty()) {
            return true;
        }
        return name.startsWith(pattern);
    }
}
