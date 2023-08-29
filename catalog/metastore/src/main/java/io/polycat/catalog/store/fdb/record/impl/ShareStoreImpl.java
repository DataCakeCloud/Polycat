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

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.ScanRecordCursorResult;
import io.polycat.catalog.common.model.ShareConsumerObject;
import io.polycat.catalog.common.model.ShareNameObject;
import io.polycat.catalog.common.model.ShareObject;
import io.polycat.catalog.common.model.SharePrivilegeObject;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.TransactionIsolationLevel;
import io.polycat.catalog.store.api.ShareStore;
import io.polycat.catalog.store.fdb.record.DirectoryStoreHelper;
import io.polycat.catalog.store.fdb.record.RecordStoreHelper;
import io.polycat.catalog.store.fdb.record.StoreTypeUtil;
import io.polycat.catalog.store.fdb.record.TransactionContextUtil;
import io.polycat.catalog.store.protos.ShareConsumer;
import io.polycat.catalog.store.protos.ShareObjectName;
import io.polycat.catalog.store.protos.SharePrivilege;
import io.polycat.catalog.store.protos.ShareProperties;

import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "fdb")
public class ShareStoreImpl implements ShareStore {

    private static class ShareStoreImplHandler {
        private static final ShareStoreImpl INSTANCE = new ShareStoreImpl();
    }

    public static ShareStoreImpl getInstance() {
        return ShareStoreImpl.ShareStoreImplHandler.INSTANCE;
    }

    private Tuple shareObjectNamePrimaryKey(String shareName) {
        return Tuple.from(shareName);
    }

    private Tuple sharePropertiesPrimaryKey(String shareId) {
        return Tuple.from(shareId);
    }

    private Tuple shareConsumerPrimaryKey(String accountId) {
        return Tuple.from(accountId);
    }

    private Tuple sharePrivilegePrimaryKey(String databaseId, String objectType, String objectId) {
        return Tuple.from(databaseId, objectType, objectId);
    }

    @Override
    public Boolean shareObjectNameExist(TransactionContext context, String projectId, String shareName)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore shareObjectNameStore = DirectoryStoreHelper.getShareObjectNameStore(fdbRecordContext, projectId);
        FDBStoredRecord<Message> storedRecord = shareObjectNameStore.loadRecord(shareObjectNamePrimaryKey(shareName));
        return storedRecord != null;
    }

    @Override
    public void insertShareObjectName(TransactionContext context, String projectId, String shareName, String shareId)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore shareObjectNameStore = DirectoryStoreHelper.getShareObjectNameStore(fdbRecordContext, projectId);
        ShareObjectName shareObjectName = ShareObjectName.newBuilder()
            .setName(shareName)
            .setShareId(shareId)
            .build();
        shareObjectNameStore.insertRecord(shareObjectName);
    }

    @Override
    public String getShareId(TransactionContext context, String projectId, String shareName) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore shareObjectNameStore = DirectoryStoreHelper.getShareObjectNameStore(fdbRecordContext, projectId);
        FDBStoredRecord<Message> storedRecord = shareObjectNameStore.loadRecord(shareObjectNamePrimaryKey(shareName));
        if (storedRecord == null) {
            throw new MetaStoreException(ErrorCode.SHARE_NOT_FOUND, shareName);
        }
        return ShareObjectName.newBuilder().mergeFrom(storedRecord.getRecord()).getShareId();
    }

    @Override
    public void deleteShareObjectName(TransactionContext context, String projectId, String shareName)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore shareObjectNameStore = DirectoryStoreHelper.getShareObjectNameStore(fdbRecordContext, projectId);
        shareObjectNameStore.deleteRecord(shareObjectNamePrimaryKey(shareName));
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

    @Override
    public ScanRecordCursorResult<List<ShareNameObject>> listShareObjectName(TransactionContext context,
        String projectId, String namePattern, int maxNum, byte[] continuation, TransactionIsolationLevel isolationLevel)
        throws MetaStoreException {
        List<ShareNameObject> shareNameObjectList = new ArrayList<>();

        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore shareObjectNameStore = DirectoryStoreHelper.getShareObjectNameStore(fdbRecordContext, projectId);
        TupleRange tupleRange = TupleRange.ALL;
        ScanRecordCursorResult<List<FDBStoredRecord<Message>>> scannedRecords = RecordStoreHelper
            .listStoreRecordsWithToken(tupleRange, shareObjectNameStore, 0, maxNum, continuation,
                StoreTypeUtil.trans2IsolationLevel(isolationLevel));

        for (FDBStoredRecord<Message> i : scannedRecords.getResult()) {
            ShareObjectName.Builder builder  = ShareObjectName.newBuilder().mergeFrom(i.getRecord());
            ShareNameObject shareNameObject = new ShareNameObject(builder.getName(), builder.getShareId());
            if (!isNamePatternValid(shareNameObject.getName() ,namePattern)) {
                continue;
            }
            shareNameObjectList.add(shareNameObject);
        }

        return new ScanRecordCursorResult<>(shareNameObjectList, scannedRecords.getContinuation().orElse(null));
    }

    @Override
    public void insertShareProperties(TransactionContext context, String projectId, String shareId, String shareName,
        String ownerAccount, String ownerUser) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore sharePropertiesStore = DirectoryStoreHelper.getSharePropertiesStore(fdbRecordContext, projectId);
        ShareProperties shareProperties = ShareProperties.newBuilder()
            .setShareId(shareId)
            .setName(shareName)
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
        FDBRecordStore sharePropertiesStore = DirectoryStoreHelper.getSharePropertiesStore(fdbRecordContext, projectId);
        ShareProperties shareProperties = ShareProperties.newBuilder()
            .setShareId(shareObject.getShareId())
            .setName(shareObject.getShareName())
            .setOwnerAccount(shareObject.getOwnerAccount())
            .setOwnerUserId(shareObject.getOwnerUser())
            .setCreateTime(shareObject.getCreateTime())
            .setCatalogId(shareObject.getCatalogId())
            .build();
        sharePropertiesStore.updateRecord(shareProperties);
    }

    @Override
    public ShareObject getShareProperties(TransactionContext context, String projectId, String shareId) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore sharePropertiesStore = DirectoryStoreHelper.getSharePropertiesStore(fdbRecordContext, projectId);
        FDBStoredRecord<Message> storedRecord = sharePropertiesStore.loadRecord(sharePropertiesPrimaryKey(shareId));
        if (storedRecord == null) {
            throw new MetaStoreException(ErrorCode.SHARE_ID_NOT_FOUND, shareId);
        }
        ShareProperties.Builder builder = ShareProperties.newBuilder().mergeFrom(storedRecord.getRecord());
        ShareObject shareObject = new ShareObject(shareId, builder.getName(), builder.getOwnerAccount(),
            builder.getOwnerUserId(), builder.getCreateTime());
        if (builder.hasCatalogId()) {
            shareObject.setCatalogId(builder.getCatalogId());
        }
        return shareObject;
    }

    @Override
    public void deleteShareProperties(TransactionContext context, String projectId, String shareId)  throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore sharePropertiesStore = DirectoryStoreHelper.getSharePropertiesStore(fdbRecordContext, projectId);
        sharePropertiesStore.deleteRecord(sharePropertiesPrimaryKey(shareId));
    }

    @Override
    public void insertShareConsumer(TransactionContext context, String projectId, String shareId, String accountId,
        String managerUser) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore shareConsumerStore = DirectoryStoreHelper.getShareConsumerStore(fdbRecordContext, projectId, shareId);
        ShareConsumer shareConsumer = ShareConsumer.newBuilder()
            .setAccountId(accountId)
            .setManagerUser(managerUser)
            .build();
        shareConsumerStore.insertRecord(shareConsumer);
    }

    @Override
    public void addUsersToShareConsumer(TransactionContext context, String projectId, String shareId, String accountId,
        String[] users) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore shareConsumerStore = DirectoryStoreHelper.getShareConsumerStore(fdbRecordContext, projectId, shareId);
        FDBStoredRecord<Message> storedRecord = shareConsumerStore.loadRecord(shareConsumerPrimaryKey(accountId));
        if (storedRecord == null) {
            throw new MetaStoreException(ErrorCode.SHARE_CONSUMER_NOT_FOUND, shareId, accountId);
        }

        ShareConsumer.Builder builder = ShareConsumer.newBuilder().mergeFrom(storedRecord.getRecord());
        for (String user : users) {
            builder.putUsers(user, user);
        }
        shareConsumerStore.updateRecord(builder.build());
    }

    public Boolean shareConsumerExist(TransactionContext context, String projectId, String shareId,
        String accountId) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore shareConsumerStore = DirectoryStoreHelper.getShareConsumerStore(fdbRecordContext, projectId, shareId);
        FDBStoredRecord<Message> storedRecord = shareConsumerStore.loadRecord(shareConsumerPrimaryKey(accountId));
        return (storedRecord != null);
    }

    public ShareConsumerObject getShareConsumers(TransactionContext context, String projectId, String shareId,
        String accountId) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore shareConsumerStore = DirectoryStoreHelper.getShareConsumerStore(fdbRecordContext, projectId, shareId);
        FDBStoredRecord<Message> storedRecord = shareConsumerStore.loadRecord(shareConsumerPrimaryKey(accountId));
        if (storedRecord == null) {
            throw new MetaStoreException(ErrorCode.SHARE_CONSUMER_NOT_FOUND, shareId, accountId);
        }
        ShareConsumer.Builder builder = ShareConsumer.newBuilder().mergeFrom(storedRecord.getRecord());
        return new ShareConsumerObject(shareId, accountId, builder.getManagerUser(), builder.getUsersMap());
    }

    @Override
    public List<ShareConsumerObject> getShareAllConsumers(TransactionContext context, String projectId, String shareId)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore shareConsumerStore = DirectoryStoreHelper.getShareConsumerStore(fdbRecordContext, projectId, shareId);
        TupleRange tupleRange = TupleRange.ALL;
        List<FDBStoredRecord<Message>> recordList = RecordStoreHelper.listStoreRecords(tupleRange,
            shareConsumerStore, Integer.MAX_VALUE, null, IsolationLevel.SERIALIZABLE);
        List<ShareConsumerObject> shareConsumerObjectList = new ArrayList<>();
        for (FDBStoredRecord<Message> record : recordList) {
            ShareConsumer.Builder builder = ShareConsumer.newBuilder().mergeFrom(record.getRecord());
            shareConsumerObjectList.add(new ShareConsumerObject(shareId, builder.getAccountId(),
                builder.getManagerUser(), builder.getUsersMap()));
            shareConsumerStore.deleteRecord(shareConsumerPrimaryKey(builder.getAccountId()));
        }
        return shareConsumerObjectList;
    }

    @Override
    public void removeUsersFromShareConsumer(TransactionContext context, String projectId, String shareId, String accountId,
        String[] users) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore shareConsumerStore = DirectoryStoreHelper.getShareConsumerStore(fdbRecordContext, projectId, shareId);
        FDBStoredRecord<Message> storedRecord = shareConsumerStore.loadRecord(shareConsumerPrimaryKey(accountId));
        if (storedRecord == null) {
            throw new MetaStoreException(ErrorCode.SHARE_CONSUMER_NOT_FOUND, shareId, accountId);
        }
        ShareConsumer.Builder builder = ShareConsumer.newBuilder().mergeFrom(storedRecord.getRecord());
        for (String user : users) {
            builder.removeUsers(user);
        }
        shareConsumerStore.updateRecord(builder.build());
    }

    @Override
    public void deleteShareConsumer(TransactionContext context, String projectId, String shareId, String accountId)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore shareConsumerStore = DirectoryStoreHelper.getShareConsumerStore(fdbRecordContext, projectId, shareId);
        shareConsumerStore.deleteRecord(shareConsumerPrimaryKey(accountId));
    }

    @Override
    public void delAllShareConsumer(TransactionContext context, String projectId, String shareId) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore shareConsumerStore = DirectoryStoreHelper.getShareConsumerStore(fdbRecordContext, projectId, shareId);
        TupleRange tupleRange = TupleRange.ALL;
        List<FDBStoredRecord<Message>> recordList = RecordStoreHelper.listStoreRecords(tupleRange,
            shareConsumerStore, Integer.MAX_VALUE, null, IsolationLevel.SERIALIZABLE);

        for (FDBStoredRecord<Message> record : recordList) {
            ShareConsumer.Builder builder = ShareConsumer.newBuilder().mergeFrom(record.getRecord());
            shareConsumerStore.deleteRecord(shareConsumerPrimaryKey(builder.getAccountId()));
        }
    }

    @Override
    public void insertSharePrivilege(TransactionContext context, String projectId, String shareId,
        String databaseId, String objectType, String objectId, long privilege) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore sharePrivilegeStore = DirectoryStoreHelper.getSharePrivilegeStore(fdbRecordContext, projectId, shareId);
        SharePrivilege sharePrivilege = SharePrivilege.newBuilder()
            .setDatabaseId(databaseId)
            .setObjectType(objectType)
            .setObjectId(objectId)
            .setPrivilege(privilege)
            .build();
        sharePrivilegeStore.insertRecord(sharePrivilege);
    }

    @Override
    public void updateSharePrivilege(TransactionContext context, String projectId, String shareId,
        String databaseId, String objectType, String objectId, long privilege) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore sharePrivilegeStore = DirectoryStoreHelper.getSharePrivilegeStore(fdbRecordContext, projectId, shareId);
        SharePrivilege sharePrivilege = SharePrivilege.newBuilder()
            .setDatabaseId(databaseId)
            .setObjectType(objectType)
            .setObjectId(objectId)
            .setPrivilege(privilege)
            .build();
        sharePrivilegeStore.updateRecord(sharePrivilege);
    }

    @Override
    public long getSharePrivilege(TransactionContext context, String projectId, String shareId,
        String databaseId, String objectType, String objectId) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore sharePrivilegeStore = DirectoryStoreHelper.getSharePrivilegeStore(fdbRecordContext, projectId, shareId);
        FDBStoredRecord<Message> storedRecord =
            sharePrivilegeStore.loadRecord(sharePrivilegePrimaryKey(databaseId, objectType, objectId));
        if (storedRecord == null) {
            return 0;
        }
        SharePrivilege.Builder builder = SharePrivilege.newBuilder().mergeFrom(storedRecord.getRecord());
        return builder.getPrivilege();
    }

    @Override
    public List<SharePrivilegeObject> getAllSharePrivilege(TransactionContext context, String projectId,
        String shareId) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore sharePrivilegeStore = DirectoryStoreHelper.getSharePrivilegeStore(fdbRecordContext, projectId, shareId);
        TupleRange tupleRange = TupleRange.ALL;
        List<FDBStoredRecord<Message>> recordList = RecordStoreHelper.listStoreRecords(tupleRange,
            sharePrivilegeStore, Integer.MAX_VALUE, null, IsolationLevel.SERIALIZABLE);
        List<SharePrivilegeObject> sharePrivilegeObjectList = new ArrayList<>();
        for (FDBStoredRecord<Message> record : recordList) {
            SharePrivilege.Builder builder = SharePrivilege.newBuilder().mergeFrom(record.getRecord());
            sharePrivilegeObjectList.add(new SharePrivilegeObject(shareId, builder.getDatabaseId(),
                builder.getObjectType(), builder.getObjectId(), builder.getPrivilege()));
        }
        return sharePrivilegeObjectList;
    }

    @Override
    public void deleteSharePrivilege(TransactionContext context, String projectId, String shareId, String databaseId,
        String objectType, String objectId) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore sharePrivilegeStore = DirectoryStoreHelper.getSharePrivilegeStore(fdbRecordContext, projectId, shareId);
        sharePrivilegeStore.deleteRecord(sharePrivilegePrimaryKey(databaseId, objectType, objectId));
    }

    @Override
    public void delAllSharePrivilege(TransactionContext context, String projectId, String shareId) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore sharePrivilegeStore = DirectoryStoreHelper.getSharePrivilegeStore(fdbRecordContext, projectId, shareId);
        TupleRange tupleRange = TupleRange.ALL;
        List<FDBStoredRecord<Message>> recordList = RecordStoreHelper.listStoreRecords(tupleRange,
            sharePrivilegeStore, Integer.MAX_VALUE, null, IsolationLevel.SERIALIZABLE);
        for (FDBStoredRecord<Message> record : recordList) {
            SharePrivilege.Builder builder = SharePrivilege.newBuilder().mergeFrom(record.getRecord());
            sharePrivilegeStore.deleteRecord(sharePrivilegePrimaryKey(builder.getDatabaseId(),
                builder.getObjectType(), builder.getObjectId()));
        }
    }

}
