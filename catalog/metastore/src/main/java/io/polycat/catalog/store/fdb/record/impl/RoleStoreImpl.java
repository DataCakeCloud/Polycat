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
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.CatalogInnerObject;
import io.polycat.catalog.common.model.PrivilegeRolesObject;
import io.polycat.catalog.common.model.RoleObject;
import io.polycat.catalog.common.model.RolePrivilegeObject;
import io.polycat.catalog.common.model.RoleUserObject;
import io.polycat.catalog.common.model.ScanRecordCursorResult;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.plugin.request.input.ShowRolePrivilegesInput;
import io.polycat.catalog.common.utils.UuidUtil;
import io.polycat.catalog.store.api.RoleStore;
import io.polycat.catalog.store.common.StoreMetadata;
import io.polycat.catalog.store.fdb.record.DirectoryStoreHelper;
import io.polycat.catalog.store.fdb.record.RecordStoreHelper;
import io.polycat.catalog.store.fdb.record.TransactionContextUtil;
import io.polycat.catalog.store.protos.*;

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
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "fdb")
public class RoleStoreImpl implements RoleStore {

    private static final Logger logger = Logger.getLogger(RoleStoreImpl.class);

    private static class RoleStoreImplHandler {
        private static final RoleStoreImpl INSTANCE = new RoleStoreImpl();
    }

    public static RoleStoreImpl getInstance() {
        return RoleStoreImpl.RoleStoreImplHandler.INSTANCE;
    }

    private Tuple roleObjectNamePrimaryKey(String roleName) {
        return Tuple.from(roleName);
    }

    private Tuple rolePropertiesPrimaryKey(String roleId) {
        return Tuple.from(roleId);
    }

    private Tuple roleUserPrimaryKey(String roleId, String userId) {
        return Tuple.from(roleId, userId);
    }

    private Tuple rolePrivilegePrimaryKey(String roleId, String objectType, String objectId) {
        return Tuple.from(roleId, objectType, objectId);
    }

    @Override
    public Boolean roleObjectNameExist(TransactionContext context, String projectId, String roleName)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore roleObjectNameStore = DirectoryStoreHelper.getRoleObjectNameStore(fdbRecordContext, projectId);
        FDBStoredRecord<Message> storedRecord = roleObjectNameStore.loadRecord(roleObjectNamePrimaryKey(roleName));
        return storedRecord != null;
    }

    @Override
    public void insertRoleObjectName(TransactionContext context, String projectId, String roleName, String roleId)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore roleObjectNameStore = DirectoryStoreHelper.getRoleObjectNameStore(fdbRecordContext, projectId);
        RoleObjectName roleObjectName = RoleObjectName.newBuilder()
            .setName(roleName)
            .setObjectId(roleId)
            .build();
        roleObjectNameStore.insertRecord(roleObjectName);
    }

    @Override
    public String getRoleId(TransactionContext context, String projectId, String roleName)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore roleObjectNameStore = DirectoryStoreHelper.getRoleObjectNameStore(fdbRecordContext, projectId);
        FDBStoredRecord<Message> storedRecord = roleObjectNameStore.loadRecord(roleObjectNamePrimaryKey(roleName));
        if (storedRecord == null) {
            throw new MetaStoreException(ErrorCode.ROLE_NOT_FOUND, roleName);
        }
        return RoleObjectName.newBuilder().mergeFrom(storedRecord.getRecord()).getObjectId();
    }

    @Override
    public void deleteRoleObjectName(TransactionContext context, String projectId, String roleName) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore roleObjectNameStore = DirectoryStoreHelper.getRoleObjectNameStore(fdbRecordContext, projectId);
        roleObjectNameStore.deleteRecord(roleObjectNamePrimaryKey(roleName));
    }

    @Override
    public void insertRoleProperties(TransactionContext context, String projectId, String roleId, String roleName,
                                     String ownerId, String comment) throws MetaStoreException {
        long createTime = RecordStoreHelper.getCurrentTime();
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore rolePropertiesStore = DirectoryStoreHelper.getRolePropertiesStore(fdbRecordContext, projectId);
        RoleProperties roleProperties = RoleProperties.newBuilder()
            .setRoleId(roleId)
            .setName(roleName)
            .setCreateTime(createTime)
            .setOwnerId(ownerId)
            .setComment(comment)
            .build();
        rolePropertiesStore.insertRecord(roleProperties);
    }

    @Override
    public void updateRoleProperties(TransactionContext context, RoleObject roleObject) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore rolePropertiesStore = DirectoryStoreHelper.getRolePropertiesStore(fdbRecordContext, roleObject.getProjectId());
        RoleProperties roleProperties = RoleProperties.newBuilder()
            .setRoleId(roleObject.getRoleId())
            .setName(roleObject.getRoleName())
            .setCreateTime(roleObject.getCreateTime())
            .setOwnerId(roleObject.getOwnerId())
            .setComment(roleObject.getComment())
            .build();
        rolePropertiesStore.updateRecord(roleProperties);
    }

    @Override
    public RoleObject getRoleProperties(TransactionContext context, String projectId, String roleId) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore rolePropertiesStore = DirectoryStoreHelper.getRolePropertiesStore(fdbRecordContext, projectId);
        FDBStoredRecord<Message> storedRecord = rolePropertiesStore.loadRecord(rolePropertiesPrimaryKey(roleId));
        if (storedRecord == null) {
            throw new MetaStoreException(ErrorCode.ROLE_ID_NOT_FOUND, roleId);
        }
        RoleProperties.Builder builder = RoleProperties.newBuilder().mergeFrom(storedRecord.getRecord());
        return new RoleObject(projectId, builder.getName(), roleId, builder.getOwnerId(), builder.getCreateTime(),
            builder.getComment());
    }

    public String getRoleName(TransactionContext context, CatalogInnerObject catalogInnerObject) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore rolePropertiesStore = DirectoryStoreHelper
            .getRolePropertiesStore(fdbRecordContext, catalogInnerObject.getProjectId());
        FDBStoredRecord<Message> storedRecord = rolePropertiesStore
            .loadRecord(rolePropertiesPrimaryKey(catalogInnerObject.getObjectId()));
        if (storedRecord == null) {
            throw new MetaStoreException(ErrorCode.ROLE_ID_NOT_FOUND, catalogInnerObject.getObjectId());
        }
        RoleProperties.Builder builder = RoleProperties.newBuilder().mergeFrom(storedRecord.getRecord());
        return builder.getName();
    }

    @Override
    public void deleteRoleProperties(TransactionContext context, String projectId, String roleId) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore rolePropertiesStore = DirectoryStoreHelper.getRolePropertiesStore(fdbRecordContext, projectId);
        rolePropertiesStore.deleteRecord(rolePropertiesPrimaryKey(roleId));
    }

    @Override
    public void insertRoleUser(TransactionContext context, String projectId, String roleId, String userId)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore roleUserStore = DirectoryStoreHelper.getRoleUserStore(fdbRecordContext, projectId);
        RoleUser roleUser = RoleUser.newBuilder()
            .setRoleId(roleId)
            .setUserId(userId)
            .build();
        roleUserStore.insertRecord(roleUser);
    }

    private List<RoleUserObject> getRoleUserByFilter(TransactionContext context, String projectId, QueryComponent filter) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore roleUserStore = DirectoryStoreHelper.getRoleUserStore(fdbRecordContext, projectId);
        List<RoleUserObject> roleUserObjectList = new ArrayList<>();
        RecordQuery query = RecordQuery.newBuilder()
            .setRecordType(StoreMetadata.ROLE_USER.getRecordTypeName())
            .setFilter(filter).build();
        try (RecordCursor<FDBQueriedRecord<Message>> cursor = roleUserStore.executeQuery(query)) {
            RecordCursorIterator<FDBQueriedRecord<Message>> iterator = cursor.asIterator();
            while (iterator.hasNext()) {
                RoleUser.Builder builder = RoleUser.newBuilder()
                    .mergeFrom(Objects.requireNonNull(iterator.next()).getRecord());
                RoleUserObject roleUserObject = new RoleUserObject(builder.getRoleId(), builder.getUserId());
                roleUserObjectList.add(roleUserObject);
            }
        }
        return roleUserObjectList;
    }

    @Override
    public List<RoleUserObject> getRoleUsersByRoleId(TransactionContext context, String projectId, String roleId)
        throws MetaStoreException {
        QueryComponent filter = Query.field("role_id").equalsValue(roleId);
        return getRoleUserByFilter(context, projectId, filter);
    }

    @Override
    public List<RoleUserObject> getRoleUsersByUserId(TransactionContext context, String projectId, String userId)
        throws MetaStoreException {
        QueryComponent filter = Query.field("user_id").equalsValue(userId);
        return getRoleUserByFilter(context, projectId, filter);
    }

    @Override
    public boolean deleteRoleUser(TransactionContext context, String projectId, String roleId, String userId)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore roleUserStore = DirectoryStoreHelper.getRoleUserStore(fdbRecordContext, projectId);
        return roleUserStore.deleteRecord(roleUserPrimaryKey(roleId, userId));
    }

    @Override
    public void delAllRoleUser(TransactionContext context, String projectId, String roleId) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore roleUserStore = DirectoryStoreHelper.getRoleUserStore(fdbRecordContext, projectId);
        QueryComponent filter = Query.field("role_id").equalsValue(roleId);
        RecordQuery query = RecordQuery.newBuilder().setRecordType(StoreMetadata.ROLE_USER.getRecordTypeName())
            .setFilter(filter).build();
        try (RecordCursor<FDBQueriedRecord<Message>> cursor = roleUserStore.executeQuery(query)) {
            RecordCursorIterator<FDBQueriedRecord<Message>> iterator = cursor.asIterator();
            while (iterator.hasNext()) {
                RoleUser.Builder roleUser = RoleUser.newBuilder()
                    .mergeFrom(Objects.requireNonNull(iterator.next()).getRecord());
                roleUserStore.deleteRecord(roleUserPrimaryKey(roleId, roleUser.getUserId()));
            }
        }
    }

    @Override
    public void insertRolePrivilege(TransactionContext context, String projectId, String roleId,
        String objectType, String rolePrivilegeObjectId, CatalogInnerObject catalogInnerObject, long privilege) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore rolePrivilegeStore = DirectoryStoreHelper.getRolePrivilegeStore(fdbRecordContext, projectId);
        RolePrivilege rolePrivilege = RolePrivilege.newBuilder()
            .setRoleId(roleId)
            .setObjectType(objectType)
            .setObjectId(rolePrivilegeObjectId)
            .setPrivilege(privilege)
            .setCatalogId(catalogInnerObject.getCatalogId())
            .setDatabaseId(catalogInnerObject.getDatabaseId())
            .build();
        rolePrivilegeStore.insertRecord(rolePrivilege);
    }

    @Override
    public void updateRolePrivilege(TransactionContext context, String projectId,
        RolePrivilegeObject rolePrivilegeObject, long newPrivilege) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore rolePrivilegeStore = DirectoryStoreHelper.getRolePrivilegeStore(fdbRecordContext, projectId);
        RolePrivilege updateRolePrivilege = RolePrivilege.newBuilder()
            .setRoleId(rolePrivilegeObject.getRoleId())
            .setObjectType(rolePrivilegeObject.getObjectType())
            .setObjectId(rolePrivilegeObject.getObjectId())
            .setPrivilege(newPrivilege)
            .setCatalogId(rolePrivilegeObject.getCatalogId())
            .setDatabaseId(rolePrivilegeObject.getDatabaseId())
            .build();
        rolePrivilegeStore.updateRecord(updateRolePrivilege);
    }

    @Override
    public RolePrivilegeObject getRolePrivilege(TransactionContext context, String projectId, String roleId,
        String objectType, String rolePrivilegeObjectId) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore rolePrivilegeStore = DirectoryStoreHelper.getRolePrivilegeStore(fdbRecordContext, projectId);
        FDBStoredRecord<Message> storedRecord = rolePrivilegeStore.
            loadRecord(rolePrivilegePrimaryKey(roleId, objectType, rolePrivilegeObjectId));
        if (storedRecord == null) {
            return null;
        }
        RolePrivilege.Builder builder = RolePrivilege.newBuilder().mergeFrom(storedRecord.getRecord());
        return new RolePrivilegeObject(roleId, builder.getObjectType(), builder.getObjectId(), builder.getPrivilege(),
            builder.getCatalogId(), builder.getDatabaseId());
    }

    @Override
    public  List<RolePrivilegeObject> getRolePrivilege(TransactionContext context, String projectId, String roleId)
        throws MetaStoreException {
        List<RolePrivilegeObject> rolePrivilegeObjectList = new ArrayList<>();
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore rolePrivilegeStore = DirectoryStoreHelper.getRolePrivilegeStore(fdbRecordContext, projectId);
        QueryComponent filter = Query.field("role_id").equalsValue(roleId);
        RecordQuery query = RecordQuery.newBuilder().setRecordType(StoreMetadata.ROLE_PRIVILEGE.getRecordTypeName())
            .setFilter(filter).build();
        try (RecordCursor<FDBQueriedRecord<Message>> cursor = rolePrivilegeStore.executeQuery(query)) {
            RecordCursorIterator<FDBQueriedRecord<Message>> iterator = cursor.asIterator();
            while (iterator.hasNext()) {
                RolePrivilege.Builder builder = RolePrivilege.newBuilder()
                    .mergeFrom(Objects.requireNonNull(iterator.next()).getRecord());
                RolePrivilegeObject rolePrivilegeObject = new RolePrivilegeObject(roleId, builder.getObjectType(),
                    builder.getObjectId(), builder.getPrivilege(), builder.getCatalogId(), builder.getDatabaseId());
                rolePrivilegeObjectList.add(rolePrivilegeObject);
            }
        }
        return rolePrivilegeObjectList;
    }

    @Override
    public void deleteRolePrivilege(TransactionContext context, String projectId, String roleId, String objectType,
        String rolePrivilegeObjectId) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore rolePrivilegeStore = DirectoryStoreHelper.getRolePrivilegeStore(fdbRecordContext, projectId);
        rolePrivilegeStore.deleteRecord(rolePrivilegePrimaryKey(roleId, objectType, rolePrivilegeObjectId));
    }

    private void delRolePrivilegeByFilter(TransactionContext context, String projectId, QueryComponent filter)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore rolePrivilegeStore = DirectoryStoreHelper.getRolePrivilegeStore(fdbRecordContext, projectId);
        RecordQuery query = RecordQuery.newBuilder().setRecordType(StoreMetadata.ROLE_PRIVILEGE.getRecordTypeName())
            .setFilter(filter).build();
        try (RecordCursor<FDBQueriedRecord<Message>> cursor = rolePrivilegeStore.executeQuery(query)) {
            RecordCursorIterator<FDBQueriedRecord<Message>> iterator = cursor.asIterator();
            while (iterator.hasNext()) {
                RolePrivilege.Builder builder = RolePrivilege.newBuilder()
                    .mergeFrom(Objects.requireNonNull(iterator.next()).getRecord());
                rolePrivilegeStore.deleteRecord(
                    rolePrivilegePrimaryKey(builder.getRoleId(), builder.getObjectType(), builder.getObjectId()));
            }
        }
    }

    @Override
    public void delAllRolePrivilege(TransactionContext context, String projectId, String roleId)
        throws MetaStoreException {
        QueryComponent filter = Query.field("role_id").equalsValue(roleId);
        delRolePrivilegeByFilter(context, projectId, filter);
    }

    @Override
    public void removeAllPrivilegeOnObject(TransactionContext context, String projectId, String objectType, String rolePrivilegeObjectId) throws MetaStoreException {
        QueryComponent filter = Query.and(
            Query.field("object_type").equalsValue(objectType),
            Query.field("object_id").equalsValue(rolePrivilegeObjectId));
        delRolePrivilegeByFilter(context, projectId, filter);
    }

    @Override
    public List<RoleObject> getAllRoleObjects(TransactionContext context, String projectId, String userId, String namePattern, boolean containOwner) {
        ArrayList<RoleObject> roleObjects = new ArrayList<>();
        makeOutBoundRoles(context, roleObjects, projectId, userId, namePattern);
        makeInBoundRoles(context, roleObjects, userId, projectId, namePattern);
        return roleObjects;
    }

    @Override
    public List<RoleObject> getAllRoleNames(TransactionContext transactionContext, String projectId, String keyword) {
        FDBRecordContext context = TransactionContextUtil.getFDBRecordContext(transactionContext);
        List<RoleObject> roleObjects = new ArrayList<>();

        FDBRecordStore roleObjectNameStore = DirectoryStoreHelper.getRoleObjectNameStore(context, projectId);
        ScanRecordCursorResult<List<FDBStoredRecord<Message>>> scannedRecords = RecordStoreHelper
                .listStoreRecordsWithToken(TupleRange.ALL, roleObjectNameStore, 0, Integer.MAX_VALUE, null, IsolationLevel.SERIALIZABLE);

        for (FDBStoredRecord<Message> i : scannedRecords.getResult()) {
            RoleObjectName objectName = RoleObjectName
                    .newBuilder()
                    .mergeFrom(i.getRecord()).build();
            if (!searchNamePattern(objectName.getName(), keyword)) {
                continue;
            }
            roleObjects.add(new RoleObject(projectId, objectName.getName(), objectName.getObjectId()));
        }
        return roleObjects;
    }

    @Override
    public List<RolePrivilegeObject> getRoleByIds(TransactionContext context, String projectId, String objectType, List<String> roleIds) {
        //TODO
        return null;
    }

    @Override
    public List<RolePrivilegeObject> showRolePrivileges(TransactionContext context, String projectId, List<String> roleIds,
        ShowRolePrivilegesInput input, int batchNum, long batchOffset) {
        return null;
    }

    @Override
    public List<PrivilegeRolesObject> showPrivilegeRoles(TransactionContext context, String projectId,
        List<String> collect, ShowRolePrivilegesInput input, int batchNum, long batchOffset) {
        return null;
    }

    @Override
    public List<RoleObject> showRoleInfos(TransactionContext context, String projectId, ShowRolePrivilegesInput input) {
        return null;
    }

    @Override
    public void createRoleSubspace(TransactionContext context, String projectId) {

    }

    @Override
    public void dropRoleSubspace(TransactionContext context, String projectId) {

    }

    @Override
    public String generateRoleObjectId(TransactionContext context, String projectId) {
        return UuidUtil.generateUUID32();
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

    private Boolean searchNamePattern(String name, String keyword) {
        if (keyword == null) {
            return true;
        }

        if (keyword.isEmpty()) {
            return true;
        }

        return name.contains(keyword);
    }


    private ScanRecordCursorResult<List<RoleObject>> listOutBoundRoleRecord(TransactionContext context,
        String projectId, String userId, String namePattern, int maxNum, byte[] continuation, IsolationLevel isolationLevel) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore roleObjectNameStore = DirectoryStoreHelper.getRoleObjectNameStore(fdbRecordContext, projectId);
        TupleRange tupleRange = TupleRange.ALL;

        List<RoleObject> roleObjects = new ArrayList<>();

        // get lasted tableId, contains in-use and dropped
        ScanRecordCursorResult<List<FDBStoredRecord<Message>>> scannedRecords = RecordStoreHelper
            .listStoreRecordsWithToken(tupleRange, roleObjectNameStore, 0, maxNum, continuation, isolationLevel);

        for (FDBStoredRecord<Message> i : scannedRecords.getResult()) {
            RoleObjectName roleObjectName  = RoleObjectName
                .newBuilder()
                .mergeFrom(i.getRecord()).build();
            if (!isNamePatternValid(roleObjectName.getName(), namePattern)) {
                continue;
            }

            RoleObject roleObject = getRoleProperties(context, projectId, roleObjectName.getObjectId());
            if (userId != null && !roleObject.getOwnerId().equals(userId)) {
                continue;
            }
            List<RoleUserObject> roleUsers = getRoleUsersByRoleId(context, projectId, roleObjectName.getObjectId());
            List<String> toUsers = new ArrayList<>();
            for (RoleUserObject roleUser : roleUsers) {
                toUsers.add(roleUser.getUserId());
                roleObject.setToUsers(toUsers);
            }

            // userId is null or '' remain all role
            if (roleUsers.size() > 0 || StringUtils.isEmpty(userId)) {
                roleObjects.add(roleObject);
            }
        }

        return new ScanRecordCursorResult<>(roleObjects, scannedRecords.getContinuation().orElse(null));
    }

    public void makeOutBoundRoles(TransactionContext context, List<RoleObject> roleObjects, String projectId,
                                   String userId, String namePattern) {
        ScanRecordCursorResult<List<RoleObject>> result = listOutBoundRoleRecord(context, projectId, userId, namePattern,
            Integer.MAX_VALUE, null, IsolationLevel.SERIALIZABLE);
         for(RoleObject roleObject : result.getResult()) {
             roleObjects.add(roleObject);
         }
    }

    public void makeInBoundRoles(TransactionContext context, List<RoleObject> roleObjects,
                                  String userId, String projectId, String namePattern) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore roleUserStore = DirectoryStoreHelper.getRoleUserStore(fdbRecordContext, projectId);
        RecordQuery roleInQuery = getInBoundRoleQuery(userId, namePattern);
        try (RecordCursor<FDBQueriedRecord<Message>> cursor = roleUserStore.executeQuery(roleInQuery)) {
            RecordCursorIterator<FDBQueriedRecord<Message>> iterator = cursor.asIterator();
            while (iterator.hasNext()) {
                RoleUser roleUser = RoleUser.newBuilder()
                        .mergeFrom(Objects.requireNonNull(iterator.next()).getRecord()).build();
                RoleObject roleObject = getRoleProperties(context, projectId, roleUser.getRoleId());
                roleObjects.add(roleObject);
            }
        }
    }

    private RecordQuery getInBoundRoleQuery(String userId, String namePattern) {
        QueryComponent filter = Query.field("user_id").equalsValue(userId);
        return RecordQuery.newBuilder().setRecordType(StoreMetadata.ROLE_USER.getRecordTypeName()).setFilter(filter).build();
    }

}
