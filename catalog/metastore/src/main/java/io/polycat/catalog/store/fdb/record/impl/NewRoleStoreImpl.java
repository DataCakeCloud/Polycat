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

import io.polycat.catalog.store.api.NewRoleStore;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.CatalogInnerObject;
import io.polycat.catalog.common.model.PrincipalSource;
import io.polycat.catalog.common.model.PrincipalType;
import io.polycat.catalog.common.model.RoleObject;
import io.polycat.catalog.common.model.RolePrincipalObject;
import io.polycat.catalog.common.model.ScanRecordCursorResult;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.store.common.StoreMetadata;
import io.polycat.catalog.store.fdb.record.DirectoryStoreHelper;
import io.polycat.catalog.store.fdb.record.RecordStoreHelper;
import io.polycat.catalog.store.fdb.record.TransactionContextUtil;
import io.polycat.catalog.store.protos.RolePrincipal;
import io.polycat.catalog.store.protos.RoleProperties;

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
public class NewRoleStoreImpl implements NewRoleStore {

    private static final Logger logger = Logger.getLogger(NewRoleStoreImpl.class);

    private static class NewRoleStoreImplHandler {
        private static final NewRoleStoreImpl INSTANCE = new NewRoleStoreImpl();
    }

    public static NewRoleStoreImpl getInstance() {
        return NewRoleStoreImpl.NewRoleStoreImplHandler.INSTANCE;
    }


    private Tuple rolePropertiesPrimaryKey(String roleId) {
        return Tuple.from(roleId);
    }

    private Tuple rolePrincipalPrimaryKey(String roleId, int principalType, int principalSource, String principalId) {
        return Tuple.from(roleId, principalType, principalSource, principalId);
    }

    @Override
    public Boolean roleNameExist(TransactionContext context, String projectId, String roleName)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore rolePropertiesStore = DirectoryStoreHelper.getRolePropertiesStore(fdbRecordContext, projectId);
        QueryComponent filter = Query.field("name").equalsValue(roleName);
        RecordQuery query = RecordQuery.newBuilder()
            .setRecordType(StoreMetadata.ROLE_PROPERTIES.getRecordTypeName())
            .setFilter(filter).build();
        try (RecordCursor<FDBQueriedRecord<Message>> cursor = rolePropertiesStore.executeQuery(query)) {
            RecordCursorIterator<FDBQueriedRecord<Message>> iterator = cursor.asIterator();
            while (iterator.hasNext()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void insertRoleProperties(TransactionContext context, String projectId, String roleId, String roleName,
        String ownerId) throws MetaStoreException {
        long createTime = RecordStoreHelper.getCurrentTime();
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore rolePropertiesStore = DirectoryStoreHelper.getRolePropertiesStore(fdbRecordContext, projectId);
        RoleProperties roleProperties = RoleProperties.newBuilder()
            .setRoleId(roleId)
            .setName(roleName)
            .setCreateTime(createTime)
            .setOwnerId(ownerId)
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
    public RoleObject getRolePropertiesByRoleId(TransactionContext context, String projectId, String roleId) throws MetaStoreException {
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

    @Override
    public RoleObject getRolePropertiesByRoleName(TransactionContext context, String projectId, String roleName)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore rolePropertiesStore = DirectoryStoreHelper.getRolePropertiesStore(fdbRecordContext, projectId);
        QueryComponent filter = Query.field("name").equalsValue(roleName);
        RecordQuery query = RecordQuery.newBuilder()
            .setRecordType(StoreMetadata.ROLE_PROPERTIES.getRecordTypeName())
            .setFilter(filter).build();
        try (RecordCursor<FDBQueriedRecord<Message>> cursor = rolePropertiesStore.executeQuery(query)) {
            RecordCursorIterator<FDBQueriedRecord<Message>> iterator = cursor.asIterator();
            while (iterator.hasNext()) {
                RoleProperties.Builder builder = RoleProperties.newBuilder().
                    mergeFrom(Objects.requireNonNull(iterator.next()).getRecord());
                return new RoleObject(projectId, roleName, builder.getRoleId(), builder.getOwnerId(), builder.getCreateTime(),
                    builder.getComment());
            }
        }
        return null;
    }

    @Override
    public String getRoleId(TransactionContext context, String projectId, String roleName)
        throws MetaStoreException {
        RoleObject roleObject = getRolePropertiesByRoleName(context, projectId, roleName);
        if (roleObject == null) {
            throw new MetaStoreException(ErrorCode.ROLE_NOT_FOUND, roleName);
        }
        return roleObject.getRoleId();
    }

    public String getRoleName(TransactionContext context, CatalogInnerObject catalogObject) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore rolePropertiesStore = DirectoryStoreHelper
            .getRolePropertiesStore(fdbRecordContext, catalogObject.getProjectId());
        FDBStoredRecord<Message> storedRecord = rolePropertiesStore
            .loadRecord(rolePropertiesPrimaryKey(catalogObject.getObjectId()));
        if (storedRecord == null) {
            throw new MetaStoreException(ErrorCode.ROLE_ID_NOT_FOUND, catalogObject.getObjectId());
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
    public void insertRolePrincipal(TransactionContext context, String projectId, String roleId,
        PrincipalType principalType, PrincipalSource principalSource, String principalId)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore rolePrincipalStore = DirectoryStoreHelper.getRolePrincipalStore(fdbRecordContext, projectId);
        RolePrincipal rolePrincipal = RolePrincipal.newBuilder()
            .setRoleId(roleId)
            .setPrincipalType(principalType.getNum())
            .setPrincipalSource(principalSource.getNum())
            .setPrincipalId(principalId)
            .build();
        rolePrincipalStore.insertRecord(rolePrincipal);
    }

    private List<RolePrincipalObject> getRolePrincipalByFilter(TransactionContext context, String projectId, QueryComponent filter) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore rolePrincipalStore = DirectoryStoreHelper.getRolePrincipalStore(fdbRecordContext, projectId);
        List<RolePrincipalObject> rolePrincipalObjectList = new ArrayList<>();
        RecordQuery query = RecordQuery.newBuilder()
            .setRecordType(StoreMetadata.ROLE_PRINCIPAL.getRecordTypeName())
            .setFilter(filter).build();
        try (RecordCursor<FDBQueriedRecord<Message>> cursor = rolePrincipalStore.executeQuery(query)) {
            RecordCursorIterator<FDBQueriedRecord<Message>> iterator = cursor.asIterator();
            while (iterator.hasNext()) {
                RolePrincipal.Builder builder = RolePrincipal.newBuilder()
                    .mergeFrom(Objects.requireNonNull(iterator.next()).getRecord());
                RolePrincipalObject rolePrincipalObject = new RolePrincipalObject(builder.getRoleId(),
                    builder.getPrincipalType(), builder.getPrincipalSource(), builder.getPrincipalId());
                rolePrincipalObjectList.add(rolePrincipalObject);
            }
        }
        return rolePrincipalObjectList;
    }

    @Override
    public List<RolePrincipalObject> getRolePrincipalsByRoleId(TransactionContext context, String projectId, String roleId)
        throws MetaStoreException {
        QueryComponent filter = Query.field("role_id").equalsValue(roleId);
        return getRolePrincipalByFilter(context, projectId, filter);
    }

    @Override
    public List<RolePrincipalObject> getRolePrincipalsByUserId(TransactionContext context, String projectId,
            PrincipalType principalType, PrincipalSource principalSource, String principalId)
        throws MetaStoreException {
        QueryComponent filter = Query.and(Query.field("principal_type").equalsValue(principalType.getNum()),
            Query.field("principal_source").equalsValue(principalSource.getNum()),
            Query.field("principal_id").equalsValue(principalId));
        return getRolePrincipalByFilter(context, projectId, filter);
    }

    @Override
    public void deleteRolePrincipal(TransactionContext context, String projectId, String roleId,
                PrincipalType principalType, PrincipalSource principalSource, String principalId)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore rolePrincipalStore = DirectoryStoreHelper.getRolePrincipalStore(fdbRecordContext, projectId);
        rolePrincipalStore.deleteRecord(rolePrincipalPrimaryKey(roleId, principalType.getNum(),
            principalSource.getNum(), principalId));
    }

    @Override
    public void delAllRolePrincipal(TransactionContext context, String projectId, String roleId) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore rolePrincipalStore = DirectoryStoreHelper.getRolePrincipalStore(fdbRecordContext, projectId);
        QueryComponent filter = Query.field("role_id").equalsValue(roleId);
        RecordQuery query = RecordQuery.newBuilder().setRecordType(StoreMetadata.ROLE_PRINCIPAL.getRecordTypeName())
            .setFilter(filter).build();
        try (RecordCursor<FDBQueriedRecord<Message>> cursor = rolePrincipalStore.executeQuery(query)) {
            RecordCursorIterator<FDBQueriedRecord<Message>> iterator = cursor.asIterator();
            while (iterator.hasNext()) {
                RolePrincipal.Builder rolePrincipal = RolePrincipal.newBuilder()
                    .mergeFrom(Objects.requireNonNull(iterator.next()).getRecord());
                rolePrincipalStore.deleteRecord(rolePrincipalPrimaryKey(roleId, rolePrincipal.getPrincipalType(),
                    rolePrincipal.getPrincipalSource(), rolePrincipal.getPrincipalId()));
            }
        }
    }


    private ScanRecordCursorResult<List<RoleObject>> listOutBoundRoleRecord(TransactionContext context,
        String projectId, String userId, String namePattern, int maxNum, byte[] continuation, IsolationLevel isolationLevel) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore rolePropertiesStore = DirectoryStoreHelper.getRolePropertiesStore(fdbRecordContext, projectId);
        TupleRange tupleRange = TupleRange.ALL;

        List<RoleObject> roleObjects = new ArrayList<>();

        // get lasted tableId, contains in-use and dropped
        ScanRecordCursorResult<List<FDBStoredRecord<Message>>> scannedRecords = RecordStoreHelper
            .listStoreRecordsWithToken(tupleRange, rolePropertiesStore, 0, maxNum, continuation, isolationLevel);

        for (FDBStoredRecord<Message> i : scannedRecords.getResult()) {
            RoleProperties roleProperties  = RoleProperties
                .newBuilder()
                .mergeFrom(i.getRecord()).build();
            if (!isNamePatternValid(roleProperties.getName(), namePattern)) {
                continue;
            }

            RoleObject roleObject =  new RoleObject(projectId, roleProperties.getName(), roleProperties.getRoleId(),
                roleProperties.getOwnerId(), roleProperties.getCreateTime(),
                roleProperties.getComment());
            if (!roleObject.getOwnerId().equals(userId)) {
                continue;
            }
            List<RolePrincipalObject> rolePrincipals = getRolePrincipalsByRoleId(context, projectId, roleProperties.getRoleId());
            List<String> toUsers = new ArrayList<>();
            for (RolePrincipalObject rolePrincipal : rolePrincipals) {
                //toUsers.add(rolePrincipal.getUserId());
                StringBuffer sb = new StringBuffer();
                sb.append(PrincipalType.getPrincipalType(rolePrincipal.getPrincipalType())).append(":")
                    .append(PrincipalSource.getPrincipalSource(rolePrincipal.getPrincipalSource())).append(":")
                    .append(rolePrincipal.getPrincipalId());

                toUsers.add(sb.toString());
                roleObject.setToUsers(toUsers);
            }

            roleObjects.add(roleObject);
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
        FDBRecordStore rolePrincipalStore = DirectoryStoreHelper.getRolePrincipalStore(fdbRecordContext, projectId);
        RecordQuery roleInQuery = getInBoundRoleQuery(userId, namePattern);
        try (RecordCursor<FDBQueriedRecord<Message>> cursor = rolePrincipalStore.executeQuery(roleInQuery)) {
            RecordCursorIterator<FDBQueriedRecord<Message>> iterator = cursor.asIterator();
            while (iterator.hasNext()) {
                RolePrincipal rolePrincipal = RolePrincipal.newBuilder()
                    .mergeFrom(Objects.requireNonNull(iterator.next()).getRecord()).build();
                RoleObject roleObject = getRolePropertiesByRoleId(context, projectId, rolePrincipal.getRoleId());
                roleObjects.add(roleObject);
            }
        }
    }

    private RecordQuery getInBoundRoleQuery(String userId, String namePattern) {
        QueryComponent filter = Query.field("principal_id").equalsValue(userId);
        return RecordQuery.newBuilder().setRecordType(StoreMetadata.ROLE_PRINCIPAL.getRecordTypeName()).setFilter(filter).build();
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
