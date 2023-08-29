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

import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.MetaPolicyHistory;
import io.polycat.catalog.common.model.MetaPrivilegePolicy;
import io.polycat.catalog.common.model.ObsPrivilegePolicy;
import io.polycat.catalog.common.model.PrincipalType;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.utils.UuidUtil;
import io.polycat.catalog.store.api.PolicyStore;
import io.polycat.catalog.store.common.StoreMetadata;
import io.polycat.catalog.store.fdb.record.DirectoryStoreHelper;
import io.polycat.catalog.store.fdb.record.RecordStoreHelper;
import io.polycat.catalog.store.fdb.record.TransactionContextUtil;
import io.polycat.catalog.store.protos.DataPrivilegePolicyRecord;
import io.polycat.catalog.store.protos.MetaPrivilegePolicyHistoryRecord;
import io.polycat.catalog.store.protos.MetaPrivilegePolicyRecord;
import io.polycat.catalog.store.protos.ShareDataPrivilegePolicyRecord;
import io.polycat.catalog.store.protos.ShareMetaPrivilegePolicyHistoryRecord;
import io.polycat.catalog.store.protos.ShareMetaPrivilegePolicyRecord;

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "fdb")
public class PolicyStoreImpl implements PolicyStore {

    private static final Logger logger = Logger.getLogger(PolicyStoreImpl.class);

    private final long timeLimit = 40000;
    private final int maxBatchRowNum = 2048;
    private static int dealMaxNumRecordPerTrans = 2048;



    private static class PolicyStoreImplHandler {
        private static final PolicyStoreImpl INSTANCE = new PolicyStoreImpl();
    }

    public static PolicyStoreImpl getInstance() {
        return PolicyStoreImpl.PolicyStoreImplHandler.INSTANCE;
    }


    private Tuple metaPrivilegePolicyPrimaryKey(String projectId,
        PrincipalType principalType, int principalSource, String principalId,
        int objectType, String objectId, boolean effect,
        long privilege) {

        if (principalType == PrincipalType.SHARE) {
            return Tuple.from(projectId, principalType.getNum(), principalSource, principalId, objectType, objectId, effect, privilege);
        }
        return Tuple.from(principalType.getNum(), principalSource, principalId, objectType, objectId, effect, privilege);
    }

    private Tuple metaPrivilegePolicyHistoryPrimaryKey(String projectId, String policyId,
        PrincipalType principalType) {

        if (principalType == PrincipalType.SHARE) {
            return Tuple.from(projectId, policyId);
        }
        return Tuple.from(principalType.getNum(), policyId);
    }

    private Tuple dataPrivilegePolicyPrimaryKey(String projectId,
        PrincipalType principalType, int principalSource, String principalId, String obsPath, String obsEndpoint) {

        if (principalType == PrincipalType.SHARE) {
            return Tuple.from(projectId, principalType.getNum(), principalSource, principalId, obsPath, obsEndpoint);
        }
        return Tuple.from(principalType.getNum(), principalSource, principalId, obsPath, obsEndpoint);
    }


    @Override
    public String insertMetaPrivilegePolicy(TransactionContext context, String projectId,
        PrincipalType principalType, int principalSource, String principalId,
        int objectType, String objectId, boolean effect,
        long privilege, boolean isOwner, String condition, String obligation,
        long updateTime, boolean grantAble) throws MetaStoreException {
        if (isOwner) {
            //privilege = getObjectOwnerPrivilegesByType(objectType);
            privilege = 0;
        }
        String policyId = UuidUtil.generateId();
        if (updateTime == 0) {
            updateTime = RecordStoreHelper.getCurrentTime();
        }
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        if (principalType == PrincipalType.SHARE) {
            FDBRecordStore shareMetaPrivilegePolicyStore = DirectoryStoreHelper.getShareMetaPrivilegePolicyStore(fdbRecordContext);
            ShareMetaPrivilegePolicyRecord.Builder shareMetaPrivilegePolicyRecordBuilder =
                ShareMetaPrivilegePolicyRecord.newBuilder()
                    .setPolicyId(policyId)
                    .setProjectId(projectId)
                    .setPrincipalType(principalType.getNum()).setPrincipalSource(principalSource)
                    .setPrincipalId(principalId)
                    .setObjectType(objectType).setObjectId(objectId)
                    .setEffect(effect).setPrivilege(privilege).setGrantAble(grantAble)
                    .setUpdateTime(updateTime);
            if (condition != null) {
                shareMetaPrivilegePolicyRecordBuilder.setCondition(condition);
            }
            if (obligation != null) {
                shareMetaPrivilegePolicyRecordBuilder.setObligation(obligation);
            }
            shareMetaPrivilegePolicyStore.insertRecord(shareMetaPrivilegePolicyRecordBuilder.build());
        } else {
            FDBRecordStore metaPrivilegePolicyStore = DirectoryStoreHelper.getMetaPrivilegePolicyStore(fdbRecordContext,
                projectId);
            MetaPrivilegePolicyRecord.Builder metaPrivilegePolicyRecordBuilder =
                MetaPrivilegePolicyRecord.newBuilder()
                    .setPolicyId(policyId)
                    .setPrincipalType(principalType.getNum()).setPrincipalSource(principalSource)
                    .setPrincipalId(principalId)
                    .setObjectType(objectType).setObjectId(objectId)
                    .setEffect(effect).setPrivilege(privilege).setGrantAble(grantAble)
                    .setUpdateTime(updateTime);
            if (condition != null) {
                metaPrivilegePolicyRecordBuilder.setCondition(condition);
            }
            if (obligation != null) {
                metaPrivilegePolicyRecordBuilder.setObligation(obligation);
            }
            metaPrivilegePolicyStore.insertRecord(metaPrivilegePolicyRecordBuilder.build());
        }
        return policyId;
    }

    @Override
    public String  insertDataPrivilegePolicy(TransactionContext context, String projectId,
        PrincipalType principalType, int principalSource, String principalId,
        String obsPath, String obsEndpoint, int permission,
        long updateTime) throws MetaStoreException {

        String policyId = UuidUtil.generateId();
        if (updateTime == 0) {
            updateTime = RecordStoreHelper.getCurrentTime();
        }
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        if (principalType == PrincipalType.SHARE) {
            FDBRecordStore shareDataPrivilegePolicyStore = DirectoryStoreHelper.getShareDataPrivilegePolicyStore(fdbRecordContext);
            ShareDataPrivilegePolicyRecord shareDataPrivilegePolicyRecord = ShareDataPrivilegePolicyRecord.newBuilder()
                .setPolicyId(policyId)
                .setProjectId(projectId)
                .setPrincipalType(principalType.getNum())
                .setPrincipalSource(principalSource)
                .setPrincipalId(principalId)
                .setObsPath(obsPath)
                .setObsEndpoint(obsEndpoint)
                .setPermission(permission)
                .setUpdateTime(updateTime)
                .build();
            shareDataPrivilegePolicyStore.insertRecord(shareDataPrivilegePolicyRecord);
        } else {
            FDBRecordStore dataPrivilegePolicyStore = DirectoryStoreHelper.getDataPrivilegePolicyStore(fdbRecordContext,
                projectId);
            DataPrivilegePolicyRecord dataPrivilegePolicyRecord = DataPrivilegePolicyRecord.newBuilder()
                .setPolicyId(policyId)
                .setPrincipalType(principalType.getNum())
                .setPrincipalSource(principalSource)
                .setPrincipalId(principalId)
                .setObsPath(obsPath)
                .setObsEndpoint(obsEndpoint)
                .setPermission(permission)
                .setUpdateTime(updateTime)
                .build();
            dataPrivilegePolicyStore.insertRecord(dataPrivilegePolicyRecord);
        }
        return policyId;
    }

    @Override
    public String deleteMetaPrivilegePolicy(TransactionContext context, String projectId,
        PrincipalType principalType, int principalSource, String principalId,
        int objectType, String objectId, boolean effect, long privilege, boolean isOwner) throws MetaStoreException {

        if (isOwner) {
            //privilege = getObjectOwnerPrivilegesByType(objectType);
            privilege = 0;
        }
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore metaPrivilegePolicyStore;
        String policyId = null;
        if (principalType == PrincipalType.SHARE) {
            metaPrivilegePolicyStore = DirectoryStoreHelper.getShareMetaPrivilegePolicyStore(fdbRecordContext);
            FDBStoredRecord<Message> storedRecord = metaPrivilegePolicyStore.
                loadRecord(metaPrivilegePolicyPrimaryKey(projectId,principalType,principalSource,principalId,
                    objectType,objectId,effect,privilege));
            if (storedRecord != null) {
                ShareMetaPrivilegePolicyRecord.Builder builder = ShareMetaPrivilegePolicyRecord
                    .newBuilder().mergeFrom(storedRecord.getRecord());
                policyId = builder.getPolicyId();
            }

        } else {
            metaPrivilegePolicyStore = DirectoryStoreHelper.getMetaPrivilegePolicyStore(fdbRecordContext,
                projectId);
            FDBStoredRecord<Message> storedRecord = metaPrivilegePolicyStore.
                loadRecord(metaPrivilegePolicyPrimaryKey(projectId,principalType,principalSource,principalId,
                    objectType,objectId,effect,privilege));
            if (storedRecord != null) {
                MetaPrivilegePolicyRecord.Builder builder = MetaPrivilegePolicyRecord
                    .newBuilder().mergeFrom(storedRecord.getRecord());
                policyId = builder.getPolicyId();
            }

        }

        metaPrivilegePolicyStore.deleteRecord(metaPrivilegePolicyPrimaryKey(projectId,principalType,principalSource,principalId,
            objectType,objectId,effect,privilege));

        return policyId;
    }

    @Override
    public String deleteDataPrivilegePolicy(TransactionContext context, String projectId,
        PrincipalType principalType, int principalSource, String principalId,
        String obsPath, String obsEndpoint) throws MetaStoreException {

        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore dataPrivilegePolicyStore;
        String policyId = null;
        if (principalType == PrincipalType.SHARE) {
            dataPrivilegePolicyStore = DirectoryStoreHelper.getShareDataPrivilegePolicyStore(fdbRecordContext);
            FDBStoredRecord<Message> storedRecord = dataPrivilegePolicyStore.
                loadRecord(dataPrivilegePolicyPrimaryKey(projectId,principalType,principalSource,principalId,
                    obsPath,obsEndpoint));
            if (storedRecord != null) {
                ShareDataPrivilegePolicyRecord.Builder builder = ShareDataPrivilegePolicyRecord
                    .newBuilder().mergeFrom(storedRecord.getRecord());
                policyId = builder.getPolicyId();
            }
        } else {
            dataPrivilegePolicyStore = DirectoryStoreHelper.getDataPrivilegePolicyStore(fdbRecordContext,
                projectId);
            FDBStoredRecord<Message> storedRecord = dataPrivilegePolicyStore.
                loadRecord(dataPrivilegePolicyPrimaryKey(projectId,principalType,principalSource,principalId,
                    obsPath,obsEndpoint));
            if (storedRecord != null) {
                DataPrivilegePolicyRecord.Builder builder = DataPrivilegePolicyRecord
                    .newBuilder().mergeFrom(storedRecord.getRecord());
                policyId = builder.getPolicyId();
            }
        }
        dataPrivilegePolicyStore.deleteRecord(dataPrivilegePolicyPrimaryKey(projectId,principalType,principalSource,principalId,
            obsPath,obsEndpoint));
        return policyId;
    }

    @Override
    public String updateMetaPrivilegePolicy(TransactionContext context, String projectId, PrincipalType principalType,
        MetaPrivilegePolicy metaPrivilegePolicy, String condition, String obligation, long updateTime,
        boolean grantAble) throws MetaStoreException  {

        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        String policyId = null;
        if (updateTime == 0) {
            updateTime = RecordStoreHelper.getCurrentTime();
        }
        if (principalType == PrincipalType.SHARE) {
            FDBRecordStore metaPrivilegePolicyStore = DirectoryStoreHelper.getShareMetaPrivilegePolicyStore(fdbRecordContext);
            ShareMetaPrivilegePolicyRecord.Builder metaPrivilegePolicyRecordBuilder = ShareMetaPrivilegePolicyRecord.newBuilder()
                .setPolicyId(metaPrivilegePolicy.getPolicyId())
                .setProjectId(metaPrivilegePolicy.getProjectId())
                .setPrincipalType(metaPrivilegePolicy.getPrincipalType())
                .setPrincipalSource(metaPrivilegePolicy.getPrincipalSource())
                .setPrincipalId(metaPrivilegePolicy.getPrincipalId())
                .setObjectType(metaPrivilegePolicy.getObjectType())
                .setObjectId(metaPrivilegePolicy.getObjectId())
                .setEffect(metaPrivilegePolicy.isEffect())
                .setPrivilege(metaPrivilegePolicy.getPrivilege())
                .setGrantAble(grantAble)
                .setUpdateTime(updateTime);
            if (condition != null) {
                metaPrivilegePolicyRecordBuilder.setCondition(condition);
            }
            if (obligation != null) {
                metaPrivilegePolicyRecordBuilder.setObligation(obligation);
            }
            policyId = metaPrivilegePolicy.getPolicyId();
            metaPrivilegePolicyStore.updateRecord(metaPrivilegePolicyRecordBuilder.build());
        } else {
            FDBRecordStore  metaPrivilegePolicyStore = DirectoryStoreHelper.getMetaPrivilegePolicyStore(fdbRecordContext,
                projectId);
            MetaPrivilegePolicyRecord.Builder metaPrivilegePolicyRecordBuilder = MetaPrivilegePolicyRecord.newBuilder()
                .setPolicyId(metaPrivilegePolicy.getPolicyId())
                .setPrincipalType(metaPrivilegePolicy.getPrincipalType())
                .setPrincipalSource(metaPrivilegePolicy.getPrincipalSource())
                .setPrincipalId(metaPrivilegePolicy.getPrincipalId())
                .setObjectType(metaPrivilegePolicy.getObjectType())
                .setObjectId(metaPrivilegePolicy.getObjectId())
                .setEffect(metaPrivilegePolicy.isEffect())
                .setPrivilege(metaPrivilegePolicy.getPrivilege())
                .setGrantAble(grantAble)
                .setUpdateTime(updateTime);
            if (condition != null) {
                metaPrivilegePolicyRecordBuilder.setCondition(condition);
            }
            if (obligation != null) {
                metaPrivilegePolicyRecordBuilder.setObligation(obligation);
            }
            policyId = metaPrivilegePolicy.getPolicyId();
            metaPrivilegePolicyStore.updateRecord(metaPrivilegePolicyRecordBuilder.build());
        }
        return policyId;
    }

    @Override
    public String updateDataPrivilegePolicy(TransactionContext context, String projectId, PrincipalType principalType,
        ObsPrivilegePolicy dataPrivilegePolicy, int  permission, long updateTime) throws MetaStoreException  {

        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        String policyId = null;
        if (updateTime == 0) {
            updateTime = RecordStoreHelper.getCurrentTime();
        }
        if (principalType == PrincipalType.SHARE) {
            FDBRecordStore dataPrivilegePolicyStore = DirectoryStoreHelper.getShareDataPrivilegePolicyStore(fdbRecordContext);
            ShareDataPrivilegePolicyRecord dataPrivilegePolicyRecord = ShareDataPrivilegePolicyRecord.newBuilder()
                .setPolicyId(dataPrivilegePolicy.getPolicyId())
                .setProjectId(dataPrivilegePolicy.getProjectId())
                .setPrincipalType(dataPrivilegePolicy.getPrincipalType())
                .setPrincipalSource(dataPrivilegePolicy.getPrincipalSource())
                .setPrincipalId(dataPrivilegePolicy.getPrincipalId())
                .setObsPath(dataPrivilegePolicy.getObsPath())
                .setObsEndpoint(dataPrivilegePolicy.getObsEndpoint())
                .setPermission(permission)
                .setUpdateTime(updateTime)
                .build();
            policyId = dataPrivilegePolicy.getPolicyId();
            dataPrivilegePolicyStore.updateRecord(dataPrivilegePolicyRecord);
        } else {
            FDBRecordStore  dataPrivilegePolicyStore = DirectoryStoreHelper.getDataPrivilegePolicyStore(fdbRecordContext,
                projectId);
            DataPrivilegePolicyRecord dataPrivilegePolicyRecord = DataPrivilegePolicyRecord.newBuilder()
                .setPolicyId(dataPrivilegePolicy.getPolicyId())
                .setPrincipalType(dataPrivilegePolicy.getPrincipalType())
                .setPrincipalSource(dataPrivilegePolicy.getPrincipalSource())
                .setPrincipalId(dataPrivilegePolicy.getPrincipalId())
                .setObsPath(dataPrivilegePolicy.getObsPath())
                .setObsEndpoint(dataPrivilegePolicy.getObsEndpoint())
                .setPermission(permission)
                .setUpdateTime(updateTime)
                .build();
            policyId = dataPrivilegePolicy.getPolicyId();
            dataPrivilegePolicyStore.updateRecord(dataPrivilegePolicyRecord);
        }
        return policyId;
    }


    @Override
    public  MetaPrivilegePolicy getMetaPrivilegePolicy(TransactionContext context, String projectId,
        PrincipalType principalType, int principalSource, String principalId,
        int objectType, String objectId, boolean effect, long privilege) throws MetaStoreException {

        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        if (principalType == PrincipalType.SHARE) {
            FDBRecordStore metaPrivilegePolicyStore = DirectoryStoreHelper.getShareMetaPrivilegePolicyStore(fdbRecordContext);
            FDBStoredRecord<Message> storedRecord = metaPrivilegePolicyStore.
                loadRecord(metaPrivilegePolicyPrimaryKey(projectId,principalType,principalSource,principalId,
                        objectType,objectId,effect,privilege));
            if (storedRecord == null) {
                return null;
            }
            ShareMetaPrivilegePolicyRecord.Builder builder = ShareMetaPrivilegePolicyRecord.newBuilder().mergeFrom(storedRecord.getRecord());
            MetaPrivilegePolicy metaPrivilegePolicyObject  =
                MetaPrivilegePolicy.builder()
                    .setPolicyId(builder.getPolicyId())
                    .setProjectId(projectId)
                    .setPrincipalType(principalType.getNum())
                    .setPrincipalSource(principalSource)
                    .setPrincipalId(principalId)
                    .setObjectType(builder.getObjectType())
                    .setObjectId(builder.getObjectId())
                    .setEffect(builder.getEffect())
                    .setPrivilege(builder.getPrivilege())
                    .setCondition(builder.getCondition())
                    .setObligation(builder.getObligation())
                    .setGrantAble(builder.getGrantAble())
                    .setUpdateTime(builder.getUpdateTime())
                    .build();
            return metaPrivilegePolicyObject;

        } else {
            FDBRecordStore metaPrivilegePolicyStore = DirectoryStoreHelper.getMetaPrivilegePolicyStore(fdbRecordContext,projectId);
            FDBStoredRecord<Message> storedRecord = metaPrivilegePolicyStore.
                loadRecord(metaPrivilegePolicyPrimaryKey(projectId,principalType,principalSource,principalId,
                    objectType,objectId,effect,privilege));
            if (storedRecord == null) {
                return null;
            }
            MetaPrivilegePolicyRecord.Builder builder = MetaPrivilegePolicyRecord.newBuilder().mergeFrom(storedRecord.getRecord());
            MetaPrivilegePolicy metaPrivilegePolicyObject  =
                MetaPrivilegePolicy.builder()
                    .setPolicyId(builder.getPolicyId())
                    .setProjectId(projectId)
                    .setPrincipalType(principalType.getNum())
                    .setPrincipalSource(principalSource)
                    .setPrincipalId(principalId)
                    .setObjectType(builder.getObjectType())
                    .setObjectId(builder.getObjectId())
                    .setEffect(builder.getEffect())
                    .setPrivilege(builder.getPrivilege())
                    .setCondition(builder.getCondition())
                    .setObligation(builder.getObligation())
                    .setGrantAble(builder.getGrantAble())
                    .setUpdateTime(builder.getUpdateTime())
                    .build();
            return metaPrivilegePolicyObject;
        }

    }

    @Override
    public ObsPrivilegePolicy getDataPrivilegePolicy(TransactionContext context, String projectId,
        PrincipalType principalType, int principalSource, String principalId,
        String obsPath, String obsEndpoint) throws MetaStoreException {

        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        if (principalType == PrincipalType.SHARE) {
            FDBRecordStore dataPrivilegePolicyStore = DirectoryStoreHelper.getShareDataPrivilegePolicyStore(fdbRecordContext);
            FDBStoredRecord<Message> storedRecord = dataPrivilegePolicyStore.
                loadRecord(dataPrivilegePolicyPrimaryKey(projectId,principalType,principalSource,principalId,
                    obsPath,obsEndpoint));
            if (storedRecord == null) {
                return null;
            }
            ShareDataPrivilegePolicyRecord.Builder builder = ShareDataPrivilegePolicyRecord.newBuilder().mergeFrom(storedRecord.getRecord());
            return new ObsPrivilegePolicy(builder.getPolicyId(), projectId,
                principalType.getNum(), principalSource, principalId,
                builder.getObsPath(), builder.getObsEndpoint(),
                builder.getPermission(), builder.getUpdateTime());
        } else {
            FDBRecordStore dataPrivilegePolicyStore = DirectoryStoreHelper.getDataPrivilegePolicyStore(fdbRecordContext, projectId);
            FDBStoredRecord<Message> storedRecord = dataPrivilegePolicyStore.
                loadRecord(dataPrivilegePolicyPrimaryKey(projectId,principalType,principalSource,principalId,
                    obsPath,obsEndpoint));
            if (storedRecord == null) {
                return null;
            }
            DataPrivilegePolicyRecord.Builder builder = DataPrivilegePolicyRecord.newBuilder().mergeFrom(storedRecord.getRecord());
            return new ObsPrivilegePolicy(builder.getPolicyId(), projectId,
                principalType.getNum(), principalSource, principalId,
                builder.getObsPath(), builder.getObsEndpoint(),
                builder.getPermission(), builder.getUpdateTime());
        }

    }

    @Override
    public List<MetaPrivilegePolicy>  getMetaPrivilegesByPrincipal(TransactionContext context, String projectId,
        PrincipalType principalType, int principalSource, String principalId) {

        List<MetaPrivilegePolicy> privilegesList = new ArrayList<>();
        byte[] continuation = null;
        while (true) {
            continuation = getMetaPrivilegesByPrincipalWithToken(context, projectId,
                principalType, principalSource, principalId, privilegesList, continuation);
            if (continuation == null) {
                break;
            }
        }
        return privilegesList;
    }

    @Override
    public byte[] getMetaPrivilegesByPrincipalWithToken(TransactionContext context, String projectId,
                                                        PrincipalType principalType, int principalSource, String principalId,
                                                        List<MetaPrivilegePolicy> privilegesList, byte[] continuation)
        throws MetaStoreException {

        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore metaPrivilegePolicyStore;
        QueryComponent filter;
        RecordQuery query;
        if (principalType == PrincipalType.SHARE) {
            metaPrivilegePolicyStore = DirectoryStoreHelper.getShareMetaPrivilegePolicyStore(fdbRecordContext);
            filter = Query.and(Query.field("project_id").equalsValue(projectId),
                Query.field("principal_type").equalsValue(principalType.getNum()),
                Query.field("principal_source").equalsValue(principalSource),
                Query.field("principal_id").equalsValue(principalId));
            query = RecordQuery.newBuilder()
                .setRecordType(StoreMetadata.GLOBAL_SHARE_META_PRIVILEGE_POLICY.getRecordTypeName())
                .setFilter(filter)
                .build();
        } else {
            metaPrivilegePolicyStore = DirectoryStoreHelper.getMetaPrivilegePolicyStore(fdbRecordContext, projectId);
            filter = Query.and(Query.field("principal_type").equalsValue(principalType.getNum()),
                Query.field("principal_source").equalsValue(principalSource),
                Query.field("principal_id").equalsValue(principalId));
            query = RecordQuery.newBuilder()
                .setRecordType(StoreMetadata.META_PRIVILEGE_POLICY.getRecordTypeName())
                .setFilter(filter)
                .build();
        }

        return listMetaPrivilegeRecordsByQuery(metaPrivilegePolicyStore, query, projectId, principalType,
            privilegesList, continuation);
    }


    @Override
    public void getMetaPrivilegeByPolicyId(TransactionContext context,
                                           String projectId, PrincipalType principalType, String policyId,
                                           List<MetaPrivilegePolicy> privilegesList) {

        byte[] continuation = null;
        while (true) {
            continuation = getMetaPrivilegeByPolicyIdWithToken(context, projectId,
                principalType, policyId, privilegesList, continuation);
            if (continuation == null) {
                break;
            }
        }
        return;
    }

    public byte[] getMetaPrivilegeByPolicyIdWithToken(TransactionContext context, String projectId,
        PrincipalType principalType, String policyId,
        List<MetaPrivilegePolicy> privilegesList, byte[] continuation)
        throws MetaStoreException {

        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore metaPrivilegePolicyStore;
        QueryComponent filter;
        RecordQuery query;
        if (principalType == PrincipalType.SHARE) {
            metaPrivilegePolicyStore = DirectoryStoreHelper.getShareMetaPrivilegePolicyStore(fdbRecordContext);
            filter =  Query.and(Query.field("policy_id").equalsValue(policyId),
                Query.field("project_id").equalsValue(projectId));
            query = RecordQuery.newBuilder()
                .setRecordType(StoreMetadata.GLOBAL_SHARE_META_PRIVILEGE_POLICY.getRecordTypeName())
                .setFilter(filter)
                .setAllowedIndex(StoreMetadata.GLOBAL_SHARE_META_PRIVILEGE_POLICY.getName() + "-secondary-index")
                .build();
        } else {
            metaPrivilegePolicyStore = DirectoryStoreHelper.getMetaPrivilegePolicyStore(fdbRecordContext, projectId);
            filter =  Query.and(Query.field("policy_id").equalsValue(policyId),
                Query.field("principal_type").equalsValue(principalType.getNum()));
            query = RecordQuery.newBuilder()
                .setRecordType(StoreMetadata.META_PRIVILEGE_POLICY.getRecordTypeName())
                .setFilter(filter)
                .setAllowedIndex(StoreMetadata.META_PRIVILEGE_POLICY.getName() + "-secondary-index")
                .build();
        }

        return listMetaPrivilegeRecordsByQuery(metaPrivilegePolicyStore, query, projectId, principalType,
            privilegesList, continuation);
    }

    @Override
    public List<ObsPrivilegePolicy>  getDataPrivilegesByPrincipal(TransactionContext context, String projectId,
        PrincipalType principalType, int principalSource, String principalId) {

        List<ObsPrivilegePolicy> privilegesList = new ArrayList<>();
        byte[] continuation = null;
        while (true) {
            continuation = getDataPrivilegesByPrincipalWithToken(context, projectId,
                principalType, principalSource, principalId, privilegesList, continuation);
            if (continuation == null) {
                break;
            }
        }
        return privilegesList;
    }


    public byte[]  getDataPrivilegesByPrincipalWithToken(TransactionContext context, String projectId,
        PrincipalType principalType, int principalSource, String principalId,
        List<ObsPrivilegePolicy> privilegesList, byte[] continuation)
        throws MetaStoreException {

        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore dataPrivilegePolicyStore;
        QueryComponent filter;
        RecordQuery query;
        if (principalType == PrincipalType.SHARE) {
            dataPrivilegePolicyStore = DirectoryStoreHelper.getShareDataPrivilegePolicyStore(fdbRecordContext);
            filter = Query.and(Query.field("project_id").equalsValue(projectId),
                Query.field("principal_type").equalsValue(principalType.getNum()),
                Query.field("principal_source").equalsValue(principalSource),
                Query.field("principal_id").equalsValue(principalId));
            query = RecordQuery.newBuilder()
                .setRecordType(StoreMetadata.GLOBAL_SHARE_DATA_PRIVILEGE_POLICY.getRecordTypeName())
                .setFilter(filter)
                .build();
        } else {
            dataPrivilegePolicyStore = DirectoryStoreHelper.getDataPrivilegePolicyStore(fdbRecordContext, projectId);
            filter = Query.and(Query.field("principal_type").equalsValue(principalType.getNum()),
                Query.field("principal_source").equalsValue(principalSource),
                Query.field("principal_id").equalsValue(principalId));
            query = RecordQuery.newBuilder()
                .setRecordType(StoreMetadata.DATA_PRIVILEGE_POLICY.getRecordTypeName())
                .setFilter(filter)
                .build();
        }

        return listDataPrivilegeRecordsByQuery(dataPrivilegePolicyStore, query, projectId, principalType,
            privilegesList, continuation);
    }

    @Override
    public List<MetaPrivilegePolicy>  getMetaPrivilegesByPrincipalOnObject(TransactionContext context, String projectId,
        PrincipalType principalType, int principalSource, String principalId, int objectType, String objectId) {

        List<MetaPrivilegePolicy> privilegesList = new ArrayList<>();
        byte[] continuation = null;
        while (true) {
            continuation = getMetaPrivilegesByPrincipalOnObjectWithToken(context, projectId,
                principalType, principalSource, principalId, objectType, objectId,
                privilegesList, continuation);
            if (continuation == null) {
                break;
            }
        }
        return privilegesList;
    }


    public byte[] getMetaPrivilegesByPrincipalOnObjectWithToken(TransactionContext context, String projectId,
        PrincipalType principalType, int principalSource, String principalId, int objectType, String objectId,
        List<MetaPrivilegePolicy> privilegePolicyList, byte[] continuation)
        throws MetaStoreException {

        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore metaPrivilegePolicyStore;
        QueryComponent filter;
        RecordQuery query;
        if (principalType == PrincipalType.SHARE) {
            metaPrivilegePolicyStore = DirectoryStoreHelper.getShareMetaPrivilegePolicyStore(fdbRecordContext);
            filter = Query.and(Query.field("project_id").equalsValue(projectId),
                Query.field("principal_type").equalsValue(principalType.getNum()),
                Query.field("principal_source").equalsValue(principalSource),
                Query.field("principal_id").equalsValue(principalId),
                Query.field("object_type").equalsValue(objectType),
                Query.field("object_id").equalsValue(objectId));
            query = RecordQuery.newBuilder()
                .setRecordType(StoreMetadata.GLOBAL_SHARE_META_PRIVILEGE_POLICY.getRecordTypeName())
                .setFilter(filter)
                .build();
        } else {
            metaPrivilegePolicyStore = DirectoryStoreHelper.getMetaPrivilegePolicyStore(fdbRecordContext, projectId);
            filter = Query.and(Query.field("principal_type").equalsValue(principalType.getNum()),
                Query.field("principal_source").equalsValue(principalSource),
                Query.field("principal_id").equalsValue(principalId),
                Query.field("object_type").equalsValue(objectType),
                Query.field("object_id").equalsValue(objectId));
            query = RecordQuery.newBuilder()
                .setRecordType(StoreMetadata.META_PRIVILEGE_POLICY.getRecordTypeName())
                .setFilter(filter)
                .build();
        }
        return listMetaPrivilegeRecordsByQuery(metaPrivilegePolicyStore, query, projectId, principalType,
            privilegePolicyList, continuation);
    }

    private  byte[] listMetaPrivilegeRecordsByQuery(FDBRecordStore store,
        RecordQuery query, String projectId, PrincipalType principalType,
        List<MetaPrivilegePolicy> metaPrivilegePolicyObjectList, byte[] continuation) {

        RecordCursorIterator<FDBQueriedRecord<Message>> cursor = store.executeQuery(query,
                continuation,
                ExecuteProperties
                    .newBuilder().setTimeLimit(timeLimit)
                    .setReturnedRowLimit(maxBatchRowNum)
                    .setIsolationLevel(IsolationLevel.SNAPSHOT)
                    .build())
            .asIterator();

        while (cursor.hasNext()) {
            if (principalType == PrincipalType.SHARE) {
                ShareMetaPrivilegePolicyRecord.Builder builder = ShareMetaPrivilegePolicyRecord.newBuilder()
                    .mergeFrom(Objects.requireNonNull(cursor.next()).getRecord());
                MetaPrivilegePolicy metaPrivilegePolicyObject  =
                     MetaPrivilegePolicy.builder()
                         .setPolicyId(builder.getPolicyId())
                         .setProjectId(projectId)
                         .setPrincipalType(builder.getPrincipalType())
                         .setPrincipalSource(builder.getPrincipalSource())
                         .setPrincipalId(builder.getPrincipalId())
                         .setObjectType(builder.getObjectType())
                         .setObjectId(builder.getObjectId())
                         .setEffect(builder.getEffect())
                         .setPrivilege(builder.getPrivilege())
                         .setCondition(builder.getCondition())
                         .setObligation(builder.getObligation())
                         .setGrantAble(builder.getGrantAble())
                         .setUpdateTime(builder.getUpdateTime())
                         .build();
                metaPrivilegePolicyObjectList.add(metaPrivilegePolicyObject);
            } else {
                MetaPrivilegePolicyRecord.Builder builder = MetaPrivilegePolicyRecord.newBuilder()
                    .mergeFrom(Objects.requireNonNull(cursor.next()).getRecord());
                MetaPrivilegePolicy metaPrivilegePolicyObject  =
                    MetaPrivilegePolicy.builder()
                        .setPolicyId(builder.getPolicyId())
                        .setProjectId(projectId)
                        .setPrincipalType(builder.getPrincipalType())
                        .setPrincipalSource(builder.getPrincipalSource())
                        .setPrincipalId(builder.getPrincipalId())
                        .setObjectType(builder.getObjectType())
                        .setObjectId(builder.getObjectId())
                        .setEffect(builder.getEffect())
                        .setPrivilege(builder.getPrivilege())
                        .setCondition(builder.getCondition())
                        .setObligation(builder.getObligation())
                        .setGrantAble(builder.getGrantAble())
                        .setUpdateTime(builder.getUpdateTime())
                        .build();
                metaPrivilegePolicyObjectList.add(metaPrivilegePolicyObject);
            }
        }

        return cursor.getContinuation();
    }

    private  byte[] listDataPrivilegeRecordsByQuery(FDBRecordStore store,
        RecordQuery query, String projectId, PrincipalType principalType,
        List<ObsPrivilegePolicy> privilegePolicyList, byte[] continuation) {

        RecordCursorIterator<FDBQueriedRecord<Message>> cursor = store.executeQuery(query,
                continuation,
                ExecuteProperties
                    .newBuilder().setTimeLimit(timeLimit)
                    .setReturnedRowLimit(maxBatchRowNum)
                    .setIsolationLevel(IsolationLevel.SNAPSHOT)
                    .build())
            .asIterator();
        while (cursor.hasNext()) {
            if (principalType == PrincipalType.SHARE) {
                ShareDataPrivilegePolicyRecord.Builder builder = ShareDataPrivilegePolicyRecord.newBuilder()
                    .mergeFrom(Objects.requireNonNull(cursor.next()).getRecord());

                ObsPrivilegePolicy dataPrivilegePolicyObject = new ObsPrivilegePolicy(builder.getPolicyId(),
                    projectId,
                    builder.getPrincipalType(), builder.getPrincipalSource(), builder.getPrincipalId(),
                    builder.getObsPath(), builder.getObsEndpoint(),
                    builder.getPermission(), builder.getUpdateTime());
                privilegePolicyList.add(dataPrivilegePolicyObject);
            } else {
                DataPrivilegePolicyRecord.Builder builder = DataPrivilegePolicyRecord.newBuilder()
                    .mergeFrom(Objects.requireNonNull(cursor.next()).getRecord());

                ObsPrivilegePolicy dataPrivilegePolicyObject = new ObsPrivilegePolicy(builder.getPolicyId(),
                    projectId,
                    builder.getPrincipalType(), builder.getPrincipalSource(), builder.getPrincipalId(),
                    builder.getObsPath(), builder.getObsEndpoint(),
                    builder.getPermission(), builder.getUpdateTime());
                privilegePolicyList.add(dataPrivilegePolicyObject);
            }

        }

        return cursor.getContinuation();
    }

    @Override
    public void delAllMetaPrivilegesOfPrincipal(TransactionContext context, String projectId,
        PrincipalType principalType, int principalSource, String principalId)
        throws MetaStoreException {

        byte[] continuation = null;
        while (true) {
            continuation = delAllMetaPrivilegesOfPrincipalWithToken(context, projectId,
                principalType, principalSource, principalId, continuation);
            if (continuation == null) {
                break;
            }
        }
        return ;
    }


    @Override
    public  byte[] delAllMetaPrivilegesOfPrincipalWithToken(TransactionContext context, String projectId,
                                                            PrincipalType principalType, int principalSource, String principalId, byte[] continuation)
        throws MetaStoreException {

        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore metaPrivilegePolicyStore;
        QueryComponent filter;
        RecordQueryPlan plan;
        RecordQuery query;
        if (principalType == PrincipalType.SHARE) {
            metaPrivilegePolicyStore = DirectoryStoreHelper.getShareMetaPrivilegePolicyStore(fdbRecordContext);
            filter = Query.and(Query.field("project_id").equalsValue(projectId),
                Query.field("principal_type").equalsValue(principalType.getNum()),
                Query.field("principal_source").equalsValue(principalSource),
                Query.field("principal_id").equalsValue(principalId));
            query = RecordQuery.newBuilder()
                .setRecordType(StoreMetadata.GLOBAL_SHARE_META_PRIVILEGE_POLICY.getRecordTypeName())
                .setFilter(filter)
                .build();
        } else {
            metaPrivilegePolicyStore = DirectoryStoreHelper.getMetaPrivilegePolicyStore(fdbRecordContext, projectId);
            filter = Query.and(Query.field("principal_type").equalsValue(principalType.getNum()),
                Query.field("principal_source").equalsValue(principalSource),
                Query.field("principal_id").equalsValue(principalId));
            query = RecordQuery.newBuilder()
                .setRecordType(StoreMetadata.META_PRIVILEGE_POLICY.getRecordTypeName())
                .setFilter(filter)
                .build();
        }

        return delPrivilegeByQuery(metaPrivilegePolicyStore, query, continuation);
    }

    @Override
    public void delAllMetaPrivilegesOfPrincipalOnObject(TransactionContext context, String projectId,
        PrincipalType principalType, int principalSource, String principalId,
        int objectType, String objectId)
        throws MetaStoreException {

        byte[] continuation = null;
        while (true) {
            continuation = delAllMetaPrivilegesOfPrincipalOnObjectWithToken(context, projectId,
                principalType, principalSource, principalId, objectType, objectId, continuation);
            if (continuation == null) {
                break;
            }
        }
        return ;
    }

    @Override
    public  byte[] delAllMetaPrivilegesOfPrincipalOnObjectWithToken(TransactionContext context, String projectId,
                                                                    PrincipalType principalType, int principalSource, String principalId,
                                                                    int objectType, String objectId, byte[] continuation) throws MetaStoreException {

        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore metaPrivilegePolicyStore;
        QueryComponent filter;
        RecordQuery query;

        if (principalType == PrincipalType.SHARE) {
            metaPrivilegePolicyStore = DirectoryStoreHelper.getShareMetaPrivilegePolicyStore(fdbRecordContext);
            filter = Query.and(Query.field("project_id").equalsValue(projectId),
                Query.field("principal_type").equalsValue(principalType.getNum()),
                Query.field("principal_source").equalsValue(principalSource),
                Query.field("principal_id").equalsValue(principalId),
                Query.field("object_type").equalsValue(objectType),
                Query.field("object_id").equalsValue(objectId));
            query = RecordQuery.newBuilder()
                .setRecordType(StoreMetadata.GLOBAL_SHARE_META_PRIVILEGE_POLICY.getRecordTypeName())
                .setFilter(filter)
                .build();
        } else {
            metaPrivilegePolicyStore = DirectoryStoreHelper.getMetaPrivilegePolicyStore(fdbRecordContext, projectId);
            filter = Query.and(Query.field("principal_type").equalsValue(principalType.getNum()),
                Query.field("principal_source").equalsValue(principalSource),
                Query.field("principal_id").equalsValue(principalId),
                Query.field("object_type").equalsValue(objectType),
                Query.field("object_id").equalsValue(objectId));
            query = RecordQuery.newBuilder()
                .setRecordType(StoreMetadata.META_PRIVILEGE_POLICY.getRecordTypeName())
                .setFilter(filter)
                .build();
        }

        return delPrivilegeByQuery(metaPrivilegePolicyStore, query, continuation);
    }

    @Override
    public void delAllMetaPrivilegeOnObject(TransactionContext context, String projectId,
        int objectType, String objectId, boolean isShare)
        throws MetaStoreException {

        byte[] continuation = null;
        while (true) {
            continuation = delAllMetaPrivilegeOnObjectWithToken(context, projectId,
                objectType, objectId, isShare, continuation);
            if (continuation == null) {
                break;
            }
        }
        return ;
    }


    public  byte[] delAllMetaPrivilegeOnObjectWithToken(TransactionContext context, String projectId,
        int objectType, String objectId, boolean isShare, byte[] continuation)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        QueryComponent filter;
        FDBRecordStore metaPrivilegePolicyStore;
        RecordQuery query;
        if (isShare) {
            metaPrivilegePolicyStore = DirectoryStoreHelper.getShareMetaPrivilegePolicyStore(fdbRecordContext);
            filter = Query.and(Query.field("project_id").equalsValue(projectId),
                Query.field("object_type").equalsValue(objectType),
                Query.field("object_id").equalsValue(objectId));
            query = RecordQuery.newBuilder()
                .setRecordType(StoreMetadata.GLOBAL_SHARE_META_PRIVILEGE_POLICY.getRecordTypeName())
                .setFilter(filter)
                .build();
        } else {
            metaPrivilegePolicyStore = DirectoryStoreHelper.getMetaPrivilegePolicyStore(fdbRecordContext,
                projectId);
            filter = Query.and(Query.field("object_type").equalsValue(objectType),
                Query.field("object_id").equalsValue(objectId));
            query = RecordQuery.newBuilder()
                .setRecordType(StoreMetadata.META_PRIVILEGE_POLICY.getRecordTypeName())
                .setFilter(filter)
                .build();
        }
        return delPrivilegeByQuery(metaPrivilegePolicyStore, query, continuation);
    }

    @Override
    public void insertMetaPrivilegePolicyHistory(TransactionContext context, String projectId,
                                                 String policyId, PrincipalType principalType, int principalSource, String principalId,
                                                 int modifyType, long updateTime) throws MetaStoreException {

        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        if (principalType == PrincipalType.SHARE) {
            FDBRecordStore shareMetaPrivilegePolicyHistoryStore = DirectoryStoreHelper.getShareMetaPrivilegePolicyHistoryStore(fdbRecordContext);
            ShareMetaPrivilegePolicyHistoryRecord.Builder shareMetaPrivilegePolicyHistoryRecordBuilder =
                ShareMetaPrivilegePolicyHistoryRecord.newBuilder()
                    .setPolicyId(policyId)
                    .setProjectId(projectId)
                    .setPrincipalType(principalType.getNum())
                    .setPrincipalSource(principalSource)
                    .setPrincipalId(principalId)
                    .setModifyType(modifyType)
                    .setUpdateTime(updateTime);
            shareMetaPrivilegePolicyHistoryStore.insertRecord(shareMetaPrivilegePolicyHistoryRecordBuilder.build());
        } else {
            FDBRecordStore metaPrivilegePolicyHistoryStore = DirectoryStoreHelper.getMetaPrivilegePolicyHistoryStore(fdbRecordContext,
                projectId);
            MetaPrivilegePolicyHistoryRecord.Builder metaPrivilegePolicyHistoryRecordBuilder = MetaPrivilegePolicyHistoryRecord.newBuilder()
                .setPolicyId(policyId)
                .setPrincipalType(principalType.getNum())
                .setPrincipalSource(principalSource)
                .setPrincipalId(principalId)
                .setModifyType(modifyType)
                .setUpdateTime(updateTime);
            metaPrivilegePolicyHistoryStore.insertRecord(metaPrivilegePolicyHistoryRecordBuilder.build());
        }

    }


    @Override
    public void updateMetaPrivilegePolicyHistory(TransactionContext context, String projectId,
                                                 String policyId, PrincipalType principalType, int principalSource, String principalId,
                                                 int modifyType, long updateTime) throws MetaStoreException {

        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        if (principalType == PrincipalType.SHARE) {
            FDBRecordStore shareMetaPrivilegePolicyHistoryStore = DirectoryStoreHelper.getShareMetaPrivilegePolicyHistoryStore(fdbRecordContext);
            ShareMetaPrivilegePolicyHistoryRecord.Builder shareMetaPrivilegePolicyHistoryRecordBuilder =
                ShareMetaPrivilegePolicyHistoryRecord.newBuilder()
                    .setPolicyId(policyId)
                    .setProjectId(projectId)
                    .setPrincipalType(principalType.getNum())
                    .setPrincipalSource(principalSource)
                    .setPrincipalId(principalId)
                    .setModifyType(modifyType)
                    .setUpdateTime(updateTime);
            shareMetaPrivilegePolicyHistoryStore.updateRecord(shareMetaPrivilegePolicyHistoryRecordBuilder.build());
        } else {
            FDBRecordStore metaPrivilegePolicyHistoryStore = DirectoryStoreHelper.getMetaPrivilegePolicyHistoryStore(fdbRecordContext,
                projectId);
            MetaPrivilegePolicyHistoryRecord.Builder metaPrivilegePolicyHistoryRecordBuilder = MetaPrivilegePolicyHistoryRecord.newBuilder()
                .setPolicyId(policyId)
                .setPrincipalType(principalType.getNum())
                .setPrincipalSource(principalSource)
                .setPrincipalId(principalId)
                .setModifyType(modifyType)
                .setUpdateTime(updateTime);
            metaPrivilegePolicyHistoryStore.updateRecord(metaPrivilegePolicyHistoryRecordBuilder.build());
        }

    }

    public void deleteMetaPrivilegePolicyHistory(TransactionContext context, String projectId,
        String policyId, PrincipalType principalType) throws MetaStoreException {

        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore metaPrivilegePolicyHistoryStore;
        if (principalType == PrincipalType.SHARE) {
            metaPrivilegePolicyHistoryStore = DirectoryStoreHelper.getShareMetaPrivilegePolicyHistoryStore(fdbRecordContext);
        } else {
            metaPrivilegePolicyHistoryStore = DirectoryStoreHelper.getMetaPrivilegePolicyHistoryStore(fdbRecordContext,
                projectId);
        }
        metaPrivilegePolicyHistoryStore.deleteRecord(metaPrivilegePolicyHistoryPrimaryKey(projectId,policyId, principalType));
    }

    public List<MetaPolicyHistory>  getAllMetaPolicyHistoryList(TransactionContext context, String projectId, long time) {

        List<MetaPolicyHistory> policyHistoryList = new ArrayList<>();
        byte[] continuation = null;
        while (true) {
            continuation = getMetaPolicyHistoryWithToken(context, projectId,
                time, policyHistoryList, continuation);
            if (continuation == null) {
                break;
            }
        }
        continuation = null;
        while (true) {
            continuation = getShareMetaPolicyHistoryWithToken(context, projectId,
                time, policyHistoryList, continuation);
            if (continuation == null) {
                break;
            }
        }

        return policyHistoryList;
    }

    @Override
    public byte[] getShareMetaPolicyHistoryWithToken(TransactionContext context, String projectId,
                                                     long time, List<MetaPolicyHistory> policyHistoryList, byte[] continuation)
        throws MetaStoreException {

        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore metaPolicyHistoryStore = DirectoryStoreHelper.getShareMetaPrivilegePolicyHistoryStore(fdbRecordContext);
        QueryComponent filter = Query.and(Query.field("project_id").equalsValue(projectId),
            Query.field("update_time").greaterThanOrEquals(time));
        RecordQuery query = RecordQuery.newBuilder()
            .setRecordType(StoreMetadata.GLOBAL_SHARE_META_PRIVILEGE_POLICY_HISTORY.getRecordTypeName())
            .setFilter(filter)
            .build();

        return listMetaPolicyHistoryByQuery(metaPolicyHistoryStore, query, projectId, PrincipalType.SHARE,
            policyHistoryList, continuation);
    }


    @Override
    public byte[] getMetaPolicyHistoryWithToken(TransactionContext context, String projectId,
                                                long time, List<MetaPolicyHistory> policyHistoryList, byte[] continuation)
        throws MetaStoreException {

        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore metaPolicyHistoryStore = DirectoryStoreHelper.getMetaPrivilegePolicyHistoryStore(fdbRecordContext, projectId);

        QueryComponent filter = Query.field("update_time").greaterThan(time);
        RecordQuery query = RecordQuery.newBuilder()
            .setRecordType(StoreMetadata.META_PRIVILEGE_POLICY_HISTORY.getRecordTypeName())
            .setFilter(filter)
            .build();
        return listMetaPolicyHistoryByQuery(metaPolicyHistoryStore, query, projectId, PrincipalType.ROLE,
            policyHistoryList, continuation);
    }

    private  byte[] listMetaPolicyHistoryByQuery(FDBRecordStore store,
        RecordQuery query, String projectId, PrincipalType principalType,
        List<MetaPolicyHistory> policyHistoryObjectList, byte[] continuation) {

        RecordCursorIterator<FDBQueriedRecord<Message>> cursor = store.executeQuery(query,
                continuation,
                ExecuteProperties
                    .newBuilder().setTimeLimit(timeLimit)
                    .setReturnedRowLimit(maxBatchRowNum)
                    .setIsolationLevel(IsolationLevel.SNAPSHOT)
                    .build())
            .asIterator();

        while (cursor.hasNext()) {
            if (principalType == PrincipalType.SHARE) {
                ShareMetaPrivilegePolicyHistoryRecord.Builder builder = ShareMetaPrivilegePolicyHistoryRecord.newBuilder()
                    .mergeFrom(Objects.requireNonNull(cursor.next()).getRecord());
                MetaPolicyHistory metaPolicyHistoryObject  =
                    new MetaPolicyHistory(builder.getPolicyId(), projectId,
                        builder.getPrincipalType(), builder.getPrincipalSource(), builder.getPrincipalId(),
                        builder.getUpdateTime(), builder.getModifyType());
                policyHistoryObjectList.add(metaPolicyHistoryObject);
            } else {
                MetaPrivilegePolicyHistoryRecord.Builder builder = MetaPrivilegePolicyHistoryRecord.newBuilder()
                    .mergeFrom(Objects.requireNonNull(cursor.next()).getRecord());
                MetaPolicyHistory metaPolicyHistoryObject  =
                    new MetaPolicyHistory(builder.getPolicyId(), projectId,
                        builder.getPrincipalType(), builder.getPrincipalSource(), builder.getPrincipalId(),
                        builder.getUpdateTime(), builder.getModifyType());
                policyHistoryObjectList.add(metaPolicyHistoryObject);
            }
        }

        return cursor.getContinuation();
    }

    private byte[] delPrivilegeByQuery(FDBRecordStore store,
        RecordQuery query, byte[] continuation)
        throws MetaStoreException {

        RecordCursorIterator<FDBQueriedRecord<Message>> cursor = store
            .executeQuery(query, continuation,
                ExecuteProperties.newBuilder().setReturnedRowLimit(maxBatchRowNum)
                    .setIsolationLevel(IsolationLevel.SNAPSHOT).build()).asIterator();
        deleteStoreRecordByCursor(store, cursor);
        return cursor.getContinuation();
    }

    /*private void delPrivilegeByQuery(FDBRecordStore store,String projectId, PrincipalType principalType, RecordQuery query)
        throws MetaStoreException {

        try (RecordCursor<FDBQueriedRecord<Message>> cursor = store.executeQuery(query)) {
            RecordCursorIterator<FDBQueriedRecord<Message>> iterator = cursor.asIterator();
            while (iterator.hasNext()) {
                if (principalType == PrincipalType.SHARE) {
                    ShareMetaPrivilegePolicyRecord.Builder builder = ShareMetaPrivilegePolicyRecord.newBuilder()
                        .mergeFrom(Objects.requireNonNull(iterator.next()).getRecord());
                    store.deleteRecord(
                        metaPrivilegePolicyPrimaryKey(builder.getProjectId(),
                            principalType, builder.getPrincipalSource(), builder.getPrincipalId(),
                            builder.getObjectType(), builder.getObjectId(),
                            builder.getEffect(), builder.getPrivilege()));
                } else {
                    MetaPrivilegePolicyRecord.Builder builder = MetaPrivilegePolicyRecord.newBuilder()
                        .mergeFrom(Objects.requireNonNull(iterator.next()).getRecord());
                    store.deleteRecord(
                        metaPrivilegePolicyPrimaryKey(projectId,
                            principalType, builder.getPrincipalSource(), builder.getPrincipalId(),
                            builder.getObjectType(), builder.getObjectId(),
                            builder.getEffect(), builder.getPrivilege()));
                }
            }
        }
    }

    private void delDataPrivilegeByQuery(FDBRecordStore store, String projectId, PrincipalType principalType, RecordQuery query)
        throws MetaStoreException {

        try (RecordCursor<FDBQueriedRecord<Message>> cursor = store.executeQuery(query)) {
            RecordCursorIterator<FDBQueriedRecord<Message>> iterator = cursor.asIterator();
            while (iterator.hasNext()) {
                if (principalType == PrincipalType.SHARE) {
                    ShareDataPrivilegePolicyRecord.Builder builder = ShareDataPrivilegePolicyRecord.newBuilder()
                        .mergeFrom(Objects.requireNonNull(iterator.next()).getRecord());
                    store.deleteRecord(
                        dataPrivilegePolicyPrimaryKey(builder.getProjectId(),
                            principalType, builder.getPrincipalSource(), builder.getPrincipalId(),
                            builder.getObsPath(), builder.getObsEndpoint()));
                } else {
                    DataPrivilegePolicyRecord.Builder builder = DataPrivilegePolicyRecord.newBuilder()
                        .mergeFrom(Objects.requireNonNull(iterator.next()).getRecord());
                    store.deleteRecord(
                        dataPrivilegePolicyPrimaryKey(projectId,
                            principalType, builder.getPrincipalSource(), builder.getPrincipalId(),
                            builder.getObsPath(), builder.getObsEndpoint()));
                }
            }
        }
    }

    */

    private static int deleteStoreRecordByCursor(FDBRecordStore store,
        RecordCursorIterator<FDBQueriedRecord<Message>> cursor) {
        int cnt = 0;
        while (cursor.hasNext()) {
            store.deleteRecord(cursor.next().getPrimaryKey());
            cnt++;
        }
        return cnt;
    }

}
