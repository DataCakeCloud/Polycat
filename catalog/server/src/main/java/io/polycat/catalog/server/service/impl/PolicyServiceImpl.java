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
package io.polycat.catalog.server.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.model.CatalogInnerObject;
import io.polycat.catalog.common.model.MetaPolicyHistory;
import io.polycat.catalog.common.model.MetaPrivilegePolicy;
import io.polycat.catalog.common.model.ObjectPrivilege;
import io.polycat.catalog.common.model.ObsPrivilegePolicy;
import io.polycat.catalog.common.model.OperationPrivilege;
import io.polycat.catalog.common.model.OperationPrivilegeType;
import io.polycat.catalog.common.model.Policy;
import io.polycat.catalog.common.model.PolicyModifyType;
import io.polycat.catalog.common.model.Principal;
import io.polycat.catalog.common.model.PrincipalSource;
import io.polycat.catalog.common.model.PrincipalType;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.plugin.request.input.PolicyInput;
import io.polycat.catalog.service.api.PolicyService;
import io.polycat.catalog.store.api.*;
import io.polycat.catalog.store.fdb.record.RecordStoreHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class PolicyServiceImpl implements PolicyService {

    private static final Logger log = Logger.getLogger(PolicyServiceImpl.class);

    //private final FDBDatabase db = RecordStoreHelper.getFdbDatabase();
    @Autowired
    private Transaction storeTransaction;
    @Autowired
    private PolicyStore policyStore;
    @Autowired
    private UserGroupStore userGroupStore;
    @Autowired
    private NewRoleStore roleStore;
    @Autowired
    private GlobalShareStore shareStore;

    private static Map<Operation, OperationPrivilege> operationPrivilegeMap = new ConcurrentHashMap<>();
    private static Map<Operation, OperationPrivilege> shareOperationPrivilegeMap = new ConcurrentHashMap<>();
    private static final String FLAG_MODIFY_PRIVILEGE = "MODIFY_PRIVILEGE";
    private static final String FLAG_MODIFY_CONDITION = "MODIFY_CONDITION";
    private static final String FLAG_MODIFY_OBLIGATION = "MODIFY_OBLIGATION";

    public PolicyServiceImpl() {
        init_operationPrivilegeMap();
        init_shareOperationPrivilegeMap();
    }
    private void init_shareOperationPrivilegeMap() {
        shareOperationPrivilegeMap.put(Operation.SELECT_TABLE,
            new OperationPrivilege(ObjectType.TABLE,
                OperationPrivilegeType.SHARE_PRIVILEGE_SELECT.getType()));
    }

    private void init_operationPrivilegeMap() {
        operationPrivilegeMap.put(Operation.CREATE_CATALOG,
            new OperationPrivilege(ObjectType.CATALOG,
                OperationPrivilegeType.CREATE.getType()));

        operationPrivilegeMap.put(Operation.DESC_CATALOG,
            new OperationPrivilege(ObjectType.CATALOG,
                OperationPrivilegeType.DESC.getType()));

        operationPrivilegeMap.put(Operation.DROP_CATALOG,
            new OperationPrivilege(ObjectType.CATALOG,
                OperationPrivilegeType.DROP.getType()));

        operationPrivilegeMap.put(Operation.ALTER_CATALOG,
            new OperationPrivilege(ObjectType.CATALOG,
                OperationPrivilegeType.ALTER.getType()));

        operationPrivilegeMap.put(Operation.USE_CATALOG,
            new OperationPrivilege(ObjectType.CATALOG,
                OperationPrivilegeType.USE.getType()));

        operationPrivilegeMap.put(Operation.CREATE_BRANCH,
            new OperationPrivilege(ObjectType.CATALOG,
                OperationPrivilegeType.CREATE_BRANCH.getType()));

        operationPrivilegeMap.put(Operation.USE_BRANCH,
            new OperationPrivilege(ObjectType.CATALOG,
                OperationPrivilegeType.USE.getType()));

        operationPrivilegeMap.put(Operation.SHOW_BRANCH,
            new OperationPrivilege(ObjectType.CATALOG,
                OperationPrivilegeType.SHOW_BRANCH.getType()));

        operationPrivilegeMap.put(Operation.MERGE_BRANCH,
            new OperationPrivilege(ObjectType.CATALOG,
                OperationPrivilegeType.MERGE_BRANCH.getType()));

        operationPrivilegeMap.put(Operation.CREATE_DATABASE,
            new OperationPrivilege(ObjectType.CATALOG,
                OperationPrivilegeType.CREATE_DATABASE.getType()));

        operationPrivilegeMap.put(Operation.SHOW_DATABASE,
            new OperationPrivilege(ObjectType.CATALOG,
                OperationPrivilegeType.SHOW_DATABASE.getType()));

        operationPrivilegeMap.put(Operation.DESC_DATABASE,
            new OperationPrivilege(ObjectType.DATABASE,
                OperationPrivilegeType.DESC.getType()));

        operationPrivilegeMap.put(Operation.DROP_DATABASE,
            new OperationPrivilege(ObjectType.DATABASE,
                OperationPrivilegeType.DROP.getType()));

        operationPrivilegeMap.put(Operation.UNDROP_DATABASE,
            new OperationPrivilege(ObjectType.DATABASE,
                OperationPrivilegeType.UNDROP.getType()));

        operationPrivilegeMap.put(Operation.ALTER_DATABASE,
            new OperationPrivilege(ObjectType.DATABASE,
                OperationPrivilegeType.ALTER.getType()));

        operationPrivilegeMap.put(Operation.USE_DATABASE,
            new OperationPrivilege(ObjectType.DATABASE,
                OperationPrivilegeType.USE.getType()));

        operationPrivilegeMap.put(Operation.CREATE_TABLE,
            new OperationPrivilege(ObjectType.DATABASE,
                OperationPrivilegeType.CREATE_TABLE.getType()));

        operationPrivilegeMap.put(Operation.SHOW_TABLE,
            new OperationPrivilege(ObjectType.DATABASE,
                OperationPrivilegeType.SHOW_TABLE.getType()));

        operationPrivilegeMap.put(Operation.CREATE_VIEW,
            new OperationPrivilege(ObjectType.DATABASE,
                OperationPrivilegeType.CREATE_VIEW.getType()));

        operationPrivilegeMap.put(Operation.SHOW_VIEW,
            new OperationPrivilege(ObjectType.DATABASE,
                OperationPrivilegeType.SHOW_VIEW.getType()));

        operationPrivilegeMap.put(Operation.DESC_TABLE,
            new OperationPrivilege(ObjectType.TABLE,
                OperationPrivilegeType.DESC.getType()));

        operationPrivilegeMap.put(Operation.DROP_TABLE,
            new OperationPrivilege(ObjectType.TABLE,
                OperationPrivilegeType.DROP.getType()));

        operationPrivilegeMap.put(Operation.PURGE_TABLE,
            new OperationPrivilege(ObjectType.TABLE,
                OperationPrivilegeType.DROP.getType()));

        operationPrivilegeMap.put(Operation.ALTER_TABLE,
            new OperationPrivilege(ObjectType.TABLE,
                OperationPrivilegeType.ALTER.getType()));

        operationPrivilegeMap.put(Operation.UNDROP_TABLE,
            new OperationPrivilege(ObjectType.TABLE,
                OperationPrivilegeType.UNDROP.getType()));

        operationPrivilegeMap.put(Operation.RESTORE_TABLE,
            new OperationPrivilege(ObjectType.TABLE,
                OperationPrivilegeType.RESTORE.getType()));

        operationPrivilegeMap.put(Operation.SELECT_TABLE,
            new OperationPrivilege(ObjectType.TABLE,
                OperationPrivilegeType.SELECT.getType()));

        operationPrivilegeMap.put(Operation.INSERT_TABLE,
            new OperationPrivilege(ObjectType.TABLE,
                OperationPrivilegeType.INSERT.getType()));

        operationPrivilegeMap.put(Operation.ALTER_VIEW,
            new OperationPrivilege(ObjectType.VIEW,
                OperationPrivilegeType.ALTER.getType()));

        operationPrivilegeMap.put(Operation.DROP_VIEW,
            new OperationPrivilege(ObjectType.VIEW,
                OperationPrivilegeType.DROP.getType()));

        operationPrivilegeMap.put(Operation.DESC_VIEW,
            new OperationPrivilege(ObjectType.VIEW,
                OperationPrivilegeType.DESC.getType()));

        operationPrivilegeMap.put(Operation.CREATE_ACCELERATOR,
            new OperationPrivilege(ObjectType.DATABASE,
                OperationPrivilegeType.CREATE_ACCELERATOR.getType()));

        operationPrivilegeMap.put(Operation.SHOW_ACCELERATORS,
            new OperationPrivilege(ObjectType.DATABASE,
                OperationPrivilegeType.SHOW_ACCELERATORS.getType()));

        operationPrivilegeMap.put(Operation.DROP_ACCELERATOR,
            new OperationPrivilege(ObjectType.ACCELERATOR,
                OperationPrivilegeType.DROP_ACCELERATOR.getType()));

        operationPrivilegeMap.put(Operation.ALTER_SHARE,
            new OperationPrivilege(ObjectType.SHARE,
                OperationPrivilegeType.ALTER.getType()));

        operationPrivilegeMap.put(Operation.DROP_SHARE,
            new OperationPrivilege(ObjectType.SHARE,
                OperationPrivilegeType.DROP.getType()));

        operationPrivilegeMap.put(Operation.DESC_SHARE,
            new OperationPrivilege(ObjectType.SHARE,
                OperationPrivilegeType.DESC.getType()));

        operationPrivilegeMap.put(Operation.ALTER_ROLE,
            new OperationPrivilege(ObjectType.ROLE,
                OperationPrivilegeType.ALTER.getType()));

        operationPrivilegeMap.put(Operation.DROP_ROLE,
            new OperationPrivilege(ObjectType.ROLE,
                OperationPrivilegeType.DROP.getType()));

        operationPrivilegeMap.put(Operation.DESC_ROLE,
            new OperationPrivilege(ObjectType.ROLE,
                OperationPrivilegeType.DESC.getType()));

        operationPrivilegeMap.put(Operation.CREATE_STREAM,
            new OperationPrivilege(ObjectType.STREAM,
                OperationPrivilegeType.CREATE_STREAM.getType()));

        operationPrivilegeMap.put(Operation.DESC_DELEGATE,
            new OperationPrivilege(ObjectType.DELEGATE,
                OperationPrivilegeType.DESC.getType()));

        operationPrivilegeMap.put(Operation.DROP_DELEGATE,
            new OperationPrivilege(ObjectType.DELEGATE,
                OperationPrivilegeType.DROP.getType()));

        operationPrivilegeMap.put(Operation.REVOKE_ALL_OPERATION_FROM_ROLE,
            new OperationPrivilege(ObjectType.ROLE,
                OperationPrivilegeType.ALTER.getType()));

        operationPrivilegeMap.put(Operation.ALTER_COLUMN,
            new OperationPrivilege(ObjectType.TABLE,
                OperationPrivilegeType.CHANGE_SCHEMA.getType()));

        operationPrivilegeMap.put(Operation.SHOW_ACCESS_STATS_FOR_CATALOG,
            new OperationPrivilege(ObjectType.CATALOG,
                OperationPrivilegeType.SHOW_ACCESSSTATS.getType()));

        operationPrivilegeMap.put(Operation.DESC_ACCESS_STATS_FOR_TABLE,
            new OperationPrivilege(ObjectType.TABLE,
                OperationPrivilegeType.DESC_ACCESSSTATS.getType()));

        operationPrivilegeMap.put(Operation.SHOW_DATA_LINEAGE_FOR_TABLE,
            new OperationPrivilege(ObjectType.TABLE,
                OperationPrivilegeType.SHOW_DATALINEAGE.getType()));

        operationPrivilegeMap.put(Operation.SET_PROPERTIES,
            new OperationPrivilege(ObjectType.TABLE,
                OperationPrivilegeType.ALTER.getType()));

        operationPrivilegeMap.put(Operation.UNSET_PROPERTIES,
            new OperationPrivilege(ObjectType.TABLE,
                OperationPrivilegeType.ALTER.getType()));

        operationPrivilegeMap.put(Operation.ADD_PARTITION,
            new OperationPrivilege(ObjectType.TABLE,
                OperationPrivilegeType.INSERT.getType()));

        operationPrivilegeMap.put(Operation.DROP_PARTITION,
            new OperationPrivilege(ObjectType.TABLE,
                OperationPrivilegeType.DELETE.getType()));

        operationPrivilegeMap.put(Operation.COPY_INTO,
            new OperationPrivilege(ObjectType.TABLE,
                OperationPrivilegeType.COPY_INTO.getType()));

        operationPrivilegeMap.put(Operation.SHOW_CREATE_TABLE,
            new OperationPrivilege(ObjectType.TABLE,
                OperationPrivilegeType.DESC.getType()));
    }



    @Override
    public void addAllPrivilegeOnObjectToPrincipal(String projectId, String principalName, PolicyInput policyInput) {

        PrincipalType type = PrincipalType.getPrincipalType(policyInput.getPrincipalType());
        PrincipalSource source = PrincipalSource.getPrincipalSource(policyInput.getPrincipalSource());
        ObjectType objectType = ObjectType.valueOf(policyInput.getObjectType().toUpperCase());
        CatalogInnerObject catalogInnerObject = ServiceImplHelper.getCatalogObject(projectId, objectType.name(), policyInput.getObjectName());
        String condition = policyInput.getCondition();
        String obligation = policyInput.getObligation();
        boolean effect = policyInput.getEffect();
        boolean grantAble = policyInput.getGrantAble();
        long[] privilegeArray = PrivilegePolicyHelper.getObjectAllPrivilegeArrayByType(objectType.getNum());
        if (privilegeArray == null) {
            throw new CatalogServerException(ErrorCode.ROLE_PRIVILEGE_INVALID);
        }
        try (TransactionContext context = storeTransaction.openTransaction()) {
            for (long privilege : privilegeArray) {
                addOneMetaPolicyToPrincipal(context, projectId, type, source.getNum(), principalName,
                    objectType.getNum(), catalogInnerObject, privilege, effect, false, condition, obligation,grantAble);
            }
            context.commit();
        }
    }


    @Override
    public void addMetaPolicyToPrincipal(String projectId, String principalName, PolicyInput policyInput) {

        PrincipalType type = PrincipalType.getPrincipalType(policyInput.getPrincipalType());
        PrincipalSource source = PrincipalSource.getPrincipalSource(policyInput.getPrincipalSource());
        List<Operation> operationList = policyInput.getOperationList();
        ObjectType objectType = ObjectType.valueOf(policyInput.getObjectType().toUpperCase());
        CatalogInnerObject catalogInnerObject = ServiceImplHelper.getCatalogObject(projectId, objectType.name(),
            policyInput.getObjectName());
        String condition = policyInput.getCondition();
        String obligation = null;
        boolean effect = policyInput.getEffect();
        boolean isOwner = policyInput.getOwner();
        boolean grantAble = policyInput.getGrantAble();
        long privilege = 0;

        try (TransactionContext context = storeTransaction.openTransaction()) {
            if (isOwner && type == PrincipalType.USER) {
                addOneMetaPolicyToPrincipal(context, projectId, type, source.getNum(), principalName,
                    objectType.getNum(), catalogInnerObject, privilege, effect, isOwner, condition, obligation, grantAble);
                return;
            }
            for (Operation operation : operationList) {
                OperationPrivilege operationPrivilege;
                if (type == PrincipalType.SHARE) {
                    operationPrivilege = shareOperationPrivilegeMap.get(operation);
                } else {
                    operationPrivilege = operationPrivilegeMap.get(operation);
                }
                if (operationPrivilege == null) {
                    throw new CatalogServerException(ErrorCode.ROLE_PRIVILEGE_INVALID);
                }
                privilege = convertPrivilege(operationPrivilege);

                if (privilege == 0) {
                    throw new CatalogServerException(ErrorCode.ROLE_PRIVILEGE_INVALID);
                }
                if (operation == Operation.SELECT_TABLE) {
                    obligation = policyInput.getObligation();
                }
                addOneMetaPolicyToPrincipal(context, projectId, type, source.getNum(), principalName,
                    objectType.getNum(), catalogInnerObject, privilege, effect, false, condition, obligation,grantAble);
            }
            context.commit();
        }
    }

    private void addOneMetaPolicyToPrincipal(TransactionContext context, String projectId, PrincipalType principalType, int principalSource, String principalName,
        int objectType, CatalogInnerObject catalogInnerObject, long privilege, boolean effect,
        boolean isOwner, String condition, String obligation, boolean grantAble) {

        String principalId = null;
        switch (principalType) {
            case USER:
                principalId = userGroupStore.getUserIdByName(context, projectId, principalSource, principalName);
                if (principalId == null) {
                    throw new MetaStoreException(ErrorCode.ROLE_ID_NOT_FOUND, principalName);
                }
                addMetaPolicyInner(context,projectId,principalType,principalSource,principalId,
                    objectType,catalogInnerObject,privilege,effect,isOwner,condition,obligation, grantAble);
                break;
            case GROUP:
                principalId = userGroupStore.getGroupIdByName(context, projectId, principalName);
                if (principalId == null) {
                    throw new MetaStoreException(ErrorCode.ROLE_ID_NOT_FOUND, principalName);
                }
                addMetaPolicyInner(context,projectId,principalType,principalSource,principalId,
                    objectType,catalogInnerObject,privilege,effect,isOwner,condition,obligation, grantAble);
                break;
            case ROLE:
                principalId = roleStore.getRoleId(context, projectId, principalName);
                if (principalId == null) {
                    throw new MetaStoreException(ErrorCode.ROLE_ID_NOT_FOUND, principalName);
                }
                addMetaPolicyInner(context,projectId,principalType,principalSource,principalId,
                    objectType,catalogInnerObject,privilege,effect,isOwner,condition,obligation,grantAble);
                break;
            case SHARE:
                principalId = shareStore.getShareId(context, projectId, principalName);
                if (principalId == null) {
                    throw new MetaStoreException(ErrorCode.SHARE_ID_NOT_FOUND, principalName);
                }
                addMetaPolicyInner(context,projectId,principalType,principalSource,principalId,
                    objectType,catalogInnerObject,privilege,effect,isOwner,condition,obligation,grantAble);
                break;
        }
    }

    private void addMetaPolicyInner(TransactionContext context, String projectId,
        PrincipalType principalType, int principalSource, String principalId,
        int objectType, CatalogInnerObject catalogInnerObject, long privilege, boolean effect, boolean isOwner,
        String condition, String obligation, boolean grantAble) {

        String objectId = getObjectIdFromObject(objectType, catalogInnerObject);
        MetaPrivilegePolicy metaPrivilegePolicy = policyStore.getMetaPrivilegePolicy(context, projectId,
            principalType, principalSource, principalId, objectType, objectId, effect, privilege);
        long updateTime = RecordStoreHelper.getCurrentTime();
        if (metaPrivilegePolicy == null) {
            String policyId = policyStore.insertMetaPrivilegePolicy(context, projectId, principalType, principalSource, principalId,
                objectType, objectId, effect, privilege, isOwner, condition, obligation, updateTime, grantAble);
            if (policyId == null) {
                throw new MetaStoreException(ErrorCode.POLICY_ID_NOT_FOUND);
            }
            policyStore.insertMetaPrivilegePolicyHistory(context,projectId,policyId,
                principalType,principalSource,principalId, PolicyModifyType.ADD.getNum(), updateTime);
        } else {
            String newObligation = modifyObigation(metaPrivilegePolicy.getObligation(), obligation, true);
            String newCondition = modifyCondition(metaPrivilegePolicy.getCondition(), condition, true);
            String policyId = policyStore.updateMetaPrivilegePolicy(context, projectId, principalType,
                metaPrivilegePolicy, newCondition, newObligation, updateTime, grantAble);
            if (policyId == null) {
                throw new MetaStoreException(ErrorCode.POLICY_ID_NOT_FOUND);
            }
            policyStore.updateMetaPrivilegePolicyHistory(context,projectId, policyId,
                principalType,principalSource,principalId, PolicyModifyType.UPDATE.getNum(), updateTime);
        }
    }

    private void addDataPolicyInner(TransactionContext context, String projectId, PrincipalType principalType, int principalSource, String principalId,
        String obsPath, String obsEndpoint, int permission) {

        ObsPrivilegePolicy dataPrivilegePolicy = policyStore.getDataPrivilegePolicy(context, projectId,
            principalType, principalSource, principalId, obsPath, obsEndpoint);
        long updateTime = RecordStoreHelper.getCurrentTime();
        if (dataPrivilegePolicy == null) {
            policyStore.insertDataPrivilegePolicy(context, projectId, principalType, principalSource, principalId,
                obsPath, obsEndpoint, permission, updateTime);
        } else {
            int newPermission = permission | dataPrivilegePolicy.getPermission();
            policyStore.updateDataPrivilegePolicy(context, projectId, principalType, dataPrivilegePolicy, newPermission,updateTime);
        }
    }

    @Override
    public void removeAllMetaPrivilegeFromPrincipal(String projectId, String principalName, PolicyInput policyInput) {
        PrincipalType type = PrincipalType.getPrincipalType(policyInput.getPrincipalType());
        PrincipalSource source = PrincipalSource.getPrincipalSource(policyInput.getPrincipalSource());
        byte[] continuation = null;
        do {
            try (TransactionContext context = storeTransaction.openTransaction()) {
                continuation = removeMetaPoliciesByPrincipalId(context, projectId, type, source.getNum(),
                    principalName, continuation);
                context.commit();
            }
        } while (continuation != null);
    }

    private byte[] removeMetaPoliciesByPrincipalId(TransactionContext context, String projectId, PrincipalType type,
        int principalSource, String principalName, byte[] continuation) {
        String principalId;
        byte[] needContinue = null;
        switch (type) {
            case USER:
                principalId = userGroupStore.getUserIdByName(context, projectId, principalSource,
                    principalName);
                if (principalId == null) {
                    throw new MetaStoreException(ErrorCode.ROLE_ID_NOT_FOUND, principalName);
                }
                needContinue = policyStore.delAllMetaPrivilegesOfPrincipalWithToken(context, projectId, type, principalSource,
                    principalId, continuation);
                break;
            case GROUP:
                principalId = userGroupStore.getGroupIdByName(context, projectId, principalName);
                if (principalId == null) {
                    throw new MetaStoreException(ErrorCode.ROLE_ID_NOT_FOUND, principalName);
                }
                needContinue = policyStore.delAllMetaPrivilegesOfPrincipalWithToken(context, projectId, type, principalSource,
                    principalId, continuation);
                break;
            case ROLE:
                principalId = roleStore.getRoleId(context, projectId, principalName);
                if (principalId == null) {
                    throw new MetaStoreException(ErrorCode.ROLE_ID_NOT_FOUND, principalName);
                }
                needContinue = policyStore.delAllMetaPrivilegesOfPrincipalWithToken(context, projectId, type, principalSource,
                    principalId, continuation);
                break;
            case SHARE:
                principalId = shareStore.getShareId(context, projectId, principalName);
                if (principalId == null) {
                    throw new MetaStoreException(ErrorCode.SHARE_ID_NOT_FOUND, principalName);
                }
                needContinue = policyStore.delAllMetaPrivilegesOfPrincipalWithToken(context, projectId, type, principalSource,
                    principalId, continuation);
                break;
        }
        return needContinue;
    }

    @Override
    public void removeAllPrivilegeOnObjectFromPrincipal(String projectId, String principalName,
                                                        PolicyInput policyInput) {
        PrincipalType type = PrincipalType.getPrincipalType(policyInput.getPrincipalType());
        PrincipalSource source = PrincipalSource.getPrincipalSource(policyInput.getPrincipalSource());
        ObjectType objectType = ObjectType.valueOf(policyInput.getObjectType().toUpperCase());
        CatalogInnerObject catalogInnerObject = ServiceImplHelper.getCatalogObject(projectId, objectType.name(), policyInput.getObjectName());
        String objectId = getObjectIdFromObject(objectType.getNum(), catalogInnerObject);
        long[] privilegeArray = PrivilegePolicyHelper.getObjectAllPrivilegeArrayByType(objectType.getNum());
        if (privilegeArray == null) {
            throw new CatalogServerException(ErrorCode.ROLE_PRIVILEGE_INVALID);
        }
        byte[] continuation = null;
        do {
            try (TransactionContext context = storeTransaction.openTransaction()) {
                continuation = removeMetaPoliciesByPrincipalIdAndObject(context, projectId,
                    type, source.getNum(), principalName,
                    objectType.getNum(), objectId, continuation);
                context.commit();
            }
        } while (continuation != null);

    }

    private byte[] removeMetaPoliciesByPrincipalIdAndObject(TransactionContext context, String projectId, PrincipalType type,
        int principalSource, String principalName, int objectType, String objectId, byte[] continuation) {
        String principalId;
        byte[] needContinue = null;
        switch (type) {
            case USER:
                principalId = userGroupStore.getUserIdByName(context, projectId, principalSource,
                    principalName);
                needContinue = policyStore.delAllMetaPrivilegesOfPrincipalOnObjectWithToken(context, projectId,
                    type, principalSource, principalId, objectType, objectId,continuation);
                break;
            case GROUP:
                principalId = userGroupStore.getGroupIdByName(context, projectId, principalName);
                needContinue = policyStore.delAllMetaPrivilegesOfPrincipalOnObjectWithToken(context, projectId,
                    type, principalSource, principalId, objectType, objectId,continuation);
                break;
            case ROLE:
                principalId = roleStore.getRoleId(context, projectId, principalName);
                needContinue = policyStore.delAllMetaPrivilegesOfPrincipalOnObjectWithToken(context, projectId,
                    type, principalSource, principalId, objectType, objectId,continuation);
                break;
            case SHARE:
                principalId = shareStore.getShareId(context, projectId, principalName);
                needContinue = policyStore.delAllMetaPrivilegesOfPrincipalOnObjectWithToken(context, projectId,
                    type, principalSource, principalId, objectType, objectId,continuation);
                break;
        }
        return needContinue;
    }


    @Override
    public void revokeMetaPolicyFromPrincipal(String projectId, String principalName,
                                              PolicyInput policyInput) {

        PrincipalType type = PrincipalType.getPrincipalType(policyInput.getPrincipalType());
        PrincipalSource source = PrincipalSource.getPrincipalSource(policyInput.getPrincipalSource());
        List<Operation> operationList = policyInput.getOperationList();
        ObjectType objectType = ObjectType.valueOf(policyInput.getObjectType().toUpperCase());
        CatalogInnerObject catalogInnerObject = ServiceImplHelper.getCatalogObject(projectId, objectType.name(), policyInput.getObjectName());
        boolean effect = policyInput.getEffect();
        boolean isOwner = policyInput.getOwner();
        boolean grantAble = policyInput.getGrantAble();
        String condition = policyInput.getCondition();
        String obligation = policyInput.getObligation();
        long privilege = 0;
        boolean isRevokeAttr = true;
        if (condition == null && obligation == null) {
            isRevokeAttr = false;
        }
        try (TransactionContext context = storeTransaction.openTransaction()) {
            if (isOwner && type == PrincipalType.USER) {
                revokeOneMetaPolicyFromPrincipal(context, projectId, type, source.getNum(), principalName,
                    objectType.getNum(), catalogInnerObject, privilege, effect, isOwner, condition, obligation, grantAble, isRevokeAttr);
                return;
            }
            for (Operation operation : operationList) {
                OperationPrivilege operationPrivilege;
                if (type == PrincipalType.SHARE) {
                    operationPrivilege = shareOperationPrivilegeMap.get(operation);
                } else {
                    operationPrivilege = operationPrivilegeMap.get(operation);
                }
                if (operationPrivilege == null) {
                    throw new CatalogServerException(ErrorCode.ROLE_PRIVILEGE_INVALID);
                }
                privilege = convertPrivilege(operationPrivilege);

                if (privilege == 0) {
                    throw new CatalogServerException(ErrorCode.ROLE_PRIVILEGE_INVALID);
                }
                revokeOneMetaPolicyFromPrincipal(context, projectId, type, source.getNum(), principalName,
                    objectType.getNum(), catalogInnerObject, privilege, effect, isOwner, condition, obligation, grantAble,isRevokeAttr);
            }
            context.commit();
        }
    }


    private void revokeOneMetaPolicyFromPrincipal(TransactionContext context, String projectId, PrincipalType principalType, int principalSource, String principalName,
        int objectType, CatalogInnerObject catalogInnerObject, long privilege, boolean effect,
        boolean isOwner, String condition, String obligation, boolean grantAble, boolean isRevokeAttr) {

        String principalId = null;
        switch (principalType) {
            case USER:
                principalId = userGroupStore.getUserIdByName(context, projectId, principalSource, principalName);
                revokeMetaPolicyInner(context,projectId,principalType,principalSource,principalId,
                    objectType,catalogInnerObject,privilege,effect,isOwner, condition,obligation,grantAble,isRevokeAttr);
                break;
            case GROUP:
                principalId = userGroupStore.getGroupIdByName(context, projectId, principalName);
                revokeMetaPolicyInner(context,projectId,principalType,principalSource,principalId,
                    objectType,catalogInnerObject,privilege,effect,isOwner,condition,obligation,grantAble,isRevokeAttr);
                break;
            case ROLE:
                principalId = roleStore.getRoleId(context, projectId, principalName);
                revokeMetaPolicyInner(context,projectId,principalType,principalSource,principalId,
                    objectType,catalogInnerObject,privilege,effect,isOwner,condition,obligation, grantAble,isRevokeAttr);
                break;
            case SHARE:
                principalId = shareStore.getShareId(context, projectId, principalName);
                revokeMetaPolicyInner(context,projectId,principalType,principalSource,principalId,
                    objectType,catalogInnerObject,privilege,effect,isOwner,condition,obligation, grantAble,isRevokeAttr);
                break;
        }
    }

    private void revokeMetaPolicyInner(TransactionContext context, String projectId, PrincipalType principalType, int principalSource, String principalId,
        int objectType, CatalogInnerObject catalogInnerObject, long privilege, boolean effect,
        boolean isOwner, String condition, String obligation, boolean grantAble, boolean isRevokeAttr) {

        String objectId = getObjectIdFromObject(objectType, catalogInnerObject);
        MetaPrivilegePolicy metaPrivilegePolicy = policyStore.getMetaPrivilegePolicy(context, projectId,
            principalType, principalSource, principalId, objectType, objectId, effect, privilege);
        if (metaPrivilegePolicy == null) {
            throw new CatalogServerException(ErrorCode.ROLE_PRIVILEGE_INVALID);
        }
        long updateTime = RecordStoreHelper.getCurrentTime();
        if (isRevokeAttr) {
            String newCondition = modifyCondition(metaPrivilegePolicy.getCondition(), condition, false);
            String newObligation = modifyObigation(metaPrivilegePolicy.getObligation(), obligation, false);
            String policyId = policyStore.updateMetaPrivilegePolicy(context, projectId, principalType, metaPrivilegePolicy,
                newCondition, newObligation, updateTime, grantAble);
            policyStore.updateMetaPrivilegePolicyHistory(context,projectId,policyId,principalType,principalSource,principalId,
                PolicyModifyType.UPDATE.getNum(),updateTime);
        } else {
            String policyId = policyStore.deleteMetaPrivilegePolicy(context, projectId, principalType, principalSource,principalId,
                objectType,objectId,effect,privilege, isOwner);
            policyStore.updateMetaPrivilegePolicyHistory(context,projectId,policyId,principalType,principalSource,principalId,
                PolicyModifyType.DELETE.getNum(),updateTime);
        }
    }


    @Override
    public List<Policy> showMetaPolicyFromPrincipal(String projectId, String type, String source,
        String principalName) {

        PrincipalType principalType = PrincipalType.getPrincipalType(type);
        PrincipalSource principalSource = PrincipalSource.getPrincipalSource(source);
        String principalId;
        byte[] continuation = null;
        List<Policy> policyList = new ArrayList<>();
        List<MetaPrivilegePolicy> privilegesList = new ArrayList<>();
        do {
            try (TransactionContext context = storeTransaction.openTransaction()) {
                switch (principalType) {
                    case USER:
                        principalId = userGroupStore.getUserIdByName(context, projectId,
                            principalSource.getNum(), principalName);
                        // linshi fangan
                        principalId =  principalName;
                        break;
                    case GROUP:
                        principalId = userGroupStore.getGroupIdByName(context, projectId, principalName);
                        break;
                    case ROLE:
                        principalId = roleStore.getRoleId(context, projectId, principalName);
                        break;
                    case SHARE:
                        principalId = shareStore.getShareId(context, projectId, principalName);
                        break;
                    default:
                        context.commit();
                        return null;
                }
                continuation = policyStore.getMetaPrivilegesByPrincipalWithToken(context, projectId,
                    principalType, principalSource.getNum(), principalId, privilegesList, continuation);
                for (MetaPrivilegePolicy metaPolicy : privilegesList) {
                    ObjectPrivilege objectPrivilege = PrivilegePolicyHelper.convertObjectPrivilege(context, projectId,
                        metaPolicy);
                    Policy policy = toPolicy(projectId, metaPolicy, objectPrivilege);
                    policyList.add(policy);
                }
                context.commit();
            }
        } while (continuation != null);

        return policyList;
    }

    @Override
    public List<MetaPrivilegePolicy> listMetaPolicyByPrincipal(String projectId, List<Principal> principalList) {
        List<MetaPrivilegePolicy> privilegesList = new ArrayList<>();
        for (Principal principal: principalList) {
            List<MetaPrivilegePolicy> princpalPolicyList = listMetaPolicyOfOnePrincipal(projectId,
                principal.getPrincipalType(), principal.getPrincipalSource(), principal.getPrincipalId());
            if (princpalPolicyList != null) {
                privilegesList.addAll(princpalPolicyList);
            }
        }
        return privilegesList;
    }


    public List<MetaPrivilegePolicy> listMetaPolicyOfOnePrincipal(String projectId, String type, String source,
        String principalName) {

        PrincipalType principalType = PrincipalType.getPrincipalType(type);
        PrincipalSource principalSource = PrincipalSource.getPrincipalSource(source);
        String principalId;
        byte[] continuation = null;
        List<MetaPrivilegePolicy> privilegesList = new ArrayList<>();
        do {
            try (TransactionContext context = storeTransaction.openTransaction()) {
                switch (principalType) {
                    case USER:
                        principalId = userGroupStore.getUserIdByName(context, projectId,
                            principalSource.getNum(), principalName);
                        // linshi fangan
                        principalId =  principalName;
                        break;
                    case GROUP:
                        principalId = userGroupStore.getGroupIdByName(context, projectId, principalName);
                        break;
                    case ROLE:
                        principalId = roleStore.getRoleId(context, projectId, principalName);
                        break;
                    case SHARE:
                        principalId = shareStore.getShareId(context, projectId, principalName);
                        break;
                    default:
                        context.commit();
                        return null;
                }
                continuation = policyStore.getMetaPrivilegesByPrincipalWithToken(context, projectId,
                    principalType, principalSource.getNum(), principalId, privilegesList, continuation);
                for (MetaPrivilegePolicy metaPolicy : privilegesList) {
                   String objectName = PrivilegePolicyHelper.convertObjectName(context,projectId,metaPolicy);
                   metaPolicy.setObjectName(objectName);
                }
                context.commit();
            }
        } while (continuation != null);

        return privilegesList;
    }

    private Policy toPolicy(String projectId, MetaPrivilegePolicy metaPolicy, ObjectPrivilege objectPrivilege) {
        Policy policy = new Policy();
        policy.setPolicyId(metaPolicy.getPolicyId());
        policy.setProjectId(projectId);
        policy.setPrincipalType(PrincipalType.getPrincipalType(metaPolicy.getPrincipalType()));
        policy.setPrincipalSource(PrincipalSource.getPrincipalSource(metaPolicy.getPrincipalSource()));
        policy.setPrincipalId(metaPolicy.getPrincipalId());

        policy.setPrivilege(objectPrivilege.getPrivilege());
        policy.setObjectType(objectPrivilege.getObjectType());
        policy.setObjectName(objectPrivilege.getObjectName());

        if (metaPolicy.getObligation() != null) {
            policy.setObligation(metaPolicy.getObligation());
        }
        if (metaPolicy.getCondition() != null) {
            policy.setCondition(metaPolicy.getCondition());
        }

        policy.setOwner(PrivilegePolicyHelper.isOwnerOfObject(metaPolicy.getPrivilege()));
        policy.setGrantAble(String.valueOf(metaPolicy.isGrantAble()));
        policy.setCreatedTime(metaPolicy.getUpdateTime());
        return policy;
    }

    @Override
    public List<MetaPolicyHistory> getUpdatedMetaPolicyIdsByTime(String projectId, long time) {
        List<MetaPolicyHistory> policyHistoryList  = new ArrayList<>();
        byte[] continuation = null;
        do {
            try (TransactionContext context = storeTransaction.openTransaction()) {
                continuation = policyStore
                    .getMetaPolicyHistoryWithToken(context, projectId, time,
                        policyHistoryList, continuation);
                context.commit();
            }
        } while (continuation != null);

        continuation = null;
        do {
            try (TransactionContext context = storeTransaction.openTransaction()) {
                continuation = policyStore
                    .getShareMetaPolicyHistoryWithToken(context, projectId, time,
                        policyHistoryList, continuation);
                context.commit();
            }
        } while (continuation != null);

        return policyHistoryList;
    }

    @Override
    public List<MetaPrivilegePolicy> getUpdatedMetaPolicyByIdList(String projectId, String type,
        List<String> policyIdList) {
        PrincipalType principalType = PrincipalType.getPrincipalType(type);
        List<MetaPrivilegePolicy> policyObjectList = new ArrayList<>();
        try (TransactionContext context = storeTransaction.openTransaction()) {
            for (String policyId : policyIdList) {
                policyStore.getMetaPrivilegeByPolicyId(context, projectId, principalType, policyId, policyObjectList);
            }
            context.commit();
        }
        try (TransactionContext context = storeTransaction.openTransaction()) {
            for (MetaPrivilegePolicy metaPolicy : policyObjectList) {
                String objectName = PrivilegePolicyHelper.convertObjectName(context, projectId, metaPolicy);
                metaPolicy.setObjectName(objectName);
            }
            context.commit();
        }
        return policyObjectList;
    }

    public static long convertPrivilege(OperationPrivilege operationPrivilege) {
        return 1L << operationPrivilege.getPrivilege();
    }

    public static String modifyCondition(String oldCondition , String newCondition, boolean isAdd) {
        if (newCondition == null || newCondition.isEmpty() ) {
            return oldCondition;
        }
        if (oldCondition == null || oldCondition.isEmpty() ) {
            return newCondition;
        }
        String[] oldConditionRules = oldCondition.split(",");
        String[] newConditionRules = newCondition.split(",");
        HashSet<String> conditionSet = null;
        String result = "";
        conditionSet = new HashSet<>(Arrays.asList(oldConditionRules));
        if (isAdd) {
            conditionSet.addAll(Arrays.asList(newConditionRules));
        } else {
            conditionSet.removeAll(Arrays.asList(newConditionRules));
        }
        result = conditionSet.stream().collect(Collectors.joining(","));
        return  result;
    }


    public static String modifyObigation(String  oldObligation, String newObligation, boolean isAdd) {
        if (newObligation.isEmpty() || newObligation == null) {
            return oldObligation;
        }
        if (oldObligation.isEmpty() || oldObligation == null) {
            return newObligation;
        }
        String[] oldObligationRules = oldObligation.split(",");
        String[] newObligationRules = newObligation.split(",");
        HashSet<String> obligationSet = null;
        String result = "";
        obligationSet = new HashSet<>(Arrays.asList(oldObligationRules));
        if (isAdd) {
            obligationSet.addAll(Arrays.asList(newObligationRules));
        } else {
            obligationSet.removeAll(Arrays.asList(newObligationRules));
        }
        result = obligationSet.stream().collect(Collectors.joining(","));
        return  result;
    }


    private String getObjectIdFromObject(int objectType, CatalogInnerObject catalogInnerObject) {
        if (objectType == ObjectType.TABLE.getNum() || objectType == ObjectType.VIEW.getNum()) {
            return catalogInnerObject.getCatalogId() + "." + catalogInnerObject.getDatabaseId() + "."
                + catalogInnerObject.getObjectId();
        } else if (objectType == ObjectType.DATABASE.getNum()) {
            return catalogInnerObject.getCatalogId() + "." + catalogInnerObject.getObjectId();
        } else {
            return catalogInnerObject.getObjectId();
        }
    }

}
