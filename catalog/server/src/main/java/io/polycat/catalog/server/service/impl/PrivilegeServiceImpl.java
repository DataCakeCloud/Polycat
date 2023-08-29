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
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import io.polycat.catalog.authentication.Authentication;
import io.polycat.catalog.authentication.model.TokenParseResult;
import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.model.AuthTableObjParam;
import io.polycat.catalog.common.model.AuthorizationResponse;
import io.polycat.catalog.common.model.AuthorizationType;
import io.polycat.catalog.common.model.Catalog;
import io.polycat.catalog.common.model.CatalogIdent;
import io.polycat.catalog.common.model.CatalogInnerObject;
import io.polycat.catalog.common.model.GrantObject;
import io.polycat.catalog.common.model.OperationPrivilege;
import io.polycat.catalog.common.model.RolePrivilegeObject;
import io.polycat.catalog.common.model.RoleUserObject;
import io.polycat.catalog.common.model.ShareConsumerObject;
import io.polycat.catalog.common.model.ShareObject;
import io.polycat.catalog.common.model.SharePrivilegeObject;
import io.polycat.catalog.common.model.TableObject;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.User;
import io.polycat.catalog.common.model.UserPrivilege;
import io.polycat.catalog.common.plugin.request.input.AuthorizationInput;
import io.polycat.catalog.common.plugin.request.input.ShareInput;
import io.polycat.catalog.common.utils.CatalogStringUtils;
import io.polycat.catalog.server.security.ApiToOperation;
import io.polycat.catalog.server.util.TransactionRunnerUtil;
import io.polycat.catalog.service.api.PrivilegeService;
import io.polycat.catalog.store.api.RoleStore;
import io.polycat.catalog.store.api.ShareStore;
import io.polycat.catalog.store.api.Transaction;
import io.polycat.catalog.store.api.UserPrivilegeStore;


import io.polycat.catalog.common.model.TableName;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;

@Slf4j
@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class PrivilegeServiceImpl implements PrivilegeService {
    @Autowired
    private Transaction storeTransaction;
    @Autowired
    private RoleStore roleStore;
    @Autowired
    private ShareStore shareStore;
    @Autowired
    private UserPrivilegeStore userPrivilegeStore;

    private Optional<UserPrivilege> getUserPrivilege(String projectId, String userId, String objectType,
                                                     String objectId) {
        return TransactionRunnerUtil.transactionReadRunThrow(context -> {
            return getUserPrivilege(context, projectId, userId, objectType, objectId);
        }).getResult();
    }

    private Optional<UserPrivilege> getUserPrivilege(TransactionContext context, String projectId, String userId, String objectType,
                                                     String objectId) {
        UserPrivilege result = userPrivilegeStore.getUserPrivilege(context, projectId, userId, objectType, objectId);
        if (result == null) {
            return Optional.empty();
        }
        return Optional.of(result);
    }

    public AuthorizationResponse authentication(String projectId, AuthorizationInput authorizationInput) {
        return authentication(projectId, authorizationInput, "");
    }

    public AuthorizationResponse authentication(String projectId, AuthorizationInput authorizationInput,
        String apiMethod) {
        AuthorizationResponse result = new AuthorizationResponse();
        CatalogStringUtils.catalogInnerNormalize(authorizationInput.getCatalogInnerObject());
        result.setAllowed(false);
        boolean isPermit;

        switch (authorizationInput.getAuthorizationType()) {
            case NORMAL_OPERATION:
                isPermit = isPermitForNormalOperation(projectId, authorizationInput, apiMethod);
                break;
            case SELECT_FROM_SHARE:
                isPermit = isPermitForSelectFromShare(authorizationInput);
                break;
            case GRANT_PRIVILEGE_TO_OBJECT:
                isPermit = isPermitForGrantObject(projectId, authorizationInput);
                break;
            case GRANT_SHARE_TO_USER:
                isPermit = isPermitForGrantShare(authorizationInput);
                break;
            default:
                throw new CatalogServerException(ErrorCode.AUTHORIZATION_TYPE_ERROR);
        }

        result.setAllowed(isPermit);
        return result;
    }

    @Override
    public AuthorizationResponse sqlAuthentication(String projectId, List<AuthorizationInput> authInputList,
                                                   Boolean ignoreUnknownObj) {
        Operation operation = Operation.NULL_OPERATION_AUTHORIZED;
        try {
            for (AuthorizationInput authIn : authInputList) {
                operation = authIn.getOperation();
                if (!authentication(projectId, authIn).getAllowed()) {
                    return new AuthorizationResponse(false, operation);
                }
            }
            return new AuthorizationResponse(true, operation);
        } catch (MetaStoreException e) {
            if (e.getErrorCode().getStatusCode().equals(HttpStatus.NOT_FOUND) && ignoreUnknownObj) {
                return new AuthorizationResponse(true, operation);
            } else {
                throw e;
            }
        }
    }

    private boolean isPermitForGrantShare(AuthorizationInput authorizationInput) {
//        ShareName shareName = ShareName.newBuilder().setProjectId(authorizationInput.getCatalogObject().getProjectId())
//            .setName(authorizationInput.getCatalogObject().getObjectName()).build();
        String shareId;
        List<ShareConsumerObject> shareConsumerObjectList = new ArrayList<>();
        try (TransactionContext context = storeTransaction.openTransaction()) {
            shareId = shareStore.getShareId(context, authorizationInput.getCatalogInnerObject().getProjectId(),
                authorizationInput.getCatalogInnerObject().getObjectName());
            shareConsumerObjectList = shareStore.getShareAllConsumers(context,
                authorizationInput.getCatalogInnerObject().getProjectId(), shareId);
            context.commit();
        }
        //List<ShareAccountRecord> shareAccountRecords = versionStore.getShareAccount(shareIdent);
        for (ShareConsumerObject shareConsumerObject : shareConsumerObjectList) {
            if (shareConsumerObject.getAccountId().equals(authorizationInput.getUser().getAccountId())
                && shareConsumerObject.getManagerUser().equals(authorizationInput.getUser().getUserId())) {
                return true;
            }
        }

        return false;
    }

    private boolean checkGrantObjectPrivilege(String projectId, AuthorizationInput authorizationInput) {

        String grantObjectId;
        if (authorizationInput.getGrantObject().getObjectType() == ObjectType.ROLE) {
            /*RoleName roleName = RoleName.newBuilder().setProjectId(projectId)
                .setName(authorizationInput.getGrantObject().getObjectName()).build();
            RoleRecord role = versionStore.getRoleByName(roleName);
            if (role == null) {
                throw new CatalogServerException(ErrorCode.ROLE_NOT_FOUND, roleName.getName());
            }
            grantObjectId = role.getRoleId();*/
            grantObjectId = TransactionRunnerUtil.transactionReadRunThrow(context ->  {
                return roleStore.getRoleId(context, projectId, authorizationInput.getGrantObject().getObjectName());
            }).getResult();
        } else if (authorizationInput.getGrantObject().getObjectType() == ObjectType.SHARE) {
            /*ShareName shareName = ShareName.newBuilder().setProjectId(projectId)
                .setName(authorizationInput.getGrantObject().getObjectName()).build();
            grantObjectId = versionStore.getShareByName(shareName).getShareId();*/
            grantObjectId = TransactionRunnerUtil.transactionReadRunThrow(context ->  {
                return shareStore.getShareId(context, projectId, authorizationInput.getGrantObject().getObjectName());
            }).getResult();
        } else {
            return false;
        }

        Optional<UserPrivilege> userGrantPrivilege = getUserPrivilege(projectId, authorizationInput.getUser().getUserId(),
            authorizationInput.getGrantObject().getObjectType().name(), grantObjectId);
        return userGrantPrivilege.isPresent() && userGrantPrivilege.get().isOwner();
    }

    private boolean isPermitForGrantObject(String projectId, AuthorizationInput authorizationInput) {

        if ((Operation.REVOKE_ALL_OPERATION_FROM_ROLE == authorizationInput.getOperation())
            || (Operation.REVOKE_ALL_OPERATION == authorizationInput.getOperation())
            || (Operation.ADD_ALL_OPERATION == authorizationInput.getOperation())) {
            if (checkGrantObjectPrivilege(projectId, authorizationInput)) {
                return true;
            }
            return false;
        }

        //1. objectName to id
        OperationPrivilege operationPrivilege = RolePrivilegeHelper.getOperationPrivilege(
            authorizationInput.getOperation());
        if (operationPrivilege == null) {
            return false;
        }

        CatalogInnerObject catalogInnerObject = RolePrivilegeHelper.getCatalogObject(projectId, operationPrivilege.getObjectType().name(),
            authorizationInput.getCatalogInnerObject().getObjectName());

        //2. get user privilege by (userId + projectId+ objectType + objectId)
        Optional<UserPrivilege> userPrivilege = getUserPrivilege(projectId, authorizationInput.getUser().getUserId(),
            operationPrivilege.getObjectType().name(), catalogInnerObject.getObjectId());
        if (!userPrivilege.isPresent() || !userPrivilege.get().isOwner()) {
            return false;
        }

        //3. get grant object id
        String grantObjectId;
        if (authorizationInput.getGrantObject().getObjectType() == ObjectType.ROLE) {
            /*RoleName roleName = RoleName.newBuilder().setProjectId(projectId)
                .setName(authorizationInput.getGrantObject().getObjectName()).build();
            RoleRecord role = versionStore.getRoleByName(roleName);
            grantObjectId = role.getRoleId();*/

            grantObjectId = TransactionRunnerUtil.transactionReadRunThrow(context -> roleStore.getRoleId(context, projectId, authorizationInput.getGrantObject().getObjectName())).getResult();
        } else if (authorizationInput.getGrantObject().getObjectType() == ObjectType.SHARE) {
            /*ShareName shareName = ShareName.newBuilder().setProjectId(projectId)
                .setName(authorizationInput.getGrantObject().getObjectName()).build();
            ShareRecord share = versionStore.getShareByName(shareName);
            grantObjectId = share.getShareId();*/
            grantObjectId = TransactionRunnerUtil.transactionReadRunThrow(context -> shareStore.getShareId(context, projectId, authorizationInput.getGrantObject().getObjectName())).getResult();
        } else {
            return false;
        }

        Optional<UserPrivilege> userGrantPrivilege = getUserPrivilege(projectId, authorizationInput.getUser().getUserId(),
            authorizationInput.getGrantObject().getObjectType().name(), grantObjectId);
        if (!userGrantPrivilege.isPresent() || !userGrantPrivilege.get().isOwner()) {
            return false;
        }

        return true;
    }

    private boolean isPermitForSelectFromShare(AuthorizationInput authorizationInput) {
        // 1. check whether we have the share record by the share name
        String projectId = authorizationInput.getCatalogInnerObject().getProjectId();
//        ShareName shareName = ShareName.newBuilder().setProjectId(projectId)
//            .setName(authorizationInput.getCatalogObject().getCatalogName()).build();
        //ShareRecord share = versionStore.getShareByName(shareName);
        ShareObject shareObject;
        shareObject = TransactionRunnerUtil.transactionReadRunThrow(context -> {
            String shareId = shareStore.getShareId(context, authorizationInput.getCatalogInnerObject().getProjectId(),
                authorizationInput.getCatalogInnerObject().getCatalogName());
            return shareStore.getShareProperties(context, authorizationInput.getCatalogInnerObject().getProjectId(),
                shareId);
        }).getResult();

        // 2. check the corresponding catalog of the share is existed and valid
        if (StringUtils.isBlank(shareObject.getCatalogId())) {
            throw new CatalogServerException(ErrorCode.SHARE_PRIVILEGE_INVALID);
        }
        CatalogIdent catalogIdent = new CatalogIdent(projectId,shareObject.getCatalogId());
        Catalog catalogRecord = CatalogObjectHelper.getCatalog(catalogIdent);
        if (catalogRecord == null) {
            return false;
        }

        // 3. check the share usage scenario of different users in the same tenant
        if (shareObject.getOwnerAccount().equals(authorizationInput.getUser().getAccountId())) {
            return authSelectShareBySameAccount(authorizationInput, projectId, shareObject.getShareId(), catalogRecord);
        }

        // 4. check the share usage scenario of different tenants
        String inShareId = shareObject.getShareId();
        List<ShareConsumerObject> shareConsumerObjectList = new ArrayList<>();
        shareConsumerObjectList = TransactionRunnerUtil.transactionReadRunThrow(context -> {
            return shareStore.getShareAllConsumers(context, authorizationInput.getCatalogInnerObject().getProjectId(),
                shareObject.getShareId());
        }).getResult();
        //List<ShareAccountRecord> shareAccountRecords = versionStore.getShareAccount(shareIdent);
        for (ShareConsumerObject shareConsumerObject : shareConsumerObjectList) {
            if (!shareConsumerObject.getAccountId().equals(authorizationInput.getUser().getAccountId())) {
                continue;
            }

            if (!shareConsumerObject.getUsers().containsKey(authorizationInput.getUser().getUserId())
                && !shareConsumerObject.getManagerUser().equals(authorizationInput.getUser().getUserId())) {
                return false;
            }

            TableName tableName = new TableName(projectId, catalogRecord.getCatalogName(),
                authorizationInput.getCatalogInnerObject().getDatabaseName(),
                authorizationInput.getCatalogInnerObject().getObjectName());
            TableObject table = TableObjectHelper.getTableObject(tableName);
            if (table == null) {
                return false;
            }
            return hasPrivilegeByShare(projectId, inShareId, ObjectType.TABLE.name(), table.getTableId());
        }
        return false;
    }

    private boolean authSelectShareBySameAccount(AuthorizationInput authorizationInput, String projectId,
        String shareId, Catalog catalogRecord) {
        TableName tableName = new TableName(projectId, catalogRecord.getCatalogName(),
            authorizationInput.getCatalogInnerObject().getDatabaseName(),
            authorizationInput.getCatalogInnerObject().getObjectName());
        TableObject table = TableObjectHelper.getTableObject(tableName);
        if (table == null) {
            throw new CatalogServerException(ErrorCode.TABLE_NOT_FOUND,
                authorizationInput.getCatalogInnerObject().getObjectName());
        }
        return hasPrivilegeByShare(projectId, shareId, ObjectType.TABLE.name(), table.getTableId());
    }

    private String constructDroppedObjectName(ObjectType objectType, AuthorizationInput authorizationInput) {

        String objectName;
        if (objectType == ObjectType.CATALOG) {
            if (null == authorizationInput.getCatalogInnerObject().getObjectId()) {
                objectName = authorizationInput.getCatalogInnerObject().getObjectName();
            } else {
                objectName = authorizationInput.getCatalogInnerObject().getObjectName()
                    + "." + authorizationInput.getCatalogInnerObject().getObjectId();
            }
        } else if (objectType == ObjectType.DATABASE) {
            if (null == authorizationInput.getCatalogInnerObject().getObjectId()) {
                objectName = authorizationInput.getCatalogInnerObject().getCatalogName()
                    + "." + authorizationInput.getCatalogInnerObject().getObjectName();
            } else {
                objectName = authorizationInput.getCatalogInnerObject().getCatalogName()
                    + "." + authorizationInput.getCatalogInnerObject().getObjectName()
                    + "." + authorizationInput.getCatalogInnerObject().getObjectId();
            }
        } else if (objectType == ObjectType.TABLE) {
            if (null == authorizationInput.getCatalogInnerObject().getObjectId()) {
                objectName = authorizationInput.getCatalogInnerObject().getCatalogName()
                    + "." + authorizationInput.getCatalogInnerObject().getDatabaseName()
                    + "." + authorizationInput.getCatalogInnerObject().getObjectName();

            } else {
                objectName = authorizationInput.getCatalogInnerObject().getCatalogName()
                    + "." + authorizationInput.getCatalogInnerObject().getDatabaseName()
                    + "." + authorizationInput.getCatalogInnerObject().getObjectName()
                    + "." + authorizationInput.getCatalogInnerObject().getObjectId();
            }
        } else {
            return null;
        }

        return objectName;
    }

    private boolean isPermitForNormalOperation(String projectId, AuthorizationInput authorizationInput,
        String apiMethod) {
        return TransactionRunnerUtil.transactionRunThrow(context -> {
            // 1. operation to object type and privilege type
            OperationPrivilege operationPrivilege = RolePrivilegeHelper.getOperationPrivilege(
                    authorizationInput.getOperation());
            if (operationPrivilege == null) {
                log.warn("Auth operation illegal: {}", authorizationInput.getOperation());
                return false;
            }

            // 2. get catalog object
            String objectType = operationPrivilege.getObjectType().name();
            CatalogInnerObject catalogInnerObject;
            String objectName = null;
            if (authorizationInput.getOperation() == Operation.UNDROP_TABLE
                    || authorizationInput.getOperation() == Operation.UNDROP_DATABASE
                    || authorizationInput.getOperation() == Operation.PURGE_TABLE) {
                catalogInnerObject = ServiceImplHelper.getDroppedCatalogObject(projectId, objectType,
                        constructDroppedObjectName(ObjectType.valueOf(objectType), authorizationInput));
            } else {
                objectName = RolePrivilegeHelper.constructObjectName(ObjectType.valueOf(objectType), authorizationInput);
                catalogInnerObject = RolePrivilegeHelper.getCatalogObject(projectId, objectType,
                        objectName);
            }
            catalogInnerObject = ServiceImplHelper.getCatalogObject(projectId,
                    objectType, objectName);
            String objectId = catalogInnerObject.getObjectId();

            // 3. judge whether user is the owner of the object. get user by (userId + projectId+ objectType + objectId)
            String userId = authorizationInput.getUser().getUserId();
            boolean isUserPermit = hasPrivilegeByUser(context, userId, projectId, objectType, catalogInnerObject, objectId);
            if (isUserPermit) {
                return true;
            }

            // 4. if user does not match, judge whether role privilege matches
            isUserPermit = hasRolePrivilegeByUser(context, operationPrivilege, apiMethod, catalogInnerObject, userId, projectId, objectName, objectId);
            if (isUserPermit) {
                return true;
            }

            return false;
        }).getResult();

    }

    private boolean hasRolePrivilegeByUser(TransactionContext context, OperationPrivilege operationPrivilege, String apiMethod,
                                           CatalogInnerObject catalogInnerObject, String userId, String projectId, String objectName, String objectId) {
        // 1. change api and operation privileges to bitmap, in order to compare with user privilege
        long inputPrivilegeBitmap = RolePrivilegeHelper.convertOperationPrivilege(operationPrivilege);
        if (inputPrivilegeBitmap == 0) {
            return false;
        }
        long apiPrivileges = getApiPrivilegesBitmap(apiMethod);
        inputPrivilegeBitmap |= apiPrivileges;

        // 2. get roles and compare with the privilege bitmap, if result is not 0, means user has the privilege
        String objectType = operationPrivilege.getObjectType().name();
        // Patch history rolePrivilegeObjectId issue
        String historyRolePrivilegeObjectId = RolePrivilegeHelper.getRolePrivilegeObjectIdSkipCheck(objectType, ServiceImplHelper.getCatalogObject(projectId,
                objectType, objectName));
        /*if (objectType.equals(ObjectType.TABLE.name()) || objectType.equals(ObjectType.VIEW.name())) {
            objectId = catalogInnerObject.getCatalogId() + "." + catalogInnerObject.getDatabaseId() + "."
                + catalogInnerObject.getObjectId();
        } else if (objectType.equals(ObjectType.DATABASE.name())) {
            objectId = catalogInnerObject.getCatalogId() + "." + catalogInnerObject.getObjectId();
        } else {
            objectId = catalogInnerObject.getObjectId();
        }*/
        return checkRolePrivileges(context, userId, projectId, objectType, objectId, objectName, inputPrivilegeBitmap, historyRolePrivilegeObjectId);
    }

    private long getApiPrivilegesBitmap(String apiMethod) {
        if (!StringUtils.isBlank(apiMethod)) {
            ApiToOperation a2o = new ApiToOperation();
            List<Operation> operationList = a2o.getOperationByApi(apiMethod).stream().map(Operation::valueOf)
                .collect(Collectors.toList());
            return operationList.stream().map(RolePrivilegeHelper::getOperationPrivilege)
                .mapToLong(RolePrivilegeHelper::convertOperationPrivilege).reduce(0, (a, b) -> a | b);
        }

        //if apiMethod is empty, return 0
        return 0;
    }

    private boolean checkRolePrivileges(TransactionContext context, String userId, String projectId, String objectType,
                                        String objectId, String objectName, long inputPrivilegeBitmap, String historyRolePrivilegeObjectId) {
        List<RoleUserObject> roleUserObjectList = roleStore.getRoleUsersByUserId(context, projectId, userId);
        //List<RoleUserObject> roleUserObjectList = versionStore.getRoleUserByUserId(projectId, userId);
        for (RoleUserObject roleUser : roleUserObjectList) {
            boolean isRolePermit = hasPrivilegeByRole(context, projectId, roleUser.getRoleId(),
                objectType, objectId, objectName, inputPrivilegeBitmap, historyRolePrivilegeObjectId);
            if (isRolePermit) {
                return true;
            }
        }
        return false;
    }

    private boolean hasPrivilegeByUser(TransactionContext context, String userId, String projectId, String objectType,
                                       CatalogInnerObject catalogInnerObject, String objectId) {
        Optional<UserPrivilege> userPrivilege = getUserPrivilege(context, projectId, userId, objectType,
                objectId);
        if (userPrivilege.isPresent() && userPrivilege.get().isOwner()) {
            return true;
        }

        if (objectType.equals(ObjectType.DATABASE.name())) {
            CatalogInnerObject catalogInnerObject1 = new CatalogInnerObject();
            catalogInnerObject1.setObjectId(catalogInnerObject.getCatalogId());
            return hasPrivilegeByUser(context, userId, projectId, ObjectType.CATALOG.name(), catalogInnerObject1, catalogInnerObject.getCatalogId());
        } else if (objectType.equals(ObjectType.TABLE.name())) {
            CatalogInnerObject catalogInnerObject1 = new CatalogInnerObject();
            catalogInnerObject1.setCatalogId(catalogInnerObject.getCatalogId());
            catalogInnerObject1.setDatabaseId(catalogInnerObject.getDatabaseId());
            catalogInnerObject1.setObjectId(catalogInnerObject.getDatabaseId());
            return hasPrivilegeByUser(context, userId, projectId, ObjectType.DATABASE.name(), catalogInnerObject1, catalogInnerObject.getDatabaseId());
        }

        return false;
    }

    private boolean hasPrivilegeByRole(TransactionContext context, String projectId, String roleId, String objectType,
                                       String objectId, String objectName, long inPrivilege, String historyRolePrivilegeObjectId) {
        long compareResult = 0;
        List<RolePrivilegeObject> rolePrivilegeList = roleStore.getRolePrivilege(context, projectId, roleId);
        //  || objectId.equals(rolePrivilegeObject.getObjectId())
        for (RolePrivilegeObject rolePrivilegeObject : rolePrivilegeList) {
            if (objectType.equals(rolePrivilegeObject.getObjectType())
                && ((objectName.equals(rolePrivilegeObject.getObjectId())
                    || RolePrivilegeHelper.checkEnableWildcard(rolePrivilegeObject.getObjectId())
                    && FilenameUtils.wildcardMatch(objectName, rolePrivilegeObject.getObjectId())) || historyRolePrivilegeObjectId.equals(rolePrivilegeObject.getObjectId())
            )) {
                compareResult = rolePrivilegeObject.getPrivilege() & inPrivilege;
                if (compareResult != 0) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean hasPrivilegeByShare(String projectId, String shareId, String objectType, String objectId) {
        // ShareIdent shareIdent = ShareIdent.newBuilder().setProjectId(projectId).setShareId(shareId).build();
        //List<SharePrivilegeRecord> sharePrivilegeRecordList = versionStore.getSharePrivilege(shareIdent);
        List<SharePrivilegeObject> sharePrivilegeObjectList = new ArrayList<>();
        sharePrivilegeObjectList = TransactionRunnerUtil.transactionReadRunThrow(context -> {
            return shareStore.getAllSharePrivilege(context, projectId, shareId);
        }).getResult();
        for (SharePrivilegeObject sharePrivilegeObject : sharePrivilegeObjectList) {
            if (sharePrivilegeObject.getObjectType().equals(objectType) && objectId.equals(sharePrivilegeObject.getObjectId())) {
                return true;
            }
        }
        throw new CatalogServerException(ErrorCode.TABLE_ID_NOT_FOUND, objectId);
    }

    private CatalogInnerObject getShareObject(AuthTableObjParam authTableObjParam) {
        CatalogInnerObject shareObject = new CatalogInnerObject();
        shareObject.setProjectId(authTableObjParam.getProjectId());
        shareObject.setObjectName(authTableObjParam.getObjectName());
        shareObject.setCatalogName(authTableObjParam.getShareName());
        if (authTableObjParam.getDatabaseName() != null) {
            shareObject.setDatabaseName(authTableObjParam.getDatabaseName());
        }
        if (authTableObjParam.getObjectId() != null) {
            shareObject.setObjectId(authTableObjParam.getObjectId());
        }
        return shareObject;
    }

    private CatalogInnerObject getCatalogObject(AuthTableObjParam authTableObjParam) {
        CatalogInnerObject catalogInnerObject = new CatalogInnerObject();
        catalogInnerObject.setProjectId(authTableObjParam.getProjectId());
        catalogInnerObject.setObjectName(authTableObjParam.getObjectName());
        if (authTableObjParam.getCatalogName() != null) {
            catalogInnerObject.setCatalogName(authTableObjParam.getCatalogName());
        }
        if (authTableObjParam.getDatabaseName() != null) {
            catalogInnerObject.setDatabaseName(authTableObjParam.getDatabaseName());
        }
        if (authTableObjParam.getObjectId() != null) {
            catalogInnerObject.setObjectId(authTableObjParam.getObjectId());
        }
        return catalogInnerObject;
    }

    private CatalogInnerObject getCatalogObject(String projectId, String catalogName, String databaseName,
        String objectName) {
        CatalogInnerObject catalogInnerObject = new CatalogInnerObject();
        catalogInnerObject.setProjectId(projectId);
        catalogInnerObject.setCatalogName(catalogName);
        catalogInnerObject.setDatabaseName(databaseName);
        catalogInnerObject.setObjectName(objectName);
        return catalogInnerObject;
    }

    private User getUser(String userId, String accountId) {
        return new User(accountId, userId);
    }

    private GrantObject getGrantObject(String objectName, ObjectType objectType, String projectId) {
        return new GrantObject(projectId, objectType, objectName);
    }

    @Override
    public boolean authenticationToken(String token) {
        return true;
    }

    private AuthorizationInput getAuthInput(String shareName, User user, AuthorizationType authType,
        Operation operation, ShareInput shareInput) {
        AuthorizationInput authInput = new AuthorizationInput();
        authInput.setAuthorizationType(authType);
        authInput.setUser(user);
        authInput.setOperation(operation);

        CatalogInnerObject catalogInnerObject = new CatalogInnerObject();
        catalogInnerObject.setProjectId(shareInput.getProjectId());
        catalogInnerObject.setObjectName(shareName);
        authInput.setCatalogInnerObject(catalogInnerObject);
        return authInput;
    }

    @Override
    public boolean authenticationForShare(String projectId, String shareName, String token,
        AuthorizationType authType, Operation operation, ShareInput shareInput) {
        TokenParseResult result;
        try {
            result = Authentication.CheckAndParseToken("LocalAuthenticator", token);
        } catch (Exception e) {
            throw new CatalogServerException(ErrorCode.AUTHORIZATION_TOKEN_ERROR);
        }

        if (!result.getAllowed()) {
            return false;
        }

        User user = getUser(result.getUserId(), result.getAccountId());
        AuthorizationInput authInput = getAuthInput(shareName, user, authType, operation, shareInput);
        return authentication(projectId, authInput).getAllowed();
    }

    @Override
    public boolean authenticationForView(String token) {
        return false;
    }

    @Override
    public boolean authenticationMockFalse(String token) {
        return false;
    }

    @Override
    public boolean authenticationForNormal(String projectId, String objectName, String token, Operation operation,
        String apiMethod) {
        TokenParseResult result;
        try {
            result = Authentication.CheckAndParseToken("LocalAuthenticator", token);
        } catch (Exception e) {
            throw new CatalogServerException(ErrorCode.AUTHORIZATION_TOKEN_ERROR);
        }

        if (!result.getAllowed()) {
            return false;
        }

        String accountId = result.getAccountId();
        String userId = result.getUserId();
        boolean passAuth;
        try {
            passAuth = authenticationForNormal(projectId, objectName, userId, accountId, operation, apiMethod);
        } catch (CatalogServerException e) {
            throw new CatalogServerException(ErrorCode.AUTHORIZATION_TYPE_ERROR);
        }
        return passAuth;
    }

    private boolean authenticationForNormal(String projectId, String objectName, String userId,
        String accountId, Operation operation, String apiMethod) {
        AuthorizationInput authorizationInput = new AuthorizationInput();
        authorizationInput.setAuthorizationType(AuthorizationType.NORMAL_OPERATION);
        authorizationInput.setOperation(operation);

        CatalogInnerObject catalogInnerObject = getCatalogObject(projectId, null, null, objectName);
        authorizationInput.setCatalogInnerObject(catalogInnerObject);
        User user = getUser(userId, accountId);
        authorizationInput.setUser(user);

        AuthorizationResponse result = authentication(projectId, authorizationInput, apiMethod);
        return result.getAllowed();
    }

    @Override
    public boolean authenticationForGrant(String projectId, String objectName, String grantName, String token,
                                          Operation operation, ObjectType objectType) {
        TokenParseResult result;
        try {
            result = Authentication.CheckAndParseToken("LocalAuthenticator", token);
        } catch (Exception e) {
            throw new CatalogServerException(ErrorCode.AUTHORIZATION_TOKEN_ERROR);
        }

        if (!result.getAllowed()) {
            return false;
        }

        String accountId = result.getAccountId();
        String userId = result.getUserId();
        return authenticationForGrant(projectId, objectName, grantName, userId, accountId, operation, objectType);
    }

    private boolean authenticationForGrant(String projectId, String objectName, String grantName, String userId,
        String accountId, Operation operation, ObjectType objectType) {
        AuthorizationInput authorizationInput = new AuthorizationInput();
        authorizationInput.setAuthorizationType(AuthorizationType.GRANT_PRIVILEGE_TO_OBJECT);
        authorizationInput.setOperation(operation);

        CatalogInnerObject catalogInnerObject = getCatalogObject(projectId, null, null, objectName);
        authorizationInput.setCatalogInnerObject(catalogInnerObject);

        GrantObject grantObject = getGrantObject(grantName, objectType, projectId);
        authorizationInput.setGrantObject(grantObject);

        User user = getUser(userId, accountId);
        authorizationInput.setUser(user);

        AuthorizationResponse result = authentication(projectId, authorizationInput);
        return result.getAllowed();
    }

    @Override
    public boolean authenticationForDatabase(String projectId, String catalogName, String databaseName, String token,
        Operation operation, String apiMethod) {
        TokenParseResult result;
        try {
            result = Authentication.CheckAndParseToken("LocalAuthenticator", token);
        } catch (Exception e) {
            throw new CatalogServerException(ErrorCode.AUTHORIZATION_TOKEN_ERROR);
        }

        if (!result.getAllowed()) {
            return false;
        }

        String accountId = result.getAccountId();
        String userId = result.getUserId();
        boolean passAuth;
        try {
            passAuth = authenticationForDatabase(projectId, catalogName, databaseName, userId, accountId, operation,
                apiMethod);
        } catch (CatalogServerException e) {
            throw new CatalogServerException(ErrorCode.AUTHORIZATION_TYPE_ERROR);
        }
        return passAuth;
    }

    public boolean authenticationForDatabase(String projectId, String catalogName, String databaseName,
        String userId, String accountId, Operation operation, String apiMethod) {
        AuthorizationInput authorizationInput = new AuthorizationInput();
        authorizationInput.setAuthorizationType(AuthorizationType.NORMAL_OPERATION);
        authorizationInput.setOperation(operation);

        CatalogInnerObject catalogInnerObject = getCatalogObject(projectId, catalogName, databaseName, databaseName);
        authorizationInput.setCatalogInnerObject(catalogInnerObject);

        User user = getUser(userId, accountId);
        authorizationInput.setUser(user);

        AuthorizationResponse result = authentication(projectId, authorizationInput, apiMethod);
        return result.getAllowed();
    }

    @Override
    public boolean authenticationForTable(String projectId, String catalogName, String databaseName, String tableName,
                                          String token, Operation operation) {
        return authenticationForTable(projectId, catalogName, databaseName, tableName, token, operation, null);
    }

    @Override
    public boolean authenticationForTable(String projectId, String catalogName, String databaseName, String tableName,
        String token, Operation operation, String apiMethod) {
        TokenParseResult result;
        try {
            result = Authentication.CheckAndParseToken("LocalAuthenticator", token);
        } catch (Exception e) {
            throw new CatalogServerException(ErrorCode.AUTHORIZATION_TOKEN_ERROR);
        }

        if (!result.getAllowed()) {
            return false;
        }

        String accountId = result.getAccountId();
        String userId = result.getUserId();
        try {
            return authenticationForTable(projectId, catalogName, databaseName, tableName, userId, accountId,
                operation, apiMethod);
        } catch (CatalogServerException e) {
            throw new CatalogServerException(ErrorCode.AUTHORIZATION_TYPE_ERROR);
        }

    }

    @Override
    public boolean authenticationForTable(AuthTableObjParam authTableObjParam, String token, Operation operation,
        String apiMethod) {
        TokenParseResult result;
        try {
            result = Authentication.CheckAndParseToken("LocalAuthenticator", token);
        } catch (Exception e) {
            throw new CatalogServerException(ErrorCode.AUTHORIZATION_TOKEN_ERROR);
        }
        if (!result.getAllowed()) {
            return false;
        }

        boolean hasPrivilege;
        AuthorizationInput authorizationInput = new AuthorizationInput();
        authorizationInput.setAuthorizationType(authTableObjParam.getAuthorizationType());
        authorizationInput.setOperation(operation);

        String accountId = result.getAccountId();
        String userId = result.getUserId();
        User user = getUser(userId, accountId);
        authorizationInput.setUser(user);

        CatalogInnerObject catalogInnerObject = buildCatalogObject(authTableObjParam);
        authorizationInput.setCatalogInnerObject(catalogInnerObject);

        switch (authTableObjParam.getAuthorizationType()) {
            case NORMAL_OPERATION:
                hasPrivilege = isPermitForNormalOperation(authTableObjParam.getProjectId(), authorizationInput, apiMethod);
                break;
            case SELECT_FROM_SHARE:
                hasPrivilege = isPermitForSelectFromShare(authorizationInput);
                break;
            default:
                throw new CatalogServerException(ErrorCode.AUTHORIZATION_TYPE_ERROR);
        }
        return hasPrivilege;
    }

    private CatalogInnerObject buildCatalogObject(AuthTableObjParam authTableObjParam) {
        switch (authTableObjParam.getAuthorizationType()) {
            case NORMAL_OPERATION:
                return getCatalogObject(authTableObjParam);
            case SELECT_FROM_SHARE:
                return getShareObject(authTableObjParam);
            default:
                throw new CatalogServerException(ErrorCode.AUTHORIZATION_TYPE_ERROR);
        }
    }

    public boolean authenticationForTable(String projectId, String catalogName, String databaseName, String tableName,
        String userId, String accountId, Operation operation, String apiMethod) {
        AuthorizationInput authorizationInput = new AuthorizationInput();
        authorizationInput.setAuthorizationType(AuthorizationType.NORMAL_OPERATION);
        authorizationInput.setOperation(operation);

        CatalogInnerObject catalogInnerObject = getCatalogObject(projectId, catalogName, databaseName, tableName);
        authorizationInput.setCatalogInnerObject(catalogInnerObject);

        User user = getUser(userId, accountId);
        authorizationInput.setUser(user);

        AuthorizationResponse result = authentication(projectId, authorizationInput, apiMethod);
        return result.getAllowed();
    }

    @Override
    public boolean authenticationForAccelerator(String projectId, String catalogName, String databaseName,
                                                String acceleratorName, String token, Operation operation) {
        return false;
    }
}
