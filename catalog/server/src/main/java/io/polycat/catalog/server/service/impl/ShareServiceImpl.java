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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.model.CatalogIdent;
import io.polycat.catalog.common.model.CatalogInnerObject;
import io.polycat.catalog.common.model.CatalogObject;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.DatabaseObject;
import io.polycat.catalog.common.model.OperationPrivilege;
import io.polycat.catalog.common.model.ScanRecordCursorResult;
import io.polycat.catalog.common.model.Share;
import io.polycat.catalog.common.model.ShareConsumerObject;
import io.polycat.catalog.common.model.ShareGrantObject;
import io.polycat.catalog.common.model.ShareNameObject;
import io.polycat.catalog.common.model.ShareObject;
import io.polycat.catalog.common.model.SharePrivilegeObject;
import io.polycat.catalog.common.model.SharePrivilegeType;
import io.polycat.catalog.common.model.TableObject;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.TransactionIsolationLevel;
import io.polycat.catalog.common.model.ViewIdent;
import io.polycat.catalog.common.model.ViewRecordObject;
import io.polycat.catalog.common.plugin.request.input.ShareInput;
import io.polycat.catalog.common.utils.UuidUtil;
import io.polycat.catalog.service.api.ShareService;
import io.polycat.catalog.store.api.ShareStore;
import io.polycat.catalog.store.api.Transaction;
import io.polycat.catalog.store.api.UserPrivilegeStore;
import io.polycat.catalog.store.api.ViewStore;

import io.polycat.catalog.common.model.TableIdent;


import io.polycat.catalog.util.CheckUtil;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class ShareServiceImpl implements ShareService {

    private static final Logger log = Logger.getLogger(ShareServiceImpl.class);

    @Autowired
    private ViewStore viewStore;
    @Autowired
    private ShareStore shareStore;
    @Autowired
    private UserPrivilegeStore userPrivilegeStore;
    @Autowired
    private Transaction storeTransaction;

    private Map<Operation, OperationPrivilege> operationPrivilegeMap = new ConcurrentHashMap<>();

    public ShareServiceImpl() {
        //init operationPrivilegeMap
        operationPrivilegeMap.put(Operation.SELECT_TABLE,
                new OperationPrivilege(ObjectType.TABLE,
                        SharePrivilegeType.SHARE_PRIVILEGE_SELECT.getType()));
    }

    /**
     * create share
     *
     * @param projectId
     * @param shareInput
     */
    @Override
    public void createShare(String projectId, ShareInput shareInput) {
        try (TransactionContext context = storeTransaction.openTransaction()) {
            if (shareStore.shareObjectNameExist(context, projectId, shareInput.getShareName())) {
                throw new MetaStoreException(ErrorCode.SHARE_ALREADY_EXISTS, shareInput.getShareName());
            }
            String shareId = UuidUtil.generateId();
            shareStore.insertShareObjectName(context, projectId, shareInput.getShareName(), shareId);
            shareStore.insertShareProperties(context, projectId, shareId, shareInput.getShareName(),
                shareInput.getAccountId(), shareInput.getUserId());
            userPrivilegeStore.insertUserPrivilege(context, projectId, shareInput.getUserId(),
                ObjectType.SHARE.name(), shareId, true, 0);
            context.commit();
        }
    }

    private void dropShareById(TransactionContext context, String projectId, String shareId) {
        ShareObject shareObject = shareStore.getShareProperties(context, projectId,
            shareId);
        shareStore.deleteShareObjectName(context, projectId, shareObject.getShareName());
        shareStore.deleteShareProperties(context, projectId, shareId);
        shareStore.delAllSharePrivilege(context, projectId, shareId);
        shareStore.delAllShareConsumer(context, projectId, shareId);
        userPrivilegeStore.deleteUserPrivilege(context, projectId, shareObject.getOwnerUser(),
            ObjectType.SHARE.name(), shareId);
    }

    /**
     *
     * @param projectId
     * @param shareId
     */
    @Override
    public void dropShareById(String projectId, String shareId) {
        CheckUtil.checkStringParameter(projectId, shareId);
        try (TransactionContext context = storeTransaction.openTransaction()) {
            dropShareById(context, projectId, shareId);
            context.commit();
        }
    }

    /**
     * drop share by name
     *
     * @param shareName
     */
    @Override
    public void dropShareByName(String projectId, String shareName) {
        CheckUtil.checkStringParameter(projectId, shareName);
        try (TransactionContext context = storeTransaction.openTransaction()) {
            String shareId = shareStore.getShareId(context, projectId, shareName);
            dropShareById(context, projectId, shareId);
            context.commit();
        }
    }

    private Share toShare(String projectId, ShareObject shareObject) {
        Share share = new Share();
        share.setProjectId(projectId);
        share.setShareId(shareObject.getShareId());
        share.setShareName(shareObject.getShareName());
        share.setOwnerAccount(shareObject.getOwnerAccount());
        if (!StringUtils.isBlank(shareObject.getCatalogId())) {
            String catalogName = getCatalogNameById(projectId, shareObject.getCatalogId());
            share.setCatalogName(catalogName);
        }
        Date date = new Date(shareObject.getCreateTime());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        share.setCreatedTime(sdf.format(date));
        return share;
    }

    private Share getShareById(TransactionContext context, String projectId, String shareId) {
        ShareObject shareObject = shareStore.getShareProperties(context, projectId, shareId);
        return toShare(projectId, shareObject);
    }

    /**
     * get share by shareName
     *
     * @param shareName
     * @return share
     */
    @Override
    public Share getShareByName(String projectId, String shareName) {
        CheckUtil.checkStringParameter(projectId, shareName);
        try (TransactionContext context = storeTransaction.openTransaction()) {
            String shareId = shareStore.getShareId(context, projectId, shareName);
            Share share = getShareById(context, projectId, shareId);
            context.commit();
            return share;
        }
    }

    /**
     *
     * @param projectId
     * @param shareId
     * @return
     */
    @Override
    public Share getShareById(String projectId, String shareId) {
        CheckUtil.checkStringParameter(projectId, shareId);
        try (TransactionContext context = storeTransaction.openTransaction()) {
            Share share = getShareById(context, projectId, shareId);
            context.commit();
            return share;
        }
    }

    /**
     *
     * @param projectId
     * @param shareName
     * @param shareInput
     */
    @Override
    public void alterShare(String projectId, String shareName, ShareInput shareInput) {
        try (TransactionContext context = storeTransaction.openTransaction()) {
            String shareId = shareStore.getShareId(context, projectId, shareName);
            ShareObject shareObject = shareStore.getShareProperties(context, projectId, shareId);
            shareObject.setShareName(shareInput.getShareName());
            shareStore.updateShareProperties(context, projectId, shareObject);
            shareStore.deleteShareObjectName(context, projectId, shareName);
            shareStore.insertShareObjectName(context, projectId, shareInput.getShareName(), shareId);
            context.commit();
        }
    }

    /**
     * add accounts to share
     *
     * @param shareName
     * @param shareInput
     */
    @Override
    public void addConsumersToShare(String projectId, String shareName, ShareInput shareInput) {
        checkAccountsAndUsers(shareInput.getAccountIds(), shareInput.getUsers());
        try (TransactionContext context = storeTransaction.openTransaction()) {
            String shareId = shareStore.getShareId(context, projectId, shareName);
            for (int i = 0; i < shareInput.getAccountIds().length; i++) {
                shareStore.insertShareConsumer(context, projectId, shareId, shareInput.getAccountIds()[i],
                    shareInput.getUsers()[i]);
            }
            context.commit();
        }
    }

    private void checkAccountsAndUsers(String[] accounts, String[] users) {
        if (accounts.length != users.length) {
            throw new CatalogServerException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    /**
     * remove accounts from share
     *
     * @param shareName
     * @param shareInput
     */
    @Override
    public void removeConsumersFromShare(String projectId, String shareName, ShareInput shareInput) {
        try (TransactionContext context = storeTransaction.openTransaction()) {
            String shareId = shareStore.getShareId(context, projectId, shareName);
            for (int i = 0; i < shareInput.getAccountIds().length; i++) {
                shareStore.deleteShareConsumer(context, projectId, shareId, shareInput.getAccountIds()[i]);
            }
            context.commit();
        }
    }

    @Override
    public void addUsersToShareConsumer(String projectId, String shareName, ShareInput shareInput) {
        try (TransactionContext context = storeTransaction.openTransaction()) {
            String shareId = shareStore.getShareId(context, projectId, shareName);
            shareStore.addUsersToShareConsumer(context, projectId, shareId,
                shareInput.getAccountId(), shareInput.getUsers());
            context.commit();
        }
    }

    @Override
    public void removeUsersFromShareConsumer(String projectId, String shareName, ShareInput shareInput) {
        try (TransactionContext context = storeTransaction.openTransaction()) {
            String shareId = shareStore.getShareId(context, projectId, shareName);
            shareStore.removeUsersFromShareConsumer(context, projectId, shareId, shareInput.getAccountId(),
                shareInput.getUsers());
            context.commit();
        }
    }

    private void addPrivilegeToShare(String projectId, String shareName, String objectType, CatalogInnerObject catalogInnerObject,
        long privilege) {
        try (TransactionContext context = storeTransaction.openTransaction()) {
            String shareId = shareStore.getShareId(context, projectId, shareName);
            ShareObject shareObject = shareStore.getShareProperties(context, projectId, shareId);
            if (StringUtils.isNotBlank(shareObject.getCatalogId())) {
                if (!shareObject.getCatalogId().equals(catalogInnerObject.getCatalogId())) {
                    throw new MetaStoreException(ErrorCode.SHARE_CATALOG_CONFLICT);
                }
            } else {
                shareObject.setCatalogId(catalogInnerObject.getCatalogId());
                shareStore.updateShareProperties(context, projectId, shareObject);
            }

            long originPrivilege = shareStore.getSharePrivilege(context, projectId, shareId,
                catalogInnerObject.getDatabaseId(),objectType, catalogInnerObject.getObjectId());
            if (originPrivilege == 0) {
                shareStore.insertSharePrivilege(context, projectId, shareId, catalogInnerObject.getDatabaseId(),
                    objectType, catalogInnerObject.getObjectId(), privilege);
            } else {
                long newPrivilege = privilege | originPrivilege;
                shareStore.updateSharePrivilege(context, projectId, shareId, catalogInnerObject.getDatabaseId(),
                    objectType, catalogInnerObject.getObjectId(), newPrivilege);
            }
            context.commit();
        }
    }

    private void removePrivilegeFromShare(String projectId, String shareName, String objectType, CatalogInnerObject catalogInnerObject,
        long privilege) throws MetaStoreException {
        try (TransactionContext context = storeTransaction.openTransaction()) {
            String shareId = shareStore.getShareId(context, projectId, shareName);
            ShareObject shareObject = shareStore.getShareProperties(context, projectId, shareId);

            long originPrivilege = shareStore.getSharePrivilege(context, projectId,
                shareId, catalogInnerObject.getDatabaseId(), objectType, catalogInnerObject.getObjectId());
            if (originPrivilege == 0) {
                throw new MetaStoreException(ErrorCode.SHARE_PRIVILEGE_INVALID);
            }

            long newPrivilege = (~privilege) & originPrivilege;
            if (newPrivilege == 0) {
                shareStore.deleteSharePrivilege(context, projectId, shareId, catalogInnerObject.getDatabaseId(),
                    objectType, catalogInnerObject.getObjectId());
                /*shareObject.setCatalogId(null);
                shareStore.updateShareProperties(context, shareName.getProjectId(), shareObject);*/
            } else {
                shareStore.updateSharePrivilege(context, projectId, shareId, catalogInnerObject.getDatabaseId(),
                    objectType, catalogInnerObject.getObjectId(), newPrivilege);
            }
            context.commit();
        }
    }

    private void modifyPrivilege(String projectId, String shareName, ShareInput shareInput, boolean isAdd) {
        OperationPrivilege operationPrivilege = operationPrivilegeMap.get(shareInput.getOperation());
        if (operationPrivilege == null) {
            throw new CatalogServerException(ErrorCode.SHARE_PRIVILEGE_INVALID);
        }

        CatalogInnerObject catalogInnerObject = ServiceImplHelper.getCatalogObject(projectId,
                operationPrivilege.getObjectType().name(), shareInput.getObjectName());
        long privilege = convertSharePrivilege(operationPrivilege);
        if (privilege == 0) {
            throw new CatalogServerException(ErrorCode.SHARE_PRIVILEGE_INVALID);
        }
        if (isAdd) {
            addPrivilegeToShare(projectId, shareName, operationPrivilege.getObjectType().name(), catalogInnerObject, privilege);
        } else {
            removePrivilegeFromShare(projectId, shareName, operationPrivilege.getObjectType().name(),
                catalogInnerObject, privilege);
        }
    }

    /**
     * add privilege to share
     *
     * @param shareName
     * @param shareInput
     */
    @Override
    public void addPrivilegeToShare(String projectId, String shareName, ShareInput shareInput) {
        modifyPrivilege(projectId, shareName, shareInput, true);
    }

    /**
     * remove privilege from share
     *
     * @param shareName
     * @param shareInput
     */
    @Override
    public void removePrivilegeFromShare(String projectId, String shareName, ShareInput shareInput) {
        modifyPrivilege(projectId, shareName, shareInput, false);
    }

    private Share convertToOutBandShareModel(String projectId, ShareObject shareObject,
        List<ShareConsumerObject> shareConsumerObjectList, List<SharePrivilegeObject> sharePrivilegeObjectList) {
        Share share = new Share();
        Date date = new Date(shareObject.getCreateTime());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        share.setCreatedTime(sdf.format(date));
        share.setKind("OUTBOUND");
        share.setProjectId(projectId);
        share.setShareId(shareObject.getShareId());
        share.setShareName(shareObject.getShareName());
        if (StringUtils.isNotBlank(shareObject.getCatalogId())) {
            String catalogName = getCatalogNameById(share.getProjectId(), shareObject.getCatalogId());
            share.setCatalogName(catalogName);

            List<ShareGrantObject> shareGrantObjects = convertShareGrantObject(projectId, shareObject.getCatalogId(),
                sharePrivilegeObjectList);
            share.setShareGrantObjects(shareGrantObjects.toArray(new ShareGrantObject[0]));
        }
        share.setOwnerAccount(shareObject.getOwnerAccount());
        share.setOwnerUser(shareObject.getOwnerUser());
        List<String> accounts = new ArrayList<>();
        List<String> managerUsers = new ArrayList<>();
        for (ShareConsumerObject shareConsumerObject : shareConsumerObjectList) {
            accounts.add(shareConsumerObject.getAccountId() + ":" + shareConsumerObject.getManagerUser());
            managerUsers.add(shareConsumerObject.getManagerUser());
        }
        share.setToAccounts(accounts.toArray(new String[accounts.size()]));
        share.setToUsers(managerUsers.toArray(new String[managerUsers.size()]));
        return share;
    }

    private Share convertToInBandShareModel(String projectId, ShareObject shareObject, String accountId, String user,
        List<SharePrivilegeObject> sharePrivilegeObjectList) {
        Share share = new Share();
        Date date = new Date(shareObject.getCreateTime());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        share.setCreatedTime(sdf.format(date));
        share.setKind("INBOUND");
        share.setProjectId(projectId);
        share.setShareId(shareObject.getShareId());
        share.setShareName(shareObject.getShareName());
        if (StringUtils.isNotBlank(shareObject.getCatalogId())) {
            String catalogName = getCatalogNameById(share.getProjectId(), shareObject.getCatalogId());
            share.setCatalogName(catalogName);

            List<ShareGrantObject> shareGrantObjects = convertShareGrantObject(projectId, shareObject.getCatalogId(),
                sharePrivilegeObjectList);
            share.setShareGrantObjects(shareGrantObjects.toArray(new ShareGrantObject[0]));
        }
        share.setOwnerAccount(shareObject.getOwnerAccount());
        share.setOwnerUser(shareObject.getOwnerUser());
        List<String> accounts = new ArrayList<>();
        List<String> users = new ArrayList<>();
        accounts.add(accountId);
        users.add(user);
        share.setToAccounts(accounts.toArray(new String[accounts.size()]));
        share.setToUsers(users.toArray(new String[users.size()]));
        return share;
    }

    private void makeOutBoundShares(TransactionContext context, String projectId, String namePattern,
        List<Share> shares) {
        ScanRecordCursorResult<List<ShareNameObject>> shareNameObjects = shareStore.listShareObjectName(context,
            projectId, namePattern, Integer.MAX_VALUE, null, TransactionIsolationLevel.SERIALIZABLE);

        for (ShareNameObject shareNameObject : shareNameObjects.getResult()) {
            ShareObject shareObject = shareStore.getShareProperties(context, projectId, shareNameObject.getShareId());
            List<ShareConsumerObject> shareConsumerObjectList = shareStore.getShareAllConsumers(context, projectId,
                shareNameObject.getShareId());
            List<SharePrivilegeObject> sharePrivilegeObjectList = shareStore.getAllSharePrivilege(context, projectId,
                shareNameObject.getShareId());
            Share share = convertToOutBandShareModel(projectId, shareObject, shareConsumerObjectList,
                sharePrivilegeObjectList);
            shares.add(share);
        }
    }

    public void makeInBoundShares(TransactionContext context, String projectId, String accountId,
        String user, String namePattern, List<Share> shares) {
        ScanRecordCursorResult<List<ShareNameObject>> shareNameObjects = shareStore.listShareObjectName(context,
            projectId, null, Integer.MAX_VALUE, null, TransactionIsolationLevel.SERIALIZABLE);

        for (ShareNameObject shareNameObject : shareNameObjects.getResult()) {
            if (!shareStore.shareConsumerExist(context, projectId, shareNameObject.getShareId(), accountId)) {
                continue;
            }
            ShareConsumerObject shareConsumerObject = shareStore.getShareConsumers(context, projectId,
                shareNameObject.getShareId(), accountId);
            if (!shareConsumerObject.getUsers().containsKey(user)
                && !shareConsumerObject.getManagerUser().equals(user)) {
                continue;
            }

            ShareObject shareObject = shareStore.getShareProperties(context, projectId, shareNameObject.getShareId());
            List<SharePrivilegeObject> sharePrivilegeObjectList = shareStore.getAllSharePrivilege(context, projectId,
                shareNameObject.getShareId());

            Share share = convertToInBandShareModel(projectId, shareObject, accountId, user, sharePrivilegeObjectList);
            shares.add(share);
        }
    }

    /**
     * get share models in project
     *
     * @param projectId
     * @param namePattern
     */
    @Override
    public List<Share> getShareModels(String projectId, String accountId, String user, String namePattern) {
        List<Share> shareList = new ArrayList<>();
        try (TransactionContext context = storeTransaction.openTransaction()) {
            makeOutBoundShares(context, projectId, namePattern, shareList);
            makeInBoundShares(context, projectId, accountId, user, namePattern, shareList);

            /*shareStore.makeOutBoundShares(context, shares, projectId, namePattern);
            shareStore.makeInBoundShares(context, projectId, shares, accountId, user, namePattern);*/
            context.commit();
        }
        /*List<Share> shareModels = convertToShareModel(shares);
        return shareModels;*/
        return shareList;
    }

    /*private List<Share> convertToShareModel(List<ShareRecord> shares) {
        List<Share> shareModels = new ArrayList<>();
        for (ShareRecord share : shares) {
            Share model = new Share();
            Date date = new Date(share.getCreatedTime());
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            model.setCreatedTime(sdf.format(date));
            model.setShareName(share.getName());
            model.setComment(share.getComment());
            model.setKind(share.getKind());
            model.setOwnerAccount(share.getOwnerAccount());
            model.setProjectId(share.getProjectId());
            model.setOwnerUser(share.getUserId());
            if (share.getKind().equals("INBOUND")) {
                model.setToUsers(share.getUsersList().toArray(new String[0]));
            }
            List<String> accounts = new ArrayList<>();
            for (int i = 0; i < share.getAccountsCount(); i++) {
                accounts.add(share.getAccounts(i) + ":" + share.getUsers(i));
            }
            model.setToAccounts(accounts.toArray(new String[accounts.size()]));
            if (share.hasCatalogId()) {
                String catalogName = getCatalogNameById(share.getProjectId(), share.getCatalogId());
                model.setCatalogName(catalogName);
                List<ShareGrantObject> shareGrantObjects = convertShareGrantObject(share);
                model.setShareGrantObjects(shareGrantObjects.toArray(new ShareGrantObject[0]));
            }
            shareModels.add(model);
        }
        return shareModels;
    }*/

    private List<ShareGrantObject> convertShareGrantObject(String projectId, String catalogId,
        List<SharePrivilegeObject> sharePrivilegeObjectList) {
        List<ShareGrantObject> shareGrantObjects = new ArrayList<>();
        for (SharePrivilegeObject sharePrivilegeObject : sharePrivilegeObjectList) {
            ShareGrantObject shareGrantObject = new ShareGrantObject();
            String databaseName = getDatabaseNameById(projectId, catalogId, sharePrivilegeObject.getDatabaseId());
            shareGrantObject.setDatabaseName(databaseName);
            List<String> objectNames = new ArrayList<>();
            String objectName = getObjectNameById(projectId, catalogId, sharePrivilegeObject.getDatabaseId(),
                sharePrivilegeObject.getObjectType(), sharePrivilegeObject.getObjectId());
            objectNames.add(objectName);
            shareGrantObject.setObjectName(objectNames.toArray(new String[0]));
            shareGrantObjects.add(shareGrantObject);
        }
        return shareGrantObjects;
    }



    private String getCatalogNameById(String projectId, String catalogId) {
        CatalogIdent catalogIdent = new CatalogIdent(projectId,catalogId);
        CatalogObject catalogRecord = CatalogObjectHelper.getCatalogObject(catalogIdent);
        if (catalogRecord != null) {
            return catalogRecord.getName();
        }
        return null;
    }

    private String getDatabaseNameById(String projectId, String catalogId, String databaseId) {
        DatabaseIdent databaseIdent = new DatabaseIdent(projectId, catalogId, databaseId);
        DatabaseObject databaseRecord = DatabaseObjectHelper.getDatabaseObject(databaseIdent);
        if (databaseRecord != null) {
            return databaseRecord.getName();
        }
        return null;
    }

    private String getObjectNameById(String projectId, String catalogId, String databaseId,
                                     String objectType, String objectId) {

        ObjectType shareObjectType = ObjectType.valueOf(objectType);
        switch (shareObjectType) {
            case TABLE:
                TableIdent tableIdent = new TableIdent(projectId, catalogId, databaseId, objectId);
                TableObject table = TableObjectHelper.getTableObject(tableIdent);
                if (table != null) {
                    return table.getName();
                }
                break;
            case VIEW:
                ViewIdent viewIdent = new ViewIdent(projectId, catalogId, databaseId, objectId);
                ViewRecordObject view = viewStore.getViewById(viewIdent);
                if (view != null) {
                    return view.getName();
                }
                break;
            default:
                break;
        }
        return null;
    }

    private long convertSharePrivilege(OperationPrivilege operationPrivilege) {
        return 1 << operationPrivilege.getPrivilege();
    }
}
