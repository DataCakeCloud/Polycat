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
import io.polycat.catalog.common.model.MetaPrivilegePolicy;
import io.polycat.catalog.common.model.OperationPrivilege;
import io.polycat.catalog.common.model.PolicyModifyType;
import io.polycat.catalog.common.model.PrincipalSource;
import io.polycat.catalog.common.model.PrincipalType;
import io.polycat.catalog.common.model.Share;
import io.polycat.catalog.common.model.ShareConsumerObject;
import io.polycat.catalog.common.model.ShareGrantObject;
import io.polycat.catalog.common.model.ShareObject;
import io.polycat.catalog.common.model.TableIdent;
import io.polycat.catalog.common.model.TableObject;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.TransactionIsolationLevel;
import io.polycat.catalog.common.model.ViewIdent;
import io.polycat.catalog.common.model.ViewRecordObject;
import io.polycat.catalog.common.plugin.request.input.ShareInput;
import io.polycat.catalog.common.utils.UuidUtil;
import io.polycat.catalog.service.api.GlobalShareService;
import io.polycat.catalog.store.api.*;
import io.polycat.catalog.store.common.StoreConvertor;
import io.polycat.catalog.store.fdb.record.RecordStoreHelper;
import io.polycat.catalog.util.CheckUtil;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class GlobalShareServiceImpl implements GlobalShareService {

    private static final Logger log = Logger.getLogger(ShareServiceImpl.class);
    @Autowired
    private ViewStore viewStore;
    @Autowired
    private CatalogStore catalogStore;
    @Autowired
    private DatabaseStore databaseStore;
    @Autowired
    private GlobalShareStore shareStore;
    @Autowired
    private PolicyStore policyStore;

    //private final DatabaseServiceImpl databaseService = DatabaseServiceImpl.getInstance();
    //private final TableServiceImpl tableService = TableServiceImpl.getInstance();
    //private final CatalogServiceImpl catalogService = CatalogServiceImpl.getInstance();
    @Autowired
    private Transaction storeTransaction;

    private Map<Operation, OperationPrivilege> operationPrivilegeMap = new ConcurrentHashMap<>();


    @Override
    public void createShare(String projectId, ShareInput shareInput) {
        try (TransactionContext context = storeTransaction.openTransaction()) {
            if (shareStore.shareObjectNameExist(context, projectId, shareInput.getShareName())) {
                throw new MetaStoreException(ErrorCode.SHARE_ALREADY_EXISTS, shareInput.getShareName());
            }
            String shareId = UuidUtil.generateId();
            CatalogInnerObject catalogInnerObject = ServiceImplHelper.getCatalogObject(projectId,
                ObjectType.CATALOG.name(), shareInput.getObjectName());

            shareStore.insertShareProperties(context, projectId,
                shareId, shareInput.getShareName(),  catalogInnerObject.getCatalogId(),
                shareInput.getAccountId(), shareInput.getUserId());

            long updateTime = RecordStoreHelper.getCurrentTime();
            String policyId = policyStore.insertMetaPrivilegePolicy(context, projectId,
                PrincipalType.USER, PrincipalSource.IAM.getNum(), shareInput.getUserId(),
                ObjectType.SHARE.getNum(), shareId, true, 0, true,
                null, null, updateTime, true);
            if (policyId == null) {
                throw new CatalogServerException(ErrorCode.POLICY_ID_NOT_FOUND);
            }
            policyStore.insertMetaPrivilegePolicyHistory(context, projectId, policyId,
                PrincipalType.USER, PrincipalSource.IAM.getNum(), shareInput.getUserId(),
                PolicyModifyType.ADD.getNum(), updateTime);
            context.commit();
        }
    }


    private void dropShareObject(TransactionContext context, String projectId, ShareObject shareObject) {

        String shareId = shareObject.getShareId();
        shareStore.deleteGlobalShareProperties(context,projectId,shareId);
        shareStore.delAllShareConsumer(context, projectId, shareId);


        policyStore.delAllMetaPrivilegesOfPrincipal(context,projectId,
            PrincipalType.SHARE, PrincipalSource.IAM.getNum(), shareId);
        policyStore.delAllMetaPrivilegesOfPrincipalOnObject(context, projectId,
            PrincipalType.USER, PrincipalSource.IAM.getNum(), shareObject.getOwnerUser(),
            ObjectType.SHARE.getNum(), shareId);
    }


    @Override
    public void dropShareById(String projectId, String shareId) {
        CheckUtil.checkStringParameter(projectId, shareId);
        try (TransactionContext context = storeTransaction.openTransaction()) {
            ShareObject shareObject = shareStore.getSharePropertiesById(context, projectId,
                shareId);
            dropShareObject(context,projectId,shareObject);

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
            ShareObject shareObject = shareStore.getSharePropertiesByName(context, projectId,
                shareName);
            dropShareObject(context,projectId,shareObject);
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

    @Override
    public Share getShareByName(String projectId, String shareName) {
        CheckUtil.checkStringParameter(projectId, shareName);
        try (TransactionContext context = storeTransaction.openTransaction()) {
            ShareObject shareObject = shareStore.getSharePropertiesByName(context, projectId, shareName);
            Share share = toShare(projectId, shareObject);
            context.commit();
            return share;
        }
    }

    @Override
    public Share getShareById(String projectId, String shareId) {
        CheckUtil.checkStringParameter(projectId, shareId);
        try (TransactionContext context = storeTransaction.openTransaction()) {
            ShareObject shareObject = shareStore.getSharePropertiesById(context, projectId, shareId);
            Share share = toShare(projectId, shareObject);
            context.commit();
            return share;
        }
    }


    @Override
    public void alterShare(String projectId, String shareName, ShareInput shareInput) {
        try (TransactionContext context = storeTransaction.openTransaction()) {
            ShareObject shareObject = shareStore.getSharePropertiesByName(context, projectId, shareName);
            shareObject.setShareName(shareInput.getShareName());
            shareStore.updateShareProperties(context, projectId, shareObject);
            context.commit();
        }
    }

    @Override
    public void addConsumersToShare(String projectId, String shareName, ShareInput shareInput) {
        checkAccountsAndUsers(shareInput.getAccountIds(), shareInput.getUsers());
        try (TransactionContext context = storeTransaction.openTransaction()) {
            ShareObject shareObject = shareStore.getSharePropertiesByName(context, projectId, shareName);
            String shareId = shareObject.getShareId();
            for (int i = 0; i < shareInput.getAccountIds().length; i++) {
                shareStore.insertShareConsumer(context, projectId, shareId, shareInput.getAccountIds()[i],
                    shareInput.getUsers()[i]);
                System.out.println("projectid: " +  projectId + " shareId: " + shareId + " accountId: " + shareInput.getAccountIds()[i]);
            }

            context.commit();
        }
    }

    private void checkAccountsAndUsers(String[] accounts, String[] users) {
        if (accounts.length != users.length) {
            throw new CatalogServerException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void removeConsumersFromShare(String projectId, String shareName, ShareInput shareInput) {
        try (TransactionContext context = storeTransaction.openTransaction()) {
            ShareObject shareObject = shareStore.getSharePropertiesByName(context, projectId, shareName);
            String shareId = shareObject.getShareId();
            for (int i = 0; i < shareInput.getAccountIds().length; i++) {
                shareStore.deleteShareConsumer(context, projectId, shareId, shareInput.getAccountIds()[i]);
            }
            context.commit();
        }
    }

    @Override
    public void addUsersToShareConsumer(String projectId, String shareName, ShareInput shareInput) {
        try (TransactionContext context = storeTransaction.openTransaction()) {
            ShareObject shareObject = shareStore.getSharePropertiesByName(context, projectId, shareName);
            String shareId = shareObject.getShareId();
            System.out.println("projectid: " +  projectId + " shareId: " + shareId + " accountId: " + shareInput.getAccountId());
            shareStore.addUsersToShareConsumer(context, projectId, shareId,
                shareInput.getAccountId(), shareInput.getUsers());
            context.commit();
        }
    }

    @Override
    public void removeUsersFromShareConsumer(String projectId, String shareName, ShareInput shareInput) {
        try (TransactionContext context = storeTransaction.openTransaction()) {
            ShareObject shareObject = shareStore.getSharePropertiesByName(context, projectId, shareName);
            String shareId = shareObject.getShareId();
            shareStore.removeUsersFromShareConsumer(context, projectId, shareId, shareInput.getAccountId(),
                shareInput.getUsers());
            context.commit();
        }
    }

    @Override
    public List<Share> getShareModels(String projectId, String accountId, String user, String namePattern) {
        List<Share> shareList = new ArrayList<>();
        try (TransactionContext context = storeTransaction.openTransaction()) {
            makeOutBoundShares(context, projectId, namePattern, shareList);
            makeInBoundShares(context, projectId, accountId, user, namePattern, shareList);
            context.commit();
        }
        return shareList;
    }

    private void makeOutBoundShares(TransactionContext context, String projectId, String namePattern,
        List<Share> shares) {
        List<ShareObject> shareObjects = shareStore.listShareObject(context,
            projectId, namePattern, Integer.MAX_VALUE, null, TransactionIsolationLevel.SERIALIZABLE);

        for (ShareObject shareObject : shareObjects) {
            //ShareObject shareObject = shareStore.getSharePropertiesByName(context, projectId, shareNameObject.getShareId());
            List<ShareConsumerObject> shareConsumerObjectList = shareStore.getShareAllConsumers(context, projectId,
                shareObject.getShareId());

            //List<SharePrivilegeObject> sharePrivilegeObjectList = shareStore.getAllSharePrivilege(context, projectId,
            //    shareNameObject.getShareId());
            List<MetaPrivilegePolicy> sharePrivilegesList = policyStore.getMetaPrivilegesByPrincipal(context,projectId,
                PrincipalType.SHARE, PrincipalSource.IAM.getNum(), shareObject.getShareId());


            Share share = convertToOutBandShareModel(projectId, shareObject, shareConsumerObjectList,
                sharePrivilegesList);
            shares.add(share);
        }
    }

    public void makeInBoundShares(TransactionContext context, String projectId, String accountId,
        String user, String namePattern, List<Share> shares) {

        List<ShareConsumerObject> shareConsumerObjectList = shareStore.getShareAllConsumersWithAccount(context, accountId);
        for (ShareConsumerObject shareConsumerObject : shareConsumerObjectList) {
            if (!shareConsumerObject.getUsers().containsKey(user)
                && !shareConsumerObject.getManagerUser().equals(user)) {
                continue;
            }

            ShareObject shareObject = shareStore.getSharePropertiesById(context, shareConsumerObject.getProjectId(),
                shareConsumerObject.getShareId());

            List<MetaPrivilegePolicy> sharePrivilegesList = policyStore.getMetaPrivilegesByPrincipal(context,
                shareObject.getProjectId(),
                PrincipalType.SHARE, PrincipalSource.IAM.getNum(), shareConsumerObject.getShareId());

            Share share = convertToInBandShareModel(projectId, shareObject, accountId, user, sharePrivilegesList);
            shares.add(share);
        }
    }

    private Share convertToOutBandShareModel(String projectId, ShareObject shareObject,
        List<ShareConsumerObject> shareConsumerObjectList, List<MetaPrivilegePolicy> sharePrivilegesList) {
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
                sharePrivilegesList);
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
        List<MetaPrivilegePolicy> sharePrivilegesList) {
        Share share = new Share();
        Date date = new Date(shareObject.getCreateTime());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        share.setCreatedTime(sdf.format(date));
        share.setKind("INBOUND");
        share.setProjectId(shareObject.getProjectId());
        share.setShareId(shareObject.getShareId());
        share.setShareName(shareObject.getShareName());
        if (StringUtils.isNotBlank(shareObject.getCatalogId())) {
            String catalogName = getCatalogNameById(shareObject.getProjectId(), shareObject.getCatalogId());
            share.setCatalogName(catalogName);

            List<ShareGrantObject> shareGrantObjects = convertShareGrantObject(shareObject.getProjectId(),
                shareObject.getCatalogId(),
                sharePrivilegesList);
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



    private List<ShareGrantObject> convertShareGrantObject(String projectId, String catalogId,
        List<MetaPrivilegePolicy> sharePrivilegesList) {
        List<ShareGrantObject> shareGrantObjects = new ArrayList<>();
        for (MetaPrivilegePolicy sharePrivilegeObject : sharePrivilegesList) {
            ShareGrantObject shareGrantObject = new ShareGrantObject();

            int objectType = sharePrivilegeObject.getObjectType();
            String objId = sharePrivilegeObject.getObjectId();
            String[] objectIdArray = objId.split("\\.");
            String databaseId = null;
            String objectId = null;
            if (objectIdArray.length == 3) {
                databaseId = objectIdArray[1];
                objectId = objectIdArray[2];
            } else if (objectIdArray.length == 2) {
                databaseId = objectIdArray[1];
                objectId = objectIdArray[1];
            }

            String databaseName = getDatabaseNameById(projectId, catalogId, databaseId);
            shareGrantObject.setDatabaseName(databaseName);
            List<String> objectNames = new ArrayList<>();
            String objectName = getObjectNameById(projectId, catalogId, databaseId, objectType, objectId);
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
        int objectType, String objectId) {

        if (databaseId == null || objectId == null) {
            return null;
        }

        ObjectType shareObjectType = ObjectType.forNum(objectType);
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
        return 1L << operationPrivilege.getPrivilege();
    }

}
