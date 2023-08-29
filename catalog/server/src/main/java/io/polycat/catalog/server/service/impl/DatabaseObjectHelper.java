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

import java.util.*;
import java.util.stream.Collectors;


import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.model.CatalogIdent;
import io.polycat.catalog.common.model.CatalogName;
import io.polycat.catalog.common.model.CatalogObject;
import io.polycat.catalog.common.model.DatabaseHistoryObject;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.DatabaseObject;
import io.polycat.catalog.common.model.DatabaseRefObject;
import io.polycat.catalog.common.model.DroppedDatabaseNameObject;
import io.polycat.catalog.common.model.ObjectNameMap;
import io.polycat.catalog.common.model.ScanRecordCursorResult;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.TransactionIsolationLevel;
import io.polycat.catalog.server.util.TransactionRunnerUtil;
import io.polycat.catalog.store.api.*;
import io.polycat.catalog.store.common.StoreConvertor;
import io.polycat.catalog.store.fdb.record.RecordStoreHelper;
import io.polycat.catalog.util.CheckUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import static io.polycat.catalog.common.Operation.CREATE_DATABASE;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class DatabaseObjectHelper {
    private static DatabaseStore databaseStore;
    private static CatalogStore catalogStore;

    private static final int DATABASE_STORE_MAX_RETRY_NUM = 256;

    @Autowired
    public void setCatalogStore(CatalogStore catalogStore) {
        this.catalogStore = catalogStore;
    }

    @Autowired
    public void setDatabaseStore(DatabaseStore databaseStore) {
        this.databaseStore = databaseStore;
    }

    public static String getDatabaseNameById(TransactionContext context, DatabaseIdent databaseIdent, Boolean includeDropped) {
        String lastVersion = VersionManagerHelper.getLatestVersion(context, databaseIdent.getProjectId(),
            databaseIdent.getCatalogId());
        Optional<DatabaseHistoryObject> databaseHistory = DatabaseHistoryHelper
            .getLatestDatabaseHistory(context, databaseIdent, lastVersion);
        if (databaseHistory.isPresent()) {
            if (includeDropped) {
                return databaseHistory.get().getName();
            } else if (!DatabaseHistoryHelper.isDropped(databaseHistory.get())) {
                return databaseHistory.get().getName();
            }
        }

        return null;
    }

    public static DatabaseIdent getOnlyDroppedDatabaseIdentOrElseThrow(DatabaseName databaseName)
        throws CatalogServerException {
        return TransactionRunnerUtil.transactionReadRunThrow(context -> getOnlyDroppedDatabaseIdentOrElseThrow(context, databaseName)).getResult();
    }

    public static DatabaseIdent getOnlyDroppedDatabaseIdentOrElseThrow(TransactionContext context,
        DatabaseName databaseName) throws CatalogServerException {
        //get catalogId
        CatalogName branchCatalogName = StoreConvertor
            .catalogName(databaseName.getProjectId(), databaseName.getCatalogName());
        CatalogObject catalogRecord = catalogStore.getCatalogByName(context, branchCatalogName);
        if (catalogRecord == null) {
            throw new CatalogServerException(ErrorCode.CATALOG_NOT_FOUND, databaseName.getCatalogName());
        }
        CatalogIdent catalogIdent = StoreConvertor.catalogIdent(databaseName.getProjectId(),
            catalogRecord.getCatalogId(), catalogRecord.getRootCatalogId());
        String catalogId = catalogIdent.getCatalogId();

        //get databaseId, The databaseName must be unique
        String databaseId = getBranchDroppedDatabaseId(context, catalogIdent, databaseName.getDatabaseName());

        return StoreConvertor
            .databaseIdent(catalogIdent, databaseId);
    }

    public static DatabaseObject getDatabaseObject(DatabaseIdent databaseIdent) {
        CheckUtil.checkStringParameter(databaseIdent.getProjectId(), databaseIdent.getCatalogId(),
            databaseIdent.getDatabaseId());
        return TransactionRunnerUtil.transactionReadRunThrow(context -> getDatabaseObject(context, databaseIdent)).getResult();
    }

    public static DatabaseObject getDatabaseObject(TransactionContext context, DatabaseName databaseName) {
        // 1. get database identifier by databaseName from objectName subspace
        DatabaseIdent databaseIdent = DatabaseObjectHelper.getDatabaseIdent(context, databaseName);
        if (databaseIdent == null) {
            return null;
        }
        // 2. get database instance from Database subspace by identifier
        DatabaseObject getDatabase = getDatabaseObject(context, databaseIdent);
        if (getDatabase == null) {
            return null;
        }

        return getDatabase;
    }

    public static DatabaseObject getDatabaseObject(DatabaseName databaseName) {
        return TransactionRunnerUtil.transactionReadRunThrow(context -> {
            return DatabaseObjectHelper.getDatabaseObject(context, databaseName);
        }).getResult();
    }

    public static DatabaseObject getDatabaseObject(TransactionContext ctx, DatabaseIdent databaseIdent) {
        DatabaseObject database = databaseStore.getDatabase(ctx, databaseIdent);
        if (database == null) {
            database = getSubBranchFakeDatabase(ctx, databaseIdent);
            if (database == null) {
                return null;
            }
        }

        return database;
    }

    public static DatabaseIdent getDatabaseIdent(DatabaseName databaseName) {
        return TransactionRunnerUtil.transactionReadRunThrow(context -> getDatabaseIdent(context, databaseName)).getResult();
    }
    public static DatabaseIdent getDatabaseIdent(TransactionContext context, DatabaseName databaseName)
        throws MetaStoreException {
        /*
         * Query the name to the ID.
         * Matching order
         * 1: Check whether the current path has a matching name. If yes, use the ID corresponding to the name.
         * 2: Check whether the parent branch has the ID corresponding to the name. If yes, Perform the check in step 3.
         *    If no, return object not exist
         * 3: Two scenarios are considered: (1) the object is dropped from the sub-branch, (2) renamed from the sub-branch.
         *    These two scenarios result in an object of the corresponding name found on the parent branch, but not available.
         *    Check whether the object ID of the parent branch exists on the branch. If the object ID exists, the name
         *    has changed.
         */

        //get catalogId
        CatalogName catalogName = StoreConvertor
            .catalogName(databaseName.getProjectId(), databaseName.getCatalogName());
        CatalogIdent catalogIdent = CatalogObjectHelper.getCatalogIdent(context, catalogName);
        if (catalogIdent == null) {
            return null;
        }

        //get databaseId
        String databaseId = getDatabaseId(context, catalogIdent, databaseName);
        if (databaseId == null) {
            return null;
        }

        return StoreConvertor.databaseIdent(databaseName.getProjectId(), catalogIdent.getCatalogId(),
            databaseId, catalogIdent.getRootCatalogId());
    }

    public static DatabaseIdent getDatabaseIdent(TransactionContext context, CatalogIdent catalogIdent,
        DatabaseName databaseName) throws MetaStoreException {

        /*
         * Query the name to the ID.
         * Matching order
         * 1: Check whether the current path has a matching name. If yes, use the ID corresponding to the name.
         * 2: Check whether the parent branch has the ID corresponding to the name. If yes, Perform the check in step 3.
         *    If no, return object not exist
         * 3: Two scenarios are considered: (1) the object is dropped from the sub-branch, (2) renamed from the sub-branch.
         *    These two scenarios result in an object of the corresponding name found on the parent branch, but not available.
         *    Check whether the object ID of the parent branch exists on the branch. If the object ID exists, the name
         *    has changed.
         */

        //get databaseId
        String databaseId = getDatabaseId(context, catalogIdent, databaseName);
        if (databaseId == null) {
            return null;
        }

        return StoreConvertor.databaseIdent(catalogIdent, databaseId);
    }

    public static String getDatabaseId(TransactionContext context, CatalogIdent subBranchCatalogIdent,
        DatabaseName databaseName)
        throws CatalogServerException {
        //step 1
        Optional<String> databaseId = databaseStore
            .getDatabaseId(context, subBranchCatalogIdent, databaseName.getDatabaseName());
        if (databaseId.isPresent()) {
            return databaseId.get();
        }

        // Check whether the branch is a sub-branch.
        // The sub-branch may inherit the database of the parent branch.
        // step 2
        return (!Objects.equals(subBranchCatalogIdent.getCatalogId(), subBranchCatalogIdent.getRootCatalogId())) ? getSubBranchFakeDatabaseId(context, subBranchCatalogIdent, databaseName) : null;
    }

    public static void throwIfDatabaseNameExist(TransactionContext context, DatabaseName databaseNameNew,
        DatabaseIdent databaseIdent) {
        // check parent branch should not have the same name  in  ObjectName subspace
        CatalogIdent catalogIdent = StoreConvertor.catalogIdent(databaseIdent.getProjectId(),
            databaseIdent.getCatalogId(), databaseIdent.getRootCatalogId());
        String databaseId = DatabaseObjectHelper.getDatabaseId(context, catalogIdent, databaseNameNew);
        if (databaseId != null) {
            throw new CatalogServerException(ErrorCode.DATABASE_ALREADY_EXIST, databaseNameNew.getDatabaseName());
        }
    }

    public static ScanRecordCursorResult<List<DatabaseRefObject>> listDatabase(TransactionContext context,
        CatalogIdent catalogIdent, int maxNum, byte[] continuation, TransactionIsolationLevel isolationLevel) {
        ScanRecordCursorResult<List<String>> databaseIdList = databaseStore
            .listDatabaseId(context, catalogIdent,
                maxNum, continuation, isolationLevel);
        List<DatabaseObject> databaseRecordList = databaseStore.getDatabaseByIds(context, catalogIdent.getProjectId(), catalogIdent.getCatalogId(), databaseIdList.getResult());
        List<DatabaseRefObject> refObjects = databaseRecordList.stream().map(x -> new DatabaseRefObject(x)).collect(Collectors.toList());
        return new ScanRecordCursorResult<>(refObjects, databaseIdList.getContinuation().orElse(null));
    }

    public static List<DatabaseRefObject> listDatabases(TransactionContext context, CatalogIdent catalogIdent,
        CatalogName catalogName, int maxNum, boolean includeDropped) {
        HashMap<String, DatabaseRefObject> databaseUndroppedInfos = listValidDatabases(context, catalogIdent, maxNum);
        HashMap<String, DatabaseRefObject> databaseDroppedInfos = listDroppedDatabases(context, catalogIdent, maxNum);

        // get all parent branch
        ParentBranchCatalogIterator parentCatalogIdentIterator = new ParentBranchCatalogIterator(context, catalogIdent);
        while (parentCatalogIdentIterator.hasNext()) {
            CatalogIdent parentCatalogIdent = parentCatalogIdentIterator.nextCatalogIdent();
            String subBranchVersion = parentCatalogIdentIterator.nextBranchVersion(parentCatalogIdent);

            HashMap<String, DatabaseRefObject> parentBranchDatabaseUndroppedInfos =
                listValidDatabases(context, parentCatalogIdent, maxNum);
            for (String databaseId : parentBranchDatabaseUndroppedInfos.keySet()) {
                DatabaseIdent parentDatabaseIdent = StoreConvertor.databaseIdent(parentCatalogIdent, databaseId);
                if (DatabaseHistoryHelper.checkBranchDatabaseIdValid(context, parentDatabaseIdent, subBranchVersion)) {
                    if (!databaseUndroppedInfos.containsKey(databaseId)
                        && !databaseDroppedInfos.containsKey(databaseId)) {
                        DatabaseRefObject parentDatabaseRef = parentBranchDatabaseUndroppedInfos.get(databaseId);
                        DatabaseRefObject databaseRefObject = new DatabaseRefObject(parentDatabaseRef);
                        databaseRefObject.setCatalogId(catalogIdent.getCatalogId());
                        databaseUndroppedInfos.put(databaseId, databaseRefObject);
                    }
                }
            }

            HashMap<String, DatabaseRefObject> parentBranchDatabaseDroppedInfos =
                listDroppedDatabases(context, parentCatalogIdent, maxNum);
            for (String databaseId : parentBranchDatabaseDroppedInfos.keySet()) {
                DatabaseIdent parentDatabaseIdent = StoreConvertor.databaseIdent(parentCatalogIdent, databaseId);
                if (DatabaseHistoryHelper.checkBranchDatabaseIdValid(context, parentDatabaseIdent, subBranchVersion)) {
                    if (!databaseUndroppedInfos.containsKey(databaseId)
                        && !databaseDroppedInfos.containsKey(databaseId)) {
                        DatabaseRefObject parentDatabaseRef = parentBranchDatabaseDroppedInfos.get(databaseId);
                        DatabaseRefObject databaseRefObject = new DatabaseRefObject(parentDatabaseRef);
                        databaseRefObject.setCatalogId(catalogIdent.getCatalogId());
                        databaseDroppedInfos.put(databaseId, databaseRefObject);
                    }
                }
            }
        }

        List<DatabaseRefObject> databaseInfoList = new ArrayList<>(databaseUndroppedInfos.values());
        if (includeDropped) {
            databaseInfoList.addAll(databaseDroppedInfos.values());
        }
        return databaseInfoList;
    }

    private static String getBranchDroppedDatabaseId(TransactionContext context, CatalogIdent branchCatalogIdent,
        String databaseName)
        throws CatalogServerException {
        List<String> droppedDatabaseIdList = getDroppedDatabaseId(context, branchCatalogIdent, databaseName);

        List<String> fakeDroppedDatabaseIdList = getSubBranchFakeDroppedDatabasesId(context, branchCatalogIdent,
            databaseName);

        droppedDatabaseIdList.addAll(fakeDroppedDatabaseIdList);

        if (droppedDatabaseIdList.isEmpty()) {
            throw new CatalogServerException(ErrorCode.DATABASE_NOT_FOUND, databaseName);
        } else if (droppedDatabaseIdList.size() != 1) {
            throw new CatalogServerException(ErrorCode.DATABASE_MULTIPLE_EXISTS, databaseName);
        }

        return droppedDatabaseIdList.get(0);
    }

    private static HashMap<String, DatabaseRefObject> listValidDatabases(TransactionContext ctx,
        CatalogIdent catalogIdent, int maxNum) {
        // let's convert database records to DatabaseInfo, then return it as a list of DBINFOs
        HashMap<String, DatabaseRefObject> dbInfos = new HashMap<>();
        ScanRecordCursorResult<List<DatabaseRefObject>> databaseRecordList = DatabaseObjectHelper
            .listDatabase(ctx, catalogIdent,
                    maxNum, null, TransactionIsolationLevel.SERIALIZABLE);
        for (DatabaseRefObject databaseRecord : databaseRecordList.getResult()) {
            dbInfos.put(databaseRecord.getDatabaseId(), databaseRecord);
        }

        return dbInfos;
    }


    private static HashMap<String, DatabaseRefObject> listDroppedDatabases(TransactionContext ctx,
        CatalogIdent catalogIdent, int maxNum) {
        // this returns an asynchronous cursor over the results of our query
        // let's convert database records to DatabaseInfo, then return it as a list of DBINFOs
        HashMap<String, DatabaseRefObject> dbInfos = new HashMap<>();

        ScanRecordCursorResult<List<DroppedDatabaseNameObject>> droppedDatabaseObjectList = databaseStore
            .listDroppedDatabaseObjectName(ctx, catalogIdent, maxNum, null, TransactionIsolationLevel.SERIALIZABLE);

        for (DroppedDatabaseNameObject droppedObjectName : droppedDatabaseObjectList.getResult()) {
            DatabaseRefObject databaseRefObject = new DatabaseRefObject();
            databaseRefObject.setProjectId(catalogIdent.getProjectId());
            databaseRefObject.setCatalogId(catalogIdent.getCatalogId());
            databaseRefObject.setDatabaseId(droppedObjectName.getObjectId());
            databaseRefObject.setName(droppedObjectName.getName());

            dbInfos.put(databaseRefObject.getDatabaseId(), databaseRefObject);
        }

        return dbInfos;
    }

    private static List<String> getSubBranchFakeDroppedDatabasesId(TransactionContext context,
        CatalogIdent subBranchCatalogIdent,
        String subBranchDatabaseName)
        throws CatalogServerException {
        List<String> droppedDatabaseIdList = new ArrayList<>();
        Map<String, String> subBranchDatabaseIds = getDatabaseIds(context, subBranchCatalogIdent, true);

        ParentBranchCatalogIterator parentCatalogIdentIterator = new ParentBranchCatalogIterator(context,
            subBranchCatalogIdent);
        while (parentCatalogIdentIterator.hasNext()) {
            CatalogIdent parentCatalogIdent = parentCatalogIdentIterator.nextCatalogIdent();
            String subBranchVersion = parentCatalogIdentIterator.nextBranchVersion(parentCatalogIdent);

            List<String> findDroppedDatabaseIdList = getDroppedDatabaseId(context, parentCatalogIdent,
                subBranchDatabaseName);
            String findValidDatabaseId = getValidDatabaseId(context, parentCatalogIdent,
                subBranchDatabaseName);
            if (findValidDatabaseId != null) {
                findDroppedDatabaseIdList.add(findValidDatabaseId);
            }

            for (String databaseId : findDroppedDatabaseIdList) {
                if (!subBranchDatabaseIds.containsKey(databaseId)) {
                    DatabaseIdent parentBranchDatabaseIdent = StoreConvertor.databaseIdent(
                        parentCatalogIdent, databaseId);
                    //step 2: check database Id dropped at branch version
                    Optional<DatabaseHistoryObject> optional = databaseStore.getLatestDatabaseHistory(context,
                        parentBranchDatabaseIdent, subBranchVersion);
                    // Two conditions should be satisfied: 1.status is dropped 2 the name same
                    if (optional.isPresent()
                        && (DatabaseHistoryHelper.isDropped(optional.get()))
                        && (optional.get().getName().equals(subBranchDatabaseName))) {
                        droppedDatabaseIdList.add(databaseId);
                    }
                }
            }
            subBranchDatabaseIds.putAll(getDatabaseIds(context, parentCatalogIdent, true));
        }
        return droppedDatabaseIdList;
    }



    private static String getSubBranchFakeDatabaseId(TransactionContext context, CatalogIdent subBranchCatalogIdent,
        DatabaseName subBranchDatabaseName) throws CatalogServerException {

        Map<String, String> subDatabaseIds = getDatabaseIds(context, subBranchCatalogIdent, true);

        ParentBranchCatalogIterator parentBranchCatalogIterator = new ParentBranchCatalogIterator(context,
            subBranchCatalogIdent);

        while (parentBranchCatalogIterator.hasNext()) {
            CatalogIdent parentCatalogIdent = parentBranchCatalogIterator.nextCatalogIdent();
            String subBranchVersion = parentBranchCatalogIterator.nextBranchVersion(parentCatalogIdent);

            List<String> findDatabaseIdList = new ArrayList<>();
            String findDatabaseId = getValidDatabaseId(context, parentCatalogIdent,
                subBranchDatabaseName.getDatabaseName());
            if (null == findDatabaseId) {
                List<String> findDroppedDatabaseIdList = getDroppedDatabaseId(context, parentCatalogIdent,
                    subBranchDatabaseName.getDatabaseName());
                findDatabaseIdList.addAll(findDroppedDatabaseIdList);
            } else {
                findDatabaseIdList.add(findDatabaseId);
            }

            for (String databaseId : findDatabaseIdList) {
                DatabaseIdent parentDatabaseIdent = StoreConvertor.databaseIdent(parentCatalogIdent, databaseId);
                if (DatabaseHistoryHelper.checkBranchDatabaseIdValid(context, parentDatabaseIdent, subBranchVersion)) {
                    if (!subDatabaseIds.containsKey(databaseId)) {
                        return databaseId;
                    }
                    return null;
                }
            }

            subDatabaseIds.putAll(getDatabaseIds(context, parentCatalogIdent, true));
        }

        return null;
    }

    // HashMap <DatabaseId, CatalogId>
    private static Map<String, String> getDatabaseIds(TransactionContext context, CatalogIdent catalogIdent,
        boolean includeDropped) {
        //Obtain database include dropped and in-use
        List<DatabaseIdent> databaseIdentList = listDatabaseIds(context, catalogIdent, includeDropped);
        Map<String, String> databaseIds = databaseIdentList.stream()
            .collect(Collectors.toMap(DatabaseIdent::getDatabaseId,
                DatabaseIdent::getCatalogId));

        return databaseIds;
    }

    public static List<DatabaseIdent> listDatabaseIds(TransactionContext context, CatalogIdent catalogIdent,
        boolean includeDropped) {
        List<DatabaseIdent> databaseIds = new ArrayList<>();

        ScanRecordCursorResult<List<String>> databaseIdList = databaseStore
            .listDatabaseId(context, catalogIdent,
                Integer.MAX_VALUE, null, TransactionIsolationLevel.SERIALIZABLE);

        for (String databaseId : databaseIdList.getResult()) {
            DatabaseIdent databaseIdent = StoreConvertor.databaseIdent(catalogIdent, databaseId);
            databaseIds.add(databaseIdent);
        }

        if (!includeDropped) {
            return databaseIds;
        }

        ScanRecordCursorResult<List<DroppedDatabaseNameObject>> droppedObjectNameList = databaseStore
            .listDroppedDatabaseObjectName(context, catalogIdent,
                Integer.MAX_VALUE, null, TransactionIsolationLevel.SERIALIZABLE);
        for (DroppedDatabaseNameObject droppedObjectName : droppedObjectNameList.getResult()) {
            DatabaseIdent databaseIdent = StoreConvertor.databaseIdent(catalogIdent, droppedObjectName.getObjectId());
            databaseIds.add(databaseIdent);
        }

        return databaseIds;

    }

    private static String getValidDatabaseId(TransactionContext context, CatalogIdent catalogIdent,
        String databaseName) {
        Optional<String> databaseId = databaseStore
            .getDatabaseId(context, catalogIdent, databaseName);
        if (!databaseId.isPresent()) {
            return null;
        }

        return databaseId.get();
    }

    private static List<String> getDroppedDatabaseId(TransactionContext context, CatalogIdent catalogIdent,
        String databaseName) {
        int maxResultes = Integer.MAX_VALUE;
        List<String> droppedDatabaseIdList = new ArrayList<>();
        ScanRecordCursorResult<List<DroppedDatabaseNameObject>> droppedObjectNameList = databaseStore
            .listDroppedDatabaseObjectName(context,
                catalogIdent, maxResultes, null, TransactionIsolationLevel.SERIALIZABLE);

        for (DroppedDatabaseNameObject droppedObjectName : droppedObjectNameList.getResult()) {
            if (databaseName.equals(droppedObjectName.getName())) {
                droppedDatabaseIdList.add(droppedObjectName.getObjectId());
            }
        }

        return droppedDatabaseIdList;
    }

    private static DatabaseObject getSubBranchFakeDatabase(TransactionContext context, DatabaseIdent subBranchDatabaseIdent)
        throws MetaStoreException {
        CatalogIdent subBranchCatalogIdent = StoreConvertor.catalogIdent(subBranchDatabaseIdent.getProjectId(),
            subBranchDatabaseIdent.getCatalogId(), subBranchDatabaseIdent.getRootCatalogId());
        CatalogObject subBranchCatalogRecord = catalogStore.getCatalogById(context, subBranchCatalogIdent);
        if (subBranchCatalogRecord == null) {
            return null;
        }

        ParentBranchCatalogIterator parentCatalogIdentIterator = new ParentBranchCatalogIterator(context,
            subBranchDatabaseIdent, Boolean.FALSE);

        while (parentCatalogIdentIterator.hasNext()) {
            CatalogIdent parentCatalogIdent = parentCatalogIdentIterator.nextCatalogIdent();
            String parentVersion = parentCatalogIdentIterator.nextBranchVersion(parentCatalogIdent);
            DatabaseIdent parentDatabaseIdent = StoreConvertor.databaseIdent(parentCatalogIdent,
                subBranchDatabaseIdent.getDatabaseId());
            Optional<DatabaseHistoryObject> databaseHistory = databaseStore.getLatestDatabaseHistory(context,
                parentDatabaseIdent, parentVersion);
            if (databaseHistory.isPresent()) {
                return new DatabaseObject(subBranchDatabaseIdent, databaseHistory.get());
            }
        }
        return null;
    }

    public static void insertDatabaseObject(TransactionContext context, DatabaseIdent databaseIdent, DatabaseObject databaseObject,
        DatabaseName dataBaseName,UserPrivilegeStore userPrivilegeStore, String catalogCommitEventId) {
        // 1. insert a database name Record into ObjectName Store
        databaseStore.insertDatabase(context, databaseIdent, databaseObject);

        String version = VersionManagerHelper
            .getNextVersion(context, databaseIdent.getProjectId(), databaseIdent.getRootCatalogId());

        // 3. insert into catalog history Store
        databaseStore.insertDatabaseHistory(context, new DatabaseHistoryObject(databaseObject), databaseIdent, false,
            version);

        // 4. insert user privilege table
        userPrivilegeStore.insertUserPrivilege(context, databaseIdent.getProjectId(), databaseObject.getUserId(),
            ObjectType.DATABASE.name(), databaseIdent.getDatabaseId(), true, 0);

        long commitTime = RecordStoreHelper.getCurrentTime();
        catalogStore.insertCatalogCommit(context, databaseIdent.getProjectId(),
            databaseIdent.getCatalogId(), catalogCommitEventId, commitTime, CREATE_DATABASE,
            commonCommitDetail(dataBaseName)
        );

        ObjectNameMapHelper.saveObjectNameMapIfNotExist(context, dataBaseName, databaseIdent, databaseObject.getProperties());
    }

    public static String commonCommitDetail(DatabaseName databaseName) {
        return new StringBuilder()
            .append("database name: ").append(databaseName.getDatabaseName())
            .toString();
    }

    public static String commonCommitDetail(DatabaseObject databaseObject) {
        return new StringBuilder()
            .append("database name: ").append(databaseObject.getName())
            .toString();
    }

}
