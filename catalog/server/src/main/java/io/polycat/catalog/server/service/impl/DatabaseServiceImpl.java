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

import com.apple.foundationdb.tuple.Tuple;
import io.polycat.catalog.server.util.TransactionFrameRunner;
import io.polycat.catalog.server.util.TransactionRunnerUtil;
import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.model.CatalogIdent;
import io.polycat.catalog.common.model.CatalogName;
import io.polycat.catalog.common.model.CatalogObject;
import io.polycat.catalog.common.model.Database;
import io.polycat.catalog.common.model.DatabaseHistory;
import io.polycat.catalog.common.model.DatabaseHistoryObject;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.DatabaseObject;
import io.polycat.catalog.common.model.DatabaseRefObject;
import io.polycat.catalog.common.model.DroppedDatabaseNameObject;
import io.polycat.catalog.common.model.ObjectNameMap;
import io.polycat.catalog.common.model.ScanRecordCursorResult;
import io.polycat.catalog.common.model.TableBrief;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.TransactionIsolationLevel;
import io.polycat.catalog.common.model.TraverseCursorResult;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;
import io.polycat.catalog.common.utils.CatalogToken;
import io.polycat.catalog.common.utils.CodecUtil;
import io.polycat.catalog.common.utils.UuidUtil;
import io.polycat.catalog.service.api.DatabaseService;
import io.polycat.catalog.store.api.CatalogStore;
import io.polycat.catalog.store.api.DatabaseStore;
import io.polycat.catalog.store.api.ObjectNameMapStore;
import io.polycat.catalog.store.api.TableMetaStore;
import io.polycat.catalog.store.api.UserPrivilegeStore;
import io.polycat.catalog.store.common.StoreConvertor;
import io.polycat.catalog.store.common.StoreValidator;
import io.polycat.catalog.store.fdb.record.RecordStoreHelper;

import io.polycat.catalog.util.CheckUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import static io.polycat.catalog.common.Operation.ALTER_DATABASE;
import static io.polycat.catalog.common.Operation.DROP_DATABASE;
import static io.polycat.catalog.common.Operation.UNDROP_DATABASE;
import static io.polycat.catalog.server.service.impl.ObjectNameMapHelper.LMS_KEY;

/**
 * DatabaseService implementation
 */
@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class DatabaseServiceImpl implements DatabaseService {

    private static final Logger logger = Logger.getLogger(DatabaseServiceImpl.class);

    private final String databaseStoreCheckSum = "catalogDatabaseStore";
    private static final int DATABASE_STORE_MAX_RETRY_NUM = 256;
    private final int maxBatchRowNum = 1024;

    @Autowired
    private ObjectNameMapStore objectNameMapStore;

    @Autowired
    private UserPrivilegeStore userPrivilegeStore;

    @Autowired
    private CatalogStore catalogStore;

    @Autowired
    private DatabaseStore databaseStore;

    @Autowired
    private TableMetaStore tableMetaStore;


    private static class DatabaseServiceImplHandler {

        private static final DatabaseServiceImpl INSTANCE = new DatabaseServiceImpl();
    }

    public static DatabaseServiceImpl getInstance() {
        return DatabaseServiceImplHandler.INSTANCE;
    }



    private DatabaseIdent createDatabase(CatalogName catalogName, DatabaseObject databaseObject)
        throws CatalogServerException {
        if (StringUtils.isBlank(catalogName.getProjectId())) {
            throw new CatalogServerException(ErrorCode.ARGUMENT_ILLEGAL, "project id is not provided");
        }

        String catalogCommitEventId = UuidUtil.generateCatalogCommitId();
        // Else, use fullName to get catalog id, and then create db
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(DATABASE_STORE_MAX_RETRY_NUM);
        return runner.run(context ->
            createDatabaseInternal(context, catalogName, databaseObject,
                catalogCommitEventId))
            .getResultAndCheck(ret -> CatalogCommitHelper.catalogCommitExist(ret, catalogCommitEventId));

    }

    private DatabaseIdent createDatabaseInternal(TransactionContext ctx, CatalogName catalogName,
        DatabaseObject databaseObject, String catalogCommitEventId)
        throws CatalogServerException {
        CatalogIdent catalogIdent = CatalogObjectHelper.getCatalogIdent(ctx, catalogName);
        if (catalogIdent == null) {
            throw new CatalogServerException(ErrorCode.CATALOG_NOT_FOUND, catalogName.getCatalogName());
        }

        String databaseId = databaseStore.generateDatabaseId(ctx, catalogIdent.getProjectId(),
            catalogIdent.getCatalogId());

        //add database map
        DatabaseIdent databaseIdent = StoreConvertor.databaseIdent(catalogIdent, databaseId);
        DatabaseName databaseName = StoreConvertor.databaseName(catalogName, databaseObject.getName());

        // check if database name already exists in ObjectName Store subspace
        DatabaseObjectHelper.throwIfDatabaseNameExist(ctx, databaseName, databaseIdent);

        DatabaseObjectHelper.insertDatabaseObject(ctx, databaseIdent, databaseObject, databaseName, userPrivilegeStore, catalogCommitEventId);

        DiscoveryHelper.updateDatabaseDiscoveryInfo(ctx, databaseName, databaseObject);

        return databaseIdent;
    }


    private DatabaseObject trans2DatabaseObject(DatabaseIdent databaseIdent, DatabaseHistoryObject databaseHistory) {
        DatabaseObject databaseObject = new DatabaseObject();
        databaseObject.setName(databaseHistory.getName());
        databaseObject.setProjectId(databaseIdent.getProjectId());
        databaseObject.setCatalogId(databaseIdent.getCatalogId());
        databaseObject.setDatabaseId(databaseIdent.getDatabaseId());
        databaseObject.getProperties().putAll(databaseHistory.getProperties());
        databaseObject.setLocation(databaseHistory.getLocation());
        databaseObject.setCreateTime(databaseHistory.getCreateTime());
        databaseObject.setDroppedTime(databaseHistory.getDroppedTime());
        databaseObject.setDescription(databaseHistory.getDescription());
        databaseObject.setUserId(databaseHistory.getUserId());
        return databaseObject;
    }

    /**
     * create db
     *
     * @param catalogName
     * @param databaseInput
     * @return
     */
    @Override
    public Database createDatabase(CatalogName catalogName, DatabaseInput databaseInput) {
        String name = databaseInput.getDatabaseName().toLowerCase();
        CheckUtil.checkNameLegality("databaseName", name);

        DatabaseObject databaseObject = new DatabaseObject();
        databaseObject.setName(name);
        databaseObject.setCreateTime(System.currentTimeMillis());
        databaseObject.setLocation(databaseInput.getLocationUri());
        databaseObject.setUserId(databaseInput.getOwner() == null ? "" : databaseInput.getOwner());

        if (databaseInput.getParameters() != null) {
            databaseObject.setProperties(databaseInput.getParameters());
        }

        if (databaseInput.getDescription() != null) {
            databaseObject.setDescription(databaseInput.getDescription());
        }
        if (databaseInput.getOwnerType() != null) {
            databaseObject.setOwnerType(databaseInput.getOwnerType());
        }
        try {
            DatabaseIdent databaseIdent = createDatabase(catalogName, databaseObject);
            databaseObject.setProjectId(databaseIdent.getProjectId());
            databaseObject.setCatalogId(databaseIdent.getCatalogId());
            databaseObject.setDatabaseId(databaseIdent.getDatabaseId());
            return DatabaseObjectConvertHelper.toDatabase(catalogName.getCatalogName(), databaseObject);
        } catch (MetaStoreException e) {
            e.printStackTrace();
            throw new CatalogServerException(e.getMessage(), e.getErrorCode());
        }

    }

    private boolean hasAnyTable(TransactionContext context, DatabaseIdent databaseIdent) {
        /*ScanRecordCursorResult<List<TableNameObject>> tableObjectList =
            tableStore.listTableObjectName(context, databaseIdent, Integer.MAX_VALUE,
                null, TransactionIsolationLevel.SERIALIZABLE);
        return !tableObjectList.getResult().isEmpty();*/

        return false;
    }

    private void deleteObjectNameMapIfExist(TransactionContext context, DatabaseName databaseName,
        DatabaseIdent databaseIdent) {
        Optional<ObjectNameMap> objectNameMapOptional = objectNameMapStore
            .getObjectNameMap(context, databaseName.getProjectId(), ObjectType.DATABASE,
                "null", databaseName.getDatabaseName());
        // 2 layer object name, which may map to multiple Catalogs
        // If catalog ids are different, the same table object map record exist
        // If the catalog Id is different from the current table,
        // the map is manually modified and the 2Layer object is mapped to another catalog.
        // In this case, the map table is not modified
        if ((objectNameMapOptional.isPresent())
            && (objectNameMapOptional.get().getUpperObjectId().equals(databaseIdent.getCatalogId()))) {
            objectNameMapStore.deleteObjectNameMap(context,
                databaseName.getProjectId(), ObjectType.DATABASE, "null",
                databaseName.getDatabaseName());
        }
    }

    private DatabaseIdent dropDatabaseInternal(TransactionContext context, DatabaseName databaseName,
        String catalogCommitEventId) {
        // check whether the database exists.
        DatabaseIdent databaseIdent = DatabaseObjectHelper.getDatabaseIdent(context, databaseName);
        if (databaseIdent == null) {
            throw new CatalogServerException(ErrorCode.DATABASE_NOT_FOUND, databaseName.getDatabaseName());
        }
        // check whether the database has any tables
        if (hasAnyTable(context, databaseIdent)) {
            throw new CatalogServerException(ErrorCode.DATABASE_TABLE_EXISTS, databaseName.getDatabaseName());
        }

        // 2. delete database from Database Subspace
        databaseStore.deleteDatabase(context, databaseIdent);

        // 3. add database into Dropped ObjectName Subspace
        String latestVersion = VersionManagerHelper
            .getLatestVersion(context, databaseIdent.getProjectId(), databaseIdent.getRootCatalogId());
        DatabaseHistoryObject databaseHistory = getLatestDatabaseHistory(context, databaseIdent, latestVersion,
            Boolean.FALSE);

        databaseStore.insertDroppedDatabaseObjectName(context, databaseIdent, databaseName.getDatabaseName());

        // 4. add this operation into Database History Subspace, setting INVISABLE true
        DatabaseHistoryObject databaseHistoryNew = new DatabaseHistoryObject(databaseHistory);
        databaseHistoryNew.setDroppedTime(RecordStoreHelper.getCurrentTime());
        String version = VersionManagerHelper
            .getNextVersion(context, databaseIdent.getProjectId(), databaseIdent.getRootCatalogId());
        databaseStore.insertDatabaseHistory(context, databaseHistoryNew, databaseIdent, true, version);

        // 5. add this operation into Catalog Commit Subspace as a log
        long commitTime = RecordStoreHelper.getCurrentTime();
        catalogStore.insertCatalogCommit(context, databaseIdent.getProjectId(),
            databaseIdent.getCatalogId(), catalogCommitEventId, commitTime, DROP_DATABASE,
            DatabaseObjectHelper.commonCommitDetail(databaseName)
        );

        deleteObjectNameMapIfExist(context, databaseName, databaseIdent);

        userPrivilegeStore.deleteUserPrivilege(context, databaseIdent.getProjectId(),
            databaseHistoryNew.getUserId(), ObjectType.DATABASE.name(), databaseIdent.getDatabaseId());

        DiscoveryHelper.dropDatabaseDiscoveryInfo(context, databaseName);

        return databaseIdent;
    }

    private String dropDatabaseByName(DatabaseName databaseName) throws CatalogServerException {
        String catalogCommitEventId = UuidUtil.generateCatalogCommitId();
        DatabaseIdent databaseIdent = null;
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(DATABASE_STORE_MAX_RETRY_NUM);
        databaseIdent = runner.run(context -> dropDatabaseInternal(context, databaseName, catalogCommitEventId))
            .getResultAndCheck(ret -> CatalogCommitHelper.catalogCommitExist(ret, catalogCommitEventId));
        return databaseIdent.getDatabaseId();
    }


    private DatabaseIdent getDatabaseIdent(DatabaseName databaseName) {
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(DATABASE_STORE_MAX_RETRY_NUM);
        return runner.run(context -> DatabaseObjectHelper.getDatabaseIdent(context, databaseName)).getResult();
    }

    /**
     * delete database
     *
     * @param databaseName          DatabaseName
     * @param ignoreUnknownDatabase
     * @param cascade               cascade
     * @return
     */
    @Override
    public String dropDatabase(DatabaseName databaseName, Boolean ignoreUnknownDatabase, Boolean deleteData,
        String cascade) {
        String droppedDbId = "";
        if (cascade.equalsIgnoreCase("true")) {
            throw new CatalogServerException(
                "WARN: Does not supporting to drop a database with tables.",
                ErrorCode.INNER_SERVER_ERROR);
        }
        try {
            droppedDbId = dropDatabaseByName(databaseName);
        } catch (CatalogServerException e) {
            if (e.getErrorCode() == ErrorCode.DATABASE_NOT_FOUND && ignoreUnknownDatabase) {
                return droppedDbId;
            } else {
                throw e;
            }
        } catch (MetaStoreException e) {
            if (e.getErrorCode() == ErrorCode.DATABASE_NOT_FOUND && ignoreUnknownDatabase) {
                return droppedDbId;
            } else {
                throw new CatalogServerException(e.getMessage(), e.getErrorCode());
            }
        }

        return droppedDbId;
    }


    private boolean checkDroppedDatabase(TransactionContext context, CatalogIdent catalogIdent, String databaseName,
        String databaseId) {
        DatabaseIdent databaseIdent = StoreConvertor.databaseIdent(catalogIdent, databaseId);
        DroppedDatabaseNameObject droppedObjectName = databaseStore
            .getDroppedDatabaseObjectName(context, databaseIdent, databaseName);

        return droppedObjectName != null;
    }

    private boolean checkParentBranchDroppedDatabase(TransactionContext context, CatalogIdent subBranchCatalogIdent,
        String databaseName, String databaseId)
        throws CatalogServerException {
        ParentBranchCatalogIterator parentCatalogIdentIterator = new ParentBranchCatalogIterator(context,
            subBranchCatalogIdent);

        while (parentCatalogIdentIterator.hasNext()) {
            CatalogIdent parentCatalogIdent = parentCatalogIdentIterator.nextCatalogIdent();
            String subBranchVersion = parentCatalogIdentIterator.nextBranchVersion(parentCatalogIdent);
            DatabaseIdent databaseIdent = StoreConvertor.databaseIdent(parentCatalogIdent, databaseId);

            // check database Id dropped at branch version
            Optional<DatabaseHistoryObject> optional = databaseStore.getLatestDatabaseHistory(context,
                databaseIdent, subBranchVersion);
            // Two conditions should be satisfied: 1.status is dropped 2 the name same
            if (optional.isPresent()
                && (DatabaseHistoryHelper.isDropped(optional.get()))
                && (optional.get().getName().equals(databaseName))) {
                return true;
            }
        }

        return false;
    }

    private DatabaseIdent getDroppedDatabaseIdent(TransactionContext context, DatabaseName databaseName, String databaseId)
        throws CatalogServerException {
        //get catalogId
        CatalogName branchCatalogName = StoreConvertor
            .catalogName(databaseName.getProjectId(), databaseName.getCatalogName());
        CatalogObject catalogObject = catalogStore.getCatalogByName(context, branchCatalogName);
        if (catalogObject == null) {
            throw new CatalogServerException(ErrorCode.CATALOG_NOT_FOUND, databaseName.getCatalogName());
        }
        CatalogIdent catalogIdent = StoreConvertor.catalogIdent(databaseName.getProjectId(),
            catalogObject.getCatalogId(), catalogObject.getRootCatalogId());

        if (checkDroppedDatabase(context, catalogIdent, databaseName.getDatabaseName(), databaseId)) {
            return StoreConvertor.databaseIdent(catalogIdent, databaseId);
        }

        if (checkParentBranchDroppedDatabase(context, catalogIdent, databaseName.getDatabaseName(), databaseId)) {
            return StoreConvertor.databaseIdent(catalogIdent, databaseId);
        }

        throw new CatalogServerException(ErrorCode.DATABASE_NOT_FOUND, databaseName.getDatabaseName());
    }

    private DatabaseIdent undropDatabaseInternal(TransactionContext context, DatabaseName databaseName, String databaseId,
        String newName, String catalogCommitEventId) {
        String oldDatabaseName = databaseName.getDatabaseName();
        DatabaseIdent databaseIdent;

        // get oldDatabaseName by databaseId from droppedObjectName
        if (StringUtils.isBlank(databaseId)) {
            // The databaseName must be unique. If there are multiple databases with the same name,
            // getDroppedDatabasedIdent return null.
            databaseIdent = DatabaseObjectHelper.getOnlyDroppedDatabaseIdentOrElseThrow(context, databaseName);
        } else {
            databaseIdent = getDroppedDatabaseIdent(context, databaseName, databaseId);
        }

        String latestVersion = VersionManagerHelper.getLatestVersion(context, databaseIdent.getProjectId(),
            databaseIdent.getRootCatalogId());
        // get databaseName and databaseIdent
        String databaseNameString = (StringUtils.isBlank(newName) ? oldDatabaseName : newName);
        DatabaseName databaseNameNew = StoreConvertor.databaseName(databaseName.getProjectId(),
            databaseName.getCatalogName(), databaseNameString);


        // check current branch should not have the same name  in  ObjectName subspace
        DatabaseObjectHelper.throwIfDatabaseNameExist(context, databaseNameNew, databaseIdent);

        // delete dropped objectName name
        databaseStore.deleteDroppedDatabaseObjectName(context, databaseIdent, oldDatabaseName);


        // generate new insertDataBaseHistory without dropped_time with newName
        DatabaseHistoryObject databaseHistory = getLatestDatabaseHistory(context, databaseIdent, latestVersion, Boolean.TRUE);

        DatabaseHistoryObject databaseHistoryNew = new DatabaseHistoryObject(databaseHistory);
        databaseHistoryNew.clearDroppedTime();
        databaseHistoryNew.setName(databaseNameString);
        String version = VersionManagerHelper.getNextVersion(context, databaseIdent.getProjectId(), databaseIdent.getRootCatalogId());
        databaseStore.insertDatabaseHistory(context, databaseHistoryNew, databaseIdent, false, version);

        // insert databaseStore
        DatabaseObject databaseObject = trans2DatabaseObject(databaseIdent, databaseHistoryNew);

        databaseStore.insertDatabase(context, databaseIdent, databaseObject);

        //saveObjectNameMapIfNotExist(context, databaseNameNew, databaseIdent, databaseHistoryNew.getProperties());

        // insert catalog commit.
        long commitTime = RecordStoreHelper.getCurrentTime();
        catalogStore.insertCatalogCommit(context, databaseIdent.getProjectId(), databaseIdent.getCatalogId(),
            catalogCommitEventId, commitTime, UNDROP_DATABASE, DatabaseObjectHelper.commonCommitDetail(databaseName));


        return databaseIdent;

    }

    private void undropDatabaseByName(DatabaseName databaseName, String databaseId, String newName)
        throws MetaStoreException {
        String catalogCommitEventId = UuidUtil.generateCatalogCommitId();
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(DATABASE_STORE_MAX_RETRY_NUM);
        DatabaseIdent databaseIdent = runner.run(context ->
            undropDatabaseInternal(context, databaseName, databaseId, newName, catalogCommitEventId))
            .getResultAndCheck(ret -> CatalogCommitHelper.catalogCommitExist(ret, catalogCommitEventId));
    }

    /**
     * undrop DB
     *
     * @param databaseName CatalogName
     * @param databaseId
     * @param rename
     */
    @Override
    public void undropDatabase(DatabaseName databaseName, String databaseId, String rename) {
        try {
            undropDatabaseByName(databaseName, databaseId, rename);
        } catch (MetaStoreException e) {
            throw new CatalogServerException(e.getMessage(), e.getErrorCode());
        }

    }

    private void checkLmsNameValid(TransactionContext context, DatabaseName databaseName, String lmsName) {
        CatalogName catalogNameNew = StoreConvertor.catalogName(databaseName.getProjectId(), lmsName);
        CatalogObject catalogRecordNew = CatalogObjectHelper.getCatalogObject(context, catalogNameNew);
        if (catalogRecordNew == null) {
            throw new CatalogServerException(ErrorCode.CATALOG_NOT_FOUND, lmsName);
        }

        DatabaseName databaseNameCheck = StoreConvertor.databaseName(databaseName.getProjectId(), lmsName,
            databaseName.getDatabaseName());
        DatabaseIdent databaseIdent = DatabaseObjectHelper.getDatabaseIdent(context, databaseNameCheck);
        if (databaseIdent == null) {
            throw new CatalogServerException(ErrorCode.DATABASE_NOT_FOUND, databaseName.getDatabaseName());
        }

        return;
    }

    private boolean switchBranchInNameMap(TransactionContext ctx, DatabaseName databaseName, DatabaseIdent databaseIdent,
        Map<String, String> propertiesMap) {
        return false;
        /*
        if (propertiesMap == null || !propertiesMap.containsKey(LMS_KEY)) {
            return false;
        }
        String destCatName = propertiesMap.get(LMS_KEY);

        checkLmsNameValid(ctx, databaseName, destCatName);
        CatalogName catalogName = StoreConvertor.catalogName(databaseIdent.getProjectId(), destCatName);
        String destCatId = CatalogObjectHelper.getCatalogObject(ctx, catalogName).getCatalogId();

        Optional<ObjectNameMap> objectNameMap = objectNameMapStore.getObjectNameMap(ctx, databaseIdent.getProjectId(),
            ObjectType.DATABASE, "null",databaseName.getDatabaseName());
        if (objectNameMap.isPresent() && objectNameMap.get().getUpperObjectId().equalsIgnoreCase(destCatId)) {
            return false;
        }
        return true;*/
    }

    private void updateObjectNameMap(TransactionContext context, DatabaseName databasePathName,
        DatabaseIdent databaseIdent,
        Map<String, String> properties) {
        objectNameMapStore.deleteObjectNameMap(context, databasePathName.getProjectId(), ObjectType.DATABASE,
            "null", databasePathName.getDatabaseName());

        String latestVersion = VersionManagerHelper.getLatestVersion(context, databaseIdent.getProjectId(),
            databaseIdent.getRootCatalogId());
        DatabaseHistoryObject databaseHistory = getLatestDatabaseHistory(context, databaseIdent, latestVersion, false);

        Optional<String> catalogName = ObjectNameMapHelper.resolveCatalogNameFromInput(properties, LMS_KEY);
        //It's either a three Layer format or an invalid value
        if (catalogName.isPresent()) {
            checkLmsNameValid(context, databasePathName, catalogName.get());
            CatalogName catalogNameNew = StoreConvertor.catalogName(databasePathName.getProjectId(), catalogName.get());
            CatalogObject catalogNew = CatalogObjectHelper.getCatalogObject(context, catalogNameNew);
            DatabaseIdent databaseIdentNew = new DatabaseIdent(databaseIdent.getProjectId(),
                catalogNew.getCatalogId(), databaseIdent.getDatabaseId(), databaseIdent.getRootCatalogId());

            // insert table properties
            databaseHistory = new DatabaseHistoryObject(databaseHistory);
            DatabaseObject currentDB = DatabaseObjectHelper.getDatabaseObject(context, databaseIdent);
            currentDB.getProperties().put(LMS_KEY, properties.get(LMS_KEY));
            databaseStore.upsertDatabase(context, databaseIdent, currentDB);
            String version = VersionManagerHelper.getNextVersion(context, databaseIdent.getProjectId(), databaseIdent.getRootCatalogId());
            databaseStore.insertDatabaseHistory(context, databaseHistory, databaseIdent,false, version);

            ObjectNameMap objectNameMap = new ObjectNameMap(databasePathName, databaseIdentNew);
            objectNameMapStore.insertObjectNameMap(context, objectNameMap);
        } else {
            throw new CatalogServerException(ErrorCode.ARGUMENT_ILLEGAL, "lms_name empty");
        }
    }

    private boolean isRenameDatabase(String newName, String curName) {
        return !newName.equals(curName);
    }


    private void insertDatabaseIntoSubBranch(TransactionContext context, DatabaseIdent parentBranchDatabaseIdent,
        DatabaseObject databaseObject) {
        CatalogIdent parentBranchCatalogIdent = StoreConvertor.catalogIdent(parentBranchDatabaseIdent.getProjectId(),
            parentBranchDatabaseIdent.getCatalogId(), parentBranchDatabaseIdent.getRootCatalogId());

        List<CatalogObject> subBranchCatalogList = catalogStore
            .getNextLevelSubBranchCatalogs(context, parentBranchCatalogIdent);

        DatabaseObject newDatabaseObject = new DatabaseObject(databaseObject);
        for (CatalogObject catalogRecord : subBranchCatalogList) {
            CatalogIdent subBranchCatalogIdent = StoreConvertor.catalogIdent(catalogRecord.getProjectId(),
                catalogRecord.getCatalogId(), catalogRecord.getRootCatalogId());

            //Check whether the sub-branch database has a name.
            DatabaseIdent subBranchDatabaseIdent = StoreConvertor.databaseIdent(subBranchCatalogIdent,
                parentBranchDatabaseIdent.getDatabaseId());

            List<DatabaseIdent> databaseIdentList = DatabaseObjectHelper
                .listDatabaseIds(context, subBranchCatalogIdent, true);
            Map<String, String> databaseIds = databaseIdentList.stream()
                .collect(Collectors.toMap(DatabaseIdent::getDatabaseId,
                    DatabaseIdent::getCatalogId));

            if (databaseIds.containsKey(subBranchDatabaseIdent.getDatabaseId())) {
                continue;
            }

            // insert an object into ObjectName subspace
            newDatabaseObject.setCatalogId(subBranchDatabaseIdent.getCatalogId());
            databaseStore.insertDatabase(context, subBranchDatabaseIdent, newDatabaseObject);
        }
    }

    private void alterTableObjectNameMap(TransactionContext context, DatabaseName databaseName,
        DatabaseName databaseNameNew,
        DatabaseIdent databaseIdent) {
        List<ObjectNameMap> objectNameMapList = objectNameMapStore
            .listObjectNameMap(context, databaseName.getProjectId(), ObjectType.TABLE,
                databaseName.getDatabaseName(), databaseIdent.getCatalogId());

        for (ObjectNameMap objectNameMap : objectNameMapList) {
            objectNameMapStore.deleteObjectNameMap(context, objectNameMap.getProjectId(), objectNameMap.getObjectType(),
                objectNameMap.getUpperObjectName(), objectNameMap.getObjectName());
            objectNameMap.setUpperObjectName(databaseNameNew.getDatabaseName());
            objectNameMapStore.insertObjectNameMap(context, objectNameMap);
        }
    }

    private void updateObjectNameMapIfExist(TransactionContext context, DatabaseName databasePathName,
        DatabaseIdent databaseIdent,
        String databaseNameNew) {
        String versionstamp = VersionManagerHelper.getLatestVersion(context, databaseIdent.getProjectId(),
            databaseIdent.getRootCatalogId());

        DatabaseHistoryObject databaseHistory = getLatestDatabaseHistory(context, databaseIdent, versionstamp, false);

        DatabaseName databasePathNameNew = new DatabaseName(databasePathName);
        databasePathNameNew.setDatabaseName(databaseNameNew);
        alterTableObjectNameMap(context, databasePathName, databasePathNameNew, databaseIdent);

        Optional<String> catalogName = ObjectNameMapHelper.resolveCatalogNameFromInput(databaseHistory.getProperties(), LMS_KEY);
        if (!catalogName.isPresent()) {
            return;
        }

        Optional<ObjectNameMap> objectNameMapOptional = objectNameMapStore
            .getObjectNameMap(context, databasePathName.getProjectId(), ObjectType.DATABASE, "null",
                databasePathName.getDatabaseName());
        // 2 layer object name, which may map to multiple Catalogs
        // If catalog ids are different, the same table object map record exist
        // If the catalog Id is different from the current table,
        // the map is manually modified and the 2Layer object is mapped to another catalog.
        // In this case, the map table is not modified
        if ((objectNameMapOptional.isPresent())
            && (!objectNameMapOptional.get().getUpperObjectId().equals(databaseIdent.getCatalogId()))) {
            return;
        }

        objectNameMapStore.deleteObjectNameMap(context, databasePathName.getProjectId(), ObjectType.DATABASE,
            "null", databasePathName.getDatabaseName());
        ObjectNameMap objectNameMap = new ObjectNameMap(databasePathNameNew, databaseIdent);
        objectNameMapStore.insertObjectNameMap(context, objectNameMap);
    }

    private DatabaseIdent alterDatabaseInternal(TransactionContext ctx, DatabaseInput dataBaseInput,
        DatabaseName currentDBFullName,  String catalogCommitEventId)
        throws CatalogServerException {
        // get old database id by fullName

        DatabaseIdent databaseIdent = DatabaseObjectHelper.getDatabaseIdent(ctx, currentDBFullName);
        if (databaseIdent == null) {
            throw new CatalogServerException(ErrorCode.DATABASE_NOT_FOUND, currentDBFullName.getDatabaseName());
        }

        String latestVersion = VersionManagerHelper.getLatestVersion(ctx, databaseIdent.getProjectId(),
                databaseIdent.getRootCatalogId());

        //todo subbranch get null
        DatabaseObject currentDB = DatabaseObjectHelper.getDatabaseObject(ctx, databaseIdent);
        DatabaseObject newDB = new DatabaseObject(currentDB);
        newDB.setName(dataBaseInput.getDatabaseName().toLowerCase());
        if (dataBaseInput.getParameters() != null) {
            newDB.getProperties().putAll(dataBaseInput.getParameters());
        }
        if (dataBaseInput.getOwner() != null) {
            newDB.setUserId(dataBaseInput.getOwner());
        }
        if (dataBaseInput.getOwnerType() != null) {
            newDB.setOwnerType(dataBaseInput.getOwnerType());
        }
        if (dataBaseInput.getDescription() != null) {
            newDB.setDescription(dataBaseInput.getDescription());
        }

        String locationUri = dataBaseInput.getLocationUri();
        if (locationUri != null && !locationUri.isEmpty()) {
            newDB.setLocation(locationUri);
        }

        // switching branches and alter properties cannot be done at the same time
        if (switchBranchInNameMap(ctx, currentDBFullName, databaseIdent, newDB.getProperties())) {
            updateObjectNameMap(ctx, currentDBFullName, databaseIdent, newDB.getProperties());
        } else {
            if (isRenameDatabase(newDB.getName(), currentDB.getName())) {
                DatabaseName databaseNameNew = new DatabaseName(currentDBFullName);
                databaseNameNew.setDatabaseName(newDB.getName());
                DatabaseObjectHelper.throwIfDatabaseNameExist(ctx, databaseNameNew, databaseIdent);

                // insert database name into next branch
                insertDatabaseIntoSubBranch(ctx, databaseIdent, currentDB);
            }

            logger.info("newDB: {}", newDB);
            // alter database record
            databaseStore.upsertDatabase(ctx, databaseIdent, newDB);

            DatabaseHistoryObject databaseHistory = getLatestDatabaseHistory(ctx, databaseIdent, latestVersion,
                Boolean.FALSE);

            // add a new version in DB History Subspace
            DatabaseHistoryObject newDatabaseHistory = new DatabaseHistoryObject(databaseHistory);
            newDatabaseHistory.setName(newDB.getName());
            newDatabaseHistory.setUserId(newDB.getUserId());
            newDatabaseHistory.setLocation(newDB.getLocation());
            newDatabaseHistory.setDescription(newDB.getDescription());
            newDatabaseHistory.getProperties().putAll(newDB.getProperties());
            String version = VersionManagerHelper.getNextVersion(ctx, databaseIdent.getProjectId(), databaseIdent.getRootCatalogId());
            databaseStore.insertDatabaseHistory(ctx, newDatabaseHistory, databaseIdent, false, version);

            updateObjectNameMapIfExist(ctx, currentDBFullName, databaseIdent, newDB.getName());
        }

        long commitTime = RecordStoreHelper.getCurrentTime();
        catalogStore.insertCatalogCommit(ctx, databaseIdent.getProjectId(),
            databaseIdent.getCatalogId(), catalogCommitEventId, commitTime, ALTER_DATABASE, DatabaseObjectHelper.commonCommitDetail(newDB)
        );

        DiscoveryHelper.updateDatabaseDiscoveryInfo(ctx, currentDBFullName, newDB);
        return databaseIdent;
    }

    private void alterDatabaseByName(DatabaseName currentDBFullName, DatabaseInput dataBaseInput)
        throws MetaStoreException {
        StoreValidator.requireDBNameNotNull(currentDBFullName);

        String catalogCommitEventId = UuidUtil.generateCatalogCommitId();
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(DATABASE_STORE_MAX_RETRY_NUM);
        DatabaseIdent databaseIdent = runner.run(context ->
            alterDatabaseInternal(context, dataBaseInput, currentDBFullName, catalogCommitEventId))
            .getResultAndCheck(ret -> CatalogCommitHelper.catalogCommitExist(ret, catalogCommitEventId));
    }

    /**
     * alter DB
     *
     * @param currentDBFullName
     * @param dataBaseInput     dataBaseDTO
     * @return
     */
    @Override
    public void alterDatabase(DatabaseName currentDBFullName, DatabaseInput dataBaseInput) {
        // support: rename db, set dbproperties, set locationUri
        CheckUtil.checkNameLegality("databaseName", dataBaseInput.getDatabaseName());

        try {
            alterDatabaseByName(currentDBFullName, dataBaseInput);
        } catch (MetaStoreException e) {
            throw new CatalogServerException(e.getMessage(), e.getErrorCode());
        }
    }

    private DatabaseIdent renameDatabaseInternal(TransactionContext ctx, DatabaseName currentDBFullName,
        String newName, String catalogCommitEventId) {
        // get old database id by fullName
        DatabaseIdent databaseIdent = DatabaseObjectHelper.getDatabaseIdent(ctx, currentDBFullName);
        if (databaseIdent == null) {
            throw new CatalogServerException(ErrorCode.DATABASE_NOT_FOUND, currentDBFullName.getDatabaseName());
        }

        String latestVersion = VersionManagerHelper.getLatestVersion(ctx, databaseIdent.getProjectId(),
            databaseIdent.getRootCatalogId());
        //todo subbranch get null
        DatabaseObject currentDB = DatabaseObjectHelper.getDatabaseObject(ctx, databaseIdent);
        DatabaseObject newDB = new DatabaseObject(currentDB);
        newDB.setName(newName);

        // check new name not duplicate
        DatabaseName databaseNameNew = new DatabaseName(currentDBFullName);
        databaseNameNew.setDatabaseName(newName);

        DatabaseObjectHelper.throwIfDatabaseNameExist(ctx, databaseNameNew, databaseIdent);

        //insert database name into next branch
        insertDatabaseIntoSubBranch(ctx, databaseIdent, currentDB);

        // Rename database
        databaseStore.upsertDatabase(ctx, databaseIdent, newDB);

        DatabaseHistoryObject databaseHistory = getLatestDatabaseHistory(ctx, databaseIdent, latestVersion, Boolean.FALSE);

        // add a new version in DB History Subspace
        DatabaseHistoryObject databaseHistoryObjectNew = new DatabaseHistoryObject(databaseHistory);
        databaseHistoryObjectNew.setName(newDB.getName());
        String version = VersionManagerHelper.getNextVersion(ctx, databaseIdent.getProjectId(), databaseIdent.getRootCatalogId());
        databaseStore.insertDatabaseHistory(ctx, databaseHistoryObjectNew,  databaseIdent,false, version);

        // update name map
        //updateObjectNameMapIfExist(ctx, currentDBFullName, databaseIdent, newDB.getName());

        DatabaseName databaseName = new DatabaseName(currentDBFullName);
        databaseName.setDatabaseName(newName);
        long commitTime = RecordStoreHelper.getCurrentTime();
        catalogStore.insertCatalogCommit(ctx, databaseIdent.getProjectId(),
            databaseIdent.getCatalogId(), catalogCommitEventId, commitTime, ALTER_DATABASE, DatabaseObjectHelper.commonCommitDetail(databaseName)
        );

        DiscoveryHelper.dropDatabaseDiscoveryInfo(ctx, currentDBFullName);
        DiscoveryHelper.updateDatabaseDiscoveryInfo(ctx, databaseNameNew, newDB);

        return databaseIdent;
    }

    private void renameDatabaseByName(DatabaseName currentDBFullName, String newDatabaseName)
        throws MetaStoreException {
        String catalogCommitEventId = UuidUtil.generateCatalogCommitId();
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(DATABASE_STORE_MAX_RETRY_NUM);
        DatabaseIdent databaseIdent = runner.run(context ->
            renameDatabaseInternal(context, currentDBFullName, newDatabaseName, catalogCommitEventId))
            .getResultAndCheck(ret -> CatalogCommitHelper.catalogCommitExist(ret, catalogCommitEventId));
    }

    @Override
    public void renameDatabase(DatabaseName currentDBFullName, String newName) {
        // rename db only
        CheckUtil.checkNameLegality("databaseName", newName);

        try {
            renameDatabaseByName(currentDBFullName, newName);
        } catch (MetaStoreException e) {
            throw new CatalogServerException(e.getMessage(), e.getErrorCode());
        }

    }

    private DatabaseObject getDatabaseInfo(TransactionContext context, DatabaseIdent databaseIdent, String catalogName,
        String branchVersion, Boolean dropped) {
        Optional<DatabaseHistoryObject> optional = databaseStore
            .getLatestDatabaseHistory(context, databaseIdent, branchVersion);
        DatabaseObject databaseInfo = null;
        if ((optional.isPresent()) && (dropped.equals(DatabaseHistoryHelper.isDropped(optional.get())))) {
            DatabaseHistoryObject databaseHistory = optional.get();
            databaseInfo = trans2DatabaseObject(databaseIdent, databaseHistory);
        }

        return databaseInfo;
    }

    private List<DatabaseObject> getBranchValidDatabaseInfo(TransactionContext context, CatalogIdent catalogIdent,
        String catalogName, String branchVersion, List<DatabaseRefObject> databaseRecordList, Boolean dropped) {
        List<DatabaseObject> databaseInfoList = new ArrayList<>();
        if (CollectionUtils.isEmpty(databaseRecordList)) {
            return databaseInfoList;
        }
        List<String> databaseIds = databaseRecordList.stream().map(DatabaseRefObject::getDatabaseId).collect(Collectors.toList());
        Map<String, DatabaseHistoryObject> databaseHistoryObjectMap = databaseStore
                .getLatestDatabaseHistoryByIds(context, catalogIdent.getProjectId(), catalogIdent.getCatalogId(), branchVersion, databaseIds);
        Set<String> historyDatabaseIds = databaseHistoryObjectMap.keySet();
        DatabaseObject databaseInfo = null;
        for (String databaseId: databaseIds) {
            if (historyDatabaseIds.contains(databaseId) && (dropped.equals(DatabaseHistoryHelper.isDropped(databaseHistoryObjectMap.get(databaseId))))) {
                databaseInfo = trans2DatabaseObject(new DatabaseIdent(catalogIdent.getProjectId(), catalogIdent.getCatalogId(), databaseId), databaseHistoryObjectMap.get(databaseId));
                databaseInfoList.add(databaseInfo);
            }
        }
        return databaseInfoList;
    }


    private TraverseCursorResult<List<DatabaseObject>> listValidDatabaseWithToken(TransactionContext context, CatalogIdent catalogIdent,
                                                                                  String subBranchVersion, int maxRowLimitNum, Boolean dropped, CatalogToken catalogToken)
        throws CatalogServerException {
        int remainNum = maxRowLimitNum;
        String method = Thread.currentThread().getStackTrace()[1].getMethodName();
        byte[] continuation = (catalogToken.getContextMapValue(method) == null) ? null
            : CodecUtil.hex2Bytes(catalogToken.getContextMapValue(method));

        CatalogObject catalogRecord = CatalogObjectHelper.getCatalogObject(catalogIdent);
        if (catalogRecord == null) {
            throw new CatalogServerException(ErrorCode.CATALOG_NOT_FOUND);
        }

        List<DatabaseObject> databaseInfos = new ArrayList<>();
        ScanRecordCursorResult<List<DatabaseObject>> databaseRecordValidList = new ScanRecordCursorResult<>(
            Collections.emptyList(), null);
        while (true) {
            int batchNum = Math.min(remainNum, maxBatchRowNum);
            byte[] continuationBatch = continuation;
            ScanRecordCursorResult<List<DatabaseRefObject>> databaseRecordList = DatabaseObjectHelper
                    .listDatabase(context, catalogIdent, batchNum, continuationBatch,
                            TransactionIsolationLevel.SNAPSHOT);

            List<DatabaseObject> validList = getBranchValidDatabaseInfo(context, catalogIdent,
                    catalogRecord.getName(), subBranchVersion, databaseRecordList.getResult(), dropped);

            databaseRecordValidList = new ScanRecordCursorResult<>(validList, databaseRecordList.getContinuation().orElse(null));

            databaseInfos.addAll(databaseRecordValidList.getResult());
            remainNum = remainNum - databaseRecordValidList.getResult().size();
            continuation = databaseRecordValidList.getContinuation().orElse(null);
            if ((continuation == null) || (remainNum == 0)) {
                break;
            }
        }

        if (continuation == null) {
            return new TraverseCursorResult<>(databaseInfos, null);
        }

        CatalogToken catalogTokenNew = CatalogToken.buildCatalogToken(databaseStoreCheckSum, method,
            CodecUtil.bytes2Hex(continuation), catalogToken.getReadVersion());
        return new TraverseCursorResult<>(databaseInfos, catalogTokenNew);
    }

    private interface ListDatabaseInBranchWithToken {
        TraverseCursorResult<List<DatabaseObject>> list(CatalogIdent catalogIdent,
            String subBranchVersion, int maxRowLimitNum, CatalogToken catalogToken, Boolean dropped);
    }

    private List<DatabaseObject> getBranchDroppedDatabaseInfo(TransactionContext context, CatalogIdent catalogIdent,
        String catalogName, String branchVersion, List<DroppedDatabaseNameObject> droppedObjectNameList,
        Boolean dropped) {
        List<DatabaseObject> databaseInfoList = new ArrayList<>();
        for (DroppedDatabaseNameObject droppedTableObjectName : droppedObjectNameList) {
            DatabaseIdent databaseIdent = StoreConvertor.databaseIdent(catalogIdent,
                droppedTableObjectName.getObjectId());
            DatabaseObject databaseInfo = getDatabaseInfo(context, databaseIdent, catalogName, branchVersion, dropped);
            if (databaseInfo != null) {
                databaseInfoList.add(databaseInfo);
            }
        }
        return databaseInfoList;
    }

    private TraverseCursorResult<List<DatabaseObject>> listDroppedDatabaseWithToken(CatalogIdent catalogIdent,
        String subBranchVersion, int maxRowLimitNum, Boolean dropped, CatalogToken catalogToken)
        throws CatalogServerException {
        List<DatabaseObject> databaseInfos = new ArrayList<>();
        int remainNum = maxRowLimitNum;
        String method = Thread.currentThread().getStackTrace()[1].getMethodName();
        byte[] continuation = (catalogToken.getContextMapValue(method) == null) ? null
            : CodecUtil.hex2Bytes(catalogToken.getContextMapValue(method));

        CatalogObject catalogRecord = CatalogObjectHelper.getCatalogObject(catalogIdent);
        if (catalogRecord == null) {
            throw new CatalogServerException(ErrorCode.CATALOG_NOT_FOUND);
        }

        ScanRecordCursorResult<List<DatabaseObject>> databaseInfoList = new ScanRecordCursorResult<>(
            Collections.emptyList(), null);
        while (true) {
            int batchNum = Math.min(remainNum, maxBatchRowNum);
            byte[] continuationBatch = continuation;
            TransactionFrameRunner runner = new TransactionFrameRunner();
            runner.setMaxAttempts(DATABASE_STORE_MAX_RETRY_NUM);
            databaseInfoList = runner.run(context -> {
                ScanRecordCursorResult<List<DroppedDatabaseNameObject>> droppedObjectNameList = databaseStore
                    .listDroppedDatabaseObjectName(
                        context, catalogIdent, batchNum, continuationBatch, TransactionIsolationLevel.SNAPSHOT);
                List<DatabaseObject> databaseList = getBranchDroppedDatabaseInfo(context, catalogIdent,
                    catalogRecord.getName(), subBranchVersion, droppedObjectNameList.getResult(), dropped);

                return new ScanRecordCursorResult<>(databaseList,
                    droppedObjectNameList.getContinuation().orElse(null));
            }).getResult();


            databaseInfos.addAll(databaseInfoList.getResult());

            remainNum = remainNum - databaseInfoList.getResult().size();
            continuation = databaseInfoList.getContinuation().orElse(null);
            if ((continuation == null) || (remainNum == 0)) {
                break;
            }
        }

        if (continuation == null) {
            return new TraverseCursorResult<>(databaseInfos, null);
        }

        CatalogToken catalogTokenNew = CatalogToken.buildCatalogToken(databaseStoreCheckSum, method,
            CodecUtil.bytes2Hex(continuation), catalogToken.getReadVersion());
        return new TraverseCursorResult<>(databaseInfos, catalogTokenNew);
    }

    private Boolean checkDatabaseValidInBranch(DatabaseIdent databaseIdent, String baseVersion) {
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(DATABASE_STORE_MAX_RETRY_NUM);
        return runner.run(context -> {
            Optional<DatabaseHistoryObject> optional = databaseStore
                .getLatestDatabaseHistory(context, databaseIdent, baseVersion);
            if (optional.isPresent()) {
                return true;
            }
            return false;
        }).getResult();
    }

    private Boolean checkDatabaseDistinctInSubbranch(TransactionContext context, CatalogIdent subCatalogIdent, String subReadVersion,
                                                     DatabaseObject databaseInfo) {
        List<Map<CatalogIdent, String>> subBranchCatalogList = getParentCatalogIdents(context, subCatalogIdent);
        Map<CatalogIdent, String> subBranchMap = new HashMap<>();
        subBranchMap.put(subCatalogIdent, subReadVersion);
        subBranchCatalogList.add(0, subBranchMap);

        for (Map<CatalogIdent, String> map : subBranchCatalogList) {
            Iterator<Map.Entry<CatalogIdent, String>> iterator = map.entrySet().iterator();
            Map.Entry<CatalogIdent, String> entry = iterator.next();
            CatalogIdent subBranchCatalogIdent = entry.getKey();
            String subBranchVersion = entry.getValue();

            if (databaseInfo.getCatalogId().equals(subBranchCatalogIdent.getCatalogId())) {
                return false;
            }

            DatabaseIdent databaseIdent = StoreConvertor
                .databaseIdent(subBranchCatalogIdent,
                    databaseInfo.getDatabaseId());
            if (checkDatabaseValidInBranch(databaseIdent, subBranchVersion)) {
                return true;
            }
        }

        return false;
    }

    private TraverseCursorResult<List<DatabaseObject>> listParentBranchDatabaseDistinct(TransactionContext context, CatalogIdent parentCatalogIdent,
                                                                                        String parentBranchVersion, CatalogIdent originCatalogIdent, CatalogName originCatalogName,
                                                                                        int maxRowLimitNum, Boolean dropped, CatalogToken catalogToken) throws CatalogServerException {
        int remainResults = maxRowLimitNum;
        CatalogToken nextToken = catalogToken;
        String method = Thread.currentThread().getStackTrace()[1].getMethodName();

        String readVersion = catalogToken.getReadVersion();

        ListDatabaseInBranchWithToken listTablesValid = (catalogIdent, branchVersion, maxResult, token, isDrop)
            -> listValidDatabaseWithToken(context, catalogIdent, branchVersion, maxResult, isDrop, token);

        ListDatabaseInBranchWithToken listTablesDropped = (catalogIdent, branchVersion, maxResult, token, isDrop)
            -> listDroppedDatabaseWithToken(catalogIdent, branchVersion, maxResult, isDrop, token);

        Map<Integer, ListDatabaseInBranchWithToken> listStepMap = new HashMap<>();
        listStepMap.put(0, listTablesValid);
        listStepMap.put(1, listTablesDropped);

        Integer step = (catalogToken.getContextMapValue(method) == null) ? 0
            : Integer.valueOf(catalogToken.getContextMapValue(method));

        List<DatabaseObject> parentBranchValidDatabaseList = new ArrayList<>();
        TraverseCursorResult<List<DatabaseObject>> parentBranchDatabases = null;
        while (step < listStepMap.size()) {
            while (true) {
                parentBranchDatabases = listStepMap.get(step).list(parentCatalogIdent,
                    parentBranchVersion, remainResults, nextToken, dropped);

                //No data is found, Exit.
                if (parentBranchDatabases.getResult().size() == 0) {
                    break;
                }

                nextToken = parentBranchDatabases.getContinuation().orElse(null);

                //check whether the table duplicate appears in the subbranch traversal
                List<DatabaseObject> parentDatabaseList = parentBranchDatabases.getResult().stream()
                    .filter(databaseInfo -> !checkDatabaseDistinctInSubbranch(context, originCatalogIdent, readVersion,
                        databaseInfo)).map(databaseInfo -> {
                        databaseInfo.setCatalogId(originCatalogIdent.getCatalogId());
                        return databaseInfo;
                    }).collect(Collectors.toList());

                parentBranchValidDatabaseList.addAll(parentDatabaseList);
                remainResults = remainResults - parentDatabaseList.size();
                //If the token is null, there is no valid data in the next traversal.
                if ((remainResults == 0) || (nextToken == null)) {
                    break;
                }
            }

            if (remainResults == 0) {
                break;
            }
            nextToken = new CatalogToken(databaseStoreCheckSum, catalogToken.getReadVersion());
            step++;
        }

        String stepValue = String.valueOf(step);
        CatalogToken catalogTokenNew = parentBranchDatabases.getContinuation().map(token -> {
            token.putContextMap(method, stepValue);
            return token;
        }).orElse(null);

        return new TraverseCursorResult<>(parentBranchValidDatabaseList, catalogTokenNew);
    }

    private TraverseCursorResult<List<DatabaseObject>> listBranchDatabaseWithToken(TransactionContext context, CatalogIdent catalogIdent,
                                                                                   CatalogName catalogName, int maxResultNum, CatalogToken catalogToken, Boolean dropped) {
        int remainResults = maxResultNum;
        List<DatabaseObject> listSum = new ArrayList<>();
        List<Map<CatalogIdent, String>> parentCatalogList = getParentCatalogIdents(context, catalogIdent);
        Map<CatalogIdent, String> subbranchMap = new HashMap<>();
        String version = catalogToken.getReadVersion();
        subbranchMap.put(catalogIdent, version);
        parentCatalogList.add(0, subbranchMap);

        String readVersion = catalogToken.getReadVersion();
        CatalogToken nextToken = catalogToken;
        String method = Thread.currentThread().getStackTrace()[1].getMethodName();
        //find the parent branch breakpoint based on the token information.
        if (catalogToken.getContextMapValue(method) != null) {
            String nextParentCatalogId = catalogToken.getContextMapValue(method);
            parentCatalogList = cutParentBranchListWithToken(parentCatalogList, nextParentCatalogId);
        }

        TraverseCursorResult<List<DatabaseObject>> parentBranchValidDatabases = null;
        CatalogIdent parentCatalogIdent = null;
        for (Map<CatalogIdent, String> map : parentCatalogList) {
            Iterator<Map.Entry<CatalogIdent, String>> iterator = map.entrySet().iterator();
            Map.Entry<CatalogIdent, String> entry = iterator.next();
            String subBranchVersion = entry.getValue();
            parentCatalogIdent = entry.getKey();

            parentBranchValidDatabases = listParentBranchDatabaseDistinct(
                    context, parentCatalogIdent, subBranchVersion, catalogIdent, catalogName, remainResults, dropped, nextToken);

            nextToken = new CatalogToken(databaseStoreCheckSum, readVersion);
            listSum.addAll(parentBranchValidDatabases.getResult());
            remainResults = remainResults - parentBranchValidDatabases.getResult().size();
            if (remainResults == 0) {
                break;
            }
        }

        String stepValue = parentCatalogIdent.getCatalogId();
        CatalogToken catalogTokenNew = parentBranchValidDatabases.getContinuation().map(token -> {
            token.putContextMap(method, stepValue);
            return token;
        }).orElse(null);

        return new TraverseCursorResult<>(listSum, catalogTokenNew);
    }

    private TraverseCursorResult<List<DatabaseObject>> listDatabase(TransactionContext context, CatalogIdent catalogIdent, CatalogName catalogName,
                                                                    boolean includeDrop, Integer maxResult, String pageToken) {
        String method = Thread.currentThread().getStackTrace()[1].getMethodName();
        Optional<CatalogToken> catalogToken = CatalogToken.parseToken(pageToken, databaseStoreCheckSum);
        if (!catalogToken.isPresent()) {
            String latestVersion = VersionManagerHelper
                    .getLatestVersion(context, catalogIdent.getProjectId(), catalogIdent.getRootCatalogId());
            catalogToken = Optional.of(CatalogToken
                            .buildCatalogToken(databaseStoreCheckSum, method, String.valueOf(false), latestVersion));
        }

        int remainResults = maxResult;
        List<DatabaseObject> listSum = new ArrayList<>();
        TraverseCursorResult<List<DatabaseObject>> stepResult = null;
        CatalogToken nextCatalogToken = catalogToken.get();
        boolean dropped = (nextCatalogToken.getContextMapValue(method) == null)
            ? false : Boolean.valueOf(nextCatalogToken.getContextMapValue(method));
        String readVersion = nextCatalogToken.getReadVersion();
        while (true) {
            stepResult = listBranchDatabaseWithToken(context, catalogIdent, catalogName, remainResults,
                nextCatalogToken, dropped);

            nextCatalogToken = new CatalogToken(databaseStoreCheckSum, readVersion);
            listSum.addAll(stepResult.getResult());
            remainResults = remainResults - stepResult.getResult().size();
            if (remainResults == 0) {
                break;
            }

            if ((dropped == includeDrop) && (!stepResult.getContinuation().isPresent())) {
                break;
            }

            dropped = true;
        }

        String stepValue = String.valueOf(dropped);
        CatalogToken catalogTokenNew = stepResult.getContinuation().map(token -> {
            token.putContextMap(method, stepValue);
            return token;
        }).orElse(null);

        return new TraverseCursorResult(listSum, catalogTokenNew);
    }

    /**
     * list DBs
     *
     * @param catalogName catalogIdentifier
     * @param includeDrop     "true" to see DBs including the deleted ones
     * @param maximumToScan   number of DBs limits in one response
     * @param pageToken       pageToken is the next consecutive key to begin with in the LIST-Request
     * @param filter          filter expression
     * @return
     */
    @Override
    public TraverseCursorResult<List<Database>> listDatabases(CatalogName catalogName,
        boolean includeDrop, Integer maximumToScan, String pageToken, String filter) {
        // check and convert parameters
        //int scanNum = checkNumericFormat(maximumToScan);
        Tuple pattern = Tuple.from(includeDrop, maximumToScan, pageToken, filter);

        StoreValidator.requireCatalogNameNotNull(catalogName);

        TraverseCursorResult<List<DatabaseObject>> databaseInfos = TransactionRunnerUtil.transactionRunThrow(
                context -> {
            CatalogObject catalogRecord = catalogStore.getCatalogByName(context, catalogName);
            if (catalogRecord == null) {
                throw new CatalogServerException(ErrorCode.CATALOG_NOT_FOUND, catalogName.getCatalogName());
            }

            CatalogIdent catalogIdent = StoreConvertor.catalogIdent(catalogName.getProjectId(), catalogRecord.getCatalogId(),
                    catalogRecord.getRootCatalogId());
            //pattern:includeDrop, scanNum, pageToken
            Boolean includeDropped = Boolean.valueOf(pattern.getItems().get(0).toString());
            int maxResultNum = Integer.valueOf(pattern.getItems().get(1).toString()).intValue();
            String nextToken = null;
            if (pageToken != null) {
                nextToken = pattern.getItems().get(2).toString();
            }
            return listDatabase(context, catalogIdent, catalogName, includeDropped,
                    maxResultNum, nextToken);
        }).getResult();
        return new TraverseCursorResult(convertToDatabaseModel(catalogName, databaseInfos.getResult()),
            databaseInfos.getContinuation().orElse(null));
    }

    @Override
    public TraverseCursorResult<List<String>> getDatabaseNames(CatalogName catalogFullName, boolean includeDrop,
        Integer maximumToScan, String pageToken, String filter) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getDatabaseNames");
    }

    private int checkNumericFormat(Integer maxToScan) {
        if (maxToScan < 0 || maxToScan > 1000) {
            throw new CatalogServerException(ErrorCode.DATABASE_REQ_PARAM_ERROR, maxToScan);
        }
        return maxToScan;
    }

    private List<Database> convertToDatabaseModel(CatalogName catalogName, List<DatabaseObject> dbInfos) {
        List<Database> dbs = new ArrayList<>();
        for (DatabaseObject dbInfo : dbInfos) {
            Database model = new Database();
            model.setCatalogName(catalogName.getCatalogName());
            model.setDatabaseName(dbInfo.getName());
            model.setDroppedTime(dbInfo.getDroppedTime());
            model.setCreateTime(dbInfo.getCreateTime());
            dbs.add(model);
        }
        return dbs;
    }

    /**
     * get the detail info of a DB corresponding to the database Name
     *
     * @param databaseName DatabaseName
     * @return Database
     */
    @Override
    public Database getDatabaseByName(DatabaseName databaseName) {
        CheckUtil.checkStringParameter(databaseName.getProjectId(), databaseName.getCatalogName(),
            databaseName.getDatabaseName());
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(DATABASE_STORE_MAX_RETRY_NUM);
        return runner.run(context -> {
            DatabaseObject databaseRecord = DatabaseObjectHelper.getDatabaseObject(context, databaseName);
            if (databaseRecord == null) {
                return null;
            }
            return DatabaseObjectConvertHelper.toDatabase(databaseName.getCatalogName(), databaseRecord);
        }).getResult();
    }

    @Override
    public DatabaseHistory getDatabaseByVersion(DatabaseName databaseName, String version) {
        try {
            StoreValidator.requireDBNameNotNull(databaseName);
        } catch (IllegalArgumentException e) {
            logger.error(e.getMessage(), e);
            return null;
        }

        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(DATABASE_STORE_MAX_RETRY_NUM);
        return runner.run(context -> {
            DatabaseIdent databaseIdent = getDatabaseIdent(databaseName);
            Optional<DatabaseHistoryObject> optional = DatabaseHistoryHelper
                .getDatabaseHistory(context, databaseIdent, version);
            if (optional.isPresent()) {
                CatalogIdent catalogIdent = StoreConvertor
                    .catalogIdent(databaseIdent.getProjectId(), databaseIdent.getCatalogId(),
                        databaseIdent.getRootCatalogId());
                CatalogObject catalogRecord = catalogStore.getCatalogById(context, catalogIdent);
                if (catalogRecord == null) {
                    throw new CatalogServerException(ErrorCode.CATALOG_ID_NOT_FOUND, catalogRecord.getCatalogId());
                }
                return optional.map(this::convertToDatabaseHistoryModel).get();
            }

            return null;
        }).getResult();
    }

    private List<DatabaseHistoryObject> listCurrentBranchDatabaseHistory(TransactionContext context,
        DatabaseIdent branchDatabaseIdent, String baseVersion) {
        return databaseStore
            .listDatabaseHistory(context, branchDatabaseIdent, Integer.MAX_VALUE, null, baseVersion).getResult();
    }

    private List<DatabaseHistoryObject> listSubBranchFakeDatabaseHistory(TransactionContext context,
        DatabaseIdent subBranchDatabaseIdent, String baseVersion) {
        List<DatabaseHistoryObject> databaseHistoryList = new ArrayList<>();

        ParentBranchCatalogIterator parentCatalogIdentIterator = new ParentBranchCatalogIterator(context,
            subBranchDatabaseIdent);

        while (parentCatalogIdentIterator.hasNext()) {
            CatalogIdent parentCatalogIdent = parentCatalogIdentIterator.nextCatalogIdent();
            String subBranchVersion = parentCatalogIdentIterator.nextBranchVersion(parentCatalogIdent);

            DatabaseIdent parentDatabaseIdent = StoreConvertor.databaseIdent(parentCatalogIdent,
                subBranchDatabaseIdent.getDatabaseId());

            String baseVersionStamp =
                baseVersion.compareTo(subBranchVersion) < 0 ? baseVersion : subBranchVersion;
            List<DatabaseHistoryObject> parentDatabaseHistoryList = listCurrentBranchDatabaseHistory(context,
                parentDatabaseIdent, baseVersionStamp);

            for (DatabaseHistoryObject databaseHistory : parentDatabaseHistoryList) {
                databaseHistoryList.add(databaseHistory);
            }

        }

        return databaseHistoryList;
    }


    @Override
    public List<DatabaseHistory> getDatabaseHistory(DatabaseName databaseName) {
        try {
            StoreValidator.requireDBNameNotNull(databaseName);
        } catch (IllegalArgumentException e) {
            logger.error(e.getMessage(), e);
            return null;
        }

        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(DATABASE_STORE_MAX_RETRY_NUM);
        return runner.run(context -> {
            DatabaseIdent databaseIdent = DatabaseObjectHelper.getDatabaseIdent(context, databaseName);
            String latestVersion = VersionManagerHelper
                .getLatestVersion(context, databaseIdent.getProjectId(), databaseIdent.getRootCatalogId());
            List<DatabaseHistoryObject> databaseHistoryList = listCurrentBranchDatabaseHistory(context, databaseIdent,
                latestVersion);
            List<DatabaseHistoryObject> subBranchFakeDatabaseHistoryList = listSubBranchFakeDatabaseHistory(context,
                databaseIdent, latestVersion);
            CatalogIdent catalogIdent = StoreConvertor
                .catalogIdent(databaseIdent.getProjectId(), databaseIdent.getCatalogId(), databaseIdent.getRootCatalogId());
            CatalogObject catalogRecord = catalogStore.getCatalogById(context, catalogIdent);
            if (catalogRecord == null) {
                throw new CatalogServerException(ErrorCode.CATALOG_ID_NOT_FOUND, catalogRecord.getCatalogId());
            }

            if (databaseHistoryList.isEmpty()) {
                return subBranchFakeDatabaseHistoryList.stream().map(this::convertToDatabaseHistoryModel)
                    .collect(Collectors.toList());
            }

            databaseHistoryList.addAll(subBranchFakeDatabaseHistoryList);
            return databaseHistoryList.stream().map(this::convertToDatabaseHistoryModel).collect(Collectors.toList());
        }).getResult();
    }

    private DatabaseHistory convertToDatabaseHistoryModel(DatabaseHistoryObject dbHistoryObj) {
        return new DatabaseHistory(dbHistoryObj.getEventId(),
            dbHistoryObj.getName(),
            dbHistoryObj.getCreateTime(),
            dbHistoryObj.getProperties(),
            dbHistoryObj.getLocation(),
            dbHistoryObj.getDescription(),
            dbHistoryObj.getUserId(),
            dbHistoryObj.getVersion(),
            dbHistoryObj.getDroppedTime());
    }

    private List<Map<CatalogIdent, String>> getParentCatalogIdents(TransactionContext context, CatalogIdent catalogIdent) {
        List<Map<CatalogIdent, String>> list = new ArrayList<>();
        ParentBranchCatalogIterator parentCatalogIdentIterator = new ParentBranchCatalogIterator(context,
                catalogIdent);
        while (parentCatalogIdentIterator.hasNext()) {
            Map<CatalogIdent, String> map = parentCatalogIdentIterator.next();
            list.add(map);
        }
        return list;
    }

    private List<Map<CatalogIdent, String>> cutParentBranchListWithToken(
        List<Map<CatalogIdent, String>> parentCatalogList, String nextCatalogId) {
        Iterator<Map<CatalogIdent, String>> mapIterator = parentCatalogList.iterator();
        while (mapIterator.hasNext()) {
            Map<CatalogIdent, String> listEntryMap = mapIterator.next();
            Iterator<CatalogIdent> catalogIdentIterator = listEntryMap.keySet().iterator();
            CatalogIdent parentCatalogIdent = catalogIdentIterator.next();
            if (nextCatalogId.equals(parentCatalogIdent.getCatalogId())) {
                break;
            }
            mapIterator.remove();
        }

        return parentCatalogList;
    }

    private TraverseCursorResult<List<DatabaseHistoryObject>> listDatabaseHistory(DatabaseIdent databaseIdent,
        int maxResultNum, String basedVersion, CatalogToken catalogToken) {
        List<DatabaseHistoryObject> databaseHistoryList = new ArrayList<>();
        int remainNum = maxResultNum;

        byte[] continuation = null;
        String method = Thread.currentThread().getStackTrace()[1].getMethodName();
        if (catalogToken.getContextMapValue(method) != null) {
            continuation = CodecUtil.hex2Bytes(catalogToken.getContextMapValue(method));
        }

        ScanRecordCursorResult<List<DatabaseHistoryObject>> batchDatabaseHistoryList;
        while (true) {
            int batchNum = Math.min(remainNum, maxBatchRowNum);
            byte[] continuationBatch = continuation;
            TransactionFrameRunner runner = new TransactionFrameRunner();
            runner.setMaxAttempts(DATABASE_STORE_MAX_RETRY_NUM);
            batchDatabaseHistoryList = runner.run(context -> databaseStore
                .listDatabaseHistory(context, databaseIdent, batchNum, continuationBatch, basedVersion)
            ).getResult();

            databaseHistoryList.addAll(batchDatabaseHistoryList.getResult());
            remainNum = remainNum - batchDatabaseHistoryList.getResult().size();
            continuation = batchDatabaseHistoryList.getContinuation().orElse(null);
            if ((continuation == null) || (remainNum == 0)) {
                break;
            }
        }

        if (continuation == null) {
            return new TraverseCursorResult<>(databaseHistoryList, null);
        }

        CatalogToken catalogTokenNew = CatalogToken.buildCatalogToken(databaseStoreCheckSum, method,
            CodecUtil.bytes2Hex(continuation), catalogToken.getReadVersion());

        return new TraverseCursorResult(databaseHistoryList, catalogTokenNew);
    }

    private TraverseCursorResult<List<DatabaseHistoryObject>> listBranchDatabaseHistoryWithToken(TransactionContext context, DatabaseIdent databaseIdent,
                                                                                                 int maxResultNum, CatalogToken catalogToken) {
        int remainResultNum = maxResultNum;

        CatalogIdent catalogIdent = StoreConvertor.catalogIdent(databaseIdent.getProjectId(),
            databaseIdent.getCatalogId(), databaseIdent.getRootCatalogId());
        List<Map<CatalogIdent, String>> parentCatalogList = getParentCatalogIdents(context, catalogIdent);
        Map<CatalogIdent, String> subBranchMap = new HashMap<>();
        String readVersion = catalogToken.getReadVersion();
        subBranchMap.put(catalogIdent, readVersion);
        parentCatalogList.add(0, subBranchMap);

        CatalogToken nextCatalogToken = catalogToken;
        //find the parent branch breakpoint based on the token information.
        String method = Thread.currentThread().getStackTrace()[1].getMethodName();
        if (catalogToken.getContextMapValue(method) != null) {
            String nextParentCatalogId = catalogToken.getContextMapValue(method);
            parentCatalogList = cutParentBranchListWithToken(parentCatalogList, nextParentCatalogId);
        }

        List<DatabaseHistoryObject> listSum = new ArrayList<>();
        TraverseCursorResult<List<DatabaseHistoryObject>> parentBranchDatabaseHistories = null;
        CatalogIdent parentCatalogIdent = null;
        for (Map<CatalogIdent, String> map : parentCatalogList) {
            Iterator<Map.Entry<CatalogIdent, String>> iterator = map.entrySet().iterator();
            Map.Entry<CatalogIdent, String> mapEntry = iterator.next();
            parentCatalogIdent = mapEntry.getKey();
            String subBranchVersion = mapEntry.getValue();

            DatabaseIdent parentDatabaseIdent = StoreConvertor.databaseIdent(parentCatalogIdent,
                databaseIdent.getDatabaseId());

            String baseVersion = (readVersion.compareTo(subBranchVersion) < 0) ? readVersion : subBranchVersion;
            parentBranchDatabaseHistories = listDatabaseHistory(parentDatabaseIdent, remainResultNum, baseVersion,
                nextCatalogToken);

            nextCatalogToken = new CatalogToken(databaseStoreCheckSum, catalogToken.getReadVersion());

            listSum.addAll( parentBranchDatabaseHistories.getResult());
            remainResultNum = remainResultNum -  parentBranchDatabaseHistories.getResult().size();
            if (remainResultNum == 0) {
                break;
            }
        }

        String stepValue = parentCatalogIdent.getCatalogId();
        CatalogToken catalogTokenNew = parentBranchDatabaseHistories.getContinuation().map(token -> {
            token.putContextMap(method, stepValue);
            return token;
        }).orElse(null);

        return new TraverseCursorResult(listSum, catalogTokenNew);
    }

    private TraverseCursorResult<List<DatabaseHistoryObject>> listDatabaseHistoryWithToken(
        DatabaseIdent databaseIdent, DatabaseName databaseName,
        int maxResults, String pageToken) {
        return TransactionRunnerUtil.transactionRunThrow(context -> {
            Optional<CatalogToken> catalogToken = CatalogToken.parseToken(pageToken, databaseStoreCheckSum);
            if (!catalogToken.isPresent()) {
                DatabaseObject databaseObject = DatabaseObjectHelper.getDatabaseObject(context, databaseIdent);
                if (databaseObject == null) {
                    throw new CatalogServerException(ErrorCode.DATABASE_NOT_FOUND, databaseName.getDatabaseName());
                }

                String latestVersion = VersionManagerHelper
                        .getLatestVersion(context, databaseIdent.getProjectId(), databaseIdent.getRootCatalogId());
                catalogToken = Optional.of(new CatalogToken(databaseStoreCheckSum, latestVersion));
            }
            return listBranchDatabaseHistoryWithToken(context,
                    databaseIdent, maxResults, catalogToken.get());
        }).getResult();
    }


    @Override
    public TraverseCursorResult<List<DatabaseHistory>> listDatabaseHistory(DatabaseName databaseName,
                                                                           int maxResults, String pageToken) {
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(DATABASE_STORE_MAX_RETRY_NUM);
        DatabaseIdent databaseIdent = runner.run(context -> {
            DatabaseIdent id = DatabaseObjectHelper.getDatabaseIdent(context, databaseName);
            if (id == null) {
                throw new CatalogServerException(ErrorCode.DATABASE_NOT_FOUND, databaseName.getDatabaseName());
            }

            return id;
        }).getResult();

        TraverseCursorResult<List<DatabaseHistoryObject>> databaseHistoryCursorResult = listDatabaseHistoryWithToken(
            databaseIdent, databaseName, maxResults, pageToken);

        List<DatabaseHistory> databaseHistoryRecordList = databaseHistoryCursorResult.getResult().stream()
            .map(this::convertToDatabaseHistoryModel).collect(Collectors.toList());

        return new TraverseCursorResult(databaseHistoryRecordList,
            databaseHistoryCursorResult.getContinuation().orElse(null));
    }

    private DatabaseHistoryObject getLatestDatabaseHistory(TransactionContext ctx, DatabaseIdent databaseIdent,
        String latestVersion, Boolean dropped) {
        Optional<DatabaseHistoryObject> databaseHistory;
        databaseHistory = DatabaseHistoryHelper.getLatestDatabaseHistory(ctx, databaseIdent, latestVersion);
        if (databaseHistory.isPresent()) {
            if (dropped.equals(DatabaseHistoryHelper.isDropped(databaseHistory.get()))) {
                return databaseHistory.get();
            }
        }

        throw new CatalogServerException(ErrorCode.DATABASE_HISTORY_NOT_FOUND, databaseIdent.getDatabaseId());
    }

    @Override
    public TraverseCursorResult<TableBrief[]> getTableFuzzy(CatalogName catalogName, String databasePattern,
        String tablePattern, List<String> tableTypes) {
        return null;
    }
}
