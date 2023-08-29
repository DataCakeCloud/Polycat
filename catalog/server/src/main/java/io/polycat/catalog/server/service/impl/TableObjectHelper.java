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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.CatalogIdent;
import io.polycat.catalog.common.model.CatalogName;
import io.polycat.catalog.common.model.CatalogObject;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.DatabaseObject;
import io.polycat.catalog.common.model.DroppedTableObject;
import io.polycat.catalog.common.model.ScanRecordCursorResult;
import io.polycat.catalog.common.model.TableBaseHistoryObject;
import io.polycat.catalog.common.model.TableCommitObject;
import io.polycat.catalog.common.model.TableIdent;
import io.polycat.catalog.common.model.TableName;
import io.polycat.catalog.common.model.TableNameObject;
import io.polycat.catalog.common.model.TableObject;
import io.polycat.catalog.common.model.TableBaseObject;
import io.polycat.catalog.common.model.TableReferenceObject;
import io.polycat.catalog.common.model.TableSchemaHistoryObject;
import io.polycat.catalog.common.model.TableSchemaObject;
import io.polycat.catalog.common.model.TableStorageHistoryObject;
import io.polycat.catalog.common.model.TableStorageObject;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.TransactionIsolationLevel;
import io.polycat.catalog.server.util.TransactionRunner;
import io.polycat.catalog.server.util.TransactionRunnerUtil;
import io.polycat.catalog.store.api.TableMetaStore;
import io.polycat.catalog.store.common.StoreConvertor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class TableObjectHelper {
    private static TableMetaStore tableMetaStore;

    @Autowired
    public void setTableMetaStore(TableMetaStore tableMetaStore) {
        this.tableMetaStore = tableMetaStore;
    }

    public static TableIdent getTableIdent(TransactionContext context, TableName tableName) throws CatalogServerException {
        //get catalogId
        CatalogName catalogName = StoreConvertor.catalogName(tableName.getProjectId(), tableName.getCatalogName());
        CatalogIdent catalogIdent = CatalogObjectHelper.getCatalogIdent(context, catalogName);
        if (catalogIdent == null) {
            throw new CatalogServerException(ErrorCode.TABLE_NOT_FOUND, String
                .format("%s.%s.%s", tableName.getCatalogName(), tableName.getDatabaseName(), tableName.getTableName()));
        }

        //get databaseId
        DatabaseName branchDatabaseName = StoreConvertor.databaseName(catalogName, tableName.getDatabaseName());
        DatabaseIdent branchDatabaseIdent = DatabaseObjectHelper.getDatabaseIdent(context, catalogIdent, branchDatabaseName);
        if (branchDatabaseIdent == null) {
            throw new CatalogServerException(ErrorCode.TABLE_NOT_FOUND, String.format("%s.%s.%s",
                tableName.getCatalogName(), tableName.getDatabaseName(), tableName.getTableName()));
        }

        //get tableId
        String tableId = getTableId(context, branchDatabaseIdent, tableName.getTableName());
        if (tableId == null) {
            throw new CatalogServerException(ErrorCode.TABLE_NOT_FOUND, tableName.getTableName());
        }

        return StoreConvertor
            .tableIdent(tableName.getProjectId(), catalogIdent.getCatalogId(), branchDatabaseIdent.getDatabaseId(),
                tableId, catalogIdent.getRootCatalogId());
    }

    public static TableIdent getDroppedTableIdent(TableName tableName) {
        try (TransactionRunner runner = new TransactionRunner()) {
            return runner.run(context -> {
               return getOnlyDroppedTableIdentOrElseThrow(context, tableName);
            });
        }
    }

    public static TableIdent getOnlyDroppedTableIdentOrElseThrow(TransactionContext context, TableName tableName)
        throws MetaStoreException {
        DatabaseIdent databaseIdent = DatabaseObjectHelper.getDatabaseIdent(context, StoreConvertor.databaseName(tableName));

        //get tableId, The tableName must be unique
        String tableId = getBranchDroppedTableId(context, databaseIdent, tableName.getTableName());

        return StoreConvertor
            .tableIdent(databaseIdent, tableId);
    }

    public static TableIdent getDroppedTableIdent(TransactionContext context, TableName tableName, String tableId)
        throws MetaStoreException {
        DatabaseIdent databaseIdent = DatabaseObjectHelper.getDatabaseIdent(context,  StoreConvertor.databaseName(tableName));

        if (checkDroppedTable(context, databaseIdent, tableName.getTableName(), tableId)) {
            return StoreConvertor.tableIdent(databaseIdent, tableId);
        }

        if (checkParentBranchDroppedTable(context, databaseIdent, tableName.getTableName(), tableId)) {
            return StoreConvertor.tableIdent(databaseIdent, tableId);
        }

        throw new MetaStoreException(ErrorCode.TABLE_NOT_FOUND, tableName.getTableName());
    }


    public static String getTableId(TransactionContext context, DatabaseIdent branchDatabaseIdent, String tableName)
        throws MetaStoreException {
        String tableId = tableMetaStore.getTableId(context, branchDatabaseIdent, tableName);
        if (tableId != null) {
            return tableId;
        }

        // Check whether the branch is a sub-branch.
        // The sub-branch may inherit the table of the parent branch.
        //step 2
        return getFakeSubBranchTableId(context, branchDatabaseIdent, tableName);
    }

    public static List<TableIdent> getTablesIdent(TransactionContext context, DatabaseIdent databaseIdent,
        boolean includeDropped) {
        //Obtain tableId include dropped and in-use
        List<TableIdent> tableIdentList = new ArrayList<>();

        ScanRecordCursorResult<List<TableNameObject>> TableObjectNameList = tableMetaStore
            .listTableName(context, databaseIdent, Integer.MAX_VALUE,
                null, TransactionIsolationLevel.SERIALIZABLE);
        for (TableNameObject i : TableObjectNameList.getResult()) {
            TableIdent tableIdent = StoreConvertor.tableIdent(databaseIdent, i.getObjectId());
            tableIdentList.add(tableIdent);
        }

        if (!includeDropped) {
            return tableIdentList;
        }

        ScanRecordCursorResult<List<DroppedTableObject>> droppedTableObjectNameList = tableMetaStore
            .listDroppedTable(context, databaseIdent, Integer.MAX_VALUE,
                null, TransactionIsolationLevel.SERIALIZABLE);
        for (DroppedTableObject i : droppedTableObjectNameList.getResult()) {
            TableIdent tableIdent = StoreConvertor.tableIdent(databaseIdent, i.getObjectId());
            tableIdentList.add(tableIdent);
        }

        return tableIdentList;
    }

    public static String getTableName(TableIdent tableIdent, Boolean includeDropped) {
        try (TransactionRunner runner = new TransactionRunner()) {
            return runner.run(context -> {
               return getTableName(context, tableIdent, includeDropped);
            });
        }
    }

    public static String getTableName(TransactionContext context, TableIdent tableIdent, Boolean includeDropped) {
        try {
            String baseVersion = VersionManagerHelper.getLatestVersion(tableIdent);
            Optional<TableCommitObject> tableCommit = TableCommitHelper
                .getLatestTableCommit(context, tableIdent, baseVersion);
            if (tableCommit.isPresent()) {
                if (includeDropped) {
                    return tableCommit.get().getTableName();
                } else if (!TableCommitHelper.isDropCommit(tableCommit.get())) {
                    return tableCommit.get().getTableName();
                }
            }

            return null;
        } catch (MetaStoreException e) {
            throw new CatalogServerException(e.getMessage(), e.getErrorCode());
        }
    }

    public static TableObject getTableObject(TableName tableName) {
        return TransactionRunnerUtil.transactionReadRunThrow(context -> {
            return getTableObject(context, tableName);
        }).getResult();
    }

    private static TableObject getTableObject(TransactionContext context, TableName tableName) {
        TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);
        return getTableObject(context, tableIdent);
    }

    public static TableObject getTableObject(TableIdent tableIdent) {
        try (TransactionRunner runner = new TransactionRunner()) {
            return runner.run(context -> {
                return getTableObject(context, tableIdent);
            });
        }
    }

    public static TableObject getTableObject(TransactionContext context, TableIdent tableIdent) {
        CatalogIdent catalogIdent = StoreConvertor.catalogIdent(tableIdent.getProjectId(), tableIdent.getCatalogId(),
            tableIdent.getRootCatalogId());
        CatalogObject catalogRecord = CatalogObjectHelper.getCatalogObject(context, catalogIdent);
        if (catalogRecord == null) {
            throw new CatalogServerException(ErrorCode.CATALOG_ID_NOT_FOUND, catalogIdent);
        }

        DatabaseIdent dbIdent = StoreConvertor.databaseIdent(tableIdent);
        DatabaseObject databaseObject = DatabaseObjectHelper.getDatabaseObject(context, dbIdent);
        if (databaseObject == null) {
            throw new CatalogServerException(ErrorCode.DATABASE_ID_NOT_FOUND, dbIdent);
        }

        // get table info instance by id
        /*TableReferenceObject tableReference = TableReferenceHelper.getTableReference(context, tableIdent);
        if (tableReference == null) {
            return null;
        }*/

        // get table schema instance by id
        TableSchemaObject tableSchema = TableSchemaHelper.getTableSchema(context, tableIdent);

        // get table storage instance by id
        TableStorageObject tableStorage = TableStorageHelper.getTableStorage(context, tableIdent);

        // get table properties
        TableBaseObject tableBase = TableBaseHelper.getTableBase(context, tableIdent);

        // get latest table commit by id
        String latestVersion = VersionManagerHelper.getLatestVersion(tableIdent);
        TableCommitObject tableCommit = TableCommitHelper
            .getLatestTableCommitOrElseThrow(context, tableIdent, latestVersion);

        TableName tableName = StoreConvertor.tableName(tableIdent.getProjectId(),
            catalogRecord.getName(), databaseObject.getName(), tableCommit.getTableName());

        return new TableObject(tableIdent, tableName, 0, tableBase, tableSchema, tableStorage, 0);
    }

    private static String getFakeSubBranchTableId(TransactionContext context, DatabaseIdent subBranchDatabaseIdent,
        String subBranchTableName)
        throws MetaStoreException {

        Map<String, String> subBranchTableIds = getTablesId(context, subBranchDatabaseIdent, true);

        ParentBranchDatabaseIterator parentBranchDatabaseIterator = new ParentBranchDatabaseIterator(context,
            subBranchDatabaseIdent);

        while (parentBranchDatabaseIterator.hasNext()) {
            DatabaseIdent parentDatabaseIdent = parentBranchDatabaseIterator.nextDatabase();
            String subBranchVersion = parentBranchDatabaseIterator.nextBranchVersion(parentDatabaseIdent);

            //obj name store indicates the current information and does not indicate the historical
            //status of the branch. You need to check the history table to obtain the historical status of the branch.
            //Consider this scenario: t1 inherit parent branch
            //(1) t1 is drooped in the current state of the parent branch.t1 is valid in the history state of creating a branch.
            //(2) t1 is created after the branch is created but does not exist in the historical version of the branch.

            // step 1: get current branch table Id
            List<String> findTableIdList = new ArrayList<>();
            String findTableId = getValidTableId(context, parentDatabaseIdent, subBranchTableName);
            if (null == findTableId) {

                // A table with the same name is created and dropped many times.
                // create t1 (name1), drop t1, create t2 (name1), drop t2
                List<String> findDroppedTableIdList = getDroppedTablesId(context, parentDatabaseIdent,
                    subBranchTableName);
                findTableIdList.addAll(findDroppedTableIdList);
            } else {
                findTableIdList.add(findTableId);
            }

            for (String tableId : findTableIdList) {
                TableIdent parentBranchTableIdent = StoreConvertor.tableIdent(parentDatabaseIdent.getProjectId(),
                    parentDatabaseIdent.getCatalogId(), subBranchDatabaseIdent.getDatabaseId(), tableId,
                    parentDatabaseIdent.getRootCatalogId());

                //step 2: check table Id valid at branch version
                if (checkBranchTableIdValid(context, parentBranchTableIdent, subBranchVersion)) {

                    //step 3:
                    //If the table id found in the parent branch does not exist in the sub-branch, the name is valid.
                    //If the same table ID exists in the sub-branch, the table name at the sub-branch has been changed.
                    //the latest name of the sub-branch is considered valid.
                    if (!subBranchTableIds.containsKey(tableId)) {
                        return tableId;
                    }

                    return null;
                }
            }

            subBranchTableIds.putAll(getTablesId(context, parentDatabaseIdent, true));
        }

        return null;
    }

    public static List<TableObject> listTables(TransactionContext context, DatabaseIdent databaseIdent,
        Boolean includeDeleted) {
        CatalogIdent catalogIdent = StoreConvertor
            .catalogIdent(databaseIdent.getProjectId(), databaseIdent.getCatalogId(), databaseIdent.getRootCatalogId());
        CatalogObject catalogRecord = CatalogObjectHelper.getCatalogObject(context, catalogIdent);
        if (catalogRecord == null) {
            throw new CatalogServerException(ErrorCode.CATALOG_ID_NOT_FOUND, catalogIdent);
        }

        Map<String, TableObject> curBranchValidTableIds = listValidTables(context, catalogRecord.getName(),
            databaseIdent);
        Map<String, TableObject> curBranchDroppedTableIds = listDroppedTables(context, catalogRecord.getName(),
            databaseIdent);

        ParentBranchDatabaseIterator parentBranchDatabaseIterator = new ParentBranchDatabaseIterator(context,
            databaseIdent);

        while (parentBranchDatabaseIterator.hasNext()) {
            DatabaseIdent parentDatabaseIdent = parentBranchDatabaseIterator.nextDatabase();
            String subBranchVersion = parentBranchDatabaseIterator.nextBranchVersion(parentDatabaseIdent);

            List<TableObject> parentBranchValidTableList = listParentBranchTables(context, catalogRecord.getName(),
                parentDatabaseIdent, subBranchVersion, false);

            for (TableObject tableObject : parentBranchValidTableList) {
                if (!(curBranchValidTableIds.containsKey(tableObject.getTableId())
                    || curBranchDroppedTableIds.containsKey(tableObject.getTableId()))) {
                    TableObject subFakeTableObject = new TableObject(tableObject);
                    subFakeTableObject.setCatalogId(databaseIdent.getCatalogId());
                    curBranchValidTableIds.put(subFakeTableObject.getTableId(), subFakeTableObject);
                }
            }

            List<TableObject> parentBranchDroppedTableList = listParentBranchTables(context, catalogRecord.getName(),
                parentDatabaseIdent, subBranchVersion, true);

            for (TableObject tableRecord : parentBranchDroppedTableList) {
                if (!(curBranchValidTableIds.containsKey(tableRecord.getTableId())
                    || curBranchDroppedTableIds.containsKey(tableRecord.getTableId()))) {
                    TableObject subFakeTableRecord = new TableObject(tableRecord);
                    subFakeTableRecord.setCatalogId(databaseIdent.getCatalogId());
                    curBranchDroppedTableIds.put(subFakeTableRecord.getTableId(), subFakeTableRecord);
                }
            }
        }

        List<TableObject> tableList = new ArrayList<>(curBranchValidTableIds.values());
        if (includeDeleted) {
            List<TableObject> droppedTableList = new ArrayList<>(curBranchDroppedTableIds.values());
            tableList.addAll(droppedTableList);
        }

        return tableList;
    }

    private static String getValidTableId(TransactionContext context, DatabaseIdent databaseIdent, String tableName) {
        String tableId = tableMetaStore.getTableId(context, databaseIdent,tableName);
        if (tableId == null) {
            return null;
        }
        return tableId;
    }

    private static List<String> getDroppedTablesId(TransactionContext context, DatabaseIdent databaseIdent, String tableName) {
        List<String> droppedTableIdList = new ArrayList<>();
        List<DroppedTableObject> droppedTableObjectNameList = tableMetaStore
            .getDroppedTablesByName(context, databaseIdent, tableName);
        for (DroppedTableObject i : droppedTableObjectNameList) {
            droppedTableIdList.add(i.getObjectId());
        }

        return droppedTableIdList;
    }

    private static boolean checkBranchTableIdValid(TransactionContext context, TableIdent parentBranchTableIdent,
        String subBranchVersion) throws MetaStoreException {
        Optional<TableCommitObject> tableCommit = tableMetaStore.getLatestTableCommit(context, parentBranchTableIdent,
            subBranchVersion);
        if (tableCommit.isPresent()) {
            return !TableCommitHelper.isDropCommit(tableCommit.get());
        }

        return false;
    }

    // HashMap < taleId, CatalogId>
    private static Map<String, String> getTablesId(TransactionContext context, DatabaseIdent databaseIdent,
        boolean includeDropped) {
        //Obtain tableId include dropped and in-use
        List<TableIdent> tableIdentList = getTablesIdent(context, databaseIdent, includeDropped);
        return tableIdentList.stream().collect(Collectors.toMap(TableIdent::getTableId, TableIdent::getCatalogId, (v1, v2) -> v1));
    }

    private static String getBranchDroppedTableId(TransactionContext context, DatabaseIdent branchDatabaseIdent,
        String tableName)
        throws MetaStoreException {
        List<String> droppedTableIdList = getDroppedTablesId(context, branchDatabaseIdent, tableName);
        List<String> fakeDroppedTableIdList = getFakeSubBranchDroppedTablesId(context, branchDatabaseIdent, tableName);

        droppedTableIdList.addAll(fakeDroppedTableIdList);
        if (droppedTableIdList.isEmpty()) {
            throw new MetaStoreException(ErrorCode.TABLE_NOT_FOUND, tableName);
        } else if (droppedTableIdList.size() != 1) {
            throw new MetaStoreException(ErrorCode.TABLE_MULTIPLE_EXISTS);
        }

        return droppedTableIdList.get(0);

    }

    private static List<String> getFakeSubBranchDroppedTablesId(TransactionContext context, DatabaseIdent subBranchDatabaseIdent,
        String subBranchTableName)
        throws MetaStoreException {

        List<String> droppedTableIdList = new ArrayList<>();

        Map<String, String> subBranchTableIds = getTablesId(context, subBranchDatabaseIdent, true);

        ParentBranchDatabaseIterator parentBranchDatabaseIterator = new ParentBranchDatabaseIterator(context,
            subBranchDatabaseIdent);

        while (parentBranchDatabaseIterator.hasNext()) {
            DatabaseIdent parentDatabaseIdent = parentBranchDatabaseIterator.nextDatabase();
            String subBranchVersion = parentBranchDatabaseIterator.nextBranchVersion(parentDatabaseIdent);

            //obj name store indicates the current information and does not indicate the historical
            //status of the branch. You need to check the history table to obtain the historical status of the branch.
            //Consider this scenario: t1 inherit parent branch
            //(1) t1 is drooped in the current state of the parent branch.t1 is valid in the history state of creating a branch.
            //(2) t1 is created after the branch is created but does not exist in the historical version of the branch.

            // step 1: get current branch table Id
            // A table with the same name is created and dropped many times.
            // create t1 (name1), drop t1, create t2 (name1), drop t2
            List<String> findDroppedTableIdList = getDroppedTablesId(context, parentDatabaseIdent, subBranchTableName);

            for (String tableId : findDroppedTableIdList) {
                TableIdent parentBranchTableIdent = StoreConvertor.tableIdent(parentDatabaseIdent, tableId);

                //step 2: check table Id dropped at branch version
                if (TableCommitHelper.checkTableDropped(context, parentBranchTableIdent, subBranchVersion)) {
                    //step 3:
                    //If the table id found in the parent branch does not exist in the sub-branch, the name is valid.
                    //If the same table ID exists in the sub-branch, the table name at the sub-branch has been changed.
                    //the latest name of the sub-branch is considered valid.
                    if (!subBranchTableIds.containsKey(tableId)) {
                        droppedTableIdList.add(tableId);
                    }
                }
            }

            subBranchTableIds.putAll(getTablesId(context, parentDatabaseIdent, true));
        }

        return droppedTableIdList;
    }

    private static boolean checkDroppedTable(TransactionContext context, DatabaseIdent databaseIdent, String tableName,
        String tableId) {
        TableIdent tableIdent = StoreConvertor.tableIdent(databaseIdent, tableId);

        DroppedTableObject droppedTableObjectName = tableMetaStore.getDroppedTable(context, tableIdent,
            tableName);

        if (droppedTableObjectName == null) {
            return false;
        }

        return ! (droppedTableObjectName.isDropPurge());
    }

    private static boolean checkParentBranchDroppedTable(TransactionContext context, DatabaseIdent subBranchDatabaseIdent,
        String tableName, String tableId) throws MetaStoreException {
        ParentBranchDatabaseIterator parentBranchDatabaseIterator = new ParentBranchDatabaseIterator(context,
            subBranchDatabaseIdent);

        while (parentBranchDatabaseIterator.hasNext()) {
            DatabaseIdent parentDatabaseIdent = parentBranchDatabaseIterator.nextDatabase();
            TableIdent tableIdent = StoreConvertor.tableIdent(parentDatabaseIdent, tableId);
            DroppedTableObject droppedTableObjectName = tableMetaStore.getDroppedTable(context,
                tableIdent, tableName);
            if (droppedTableObjectName != null) {
                return true;
            }
        }

        return false;
    }

    private static Map<String, TableObject> listDroppedTables(TransactionContext context, String catalogName, DatabaseIdent databaseIdent) {
        HashMap<String, TableObject> tables = new HashMap<String, TableObject>();
        int maxResults = Integer.MAX_VALUE;
        ScanRecordCursorResult<List<DroppedTableObject>> droppedTableObjectNameResult = tableMetaStore
            .listDroppedTable(context, databaseIdent, maxResults, null, TransactionIsolationLevel.SERIALIZABLE);

        for (DroppedTableObject droppedTableObjectName : droppedTableObjectNameResult.getResult()) {
            // get table full name by id
            DatabaseObject dbRecord = DatabaseObjectHelper.getDatabaseObject(context, databaseIdent);

            // get lastest table history by id
            TableIdent tableIdent = StoreConvertor.tableIdent(databaseIdent, droppedTableObjectName.getObjectId());
            String latestVersion = VersionManagerHelper.getLatestVersion(tableIdent);
            TableCommitObject tableCommit = TableCommitHelper
                .getLatestTableCommitOrElseThrow(context, tableIdent, latestVersion);
            TableBaseHistoryObject tableBaseHistory = TableBaseHelper
                .getLatestTableBaseOrElseThrow(context, tableIdent, latestVersion);
            TableSchemaHistoryObject tableSchemaHistory = TableSchemaHelper
                .getLatestTableSchemaOrElseThrow(context, tableIdent, latestVersion);
            TableStorageHistoryObject tableStorageHistory = TableStorageHelper
                .getLatestTableStorageOrElseThrow(context, tableIdent, latestVersion);

            TableName tableName = StoreConvertor.tableName(tableIdent.getProjectId(), catalogName,
                dbRecord.getName(),
                droppedTableObjectName.getName());
            /*TableObject table = TableObjectConvertHelper.buildTable(tableIdent, tableName,
                null, new TableStorageObject(tableStorageHistory), tableSchemaHistory.getTableSchemaObject(),
                tableCommit.getCreateTime(),
                true, tableCommit.getDroppedTime());*/
            TableObject tableObject = new TableObject(tableIdent, tableName, 0, tableBaseHistory.getTableBaseObject(),
                tableSchemaHistory.getTableSchemaObject(), tableStorageHistory.getTableStorageObject(),
                tableCommit.getDroppedTime());

            tables.put(droppedTableObjectName.getObjectId(), tableObject);
        }
        return tables;
    }

    /**
     * List valid tables
     *
     * @param context       FDB context
     * @param databaseIdent database identifier
     * @return tableId to TableRecord mapping
     */
    private static Map<String, TableObject> listValidTables(TransactionContext context, String catalogName,
        DatabaseIdent databaseIdent) {
        ScanRecordCursorResult<List<TableNameObject>> tableObjectNames = tableMetaStore
            .listTableName(context, databaseIdent,
                Integer.MAX_VALUE, null, TransactionIsolationLevel.SERIALIZABLE);
        String latestVersion = VersionManagerHelper.getLatestVersion(context, databaseIdent.getProjectId(),
            databaseIdent.getRootCatalogId());
        HashMap<String, TableObject> tables = new HashMap<String, TableObject>();
        for (TableNameObject tableObjectName : tableObjectNames.getResult()) {
            TableIdent tableIdent = StoreConvertor.tableIdent(databaseIdent,
                tableObjectName.getObjectId());

            // get table schema instance by id
            TableSchemaObject tableSchema = TableSchemaHelper.getTableSchema(context, tableIdent);
            // get latest table history by id
            TableCommitObject tableCommit = TableCommitHelper
                .getLatestTableCommitOrElseThrow(context, tableIdent, latestVersion);
            TableBaseObject tableBase = TableBaseHelper.getTableBase(context, tableIdent);
            TableStorageObject tableStorage = TableStorageHelper.getTableStorage(context, tableIdent);

            // get table full name by id
            DatabaseIdent dbIdent = StoreConvertor.databaseIdent(tableIdent);
            DatabaseObject dbRecord = DatabaseObjectHelper.getDatabaseObject(context, dbIdent);

            // TODO table properties, partition
            TableName tableName = StoreConvertor.tableName(tableIdent.getProjectId(), catalogName,
                dbRecord.getName(), tableObjectName.getName());
            /*TableObject table = TableObjectConvertHelper.buildTable(tableIdent, tableName,
                null, tableStorage, tableSchema,
                tableCommit.getCreateTime(), false, 0);*/
            TableObject tableObject = new TableObject(tableIdent, tableName, 0, tableBase, tableSchema, tableStorage,
                tableCommit.getDroppedTime());

            tables.put(tableIdent.getTableId(), tableObject);
        }
        return tables;
    }

    private static List<TableObject> listParentBranchTables(TransactionContext context, String catalogName,
        DatabaseIdent parentBranchDatabaseIdent,
        String subBranchVersion, boolean dropped) throws MetaStoreException {

        List<TableObject> parentTableList = new ArrayList<>();

        // <tableId, catalogId>
        List<TableIdent> tableIdentList = TableObjectHelper.getTablesIdent(context, parentBranchDatabaseIdent, true);
        Map<String, String> latestTableIds = tableIdentList.stream().collect(Collectors.toMap(TableIdent::getTableId,
            TableIdent::getCatalogId));

        // get table history, Check whether the current tableId set exists in the history table
        for (String i : latestTableIds.keySet()) {

            TableIdent tableIdent = StoreConvertor.tableIdent(parentBranchDatabaseIdent, i);

            Optional<TableSchemaHistoryObject> tableSchemaHistory = tableMetaStore.getLatestTableSchema(context, tableIdent,
                subBranchVersion);
            Optional<TableBaseHistoryObject> tableBaseHistory = tableMetaStore.getLatestTableBase(context, tableIdent,
                subBranchVersion);
            Optional<TableCommitObject> tableCommit = tableMetaStore.getLatestTableCommit(context, tableIdent, subBranchVersion);
            Optional<TableStorageHistoryObject> tableStorageHistory = tableMetaStore.getLatestTableStorage(context, tableIdent,
                subBranchVersion);

            DatabaseIdent dbIdent = StoreConvertor.databaseIdent(tableIdent);
            DatabaseObject dbRecord = DatabaseObjectHelper.getDatabaseObject(context, dbIdent);

            if (tableCommit.isPresent() && tableBaseHistory.isPresent()
                && tableSchemaHistory.isPresent() && tableStorageHistory.isPresent()
                && (dropped == TableCommitHelper.isDropCommit(tableCommit.get()))) {
                TableName tableName = StoreConvertor.tableName(tableIdent.getProjectId(), catalogName,
                    dbRecord.getName(), tableCommit.get().getTableName());
                /*TableObject table = TableObjectConvertHelper.buildTable(tableIdent, tableName,
                    null, new TableStorageObject(tableStorageHistory.get()), tableSchemaHistory.get().getTableSchemaObject(),
                    tableCommit.get().getCreateTime(),
                    dropped, tableCommit.get().getDroppedTime());*/
                TableObject tableObject = new TableObject(tableIdent, tableName, 0, tableBaseHistory.get().getTableBaseObject(),
                    tableSchemaHistory.get().getTableSchemaObject(), tableStorageHistory.get().getTableStorageObject(),
                    tableCommit.get().getDroppedTime());

                parentTableList.add(tableObject);
            }
        }

        return parentTableList;

    }

    public static TableIdent getTableIdentByName(TableName tableName) {
        try (TransactionRunner runner = new TransactionRunner()) {
            return runner.run(context -> {
               return  TableObjectHelper.getTableIdent(context, tableName);
            });
        }
    }
}
