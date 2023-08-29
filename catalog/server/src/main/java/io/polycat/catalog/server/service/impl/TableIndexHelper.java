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

import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.TableBaseObject;
import io.polycat.catalog.common.model.TableCommitObject;
import io.polycat.catalog.common.model.TableIdent;
import io.polycat.catalog.common.model.TableIndexInfo;
import io.polycat.catalog.common.model.TableIndexInfoObject;
import io.polycat.catalog.common.model.TableIndexesHistoryObject;
import io.polycat.catalog.common.model.TableIndexesObject;
import io.polycat.catalog.common.model.TableName;
import io.polycat.catalog.common.model.TableOperationType;
import io.polycat.catalog.common.model.TableSchemaObject;
import io.polycat.catalog.common.model.TableStorageObject;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.store.api.TableDataStore;
import io.polycat.catalog.store.api.TableMetaStore;
import io.polycat.catalog.store.common.StoreConvertor;
import io.polycat.catalog.store.fdb.record.RecordStoreHelper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import static java.util.stream.Collectors.toList;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class TableIndexHelper {
    private static TableMetaStore tableMetaStore;

    private static TableDataStore tableDataStore;

    @Autowired
    public void setTableMetaStore(TableMetaStore tableMetaStore) {
        this.tableMetaStore = tableMetaStore;
    }

    @Autowired
    public void setTableDataStore(TableDataStore tableStore) {
        this.tableDataStore = tableStore;
    }

    public static TableIndexInfo toTableIndexInfo(TableIndexInfoObject tableIndexInfoObject) {
        TableIndexInfo tableIndexInfo = new TableIndexInfo();
        tableIndexInfo.setDatabaseId(tableIndexInfoObject.getDatabaseId());
        tableIndexInfo.setIndexId(tableIndexInfoObject.getIndexId());
        tableIndexInfo.setMV(tableIndexInfoObject.isMV());
        return tableIndexInfo;
    }

    public static void addOrModifyIndexInfo(TransactionContext context, TableName tableName,
        TableIndexInfoObject tableIndexInfo, TableOperationType operationType, String indexName) {
        // get table ident
        TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);

        String latestVersion = VersionManagerHelper.getLatestVersion(tableIdent);

        // update table reference store with current timestamp
        /*TableReferenceObject tableReference = TableReferenceHelper.getTableReferenceOrElseThrow(context, tableIdent);
        tableReference.setMetaUpdateTime(System.currentTimeMillis());*/

        int historySubspaceFlag = tableMetaStore.getTableHistorySubspaceFlag(context, tableIdent);

        TableBaseObject tableBaseObject = TableBaseHelper
            .getLatestTableBaseOrElseThrow(context, tableIdent, latestVersion).getTableBaseObject();
        TableSchemaObject tableSchemaObject = TableSchemaHelper
            .getLatestTableSchemaOrElseThrow(context, tableIdent, latestVersion).getTableSchemaObject();
        TableStorageObject tableStorageObject = TableStorageHelper
            .getLatestTableStorageOrElseThrow(context, tableIdent, latestVersion).getTableStorageObject();
        tableMetaStore.upsertTableReference(context, tableIdent);
        tableMetaStore.upsertTable(context, tableIdent, tableName.getTableName(), historySubspaceFlag,
            tableBaseObject, tableSchemaObject, tableStorageObject);
        /*if (null != tableStore.getTableReference(context, tableIdent)) {
            tableStore.updateTableReference(context, tableIdent, tableReference);
        } else {
            tableStore.insertTableReference(context, tableIdent, tableReference.getName());
        }*/

        // get the latest table indexes for the table
        Optional<TableIndexesHistoryObject> optionalTableIndexesHistory =
            getLatestTableIndexes(context, tableIdent, latestVersion);

        List<TableIndexInfoObject> tableIndexInfoList =
            optionalTableIndexesHistory.map((t) -> new ArrayList(t.getTableIndexInfoObjectList()))
                .orElseGet(ArrayList::new);

        // add or drop index
        if (operationType == TableOperationType.DDL_CREATE_INDEX) {
            tableIndexInfoList.add(tableIndexInfo);
        } else if (operationType == TableOperationType.DDL_DROP_INDEX) {
            boolean ifIndexExists = tableIndexInfoList.stream()
                .anyMatch(f -> f.getIndexId().equalsIgnoreCase(tableIndexInfo.getIndexId()));
            if (ifIndexExists) {
                tableIndexInfoList.remove(tableIndexInfo);
            } else {
                throw new MetaStoreException(ErrorCode.MV_NOT_FOUND, indexName);
            }
        }

        // insert new index info into table_indexes and table_indexes_history subspace
        TableIndexesObject tableIndexes =
            tableDataStore.insertTableIndexes(context, tableIdent, tableIndexInfoList);

        tableDataStore.insertTableIndexesHistory(context, tableIdent, latestVersion, tableIndexes);

        // update table commit
        long commitTime = RecordStoreHelper.getCurrentTime();
        TableCommitObject tableCommit =
            TableCommitHelper.getLatestTableCommit(context, tableIdent, latestVersion).get();
        tableMetaStore.insertTableCommit(context, tableIdent,
            TableCommitHelper.buildNewTableCommit(tableCommit, latestVersion, commitTime, 0, operationType));
    }

    public static Optional<TableIndexesHistoryObject> getLatestTableIndexes(TransactionContext context,
        TableIdent tableIdent, String basedVersion) throws MetaStoreException {
        Optional<TableIndexesHistoryObject> tableIndexesHistory =
            tableDataStore.getLatestTableIndexes(context, tableIdent, basedVersion);
        if (!tableIndexesHistory.isPresent()) {
            tableIndexesHistory =
                getLatestSubBranchFakeTableIndexes(context, tableIdent, basedVersion);
        }

        return tableIndexesHistory;
    }

    public static List<TableIndexInfo> getTableIndexesByName(TransactionContext context, TableName tableName)
        throws MetaStoreException {
        TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);
        // get table indexes by id
        return getTableIndexesById(context, tableIdent, tableName.getTableName());

    }

    private static Optional<TableIndexesHistoryObject> getLatestSubBranchFakeTableIndexes(
        TransactionContext context, TableIdent subBranchTableIdent, String version)
        throws MetaStoreException {
        ParentBranchDatabaseIterator parentBranchDatabaseIterator =
            new ParentBranchDatabaseIterator(context, subBranchTableIdent);

        while (parentBranchDatabaseIterator.hasNext()) {
            DatabaseIdent parentDatabaseIdent = parentBranchDatabaseIterator.nextDatabase();
            String subBranchVersion =
                parentBranchDatabaseIterator.nextBranchVersion(parentDatabaseIdent);

            //The query version must be earlier than the branch creation version.
            TableIdent parentTableIdent =
                StoreConvertor.tableIdent(parentDatabaseIdent, subBranchTableIdent.getTableId());
            String baseVersionStamp =
                version.compareTo(subBranchVersion) < 0 ? version : subBranchVersion;
            Optional<TableIndexesHistoryObject> tableIndexesHistory =
                tableDataStore.getLatestTableIndexes(context, parentTableIdent, baseVersionStamp);
            if (tableIndexesHistory.isPresent()) {
                return Optional.of(tableIndexesHistory.get());
            }
        }
        return Optional.empty();
    }

    private static List<TableIndexInfo> getTableIndexesById(TransactionContext context,
        TableIdent tableIdent, String tableName) throws MetaStoreException {
        // query the table indexes store and return the table indexes created on the table
        TableIndexesObject tableIndexes =
            tableDataStore.getTableIndexes(context, tableIdent, tableName);
        if (tableIndexes == null) {
            if (!TableCommitHelper.checkTableDropped(context, tableIdent)) {
                tableIndexes = getSubBranchFakeTableIndexes(context, tableIdent);
            }
        }
        if (null == tableIndexes) {
            return new ArrayList<>();
        }

        return tableIndexes.getTableIndexInfoObjectList().stream().map(TableIndexHelper::toTableIndexInfo)
            .collect(toList());
    }

    private static TableIndexesObject getSubBranchFakeTableIndexes(TransactionContext context,
        TableIdent subBranchTableIdent) throws MetaStoreException {
        ParentBranchDatabaseIterator parentBranchDatabaseIterator =
            new ParentBranchDatabaseIterator(context, subBranchTableIdent);

        while (parentBranchDatabaseIterator.hasNext()) {
            DatabaseIdent parentDatabaseIdent = parentBranchDatabaseIterator.nextDatabase();
            String subBranchVersion =
                parentBranchDatabaseIterator.nextBranchVersion(parentDatabaseIdent);

            TableIdent parentBranchTableIdent =
                StoreConvertor.tableIdent(parentDatabaseIdent,
                    subBranchTableIdent.getTableId());
            Optional<TableIndexesHistoryObject> tableIndexesHistory =
                tableDataStore.getLatestTableIndexes(context, parentBranchTableIdent, subBranchVersion);

            if (tableIndexesHistory.isPresent()) {
                return new TableIndexesObject(tableIndexesHistory.get());
            }
        }

        return null;
    }

}
