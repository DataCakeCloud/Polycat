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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.model.DataFileObject;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.OperationObject;
import io.polycat.catalog.common.model.PartitionObject;
import io.polycat.catalog.common.model.TableBaseHistoryObject;
import io.polycat.catalog.common.model.TableBaseObject;
import io.polycat.catalog.common.model.TableCommitObject;
import io.polycat.catalog.common.model.TableHistoryObject;
import io.polycat.catalog.common.model.TableIdent;
import io.polycat.catalog.common.model.TableIndexesHistoryObject;
import io.polycat.catalog.common.model.TableIndexesObject;
import io.polycat.catalog.common.model.TableObject;
import io.polycat.catalog.common.model.TableOperationType;
import io.polycat.catalog.common.model.TableSchemaHistoryObject;
import io.polycat.catalog.common.model.TableSchemaObject;
import io.polycat.catalog.common.model.TableStorageHistoryObject;
import io.polycat.catalog.common.model.TableStorageObject;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.store.api.TableDataStore;
import io.polycat.catalog.store.api.TableMetaStore;
import io.polycat.catalog.store.api.UserPrivilegeStore;
import io.polycat.catalog.store.common.StoreConvertor;
import io.polycat.catalog.store.fdb.record.RecordStoreHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class TableBranchHelper {

    private static TableMetaStore tableMetaStore;
    private static TableDataStore tableDataStore;
    private static UserPrivilegeStore userPrivilegeStore;

    @Autowired
    public void setTableMetaStore(TableMetaStore tableMetaStore) {
        TableBranchHelper.tableMetaStore = tableMetaStore;
    }

    @Autowired
    public void setTableDataStore(TableDataStore tableDataStore) {
        TableBranchHelper.tableDataStore = tableDataStore;
    }

    @Autowired
    public void setUserPrivilegeStore(UserPrivilegeStore userPrivilegeStore) {
        TableBranchHelper.userPrivilegeStore = userPrivilegeStore;
    }


    public static void mergeNewTable(TransactionContext context, String userId,
        TableObject tableObject, DatabaseIdent destDatabaseIdent) throws CatalogServerException {

        TableIdent srcTableIdent = StoreConvertor.tableIdent(tableObject.getProjectId(), tableObject.getCatalogId(),
            tableObject.getDatabaseId(), tableObject.getTableId(), destDatabaseIdent.getRootCatalogId());
        String versionstamp = VersionManagerHelper.getLatestVersion(srcTableIdent);

        // check whether table already exists
        String tableId = TableObjectHelper.getTableId(context, destDatabaseIdent, tableObject.getName());
        if (tableId != null) {
            throw new MetaStoreException(ErrorCode.TABLE_ALREADY_EXIST, tableObject.getName());
        }

        TableIdent tableIdent = StoreConvertor.tableIdent(destDatabaseIdent.getProjectId(),
            destDatabaseIdent.getCatalogId(),
            destDatabaseIdent.getDatabaseId(),
            tableObject.getTableId(),
            destDatabaseIdent.getRootCatalogId());

        tableDataStore.createTableDataPartitionSetSubspace(context, tableIdent);
        tableDataStore.createTableIndexPartitionSetSubspace(context, tableIdent);
        int historySubspaceFlag = TableHistorySubspaceHelper.createHistorySubspace(context, tableIdent);

        TableSchemaHistoryObject srcTableSchemaHistory = tableMetaStore
            .getLatestTableSchemaOrElseThrow(context, srcTableIdent, versionstamp);
        TableSchemaObject tableSchema = srcTableSchemaHistory.getTableSchemaObject();

        TableBaseHistoryObject srcTableBaseHistoryObject = tableMetaStore
            .getLatestTableBaseOrElseThrow(context, srcTableIdent, versionstamp);
        TableBaseObject tableBase = srcTableBaseHistoryObject.getTableBaseObject();

        TableStorageHistoryObject srcTableStorageHistoryObject = tableMetaStore
            .getLatestTableStorageOrElseThrow(context, srcTableIdent, versionstamp);
        TableStorageObject tableStorage = srcTableStorageHistoryObject.getTableStorageObject();

        tableMetaStore.insertTableReference(context, tableIdent);
        tableMetaStore.insertTable(context, tableIdent, tableObject.getName(), historySubspaceFlag, tableBase,
            tableSchema, tableStorage);

        /*// insert an object into ObjectName subspace
        tableStore.insertTableName(context, tableIdent, tableObject.getName());
        // insert a TableReference into TableReference subspace
        tableStore.insertTableReference(context, tableIdent, tableObject.getName());
        // insert the Table properties into TableProperties subspace
        tableStore.insertTableBase(context, tableIdent, tableBase);
        // insert a TableSchema into TableSchema subspace
        tableStore.insertTableSchema(context, tableIdent, tableSchema);
        // insert the table storage info into TableStorage subspace
        tableStore.insertTableStorage(context, tableIdent, tableStorage);*/

        // insert the Table base into TableBaseHistory subspace
        tableMetaStore.insertTableBaseHistory(context, tableIdent, versionstamp, tableBase);

        // insert the TableSchema into TableSchemaHistory subspace
        tableMetaStore.insertTableSchemaHistory(context, tableIdent, versionstamp, tableSchema);

        // insert the table storage info into TableStorageHistory subspace
        tableMetaStore.insertTableStorageHistory(context, tableIdent, versionstamp, tableStorage);

        // insert table indexes if present in the table
        Optional<TableIndexesHistoryObject> optionalTableIndexesHistory =
            tableDataStore.getLatestTableIndexes(context, tableIdent, versionstamp);
        if (optionalTableIndexesHistory.isPresent()) {
            TableIndexesObject tableIndexes = tableDataStore.insertTableIndexes(context, tableIdent,
                optionalTableIndexesHistory.get().getTableIndexInfoObjectList());
            String version = VersionManagerHelper
                .getNextVersion(context, tableIdent.getProjectId(), tableIdent.getCatalogId());
            // insert table indexes history into TableIndexesHistory subspace
            tableDataStore.insertTableIndexesHistory(context, tableIdent, version, tableIndexes);
        }

        // insert the tableData into TableHistory subspace
        TableHistoryObject srcTableHistory = tableDataStore.getLatestTableHistoryOrElseThrow(context, srcTableIdent,
            versionstamp);
        PartitionHelper.insertTablePartitionForMerge(context, tableIdent, srcTableHistory);

        // inserts data of a specified schema version.
        insertTablePartitionSchemaHistory(context, srcTableIdent,
            tableDataStore.getAllPartitionsFromTableHistory(context, tableIdent, srcTableHistory), tableIdent);

        // insert the TableCommit into TableCommit subspace
        Optional<TableCommitObject> tableCommit = tableMetaStore.getLatestTableCommit(context, tableIdent, versionstamp);
        TableCommitObject tableCommitNew;
        long commitTime = RecordStoreHelper.getCurrentTime();
        if (!tableCommit.isPresent()) {
            List<OperationObject> operationList = getMergeOperationList(
                tableDataStore.getAllPartitionsFromTableHistory(context, tableIdent, srcTableHistory));

            tableCommitNew = new TableCommitObject(tableIdent.getProjectId(), tableIdent.getCatalogId(),
                tableIdent.getDatabaseId(), tableIdent.getTableId(), tableObject.getName(), commitTime, commitTime,
                operationList, 0, versionstamp);
        } else {
            List<OperationObject> operationList = getMergeOperationList(
                tableDataStore.getAllPartitionsFromTableHistory(context, tableIdent, srcTableHistory));
            tableCommitNew = TableCommitHelper.buildNewTableCommit(tableCommit.get(), commitTime, 0, operationList);
        }

        tableMetaStore.insertTableCommit(context, tableIdent, tableCommitNew);

        userPrivilegeStore.insertUserPrivilege(context, tableIdent.getProjectId(), userId, ObjectType.TABLE.name(),
            tableIdent.getTableId(), true, 0);
    }

    /**
     * Merge commits from src table to dest table
     */
    public static void mergeTable(TransactionContext context, TableObject srcTableRecord,
        TableObject destTableRecord, String sameMaxVersion) throws CatalogServerException {

        TableIdent srcTableIdent = StoreConvertor.tableIdent(srcTableRecord.getProjectId(),
            srcTableRecord.getCatalogId(), srcTableRecord.getDatabaseId(), srcTableRecord.getTableId(), null);
        List<TableCommitObject> srcTableCommits = TableCommitHelper.listTableCommits(context, srcTableIdent,
            sameMaxVersion, VersionManagerHelper.getLatestVersion(srcTableIdent));
        List<TableSchemaHistoryObject> newSchemaFromSrc = tableMetaStore.listSchemaHistoryFromTimePoint(context,
            sameMaxVersion, srcTableIdent);

        TableIdent destTableIdent = StoreConvertor.tableIdent(destTableRecord.getProjectId(),
            destTableRecord.getCatalogId(), destTableRecord.getDatabaseId(), destTableRecord.getTableId(), null);
        List<TableCommitObject> destTableCommits = TableCommitHelper.listTableCommits(context, destTableIdent,
            sameMaxVersion, VersionManagerHelper.getLatestVersion(destTableIdent));

        if (srcTableCommits.size() == 0) {
            return;
        }

        if (isConflict(srcTableCommits, destTableCommits)) {
            throw new CatalogServerException(ErrorCode.TABLE_CONFLICT, srcTableRecord.getName());
        }

        Optional<TableHistoryObject> baseTableHistory = TableHistoryHelper.getLatestTableHistory(context,
            destTableIdent,
            sameMaxVersion);

        List<PartitionObject> newPartitionList = getNewPartitions(context, baseTableHistory.get(), srcTableIdent,
            srcTableCommits);

        TableHistoryObject destTableHistory;
        if (destTableCommits.size() == 0) {
            destTableHistory = baseTableHistory.get();
        } else {
            destTableHistory = TableHistoryHelper.getTableHistoryOrElseThrow(context, destTableIdent,
                destTableCommits.get(0).getVersion());
        }

        List<PartitionObject> newAllPartitionList = new ArrayList<>(
            tableDataStore.getAllPartitionsFromTableHistory(context, srcTableIdent, destTableHistory));
        //support repeated merger

        newPartitionList = aExceptSegListB(newPartitionList, newAllPartitionList);
        PartitionHelper.insertTablePartitions(context, destTableIdent, destTableHistory, newPartitionList);
        insertTablePartitionSchemaHistory(context, srcTableIdent, newPartitionList, destTableIdent);

        String version = VersionManagerHelper.getLatestVersion(destTableIdent);

        for (TableSchemaHistoryObject schemaHistory : newSchemaFromSrc) {
            tableMetaStore.insertTableSchemaHistory(context, destTableIdent, version, schemaHistory.getTableSchemaObject());
        }

        tableMetaStore.upsertTableReference(context, destTableIdent);

        if (newSchemaFromSrc.size() > 0) {
            TableObject tableObject = TableObjectHelper.getTableObject(context, destTableIdent);
            tableMetaStore.upsertTable(context, destTableIdent, srcTableRecord.getName(),
                tableObject.getHistorySubspaceFlag(), tableObject.getTableBaseObject(),
                newSchemaFromSrc.get(newSchemaFromSrc.size()-1).getTableSchemaObject(),
                tableObject.getTableStorageObject());
        }

        //todo: table schema
        //tableMetaStore.upsertTableSchema(context, destTableIdent, newSchemaFromSrc.get(0).getTableSchemaObject());

        //todo : table properties    table storage
        // insert table reference
        //tableMetaStore.upsertTableReference(context, destTableIdent);

        // insert the TableCommit into TableCommit subspace
        TableCommitObject tableCommit = TableCommitHelper
            .getLatestTableCommit(context, destTableIdent, sameMaxVersion).get();
        long commitTime = RecordStoreHelper.getCurrentTime();
        List<OperationObject> tableOperationList = getMergeOperationList(newPartitionList);
        tableMetaStore.insertTableCommit(context, destTableIdent,
            TableCommitHelper.buildNewTableCommit(tableCommit, commitTime, 0, tableOperationList));
    }

    private static void insertTablePartitionSchemaHistory(TransactionContext context, TableIdent srcTableIdent,
        List<PartitionObject> partitions, TableIdent tableIdent) {
        Map<String, TableSchemaObject> schemaMap = collectSchemaVersions(context, srcTableIdent, partitions);
        for (Map.Entry<String, TableSchemaObject> entry : schemaMap.entrySet()) {
            String curVersion = entry.getKey();

            // get cur branch table schema
            Optional<TableSchemaHistoryObject> schema = TableSchemaHelper.getLatestTableSchema(context, tableIdent,
                curVersion);
            // table schema is not exist
            if (!schema.isPresent()) {
                // insert table schema to cur branch
                tableMetaStore.upsertTableSchemaHistory(context, tableIdent, curVersion, entry.getValue());
            }
        }
    }

    private static Map<String, TableSchemaObject> collectSchemaVersions(TransactionContext context,
        TableIdent tableIdent,
        List<PartitionObject> partitions) {
        Map<String, TableSchemaObject> schemaMap = new HashMap<>(partitions.size());
        try {
            partitions.forEach(partition -> {
                if (!partition.getSchemaVersion().isEmpty()) {
                    String schemaVersion = partition.getSchemaVersion();
                    if (!schemaMap.containsKey(schemaVersion)) {
                        String versionstamp = schemaVersion;
                        Optional<TableSchemaHistoryObject> schema = TableSchemaHelper.getLatestTableSchema(context,
                            tableIdent, versionstamp);
                        if (!schema.isPresent()) {
                            throw new RuntimeException(
                                "Failed to get schema of version: " + schemaVersion);
                        }
                        schemaMap.put(schemaVersion, schema.get().getTableSchemaObject());
                    }
                }
            });
        } catch (MetaStoreException e) {
            throw e;
        }
        return schemaMap;
    }

    private static List<OperationObject> getMergeOperationList(List<PartitionObject> newPartitionList) {
        long addedNums = 0;
        int fileCount = 0;

        for (PartitionObject partition : newPartitionList) {
            addedNums += partition.getFile().stream().map(DataFileObject::getRowCount).reduce(0L, Long::sum);
            fileCount += partition.getFile().size();
        }

        OperationObject operation = new OperationObject(TableOperationType.DDL_MERGE_BRANCH, addedNums, 0, 0,
            fileCount);
        return Collections.singletonList(operation);
    }

    private static List<PartitionObject> getNewPartitions(TransactionContext context,
        TableHistoryObject baseTableHistory,
        TableIdent srcTableIdent, List<TableCommitObject> srcTableCommits) {
        Optional<TableHistoryObject> srcTableHistory = TableHistoryHelper.getLatestTableHistory(context, srcTableIdent,
            srcTableCommits.get(0).getVersion());

        List<PartitionObject> partitionList1 = tableDataStore
            .getAllPartitionsFromTableHistory(context, srcTableIdent, baseTableHistory);
        List<PartitionObject> partitionList2 = new ArrayList<>(
            tableDataStore.getAllPartitionsFromTableHistory(context, srcTableIdent, srcTableHistory.get()));
        partitionList2 = aExceptSegListB(partitionList2, partitionList1);

        return partitionList2;
    }

    private static boolean isConflict(List<TableCommitObject> srcTableCommits,
        List<TableCommitObject> destTableCommits) {
        if (destTableCommits.size() == 0) {
            return false;
        }

        for (TableCommitObject destTableCommit : destTableCommits) {
            List<OperationObject> destOperationList = destTableCommit.getOperations();
            for (OperationObject operation : destOperationList) {
                if (operation.getOperationType().name().equals(TableOperationType.DDL_DROP_TABLE.name())) {
                    return true;
                }
            }
        }

        //todo Conflict check

        return false;
    }


    // finding the difference set of list A and B, i.e., a - b, a.removeAll(b)
    private static List<PartitionObject> aExceptSegListB(List<PartitionObject> a, List<PartitionObject> b) {
        Objects.requireNonNull(a);
        Objects.requireNonNull(b);

        int size = a.size();
        int idx = 0;
        for (int r = 0; r < size; ++r) {
            if (!partitionContains(b, a.get(r))) {
                a.set(idx++, a.get(r));
            }
        }

        // let the rest of the list to be eligible for gc
        return new ArrayList<>(a.subList(0, idx));
    }

    private static boolean segEquals(PartitionObject partition, PartitionObject other) {
        return partition.getPartitionId().equals(other.getPartitionId());
    }

    private static boolean partitionContains(List<PartitionObject> partitionList, PartitionObject o) {
        for (PartitionObject partition : partitionList) {
            if (segEquals(partition, o)) {
                return true;
            }
        }
        return false;
    }


}
