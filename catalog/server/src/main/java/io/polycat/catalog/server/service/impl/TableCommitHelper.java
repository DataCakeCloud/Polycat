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
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.OperationObject;
import io.polycat.catalog.common.model.ScanRecordCursorResult;
import io.polycat.catalog.common.model.TableCommitObject;
import io.polycat.catalog.common.model.TableIdent;
import io.polycat.catalog.common.model.TableOperationType;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.TransactionIsolationLevel;
import io.polycat.catalog.server.util.TransactionRunner;
import io.polycat.catalog.store.api.TableMetaStore;
import io.polycat.catalog.store.common.StoreConvertor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class TableCommitHelper {
    private static TableMetaStore tableMetaStore;

    @Autowired
    public void setTableMetaStore(TableMetaStore tableMetaStore) {
        this.tableMetaStore = tableMetaStore;
    }

    public static TableCommitObject buildNewTableCommit(TableCommitObject latestTableCommit, String version,
        long commitTime, long dropTime, TableOperationType operationType) {
        latestTableCommit.setVersion(version);
        latestTableCommit.setCommitTime(commitTime);
        latestTableCommit.setDroppedTime(dropTime);
        OperationObject tableOperation = new OperationObject(operationType, 0, 0, 0, 0);
        latestTableCommit.setOperations(Collections.singletonList(tableOperation));

        return latestTableCommit;
    }

    public static TableCommitObject buildNewTableCommit(TableCommitObject latestTableCommit, long commitTime, long dropTime,
        List<OperationObject> operationList) {
        latestTableCommit.setCommitTime(commitTime);
        latestTableCommit.setDroppedTime(dropTime);
        latestTableCommit.setOperations(operationList);

        return latestTableCommit;
    }

    public static ScanRecordCursorResult<List<TableCommitObject>> listTableCommit(TableIdent tableIdent) {
        try (TransactionRunner runner = new TransactionRunner()) {
            return runner.run(context -> {
               return tableMetaStore.listTableCommit(context, tableIdent, Integer.MAX_VALUE, null,
                   TransactionIsolationLevel.SERIALIZABLE, VersionManagerHelper.getLatestVersion(tableIdent));
            });
        }
    }

    public static Optional<TableCommitObject> getLatestTableCommit(TransactionContext context, TableIdent tableIdent,
        String basedVersion) throws MetaStoreException {
        Optional<TableCommitObject> tableCommit = tableMetaStore.getLatestTableCommit(context, tableIdent, basedVersion);
        if (!tableCommit.isPresent()) {
            tableCommit = getLatestSubBranchFakeTableCommit(context, tableIdent, basedVersion);
        }

        return tableCommit;
    }

    public static TableCommitObject getLatestTableCommitOrElseThrow(TransactionContext context, TableIdent tableIdent,
        String basedVersion) throws MetaStoreException {
        Optional<TableCommitObject> tableCommit = getLatestTableCommit(context, tableIdent, basedVersion);
        if (!tableCommit.isPresent()) {
            throw new MetaStoreException(ErrorCode.TABLE_COMMIT_NOT_FOUND, tableIdent.getTableId());
        }
        return tableCommit.get();
    }

    public static Boolean isDropCommit(TableCommitObject tableCommitObject) {
        return tableCommitObject.getDroppedTime() != 0;
    }

    public static Boolean checkTableDropped(TransactionContext context, TableIdent tableIdent) {
        String versionstamp = VersionManagerHelper.getLatestVersion(tableIdent);
        Optional<TableCommitObject> tableCommit = tableMetaStore.getLatestTableCommit(context, tableIdent, versionstamp);
        return tableCommit.map(commit -> { return isDropCommit(commit);}).orElse(false);
    }

    public static boolean checkTableDropped(TransactionContext context, TableIdent tableIdent,
        String version) throws MetaStoreException {
        Optional<TableCommitObject> tableCommit = tableMetaStore.getLatestTableCommit(context, tableIdent, version);
        return tableCommit.map(commit -> { return isDropCommit(commit);}).orElse(false);
    }

    public static List<TableCommitObject> listTableCommits(TransactionContext context, TableIdent tableIdent,
        String startVersion, String endVersion) {
        List<TableCommitObject> tableCommitList = tableMetaStore.listTableCommit(context, tableIdent, startVersion, endVersion);

        List<TableCommitObject> parentTableCommitList = listSubBranchFakeTableCommits(context, tableIdent, endVersion,
            (tableIdent1, endVersion1) -> listParentBranchTableCommits(context, tableIdent1,
                startVersion, endVersion1));

        if (tableCommitList.isEmpty()) {
            return parentTableCommitList;
        }

        tableCommitList.addAll(parentTableCommitList);

        return tableCommitList;
    }

    private static Optional<TableCommitObject> getLatestSubBranchFakeTableCommit(TransactionContext context,
        TableIdent subBranchTableIdent, String version) throws MetaStoreException {
        ParentBranchDatabaseIterator parentBranchDatabaseIterator = new ParentBranchDatabaseIterator(context,
            subBranchTableIdent);

        while (parentBranchDatabaseIterator.hasNext()) {
            DatabaseIdent parentDatabaseIdent = parentBranchDatabaseIterator.nextDatabase();
            String subBranchVersion = parentBranchDatabaseIterator.nextBranchVersion(parentDatabaseIdent);

            //The query version must be earlier than the branch creation version.
            TableIdent parentTableIdent = StoreConvertor.tableIdent(parentDatabaseIdent,
                subBranchTableIdent.getTableId());

            String baseVersionStamp = version.compareTo(subBranchVersion) < 0 ? version : subBranchVersion;
            Optional<TableCommitObject> tableCommit = tableMetaStore.getLatestTableCommit(context, parentTableIdent,
                baseVersionStamp);
            if (tableCommit.isPresent()) {
                tableCommit.get().setCatalogId(subBranchTableIdent.getCatalogId());
                return tableCommit;
            }
        }

        return Optional.empty();
    }

    private static List<TableCommitObject> listSubBranchFakeTableCommits(TransactionContext context,
        TableIdent subBranchTableIdent,
        String endVersion, BiFunction<TableIdent, String, List<TableCommitObject>> function)
        throws MetaStoreException {

        List<TableCommitObject> tableCommitList = new ArrayList<>();

        ParentBranchDatabaseIterator parentBranchDatabaseIterator = new ParentBranchDatabaseIterator(context,
            subBranchTableIdent);

        while (parentBranchDatabaseIterator.hasNext()) {
            DatabaseIdent parentDatabaseIdent = parentBranchDatabaseIterator.nextDatabase();
            String subBranchVersion = parentBranchDatabaseIterator.nextBranchVersion(parentDatabaseIdent);

            TableIdent parentTableIdent = StoreConvertor.tableIdent(parentDatabaseIdent,
                subBranchTableIdent.getTableId());

            String newEndVersion = (endVersion.compareTo(subBranchVersion) < 0) ? endVersion : subBranchVersion;

            List<TableCommitObject> parentBranchTableCommitList = function.apply(parentTableIdent, newEndVersion);

            tableCommitList.addAll(parentBranchTableCommitList);

        }

        return tableCommitList;
    }

    private static List<TableCommitObject> listParentBranchTableCommits(TransactionContext context,
        TableIdent parentBranchTableIdent,
        String startVersion, String endVersion)
        throws MetaStoreException {

        List<TableCommitObject> tableCommitList = tableMetaStore
            .listTableCommit(context, parentBranchTableIdent, startVersion, endVersion);

        return tableCommitList;
    }
}
