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

import java.util.Optional;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.TableHistoryObject;
import io.polycat.catalog.common.model.TableIdent;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.store.api.TableDataStore;
import io.polycat.catalog.store.common.StoreConvertor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class TableHistoryHelper {

    private static TableDataStore tableDataStore;

    @Autowired
    public void setTableDataStore(TableDataStore tableStore) {
        this.tableDataStore = tableStore;
    }

    public static Optional<TableHistoryObject> getLatestTableHistory(TransactionContext context, TableIdent tableIdent,
        String basedVersion) throws MetaStoreException {
        Optional<TableHistoryObject> tableHistory = tableDataStore.getLatestTableHistory(context, tableIdent, basedVersion);
        if (!tableHistory.isPresent()) {
            tableHistory = getLatestSubBranchFakeTableHistory(context, tableIdent, basedVersion);
        }

        return tableHistory;
    }

    public static TableHistoryObject getLatestTableHistoryOrElseThrow(TransactionContext context, TableIdent tableIdent,
        String basedVersion) throws MetaStoreException {
        Optional<TableHistoryObject> tableHistory = getLatestTableHistory(context, tableIdent, basedVersion);

        if (!tableHistory.isPresent()) {
            throw new CatalogServerException(ErrorCode.TABLE_DATA_HISTORY_NOT_FOUND, tableIdent.getTableId());
        }
        return tableHistory.get();
    }

    public static TableHistoryObject getTableHistoryOrElseThrow(TransactionContext context, TableIdent tableIdent,
        String version) throws MetaStoreException {
        Optional<TableHistoryObject> tableHistory = getTableHistory(context, tableIdent, version);
        if (!tableHistory.isPresent()) {
            throw new MetaStoreException(ErrorCode.TABLE_COMMIT_NOT_FOUND);
        }

        return tableHistory.get();
    }

    public static Optional<TableHistoryObject> getTableHistory(TransactionContext context, TableIdent tableIdent,
        String version) {
        Optional<TableHistoryObject> tableHistory = tableDataStore.getTableHistory(context, tableIdent, version);
        if (!tableHistory.isPresent()) {
            tableHistory = getSubBranchFakeTableHistory(context, tableIdent, version);
        }

        return tableHistory;
    }

    private static Optional<TableHistoryObject> getLatestSubBranchFakeTableHistory(TransactionContext context,
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
            Optional<TableHistoryObject> tableHistory = tableDataStore.getLatestTableHistory(context, parentTableIdent,
                baseVersionStamp);
            if (tableHistory.isPresent()) {
                return Optional.of(tableHistory.get());
            }
        }
        return Optional.empty();
    }

    private static Optional<TableHistoryObject> getSubBranchFakeTableHistory(TransactionContext context,
        TableIdent subBranchTableIdent, String version) throws MetaStoreException {
        DatabaseIdent parentDatabaseIdent = getParentBranchDatabaseIdent(context, subBranchTableIdent, version);

        TableIdent parentTableIdent = StoreConvertor.tableIdent(parentDatabaseIdent,
            subBranchTableIdent.getTableId());

        Optional<TableHistoryObject> tableHistory = tableDataStore.getTableHistory(context, parentTableIdent, version);
        if (!tableHistory.isPresent()) {
            return Optional.empty();
        }

        return Optional.of(tableHistory.get());
    }

    private static DatabaseIdent getParentBranchDatabaseIdent(TransactionContext context,
        TableIdent subBranchTableIdent,
        String version) {
        ParentBranchDatabaseIterator parentBranchDatabaseIterator = new ParentBranchDatabaseIterator(context,
            subBranchTableIdent, version);

        if (parentBranchDatabaseIterator.hasNext()) {
            DatabaseIdent parentDatabaseIdent = parentBranchDatabaseIterator.nextDatabase();
            return parentDatabaseIdent;
        }

        return null;
    }


}
