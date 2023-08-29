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

import java.util.List;
import java.util.Optional;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.ScanRecordCursorResult;
import io.polycat.catalog.common.model.TableBaseHistoryObject;
import io.polycat.catalog.common.model.TableBaseObject;
import io.polycat.catalog.common.model.TableIdent;
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
public class TableBaseHelper {

    private static TableMetaStore tableMetaStore;

    @Autowired
    public void setTableMetaStore(TableMetaStore tableMetaStore) {
        this.tableMetaStore = tableMetaStore;
    }

    public static ScanRecordCursorResult<List<TableBaseHistoryObject>> listTableBaseHistory(
        TableIdent tableIdent) throws CatalogServerException {
        try (TransactionRunner runner = new TransactionRunner()) {
            return runner.run(context -> {
                return tableMetaStore
                    .listTableBaseHistory(context, tableIdent, Integer.MAX_VALUE, null,
                        TransactionIsolationLevel.SERIALIZABLE,
                        VersionManagerHelper.getLatestVersion(tableIdent));
            });
        }
    }

    public static TableBaseHistoryObject getLatestTableBaseOrElseThrow(TransactionContext context,
        TableIdent tableIdent, String basedVersion) throws CatalogServerException {
        Optional<TableBaseHistoryObject> tableBaseHistory = getLatestTableBase(context, tableIdent, basedVersion);
        if (!tableBaseHistory.isPresent()) {
            throw new CatalogServerException(ErrorCode.TABLE_BASE_NOT_FOUND, tableIdent.getTableId());
        }
        return tableBaseHistory.get();
    }

    public static Optional<TableBaseHistoryObject> getLatestTableBase(TransactionContext context, TableIdent tableIdent,
        String basedVersion) {
        Optional<TableBaseHistoryObject> tableBaseHistory = tableMetaStore.getLatestTableBase(context, tableIdent,
            basedVersion);
        if (!tableBaseHistory.isPresent()) {
            tableBaseHistory = getLatestSubBranchFakeTableBase(context, tableIdent, basedVersion);
        }
        return tableBaseHistory;
    }

    private static Optional<TableBaseHistoryObject> getLatestSubBranchFakeTableBase(TransactionContext context,
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
            Optional<TableBaseHistoryObject> tablePropertiesHistory = tableMetaStore.getLatestTableBase(context,
                parentTableIdent, baseVersionStamp);
            if (tablePropertiesHistory.isPresent()) {
                return tablePropertiesHistory;
            }
        }
        return Optional.empty();
    }

    public static TableBaseObject getTableBase(TransactionContext context, TableIdent tableIdent)
        throws CatalogServerException {
        TableBaseObject tableBase = tableMetaStore.getTableBase(context, tableIdent);
        if (tableBase == null) {
            tableBase = getSubBranchFakeTableProperties(context, tableIdent);
        }
        return tableBase;
    }

    private static TableBaseObject getSubBranchFakeTableProperties(TransactionContext context,
        TableIdent subBranchTableIdent) throws CatalogServerException {
        ParentBranchDatabaseIterator parentBranchDatabaseIterator = new ParentBranchDatabaseIterator(context,
            subBranchTableIdent);

        while (parentBranchDatabaseIterator.hasNext()) {
            DatabaseIdent parentDatabaseIdent = parentBranchDatabaseIterator.nextDatabase();
            String subBranchVersion = parentBranchDatabaseIterator.nextBranchVersion(parentDatabaseIdent);

            TableIdent parentBranchTableIdent = StoreConvertor.tableIdent(parentDatabaseIdent,
                subBranchTableIdent.getTableId());
            Optional<TableBaseHistoryObject> tableBaseHistoryObject = tableMetaStore.getLatestTableBase(context,
                parentBranchTableIdent, subBranchVersion);

            if (tableBaseHistoryObject.isPresent()) {
                return tableBaseHistoryObject.get().getTableBaseObject();
            }
        }

        return null;
    }

}
