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
import io.polycat.catalog.common.model.TableIdent;
import io.polycat.catalog.common.model.TableSchemaHistoryObject;
import io.polycat.catalog.common.model.TableSchemaObject;
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
public class TableSchemaHelper {
    private static TableMetaStore tableMetaStore;

    @Autowired
    public void setTableMetaStore(TableMetaStore tableMetaStore) {
        this.tableMetaStore = tableMetaStore;
    }

    public static ScanRecordCursorResult<List<TableSchemaHistoryObject>> listTableSchemaHistory(TableIdent tableIdent) {
        try (TransactionRunner runner = new TransactionRunner()) {
            return runner.run(context -> {
               return tableMetaStore
                   .listTableSchemaHistory(context, tableIdent, Integer.MAX_VALUE, null,
                       TransactionIsolationLevel.SERIALIZABLE,
                       VersionManagerHelper.getLatestVersion(tableIdent));
            });
        }
    }

    public static TableSchemaHistoryObject getLatestTableSchemaOrElseThrow(TransactionContext context, TableIdent tableIdent,
        String basedVersion) throws MetaStoreException {
        Optional<TableSchemaHistoryObject> tableSchemaHistory = getLatestTableSchema(context, tableIdent, basedVersion);
        if (!tableSchemaHistory.isPresent()) {
            throw new CatalogServerException(ErrorCode.TABLE_SCHEMA_NOT_FOUND, tableIdent.getTableId());
        }
        return tableSchemaHistory.get();
    }


    public static TableSchemaObject getTableSchemaOrElseThrow(TransactionContext context, TableIdent tableIdent)
        throws MetaStoreException {
        TableSchemaObject tableSchemaObject = getTableSchema(context, tableIdent);
        if (tableSchemaObject == null) {
            throw new CatalogServerException(ErrorCode.TABLE_SCHEMA_NOT_FOUND, tableIdent.getTableId());
        }
        return tableSchemaObject;
    }

    public static Optional<TableSchemaHistoryObject> getLatestTableSchema(TransactionContext context, TableIdent tableIdent,
        String basedVersion) {
        Optional<TableSchemaHistoryObject> tableSchemaHistory = tableMetaStore.getLatestTableSchema(context, tableIdent,
            basedVersion);
        if (!tableSchemaHistory.isPresent()) {
            tableSchemaHistory = getLatestSubBranchFakeTableSchema(context, tableIdent, basedVersion);
        }
        return tableSchemaHistory;
    }

    public static TableSchemaObject getTableSchema(TransactionContext context, TableIdent tableIdent) {
        TableSchemaObject tableSchema = tableMetaStore.getTableSchema(context, tableIdent);
        if (null == tableSchema) {
            tableSchema = getSubBranchFakeTableSchema(context, tableIdent);
            if (null == tableSchema) {
                return null;
            }
        }
        return tableSchema;
    }

    private static TableSchemaObject getSubBranchFakeTableSchema(TransactionContext context, TableIdent subBranchTableIdent)
        throws MetaStoreException {
        ParentBranchDatabaseIterator parentBranchDatabaseIterator = new ParentBranchDatabaseIterator(context,
            subBranchTableIdent);

        while (parentBranchDatabaseIterator.hasNext()) {
            DatabaseIdent parentDatabaseIdent = parentBranchDatabaseIterator.nextDatabase();
            String subBranchVersion = parentBranchDatabaseIterator.nextBranchVersion(parentDatabaseIdent);
            TableIdent parentBranchTableIdent = StoreConvertor.tableIdent(parentDatabaseIdent,
                subBranchTableIdent.getTableId());
            Optional<TableSchemaHistoryObject> tableSchemaHistory = tableMetaStore.getLatestTableSchema(context,
                parentBranchTableIdent, subBranchVersion);
            if (tableSchemaHistory.isPresent()) {
                return tableSchemaHistory.get().getTableSchemaObject();
            }
        }

        return null;
    }

    private static Optional<TableSchemaHistoryObject> getLatestSubBranchFakeTableSchema(TransactionContext context,
        TableIdent subBranchTableIdent,
        String version) throws MetaStoreException {
        ParentBranchDatabaseIterator parentBranchDatabaseIterator = new ParentBranchDatabaseIterator(context,
            subBranchTableIdent);

        while (parentBranchDatabaseIterator.hasNext()) {
            DatabaseIdent parentDatabaseIdent = parentBranchDatabaseIterator.nextDatabase();
            String subBranchVersion = parentBranchDatabaseIterator.nextBranchVersion(parentDatabaseIdent);

            //The query version must be earlier than the branch creation version.
            TableIdent parentTableIdent = StoreConvertor.tableIdent(parentDatabaseIdent,
                subBranchTableIdent.getTableId());

            String baseVersionStamp = version.compareTo(subBranchVersion) < 0 ? version : subBranchVersion;

            Optional<TableSchemaHistoryObject> tableSchemaHistory = tableMetaStore.getLatestTableSchema(context, parentTableIdent,
                baseVersionStamp);
            if (tableSchemaHistory.isPresent()) {
                return Optional.of(tableSchemaHistory.get());
            }
        }

        return Optional.empty();
    }
}
