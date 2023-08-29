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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.ScanRecordCursorResult;
import io.polycat.catalog.common.model.SerDeInfo;
import io.polycat.catalog.common.model.StorageDescriptor;
import io.polycat.catalog.common.model.TableIdent;
import io.polycat.catalog.common.model.TableStorageHistoryObject;
import io.polycat.catalog.common.model.TableStorageObject;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.TransactionIsolationLevel;
import io.polycat.catalog.server.util.TransactionRunner;
import io.polycat.catalog.store.api.TableMetaStore;
import io.polycat.catalog.store.common.StoreConvertor;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class TableStorageHelper {

    private static TableMetaStore tableMetaStore;

    @Autowired
    public void setTableMetaStore(TableMetaStore tableMetaStore) {
        this.tableMetaStore = tableMetaStore;
    }

    public static ScanRecordCursorResult<List<TableStorageHistoryObject>> listTableStorageHistory(
        TableIdent tableIdent) {
        try (TransactionRunner runner = new TransactionRunner()) {
            return runner.run(context -> {
                return tableMetaStore.listTableStorageHistory(context, tableIdent, Integer.MAX_VALUE, null,
                    TransactionIsolationLevel.SERIALIZABLE,
                    VersionManagerHelper.getLatestVersion(context, tableIdent.getProjectId(),
                        tableIdent.getRootCatalogId()));
            });
        }
    }

    public static TableStorageObject getTableStorage(TransactionContext context, TableIdent tableIdent) {
        TableStorageObject tableStorage = tableMetaStore.getTableStorage(context, tableIdent);
        if (tableStorage == null) {
            tableStorage = getSubBranchFakeTableStorage(context, tableIdent);
        }
        return tableStorage;
    }

    public static Optional<TableStorageHistoryObject> getLatestTableStorage(TransactionContext context,
        TableIdent tableIdent,
        String basedVersion) {
        Optional<TableStorageHistoryObject> tableStorageHistory = tableMetaStore.getLatestTableStorage(context, tableIdent,
            basedVersion);
        if (!tableStorageHistory.isPresent()) {
            tableStorageHistory = getLatestSubBranchFakeTableStorage(context, tableIdent, basedVersion);
        }

        return tableStorageHistory;
    }

    public static TableStorageHistoryObject getLatestTableStorageOrElseThrow(TransactionContext context,
        TableIdent tableIdent,
        String basedVersion) throws MetaStoreException {
        Optional<TableStorageHistoryObject> tableStorageHistory = getLatestTableStorage(context, tableIdent,
            basedVersion);
        if (!tableStorageHistory.isPresent()) {
            throw new MetaStoreException(ErrorCode.TABLE_STORAGE_NOT_FOUND, tableIdent.getTableId());
        }
        return tableStorageHistory.get();
    }

    private static TableStorageObject getSubBranchFakeTableStorage(TransactionContext context,
        TableIdent subBranchTableIdent)
        throws MetaStoreException {
        ParentBranchDatabaseIterator parentBranchDatabaseIterator = new ParentBranchDatabaseIterator(context,
            subBranchTableIdent);

        while (parentBranchDatabaseIterator.hasNext()) {
            DatabaseIdent parentDatabaseIdent = parentBranchDatabaseIterator.nextDatabase();
            String subBranchVersion = parentBranchDatabaseIterator.nextBranchVersion(parentDatabaseIdent);

            TableIdent parentBranchTableIdent = StoreConvertor.tableIdent(parentDatabaseIdent,
                subBranchTableIdent.getTableId());
            Optional<TableStorageHistoryObject> tableStorageHistory = tableMetaStore.getLatestTableStorage(context,
                parentBranchTableIdent,
                subBranchVersion);

            if (tableStorageHistory.isPresent()) {
                //todo fill table storage
                return tableStorageHistory.get().getTableStorageObject();
            }
        }

        return null;
    }

    private static Optional<TableStorageHistoryObject> getLatestSubBranchFakeTableStorage(TransactionContext context,
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
            Optional<TableStorageHistoryObject> tableStorageHistory = tableMetaStore.getLatestTableStorage(context,
                parentTableIdent,
                baseVersionStamp);
            if (tableStorageHistory.isPresent()) {
                return tableStorageHistory;
            }
        }
        return Optional.empty();
    }

    public static void updateTableStorageObj(TableStorageObject tableStorageObject, StorageDescriptor storageDescriptor) {
        if (StringUtils.isNotEmpty(storageDescriptor.getFileFormat())) {
            tableStorageObject.setFileFormat(storageDescriptor.getFileFormat());
        }
        if (StringUtils.isNotEmpty(storageDescriptor.getInputFormat())) {
            tableStorageObject.setFileFormat(storageDescriptor.getInputFormat());
        }
        if (StringUtils.isNotEmpty(storageDescriptor.getLocation())) {
            tableStorageObject.setLocation(storageDescriptor.getLocation());
        }
        if (StringUtils.isNotEmpty(storageDescriptor.getOutputFormat())) {
            tableStorageObject.setOutputFormat(storageDescriptor.getOutputFormat());
        }
        if (StringUtils.isNotEmpty(storageDescriptor.getSourceShortName())) {
            tableStorageObject.setSourceShortName(storageDescriptor.getSourceShortName());
        }
        if (storageDescriptor.getSortColumns() != null && !storageDescriptor.getSortColumns().isEmpty()) {
            tableStorageObject.setSortColumns(storageDescriptor.getSortColumns());
        }
        if(storageDescriptor.getBucketColumns() != null && !storageDescriptor.getBucketColumns().isEmpty()) {
            tableStorageObject.setBucketColumns(storageDescriptor.getBucketColumns());
        }
        if (storageDescriptor.getNumberOfBuckets() != null) {
            tableStorageObject.setNumberOfBuckets(storageDescriptor.getNumberOfBuckets());
        }
        if (storageDescriptor.getParameters() != null && !storageDescriptor.getParameters().isEmpty()) {
            final HashMap<String, String> params = new HashMap<>();
            if (tableStorageObject.getParameters() != null) {
                params.putAll(tableStorageObject.getParameters());
            }
            params.putAll(storageDescriptor.getParameters());
            tableStorageObject.setParameters(params);
        }
        if (storageDescriptor.getCompressed() != null) {
            tableStorageObject.setCompressed(storageDescriptor.getCompressed());
        }
        if (storageDescriptor.getStoredAsSubDirectories() != null) {
            tableStorageObject.setStoredAsSubDirectories(storageDescriptor.getStoredAsSubDirectories());
        }
        if (storageDescriptor.getSerdeInfo() != null) {
            final SerDeInfo serdeInfo = storageDescriptor.getSerdeInfo();
            final SerDeInfo tableStorageObjectSerdeInfo = tableStorageObject.getSerdeInfo();
            if (StringUtils.isNotEmpty(serdeInfo.getName())) {
                tableStorageObjectSerdeInfo.setName(serdeInfo.getName());
            }
            if (StringUtils.isNotEmpty(serdeInfo.getSerializationLibrary())) {
                tableStorageObjectSerdeInfo.setSerializationLibrary(serdeInfo.getSerializationLibrary());
            }
            if (serdeInfo.getParameters() != null && !serdeInfo.getParameters().isEmpty()) {
                Map<String, String> params = new LinkedHashMap<>();
                if (tableStorageObjectSerdeInfo.getParameters() != null) {
                    params.putAll(tableStorageObjectSerdeInfo.getParameters());
                }
                params.putAll(serdeInfo.getParameters());
                tableStorageObjectSerdeInfo.setParameters(params);
            }
        }


    }

}
