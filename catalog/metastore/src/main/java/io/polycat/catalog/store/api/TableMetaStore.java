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
package io.polycat.catalog.store.api;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.CatalogIdent;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.DroppedTableObject;
import io.polycat.catalog.common.model.ScanRecordCursorResult;
import io.polycat.catalog.common.model.TableBaseHistoryObject;
import io.polycat.catalog.common.model.TableBaseObject;
import io.polycat.catalog.common.model.TableCommitObject;
import io.polycat.catalog.common.model.TableIdent;
import io.polycat.catalog.common.model.TableName;
import io.polycat.catalog.common.model.TableNameObject;
import io.polycat.catalog.common.model.TableObject;
import io.polycat.catalog.common.model.TableReferenceObject;
import io.polycat.catalog.common.model.TableSchemaHistoryObject;
import io.polycat.catalog.common.model.TableSchemaObject;
import io.polycat.catalog.common.model.TableStorageHistoryObject;
import io.polycat.catalog.common.model.TableStorageObject;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.TransactionIsolationLevel;


public interface TableMetaStore {

    /*
    table subspace
     */

    void createTableSubspace(TransactionContext context, CatalogIdent catalogIdent) throws MetaStoreException;

    void dropTableSubspace(TransactionContext context, CatalogIdent catalogIdent) throws MetaStoreException;

    String generateTableId(TransactionContext context, String projectId, String catalogId) throws MetaStoreException;

    void insertTable(TransactionContext context, TableIdent tableIdent, String tableName,
        int historySubspaceFlag, TableBaseObject tableBaseObject, TableSchemaObject tableSchemaObject,
        TableStorageObject tableStorageObject) throws MetaStoreException;

    void upsertTable(TransactionContext context, TableIdent tableIdent, String tableName,
        int historySubspaceFlag, TableBaseObject tableBaseObject, TableSchemaObject tableSchemaObject,
        TableStorageObject tableStorageObject) throws MetaStoreException;

    TableObject getTable(TransactionContext context, TableIdent tableIdent,
        TableName tableName) throws MetaStoreException;

    int getTableHistorySubspaceFlag(TransactionContext context, TableIdent tableIdent) throws MetaStoreException;

    void deleteTable(TransactionContext context, TableIdent tableIdent, String tableName) throws MetaStoreException;

    /**
     * table object name
     */

    String getTableId(TransactionContext context, DatabaseIdent databaseIdent, String tableName)
        throws MetaStoreException;

    ScanRecordCursorResult<List<TableNameObject>> listTableName(TransactionContext context,
        DatabaseIdent databaseIdent, int maxNum, byte[] continuation, TransactionIsolationLevel isolationLevel)
        throws MetaStoreException;

    String createTableObjectNameTmpTable(TransactionContext context, DatabaseIdent databaseIdent) throws MetaStoreException;

    /**
     * table reference
     */

    void createTableReferenceSubspace(TransactionContext context, CatalogIdent catalogIdent) throws MetaStoreException;

    void dropTableReferenceSubspace(TransactionContext context, CatalogIdent catalogIdent) throws MetaStoreException;

    void insertTableReference(TransactionContext context, TableIdent tableIdent) throws MetaStoreException;

    void upsertTableReference(TransactionContext context, TableIdent tableIdent) throws MetaStoreException;

    TableReferenceObject getTableReference(TransactionContext context, TableIdent tableIdent) throws MetaStoreException;

    void deleteTableReference(TransactionContext context, TableIdent tableIdent) throws MetaStoreException;


    /**
     * table base
     */

    TableBaseObject getTableBase(TransactionContext context, TableIdent tableIdent) throws MetaStoreException;

    /**
     * table schema
     */

    TableSchemaObject getTableSchema(TransactionContext context, TableIdent tableIdent) throws MetaStoreException;


    /**
     * table storage
     */

    TableStorageObject getTableStorage(TransactionContext context, TableIdent tableIdent) throws MetaStoreException;

    /**
     * table base history subspace
     */

    void createTableBaseHistorySubspace(TransactionContext context, String projectId) throws MetaStoreException;

    void dropTableBaseHistorySubspace(TransactionContext context, TableIdent tableIdent) throws MetaStoreException;

    void insertTableBaseHistory(TransactionContext context, TableIdent tableIdent,
        String version, TableBaseObject tableBaseObject) throws MetaStoreException;

    Optional<TableBaseHistoryObject> getLatestTableBase(TransactionContext context,
        TableIdent tableIdent, String basedVersion) throws MetaStoreException;

    Map<String, TableBaseHistoryObject> getLatestTableBaseByIds(TransactionContext context,
                                                        String projectId, String catalogId, String databaseId, String basedVersion, List<String> tableIds) throws MetaStoreException;

    TableBaseHistoryObject getLatestTableBaseOrElseThrow(TransactionContext context, TableIdent tableIdent,
        String basedVersion) throws MetaStoreException;

    ScanRecordCursorResult<List<TableBaseHistoryObject>> listTableBaseHistory(TransactionContext context,
        TableIdent tableIdent, int maxNum, byte[] continuation, TransactionIsolationLevel isolationLevel,
        String baseVersion) throws MetaStoreException;

    byte[] deleteTableBaseHistory(TransactionContext context, TableIdent tableIdent, String startVersion,
        String endVersion, byte[] continuation) throws MetaStoreException;

    /**
     * table schema history subspace
     */

    void createTableSchemaHistorySubspace(TransactionContext context, String projectId) throws MetaStoreException;

    void dropTableSchemaHistorySubspace(TransactionContext context, String projectId) throws MetaStoreException;

    void insertTableSchemaHistory(TransactionContext context, TableIdent tableIdent, String version,
        TableSchemaObject tableSchemaObject) throws MetaStoreException;

    void upsertTableSchemaHistory(TransactionContext context, TableIdent tableIdent, String version,
        TableSchemaObject tableSchemaObject) throws MetaStoreException;

    Optional<TableSchemaHistoryObject> getLatestTableSchema(TransactionContext context, TableIdent tableIdent,
        String basedVersion) throws MetaStoreException;

    TableSchemaHistoryObject getLatestTableSchemaOrElseThrow(TransactionContext context, TableIdent tableIdent,
        String basedVersion) throws MetaStoreException;

    ScanRecordCursorResult<List<TableSchemaHistoryObject>> listTableSchemaHistory(TransactionContext context,
        TableIdent tableIdent, int maxNum, byte[] continuation, TransactionIsolationLevel isolationLevel,
        String baseVersion) throws MetaStoreException;

    List<TableSchemaHistoryObject> listSchemaHistoryFromTimePoint(TransactionContext context,
        String fromTime, TableIdent tableIdent) throws MetaStoreException;

    byte[] deleteTableSchemaHistory(TransactionContext context, TableIdent tableIdent, String startVersion,
        String endVersion, byte[] continuation) throws MetaStoreException;

    /**
     * table storage history subspace
     */

    void createTableStorageHistorySubspace(TransactionContext context, String projectId) throws MetaStoreException;

    void dropTableStorageHistorySubspace(TransactionContext context, String projectId) throws MetaStoreException;

    void insertTableStorageHistory(TransactionContext context, TableIdent tableIdent,
        String version, TableStorageObject tableStorageObject) throws MetaStoreException;

    Optional<TableStorageHistoryObject> getLatestTableStorage(TransactionContext context,
        TableIdent tableIdent, String basedVersion) throws MetaStoreException;

    TableStorageHistoryObject getLatestTableStorageOrElseThrow(TransactionContext context, TableIdent tableIdent,
        String basedVersion) throws MetaStoreException;

    ScanRecordCursorResult<List<TableStorageHistoryObject>> listTableStorageHistory(TransactionContext context,
        TableIdent tableIdent, int maxNum, byte[] continuation, TransactionIsolationLevel isolationLevel,
        String baseVersion) throws MetaStoreException;

    byte[] deleteTableStorageHistory(TransactionContext context, TableIdent tableIdent, String startVersion,
        String endVersion, byte[] continuation) throws MetaStoreException;

    /**
     * table commit subspace
     */

    void createTableCommitSubspace(TransactionContext context, String projectId) throws MetaStoreException;

    void dropTableCommitSubspace(TransactionContext context, String projectId) throws MetaStoreException;

    void insertTableCommit(TransactionContext context, TableIdent tableIdent, TableCommitObject tableCommit)
        throws MetaStoreException;

    Optional<TableCommitObject> getTableCommit(TransactionContext context, TableIdent tableIdent, String version)
        throws MetaStoreException;

    Optional<TableCommitObject> getLatestTableCommit(TransactionContext context, TableIdent tableIdent,
        String basedVersion) throws MetaStoreException;

    ScanRecordCursorResult<List<TableCommitObject>> listTableCommit(TransactionContext context,
        TableIdent tableIdent, int maxNum, byte[] continuation, TransactionIsolationLevel isolationLevel,
        String baseVersion) throws MetaStoreException;

    List<TableCommitObject> listTableCommit(TransactionContext context, TableIdent tableIdent,
        String startVersion, String endVersion) throws MetaStoreException;

    byte[] deleteTableCommit(TransactionContext context, TableIdent tableIdent, String startVersion, String endVersion,
        byte[] continuation) throws MetaStoreException;

    /**
     * table dropped object
     */

    void createDroppedTableSubspace(TransactionContext context, CatalogIdent catalogIdent) throws MetaStoreException;

    void dropDroppedTableSubspace(TransactionContext context, CatalogIdent catalogIdent) throws MetaStoreException;

    void insertDroppedTable(TransactionContext context, TableIdent tableIdent, TableName tableName,
        long createTime, long droppedTime, boolean isPurge) throws MetaStoreException;

    DroppedTableObject getDroppedTable(TransactionContext context, TableIdent tableIdent,
        String tableName) throws MetaStoreException;

    ScanRecordCursorResult<List<DroppedTableObject>> listDroppedTable(TransactionContext context,
        DatabaseIdent databaseIdent, int maxNum, byte[] continuation, TransactionIsolationLevel isolationLevel)
        throws MetaStoreException;

    List<DroppedTableObject> getDroppedTablesByName(TransactionContext context,
        DatabaseIdent databaseIdent, String tableName) throws MetaStoreException;

    void deleteDroppedTable(TransactionContext context, TableIdent tableIdent, TableName tableName)
        throws MetaStoreException;

    Map<String, TableCommitObject> getLatestTableCommitByIds(TransactionContext context, String projectId, String catalogId, String databaseId, String branchVersion, List<String> tableIds);

    Map<String, TableSchemaHistoryObject> getLatestTableSchemaByIds(TransactionContext context, String projectId, String catalogId, String databaseId, String branchVersion, List<String> tableIds);

    Map<String, TableStorageHistoryObject> getLatestTableStorageByIds(TransactionContext context, String projectId, String catalogId, String databaseId, String branchVersion, List<String> tableIds);

    Map<String, TableCommitObject> getLatestTableCommitByTmpTable(TransactionContext context, String projectId, String catalogId, String databaseId, String branchVersion, String tmpTable);

    Map<String, TableSchemaHistoryObject> getLatestTableSchemaByTmpTable(TransactionContext context, String projectId, String catalogId, String databaseId, String branchVersion, String tmpTable);

    Map<String, TableStorageHistoryObject> getLatestTableStorageByTmpTable(TransactionContext context, String projectId, String catalogId, String databaseId, String branchVersion, String tmpTable);

    Map<String, TableBaseHistoryObject> getLatestTableBaseByTmpTable(TransactionContext context, String projectId, String catalogId, String databaseId, String branchVersion, String tmpTable);
}
