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
import io.polycat.catalog.common.model.DatabaseHistoryObject;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.DatabaseObject;
import io.polycat.catalog.common.model.DroppedDatabaseNameObject;
import io.polycat.catalog.common.model.ScanRecordCursorResult;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.TransactionIsolationLevel;


public interface DatabaseStore {

    /**
     * database
     */

    void createDatabaseSubspace(TransactionContext context, String projectId);

    void dropDatabaseSubspace(TransactionContext context, String projectId);

    String generateDatabaseId(TransactionContext context, String projectId, String catalogId) throws MetaStoreException;

    void insertDatabase(TransactionContext context, DatabaseIdent databaseIdent, DatabaseObject databaseObject);

    void deleteDatabase(TransactionContext context, DatabaseIdent databaseIdent);

    DatabaseObject getDatabase(TransactionContext context, DatabaseIdent databaseIdent);

    List<DatabaseObject> getDatabaseByIds(TransactionContext ctx, String projectId, String catalogId, List<String> databaseIds);

    void updateDatabase(TransactionContext context, DatabaseIdent databaseIdent, DatabaseObject newDatabaseObject);

    void upsertDatabase(TransactionContext context, DatabaseIdent databaseIdent, DatabaseObject databaseObject);

    Optional<String> getDatabaseId(TransactionContext context, CatalogIdent catalogIdent,
        String databaseName);

    ScanRecordCursorResult<List<String>> listDatabaseId(TransactionContext context,
        CatalogIdent catalogIdent, int maxNum, byte[] continuation, TransactionIsolationLevel isolationLevel);

    /**
     * database history
     */

    void createDatabaseHistorySubspace(TransactionContext context, String projectId);

    void dropDatabaseHistorySubspace(TransactionContext context, String projectId);

    void insertDatabaseHistory(TransactionContext context, DatabaseHistoryObject databaseHistoryObject,
        DatabaseIdent databaseIdent, boolean dropped, String version);

    Optional<DatabaseHistoryObject> getLatestDatabaseHistory(TransactionContext context, DatabaseIdent databaseIdent,
       String basedVersion);

    Map<String, DatabaseHistoryObject> getLatestDatabaseHistoryByIds(TransactionContext context, String projectId, String catalogId,
                                                                     String basedVersion, List<String> databaseIds);

    Optional<DatabaseHistoryObject> getDatabaseHistory(TransactionContext context, DatabaseIdent databaseIdent,
       String baseVersion);

    ScanRecordCursorResult<List<DatabaseHistoryObject>> listDatabaseHistory(TransactionContext context, DatabaseIdent databaseIdent,
        int maxBatchRowNum, byte[] continuation, String baseVersion);


    /**
     * database dropped object name
     */

    void createDroppedDatabaseNameSubspace(TransactionContext context, String projectId);

    void dropDroppedDatabaseNameSubspace(TransactionContext context, String projectId);

    void deleteDroppedDatabaseObjectName(TransactionContext context, DatabaseIdent databaseIdent,
        String databaseName);

    DroppedDatabaseNameObject getDroppedDatabaseObjectName(TransactionContext context, DatabaseIdent databaseIdent,
        String databaseName);

    ScanRecordCursorResult<List<DroppedDatabaseNameObject>> listDroppedDatabaseObjectName(TransactionContext context,
        CatalogIdent catalogIdent, int maxNum, byte[] continuation, TransactionIsolationLevel isolationLevel);

    void insertDroppedDatabaseObjectName(TransactionContext context, DatabaseIdent databaseIdent,
        String databaseName);
}
