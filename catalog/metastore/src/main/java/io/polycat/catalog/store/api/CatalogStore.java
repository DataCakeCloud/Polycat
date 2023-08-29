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
import java.util.Optional;

import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.model.CatalogCommitObject;
import io.polycat.catalog.common.model.CatalogHistoryObject;
import io.polycat.catalog.common.model.CatalogId;
import io.polycat.catalog.common.model.CatalogIdent;
import io.polycat.catalog.common.model.CatalogName;
import io.polycat.catalog.common.model.CatalogObject;
import io.polycat.catalog.common.model.ScanRecordCursorResult;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.TransactionIsolationLevel;


public interface CatalogStore {

    /**
     * catalog
     */
    void createCatalogSubspace(TransactionContext context, String projectId) throws MetaStoreException;

    void dropCatalogSubspace(TransactionContext context, String projectId) throws MetaStoreException;

    String generateCatalogId(TransactionContext context, String projectId) throws MetaStoreException;

    void insertCatalog(TransactionContext context, CatalogIdent catalogIdent, CatalogObject catalogObject) throws MetaStoreException;

    void updateCatalog(TransactionContext context, CatalogObject catalogObject, CatalogObject newCatalogObject) throws MetaStoreException;

    void deleteCatalog(TransactionContext context, CatalogName catalogName, CatalogIdent catalogIdent) throws MetaStoreException;

    CatalogObject getCatalogById(TransactionContext context, CatalogIdent catalogIdent);

    CatalogId getCatalogId(TransactionContext context, String projectId, String catalogName) throws MetaStoreException;

    CatalogObject getCatalogByName(TransactionContext context, CatalogName catalogName)
        throws MetaStoreException;

    ScanRecordCursorResult<List<CatalogObject>> listCatalog(TransactionContext context,
        String projectId, int maxNum, byte[] continuation, TransactionIsolationLevel isolationLevel) throws MetaStoreException;

    /**
     * catalog history
     */

    void createCatalogHistorySubspace(TransactionContext context, String projectId) throws MetaStoreException;

    void dropCatalogHistorySubspace(TransactionContext context, String projectId) throws MetaStoreException;

    void deleteCatalogHistory(TransactionContext context, CatalogIdent catalogIdent) throws MetaStoreException;

    void insertCatalogHistory(TransactionContext context, CatalogIdent catalogIdent, CatalogObject catalogObject,
        String version) throws MetaStoreException;

    CatalogHistoryObject getLatestCatalogHistory(TransactionContext context, CatalogIdent catalogIdent,
        String latestVersion) throws MetaStoreException;

    CatalogHistoryObject getCatalogHistoryByVersion(TransactionContext context, CatalogIdent catalogIdent,
        String version) throws MetaStoreException;

    /**
     * catalog commit
     */

    void createCatalogCommitSubspace(TransactionContext context, String projectId) throws MetaStoreException;

    void dropCatalogCommitSubspace(TransactionContext context, String projectId) throws MetaStoreException;

    void deleteCatalogCommit(TransactionContext context, CatalogIdent catalogIdent) throws MetaStoreException;

    void insertCatalogCommit(TransactionContext context, String projectId, String catalogId, String commitId,
        long commitTime, Operation operation, String detail, String version) throws MetaStoreException;

    // todo : after all user insertCatalogCommit with version, this interface will delete
    void insertCatalogCommit(TransactionContext context, String projectId, String catalogId, String commitId,
        long commitTime, Operation operation, String detail) throws MetaStoreException;

    Boolean catalogCommitExist(TransactionContext context, CatalogIdent catalogIdent, String commitId)
        throws MetaStoreException;

    // todo : after all user catalogCommitExist, this interface will delete
    Optional<CatalogCommitObject> getCatalogCommit(TransactionContext context, CatalogIdent catalogIdent,
        String commitId) throws MetaStoreException;

    CatalogCommitObject getLatestCatalogCommit(TransactionContext context, CatalogIdent catalogIdent,
        String baseVersion) throws MetaStoreException;

    ScanRecordCursorResult<List<CatalogCommitObject>> listCatalogCommit(TransactionContext context,
        CatalogIdent catalogIdent, int maxNum, byte[] continuation, byte[] version);


    /**
     * branch
     */
    void createBranchSubspace(TransactionContext context, String projectId);

    void dropBranchSubspace(TransactionContext context, String projectId);

    void deleteBranch(TransactionContext context, CatalogIdent catalogIdent);

    boolean hasSubBranchCatalog(TransactionContext context, CatalogIdent parentBranchCatalogIdent);

    void insertCatalogSubBranch(TransactionContext context, CatalogIdent catalogIdent, String subBranchCatalogId,
        String parentVersion);

    List<CatalogObject> getParentBranchCatalog(TransactionContext context, CatalogIdent subbranchCatalogIdent);

    List<CatalogObject> getNextLevelSubBranchCatalogs(TransactionContext context, CatalogIdent parentBranchCatalogIdent);

}
