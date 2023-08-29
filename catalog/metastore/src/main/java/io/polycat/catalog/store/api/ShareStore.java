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

import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.ScanRecordCursorResult;
import io.polycat.catalog.common.model.ShareConsumerObject;
import io.polycat.catalog.common.model.ShareNameObject;
import io.polycat.catalog.common.model.ShareObject;
import io.polycat.catalog.common.model.SharePrivilegeObject;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.TransactionIsolationLevel;

public interface ShareStore {

    Boolean shareObjectNameExist(TransactionContext context, String projectId, String shareName)
        throws MetaStoreException;

    void insertShareObjectName(TransactionContext context, String projectId, String shareName, String shareId)
        throws MetaStoreException;

    String getShareId(TransactionContext context, String projectId, String shareName) throws MetaStoreException;

    void deleteShareObjectName(TransactionContext context, String projectId, String shareName) throws MetaStoreException;

    ScanRecordCursorResult<List<ShareNameObject>> listShareObjectName(TransactionContext context,
        String projectId, String namePattern, int maxNum, byte[] continuation, TransactionIsolationLevel isolationLevel)
        throws MetaStoreException;

    void insertShareProperties(TransactionContext context, String projectId, String shareId, String shareName,
        String ownerAccount, String ownerUser) throws MetaStoreException;

    void updateShareProperties(TransactionContext context, String projectId, ShareObject shareObject)
        throws MetaStoreException;

    ShareObject getShareProperties(TransactionContext context, String projectId, String shareId) throws MetaStoreException;

    void deleteShareProperties(TransactionContext context, String projectId, String shareId)  throws MetaStoreException;

    void insertShareConsumer(TransactionContext context, String projectId, String shareId, String accountId,
        String managerUser) throws MetaStoreException;

    void addUsersToShareConsumer(TransactionContext context, String projectId, String shareId, String accountId,
        String[] users) throws MetaStoreException;

    Boolean shareConsumerExist(TransactionContext context, String projectId, String shareId,
        String accountId) throws MetaStoreException;

    ShareConsumerObject getShareConsumers(TransactionContext context, String projectId, String shareId,
        String accountId) throws MetaStoreException;

    List<ShareConsumerObject> getShareAllConsumers(TransactionContext context, String projectId, String shareId)
        throws MetaStoreException;

    void removeUsersFromShareConsumer(TransactionContext context, String projectId, String shareId, String accountId,
        String[] users) throws MetaStoreException;

    void deleteShareConsumer(TransactionContext context, String projectId, String shareId, String accountId)
        throws MetaStoreException;

    void delAllShareConsumer(TransactionContext context, String projectId, String shareId) throws MetaStoreException;

    void insertSharePrivilege(TransactionContext context, String projectId, String shareId,
        String databaseId, String objectType, String objectId, long privilege) throws MetaStoreException;

    void updateSharePrivilege(TransactionContext context, String projectId, String shareId,
        String databaseId, String objectType, String objectId, long privilege) throws MetaStoreException;

    long getSharePrivilege(TransactionContext context, String projectId, String shareId,
        String databaseId, String objectType, String objectId) throws MetaStoreException;

    List<SharePrivilegeObject> getAllSharePrivilege(TransactionContext context, String projectId,
        String shareId) throws MetaStoreException;

    void deleteSharePrivilege(TransactionContext context, String projectId, String shareId, String databaseId,
        String objectType, String objectId) throws MetaStoreException;

    void delAllSharePrivilege(TransactionContext context, String projectId, String shareId) throws MetaStoreException;
}
