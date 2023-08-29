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
import io.polycat.catalog.common.model.ShareConsumerObject;
import io.polycat.catalog.common.model.ShareNameObject;
import io.polycat.catalog.common.model.ShareObject;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.TransactionIsolationLevel;

public interface GlobalShareStore {

    Boolean shareObjectNameExist(TransactionContext context, String projectId, String shareName)
        throws MetaStoreException;

    void insertShareProperties(TransactionContext context, String projectId, String shareId, String shareName,
        String catalogId, String ownerAccount, String ownerUser) throws MetaStoreException;

    void updateShareProperties(TransactionContext context, String projectId, ShareObject shareObject)
        throws MetaStoreException;

    ShareObject getSharePropertiesById(TransactionContext context, String projectId, String shareId)
        throws MetaStoreException;

    ShareObject getSharePropertiesByName(TransactionContext context, String projectId, String shareId)
        throws MetaStoreException;

    String getShareId(TransactionContext context, String projectId, String shareName)
        throws MetaStoreException;

    void deleteGlobalShareProperties(TransactionContext context, String projectId, String shareId)
        throws MetaStoreException;

    List<ShareObject> listShareObject(TransactionContext context,
        String projectId, String namePattern, int maxNum, byte[] continuation, TransactionIsolationLevel isolationLevel)
        throws MetaStoreException;

    void insertShareConsumer(TransactionContext context, String projectId, String shareId, String accountId,
        String managerUser) throws MetaStoreException;

    void addUsersToShareConsumer(TransactionContext context, String projectId, String shareId, String accountId,
        String[] users) throws MetaStoreException;

    List<ShareConsumerObject> getShareAllConsumers(TransactionContext context, String projectId, String shareId)
        throws MetaStoreException;

    void removeUsersFromShareConsumer(TransactionContext context, String projectId, String shareId, String accountId,
        String[] users) throws MetaStoreException;

    void deleteShareConsumer(TransactionContext context, String projectId, String shareId, String accountId)
        throws MetaStoreException;

    void delAllShareConsumer(TransactionContext context, String projectId, String shareId)
        throws MetaStoreException;

    List<ShareConsumerObject> getShareAllConsumersWithAccount(TransactionContext context, String accountId);
}
