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
package io.polycat.catalog.store.gaussdb;

import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.*;
import io.polycat.catalog.store.api.ShareStore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "gauss")
public class ShareStoreImpl implements ShareStore {
    @Override
    public Boolean shareObjectNameExist(TransactionContext context, String projectId, String shareName) throws MetaStoreException {
        return null;
    }

    @Override
    public void insertShareObjectName(TransactionContext context, String projectId, String shareName, String shareId) throws MetaStoreException {

    }

    @Override
    public String getShareId(TransactionContext context, String projectId, String shareName) throws MetaStoreException {
        return null;
    }

    @Override
    public void deleteShareObjectName(TransactionContext context, String projectId, String shareName) throws MetaStoreException {

    }

    @Override
    public ScanRecordCursorResult<List<ShareNameObject>> listShareObjectName(TransactionContext context, String projectId, String namePattern, int maxNum, byte[] continuation, TransactionIsolationLevel isolationLevel) throws MetaStoreException {
        return null;
    }

    @Override
    public void insertShareProperties(TransactionContext context, String projectId, String shareId, String shareName, String ownerAccount, String ownerUser) throws MetaStoreException {

    }

    @Override
    public void updateShareProperties(TransactionContext context, String projectId, ShareObject shareObject) throws MetaStoreException {

    }

    @Override
    public ShareObject getShareProperties(TransactionContext context, String projectId, String shareId) throws MetaStoreException {
        return null;
    }

    @Override
    public void deleteShareProperties(TransactionContext context, String projectId, String shareId) throws MetaStoreException {

    }

    @Override
    public void insertShareConsumer(TransactionContext context, String projectId, String shareId, String accountId, String managerUser) throws MetaStoreException {

    }

    @Override
    public void addUsersToShareConsumer(TransactionContext context, String projectId, String shareId, String accountId, String[] users) throws MetaStoreException {

    }

    @Override
    public Boolean shareConsumerExist(TransactionContext context, String projectId, String shareId, String accountId) throws MetaStoreException {
        return null;
    }

    @Override
    public ShareConsumerObject getShareConsumers(TransactionContext context, String projectId, String shareId, String accountId) throws MetaStoreException {
        return null;
    }

    @Override
    public List<ShareConsumerObject> getShareAllConsumers(TransactionContext context, String projectId, String shareId) throws MetaStoreException {
        return null;
    }

    @Override
    public void removeUsersFromShareConsumer(TransactionContext context, String projectId, String shareId, String accountId, String[] users) throws MetaStoreException {

    }

    @Override
    public void deleteShareConsumer(TransactionContext context, String projectId, String shareId, String accountId) throws MetaStoreException {

    }

    @Override
    public void delAllShareConsumer(TransactionContext context, String projectId, String shareId) throws MetaStoreException {

    }

    @Override
    public void insertSharePrivilege(TransactionContext context, String projectId, String shareId, String databaseId, String objectType, String objectId, long privilege) throws MetaStoreException {

    }

    @Override
    public void updateSharePrivilege(TransactionContext context, String projectId, String shareId, String databaseId, String objectType, String objectId, long privilege) throws MetaStoreException {

    }

    @Override
    public long getSharePrivilege(TransactionContext context, String projectId, String shareId, String databaseId, String objectType, String objectId) throws MetaStoreException {
        return 0;
    }

    @Override
    public List<SharePrivilegeObject> getAllSharePrivilege(TransactionContext context, String projectId, String shareId) throws MetaStoreException {
        return null;
    }

    @Override
    public void deleteSharePrivilege(TransactionContext context, String projectId, String shareId, String databaseId, String objectType, String objectId) throws MetaStoreException {

    }

    @Override
    public void delAllSharePrivilege(TransactionContext context, String projectId, String shareId) throws MetaStoreException {

    }
}
