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
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.store.api.UserGroupStore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import java.util.HashSet;

@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "gauss")
public class UserGroupStoreImpl implements UserGroupStore {
    @Override
    public String getUserIdByName(TransactionContext context, String projectId, int userSource, String userName) throws MetaStoreException {
        return null;
    }

    @Override
    public void insertUserObject(TransactionContext context, String projectId, int userSource, String userName, String userId) throws MetaStoreException {

    }

    @Override
    public void deleteUserObjectById(TransactionContext context, String projectId, String userId) throws MetaStoreException {

    }

    @Override
    public HashSet<String> getAllUserIdByDomain(TransactionContext context, String projectId, String domainId) throws MetaStoreException {
        return null;
    }

    @Override
    public String getGroupIdByName(TransactionContext context, String projectId, String groupName) throws MetaStoreException {
        return null;
    }

    @Override
    public void insertGroupObject(TransactionContext context, String projectId, String groupName, String groupId) throws MetaStoreException {

    }

    @Override
    public void deleteGroupObjectById(TransactionContext context, String projectId, String groupId) throws MetaStoreException {

    }

    @Override
    public HashSet<String> getAllGroupIdByDomain(TransactionContext context, String projectId, String domainId) throws MetaStoreException {
        return null;
    }

    @Override
    public void insertGroupUser(TransactionContext context, String projectId, String groupId, String userId) throws MetaStoreException {

    }

    @Override
    public HashSet<String> getUserIdSetByGroupId(TransactionContext context, String projectId, String groupId) {
        return null;
    }

    @Override
    public HashSet<String> getGroupIdSetByUserId(TransactionContext context, String projectId, String userId) throws MetaStoreException {
        return null;
    }

    @Override
    public void deleteGroupUser(TransactionContext context, String projectId, String groupId, String userId) throws MetaStoreException {

    }

    @Override
    public void delAllGroupUserByGroup(TransactionContext context, String projectId, String groupId) throws MetaStoreException {

    }

    @Override
    public void delAllGroupUserByUser(TransactionContext context, String projectId, String userId) throws MetaStoreException {

    }
}
