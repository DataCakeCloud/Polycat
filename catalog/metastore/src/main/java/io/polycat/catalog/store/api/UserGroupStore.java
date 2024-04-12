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

import java.util.HashSet;

import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.TransactionContext;

public interface UserGroupStore {

    // user
    String  getUserIdByName(TransactionContext context, String projectId, int userSource, String userName)
        throws MetaStoreException;

    void insertUserObject(TransactionContext context, String projectId, int userSource,
        String userName, String userId)
        throws MetaStoreException;

    void deleteUserObjectById(TransactionContext context, String projectId, String userId)
        throws MetaStoreException;

    HashSet<String> getAllUserIdByDomain(TransactionContext context, String projectId, String domainId) throws MetaStoreException;


    //group
    String  getGroupIdByName(TransactionContext context, String projectId, String groupName)
        throws MetaStoreException;

    void insertGroupObject(TransactionContext context, String projectId, String groupName, String groupId)
        throws MetaStoreException;

    void deleteGroupObjectById(TransactionContext context, String projectId, String groupId) throws MetaStoreException;

    HashSet<String> getAllGroupIdByDomain(TransactionContext context, String projectId, String domainId)
        throws MetaStoreException;

    // group-user
    void insertGroupUser(TransactionContext context, String projectId, String groupId, String userId)
        throws MetaStoreException;


    HashSet<String> getUserIdSetByGroupId(TransactionContext context, String projectId, String groupId);

    HashSet<String> getGroupIdSetByUserId(TransactionContext context, String projectId, String userId)
        throws MetaStoreException;

    void deleteGroupUser(TransactionContext context, String projectId, String groupId, String userId)
        throws MetaStoreException;

    void delAllGroupUserByGroup(TransactionContext context, String projectId, String groupId) throws MetaStoreException;

    void delAllGroupUserByUser(TransactionContext context, String projectId, String userId) throws MetaStoreException;

}
