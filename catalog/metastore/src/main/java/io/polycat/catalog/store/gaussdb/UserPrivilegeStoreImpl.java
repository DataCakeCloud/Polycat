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

import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.UserPrivilege;
import io.polycat.catalog.store.api.UserPrivilegeStore;
import io.polycat.catalog.store.common.StoreSqlConvertor;
import io.polycat.catalog.store.mapper.UserPrivilegeMapper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.util.List;

@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "gauss")
public class UserPrivilegeStoreImpl implements UserPrivilegeStore {

    private static final Logger log = Logger.getLogger(UserPrivilegeStoreImpl.class);

    @Autowired
    private UserPrivilegeMapper userPrivilegeMapper;


    @Override
    public void createUserPrivilegeSubspace(TransactionContext context, String projectId) throws MetaStoreException {
        try {
            userPrivilegeMapper.createUserPrivilegeSubspace(projectId);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void dropUserPrivilegeSubspace(TransactionContext context, String projectId) throws MetaStoreException {
        try {
            userPrivilegeMapper.dropUserPrivilegeSubspace(projectId);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void insertUserPrivilege(TransactionContext context, String projectId, String userId, String objectType,
        String objectId, Boolean isOwner, long privilege) throws MetaStoreException {

        try {
            userPrivilegeMapper.insertUserPrivilege(projectId, userId, objectType, objectId, isOwner, privilege);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public UserPrivilege getUserPrivilege(TransactionContext context, String projectId, String userId, String objectType,
                                          String objectId) throws MetaStoreException {
        try {
            return userPrivilegeMapper.getUserPrivilege(projectId, userId, objectType, objectId);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public List<UserPrivilege> getUserPrivileges(TransactionContext context, String projectId, String userId, String objectType) throws MetaStoreException {
        try {
            String filterSql = StoreSqlConvertor.get().equals(UserPrivilege.Fields.userId, userId).AND()
                    .equals(UserPrivilege.Fields.objectType, objectType).AND()
                    .equals(UserPrivilege.Fields.isOwner, true).getFilterSql();
            return userPrivilegeMapper.getUserPrivilegesByFilter(projectId, filterSql);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void deleteUserPrivilege(TransactionContext context, String projectId, String userId, String objectType,
                                    String objectId) throws MetaStoreException {
        try {
            userPrivilegeMapper.deleteUserPrivilege(projectId, userId, objectType, objectId);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public byte[] deleteUserPrivilegeByToken(TransactionContext context, String projectId, String objectType,
        String objectId, byte[] continuation) throws MetaStoreException {
        return new byte[0];
    }
}
