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
import io.polycat.catalog.store.api.NewRoleStore;
import io.polycat.catalog.common.model.*;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "gauss")
public class NewRoleStoreImpl implements NewRoleStore {
    @Override
    public Boolean roleNameExist(TransactionContext context, String projectId, String roleName) throws MetaStoreException {
        return null;
    }

    @Override
    public void insertRoleProperties(TransactionContext context, String projectId, String roleId, String roleName, String ownerId) throws MetaStoreException {

    }

    @Override
    public void updateRoleProperties(TransactionContext context, RoleObject roleObject) throws MetaStoreException {

    }

    @Override
    public RoleObject getRolePropertiesByRoleId(TransactionContext context, String projectId, String roleId) throws MetaStoreException {
        return null;
    }

    @Override
    public RoleObject getRolePropertiesByRoleName(TransactionContext context, String projectId, String roleName) throws MetaStoreException {
        return null;
    }

    @Override
    public String getRoleId(TransactionContext context, String projectId, String roleName) throws MetaStoreException {
        return null;
    }

    @Override
    public void deleteRoleProperties(TransactionContext context, String projectId, String roleId) throws MetaStoreException {

    }

    @Override
    public void insertRolePrincipal(TransactionContext context, String projectId, String roleId, PrincipalType principalType, PrincipalSource principalSource, String PrincipalId) throws MetaStoreException {

    }

    @Override
    public List<RolePrincipalObject> getRolePrincipalsByRoleId(TransactionContext context, String projectId, String roleId) throws MetaStoreException {
        return null;
    }

    @Override
    public List<RolePrincipalObject> getRolePrincipalsByUserId(TransactionContext context, String projectId, PrincipalType principalType, PrincipalSource principalSource, String principalId) throws MetaStoreException {
        return null;
    }

    @Override
    public void deleteRolePrincipal(TransactionContext context, String projectId, String roleId, PrincipalType principalType, PrincipalSource principalSource, String principalId) throws MetaStoreException {

    }

    @Override
    public void delAllRolePrincipal(TransactionContext context, String projectId, String roleId) throws MetaStoreException {

    }
}
