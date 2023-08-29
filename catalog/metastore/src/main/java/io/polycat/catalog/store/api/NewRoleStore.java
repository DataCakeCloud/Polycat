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
import io.polycat.catalog.common.model.PrincipalSource;
import io.polycat.catalog.common.model.PrincipalType;
import io.polycat.catalog.common.model.RoleObject;
import io.polycat.catalog.common.model.RolePrincipalObject;
import io.polycat.catalog.common.model.ScanRecordCursorResult;
import io.polycat.catalog.common.model.TransactionContext;

import com.apple.foundationdb.record.IsolationLevel;

public interface NewRoleStore {

    Boolean roleNameExist(TransactionContext context, String projectId, String roleName) throws MetaStoreException;

    void insertRoleProperties(TransactionContext context, String projectId, String roleId, String roleName,
        String ownerId) throws MetaStoreException;

    void updateRoleProperties(TransactionContext context, RoleObject roleObject) throws MetaStoreException;

    RoleObject getRolePropertiesByRoleId(TransactionContext context, String projectId, String roleId) throws MetaStoreException;

    RoleObject getRolePropertiesByRoleName(TransactionContext context, String projectId, String roleName)
        throws MetaStoreException;

    String getRoleId(TransactionContext context, String projectId, String roleName)
        throws MetaStoreException;

    void deleteRoleProperties(TransactionContext context, String projectId, String roleId) throws MetaStoreException;

    public void insertRolePrincipal(TransactionContext context, String projectId, String roleId,
        PrincipalType principalType, PrincipalSource principalSource, String PrincipalId)
        throws MetaStoreException;

    List<RolePrincipalObject> getRolePrincipalsByRoleId(TransactionContext context, String projectId, String roleId)
        throws MetaStoreException;

    List<RolePrincipalObject> getRolePrincipalsByUserId(TransactionContext context, String projectId,
        PrincipalType principalType, PrincipalSource principalSource, String principalId)
        throws MetaStoreException;

    void deleteRolePrincipal(TransactionContext context, String projectId, String roleId,
        PrincipalType principalType, PrincipalSource principalSource, String principalId)
        throws MetaStoreException;

    void delAllRolePrincipal(TransactionContext context, String projectId, String roleId) throws MetaStoreException;
}
