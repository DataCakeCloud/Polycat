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
import io.polycat.catalog.common.model.PrivilegeRolesObject;
import io.polycat.catalog.common.model.RoleObject;
import io.polycat.catalog.common.model.RolePrivilegeObject;
import io.polycat.catalog.common.model.RoleUserObject;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.CatalogInnerObject;

import io.polycat.catalog.common.plugin.request.input.ShowRolePrivilegesInput;

public interface RoleStore {

    void createRoleSubspace(TransactionContext context, String projectId);

    void dropRoleSubspace(TransactionContext context, String projectId);

    String generateRoleObjectId(TransactionContext context, String projectId);

    Boolean roleObjectNameExist(TransactionContext context, String projectId, String roleName) throws MetaStoreException;

    void insertRoleObjectName(TransactionContext context, String projectId, String roleName, String roleId)
        throws MetaStoreException;

    String getRoleId(TransactionContext context, String projectId, String roleName) throws MetaStoreException;

    void deleteRoleObjectName(TransactionContext context, String projectId, String roleName) throws MetaStoreException;

    void insertRoleProperties(TransactionContext context, String projectId, String roleId, String roleName,
                              String ownerId, String comment) throws MetaStoreException;

    void updateRoleProperties(TransactionContext context, RoleObject roleObject) throws MetaStoreException;

    RoleObject getRoleProperties(TransactionContext context, String projectId, String roleId) throws MetaStoreException;

    void deleteRoleProperties(TransactionContext context, String projectId, String roleId) throws MetaStoreException;

    void insertRoleUser(TransactionContext context, String projectId, String roleId, String userId)
        throws MetaStoreException;

    List<RoleUserObject> getRoleUsersByRoleId(TransactionContext context, String projectId, String roleId)
        throws MetaStoreException;

    List<RoleUserObject> getRoleUsersByUserId(TransactionContext context, String projectId, String userId)
        throws MetaStoreException;

    boolean deleteRoleUser(TransactionContext context, String projectId, String roleId, String userId)
        throws MetaStoreException;

    void delAllRoleUser(TransactionContext context, String projectId, String roleId) throws MetaStoreException;

    void insertRolePrivilege(TransactionContext context, String projectId, String roleId,
        String objectType, String rolePrivilegeObjectId, CatalogInnerObject catalogInnerObject, long privilege) throws MetaStoreException;

    void updateRolePrivilege(TransactionContext context, String projectId,
        RolePrivilegeObject rolePrivilegeObject, long newPrivilege) throws MetaStoreException;

    RolePrivilegeObject getRolePrivilege(TransactionContext context, String projectId, String roleId,
        String objectType, String rolePrivilegeObjectId) throws MetaStoreException;

    List<RolePrivilegeObject> getRolePrivilege(TransactionContext context, String projectId, String roleId)
        throws MetaStoreException;

    void deleteRolePrivilege(TransactionContext context, String projectId, String roleId, String objectType,
        String objectId) throws MetaStoreException;

    void delAllRolePrivilege(TransactionContext context, String projectId, String roleId) throws MetaStoreException;

    void removeAllPrivilegeOnObject(TransactionContext context, String projectId, String objectType, String rolePrivilegeObjectId)
        throws MetaStoreException;

    List<RoleObject> getAllRoleObjects(TransactionContext context, String projectId, String userId, String namePattern, boolean containOwner);

    List<RoleObject> getAllRoleNames(TransactionContext context, String projectId, String keyword);

    List<RolePrivilegeObject> getRoleByIds(TransactionContext context, String projectId, String objectType, List<String> collect);

    List<RolePrivilegeObject> showRolePrivileges(TransactionContext context, String projectId, List<String> roleIds,
        ShowRolePrivilegesInput input, int batchNum, long batchOffset);

    List<RoleObject> showRoleInfos(TransactionContext context, String projectId, ShowRolePrivilegesInput input);

    List<PrivilegeRolesObject> showPrivilegeRoles(TransactionContext context, String projectId, List<String> collect, ShowRolePrivilegesInput input, int batchNum, long batchOffset);
}