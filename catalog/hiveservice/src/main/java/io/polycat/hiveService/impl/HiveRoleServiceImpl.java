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
package io.polycat.hiveService.impl;

import java.util.List;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.model.PrivilegeRoles;
import io.polycat.catalog.common.model.Role;
import io.polycat.catalog.common.model.TraverseCursorResult;
import io.polycat.catalog.common.plugin.request.input.RoleInput;
import io.polycat.catalog.common.plugin.request.input.ShowRolePrivilegesInput;
import io.polycat.catalog.service.api.RoleService;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "hive")
public class HiveRoleServiceImpl implements RoleService {

    @Override
    public void createRole(String projectId, RoleInput roleInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "createRole");
    }

    @Override
    public void dropRoleById(String projectId, String roleId) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "dropRoleById");
    }

    @Override
    public void dropRoleByName(String projectId, String roleName) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "dropRoleByName");
    }

    @Override
    public Role getRoleByName(String projectId, String roleName) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getRoleByName");
    }

    @Override
    public Role getRoleById(String projectId, String roleId) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getRoleById");
    }

    @Override
    public void alterRole(String projectId, String roleName, RoleInput roleInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "alterRole");
    }

    @Override
    public void addPrivilegeToRole(String projectId, String roleName, RoleInput roleInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "addPrivilegeToRole");
    }

    @Override
    public void addAllPrivilegeOnObjectToRole(String projectId, String roleName, RoleInput roleInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "addAllPrivilegeOnObjectToRole");
    }

    @Override
    public void removePrivilegeFromRole(String projectId, String roleName, RoleInput roleInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "removePrivilegeFromRole");
    }

    @Override
    public void removeAllPrivilegeOnObjectFromRole(String projectId, String roleName, RoleInput roleInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "removeAllPrivilegeOnObjectFromRole");
    }

    @Override
    public void addUserToRole(String projectId, String roleName, RoleInput roleInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "addUserToRole");
    }

    @Override
    public void removeUserFromRole(String projectId, String roleName, RoleInput roleInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "removeUserFromRole");
    }

    @Override
    public List<Role> getRoleModels(String projectId, String userId, String namePattern, boolean containOwner) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getRoleModels");
    }

    @Override
    public List<Role> getRoleNames(String projectId, String keyword) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getRoleNames");
    }

    @Override
    public List<String> showPermObjectsByUser(String projectId, String userId, String objectType, String filterJson) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "showPrivilegesByUser");
    }

    @Override
    public TraverseCursorResult<List<Role>> showRolePrivileges(String projectId, ShowRolePrivilegesInput input,
        Integer limit, String pageToken) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "showPrivileges");
    }

    @Override
    public TraverseCursorResult<List<PrivilegeRoles>> showPrivilegeRoles(String projectId,
        ShowRolePrivilegesInput input, Integer limit, String pageToken) {
        return null;
    }
}
