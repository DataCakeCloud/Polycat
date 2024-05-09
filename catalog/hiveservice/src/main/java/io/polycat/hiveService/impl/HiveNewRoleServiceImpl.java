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
import io.polycat.catalog.common.model.Role;
import io.polycat.catalog.common.plugin.request.input.RoleInput;
import io.polycat.catalog.service.api.NewRoleService;

public class HiveNewRoleServiceImpl implements NewRoleService {

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
    public void alterRoleName(String projectId, String roleName, RoleInput roleInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "alterRoleName");
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
    public List<Role> getRoleModels(String projectId, String userId, String namePattern) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getRoleModels");
    }
}
