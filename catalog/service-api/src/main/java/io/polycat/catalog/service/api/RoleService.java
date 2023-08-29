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
package io.polycat.catalog.service.api;

import java.util.List;

import io.polycat.catalog.common.model.Role;
import io.polycat.catalog.common.plugin.request.input.RoleInput;

public interface RoleService {

    /**
     * create role
     *
     * @param projectId
     * @param roleInput
     */
    void createRole(String projectId, RoleInput roleInput);

    /**
     * drop role by id
     *
     * @param projectId
     * @param roleId
     */
    void dropRoleById(String projectId, String roleId);

    /**
     * drop role by name
     *  @param projectId
     * @param roleName
     */
    void dropRoleByName(String projectId, String roleName);

    /**
     *
     * @param projectId
     * @param roleName
     * @return
     */
    Role getRoleByName(String projectId, String roleName);

    /**
     *
     * @param projectId
     * @param roleId
     * @return
     */
    Role getRoleById(String projectId, String roleId);

    /**
     *
     * @param projectId
     * @param roleName
     * @param roleInput
     */
    void alterRole(String projectId, String roleName, RoleInput roleInput);

    /**
     *
     * @param projectId
     * @param roleName
     * @param roleInput
     */
    void addPrivilegeToRole(String projectId, String roleName, RoleInput roleInput);

    /**
     *
     * @param projectId
     * @param roleName
     * @param roleInput
     */
    void addAllPrivilegeOnObjectToRole(String projectId, String roleName, RoleInput roleInput);

    /**
     *
     * @param projectId
     * @param roleName
     * @param roleInput
     */
    void removePrivilegeFromRole(String projectId, String roleName, RoleInput roleInput);

    /**
     *
     * @param projectId
     * @param roleName
     * @param roleInput
     */
    void removeAllPrivilegeOnObjectFromRole(String projectId, String roleName, RoleInput roleInput);

    /**
     *
     * @param projectId
     * @param roleName
     * @param roleInput
     */
    void addUserToRole(String projectId, String roleName, RoleInput roleInput);

    /**
     *
     * @param projectId
     * @param roleName
     * @param roleInput
     */
    void removeUserFromRole(String projectId, String roleName, RoleInput roleInput);

    /**
     * get role models in project
     *
     * @param projectId
     * @param namePattern
     */
    List<Role> getRoleModels(String projectId, String userId, String namePattern);

    /**
     * 获取所有的 Role name in projectId
     * @param projectId
     * @param keyword
     * @return
     */
    List<Role> getRoleNames(String projectId, String keyword);

    List<String> showPermObjectsByUser(String projectId, String userId, String objectType, String filterJson);
}