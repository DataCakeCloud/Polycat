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
package io.polycat.catalog.server.service.impl;

import io.polycat.catalog.common.model.Role;
import io.polycat.catalog.common.plugin.request.input.RoleInput;
import io.polycat.catalog.service.api.RoleService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;


@Slf4j
@SpringBootTest
public class PGRoleServiceImplTest extends PGBaseServiceImplTest {

    private static final String roleName = "roleName";
    private static final String ownerUser = "ownerUser";
    private static final String comment = "comment";

    @Autowired
    private RoleService roleService;

    @Test
    public void create_role_should_success() {
        RoleInput roleInput = makeRoleInput(roleName, ownerUser, comment);
        functionAssertDoesNotThrow(() -> roleService.createRole(PROJECT_ID, roleInput));
        Role role = roleService.getRoleByName(PROJECT_ID, roleName);
        log.info("RoleData: {}", role);
        valueAssertEquals(roleName, role.getRoleName());
        Role roleById = roleService.getRoleById(PROJECT_ID, role.getRoleId());
        valueAssertEquals(role, roleById);
        valueAssertEquals(0, roleService.getRoleModels(PROJECT_ID, "test_user", "test", true).size());
        valueAssertEquals(0, roleService.getRoleModels(PROJECT_ID, ownerUser, "test", true).size());
        roleService.createRole(PROJECT_ID, makeRoleInput("test1", ownerUser, comment));
        valueAssertEquals(1, roleService.getRoleModels(PROJECT_ID, ownerUser, "test", true).size());
        valueAssertEquals(2, roleService.getRoleModels(PROJECT_ID, ownerUser, "", true).size());
    }

    private RoleInput makeRoleInput(String roleName, String ownerUser, String comment) {
        RoleInput roleInput = new RoleInput();
        roleInput.setRoleName(roleName);
        roleInput.setOwnerUser(ownerUser);
        roleInput.setComment(comment);
        return roleInput;
    }

}
