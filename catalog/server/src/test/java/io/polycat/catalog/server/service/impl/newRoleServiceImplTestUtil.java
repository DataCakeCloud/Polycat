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

import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.model.Role;
import io.polycat.catalog.common.plugin.request.input.RoleInput;
import io.polycat.catalog.service.api.NewRoleService;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@Disabled
public class newRoleServiceImplTestUtil extends TestUtil{

    @Autowired
    private NewRoleService newRoleService;

    private final String role1 = "role1";

    private static final String[] catalogPrivileges = {"DESC CATALOG", "DROP CATALOG", "ALTER CATALOG", "USE CATALOG",
        "CREATE BRANCH", "SHOW BRANCH", "CREATE DATABASE", "SHOW DATABASE", "SHOW_ACCESSSTATS CATALOG"};

    private static final String[] tablePrivileges = {"DROP TABLE", "ALTER TABLE", "UNDROP TABLE", "RESTORE TABLE",
        "SELECT TABLE", "INSERT TABLE", "DESC TABLE", "DESC_ACCESSSTATS TABLE", "SHOW_DATALINEAGE TABLE"};
    @BeforeAll
    public static void beforeClass() {
        //createCatalogBeforeClass();
    }

    @Test
    public void createDropRoleTest() {
        RoleInput roleInput = new RoleInput();
        roleInput.setRoleName(role1);
        roleInput.setOwnerUser(userId);
        newRoleService.createRole(projectId, roleInput);

        roleInput.setObjectType(ObjectType.CATALOG.name());
        roleInput.setRoleName(role1);
        roleInput.setObjectName(catalogNameString);
        Role role = newRoleService.getRoleByName(projectId, role1);
        assertTrue(role.getRolePrivileges().length == 0);

        newRoleService.dropRoleByName(projectId, role1);
    }


    @Test
    public void roleUserTest() {
        RoleInput roleInput = new RoleInput();
        roleInput.setRoleName(role1);
        roleInput.setOwnerUser(userId);
        newRoleService.createRole(projectId, roleInput);

        String[] users = {"USER:IAM:xy12345", "USER:IAM:pz12345"};
        roleInput.setUserId(users);
        newRoleService.addUserToRole(projectId, role1, roleInput);

        Role role = newRoleService.getRoleByName(projectId, role1);
        assertTrue(role.getToUsers().length == 2);

        newRoleService.removeUserFromRole(projectId, role1, roleInput);
        newRoleService.dropRoleByName(projectId, role1);
    }
}

