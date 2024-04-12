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
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.model.CatalogInnerObject;
import io.polycat.catalog.common.model.Role;
import io.polycat.catalog.common.model.RolePrivilege;
import io.polycat.catalog.common.plugin.request.input.RoleInput;
import io.polycat.catalog.store.common.StoreConvertor;
import io.polycat.catalog.common.model.TableName;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
public class RoleServiceImplTestUtil extends  TestUtil{
    private final String role1 = "role1";

    private static final String[] catalogPrivileges = {"DESC CATALOG", "DROP CATALOG", "ALTER CATALOG", "USE CATALOG",
            "CREATE BRANCH", "SHOW BRANCH", "CREATE DATABASE", "SHOW DATABASE", "SHOW_ACCESSSTATS CATALOG"};

    private static final String[] tablePrivileges = {"DROP TABLE", "ALTER TABLE", "UNDROP TABLE", "RESTORE TABLE",
        "SELECT TABLE", "INSERT TABLE", "DESC TABLE", "DESC_ACCESSSTATS TABLE", "SHOW_DATALINEAGE TABLE"};
    boolean isFirstTest = true;
    private void beforeClass() {
        if (isFirstTest) {
            createCatalogBeforeClass();
            createDatabaseBeforeClass();
            createTableBeforeClass();
            isFirstTest = false;
        }
    }

    @BeforeEach
    public void beforeEach() {
        beforeClass();
    }

    @Test
    public void roleAllPrivilegeTest() {
        RoleInput roleInput = new RoleInput();
        roleInput.setRoleName(role1);
        roleInput.setOwnerUser(userId);
        roleService.createRole(projectId, roleInput);

        roleInput.setObjectType(ObjectType.CATALOG.name());
        roleInput.setRoleName(role1);
        roleInput.setObjectName(catalogNameString);
        roleService.addAllPrivilegeOnObjectToRole(projectId, role1, roleInput);
        Role role = roleService.getRoleByName(projectId, role1);
        for (RolePrivilege rolePrivilege : role.getRolePrivileges()) {
            assertEquals(rolePrivilege.getName(), catalogNameString);
            boolean flag = false;
            for (String privilege : catalogPrivileges) {
                if (privilege.equals(rolePrivilege.getPrivilege())) {
                    flag = true;
                }
            }
            assertTrue(flag, "privilege not find : " + rolePrivilege.getPrivilege());
        }

        roleService.removeAllPrivilegeOnObjectFromRole(projectId, role1, roleInput);
        role = roleService.getRoleByName(projectId, role1);
        assertTrue(role.getRolePrivileges().length == 0);

        roleService.dropRoleByName(projectId, role1);
    }

    @Test
    public void rolePrivilegeTest() {
        RoleInput roleInput = new RoleInput();
        roleInput.setRoleName(role1);
        roleInput.setOwnerUser(userId);
        roleService.createRole(projectId, roleInput);

        roleInput.setObjectType(ObjectType.CATALOG.name());
        roleInput.setRoleName(role1);
        roleInput.setObjectName(catalogNameString);
        roleInput.setOperation(Operation.ALTER_CATALOG);
        roleService.addPrivilegeToRole(projectId, role1, roleInput);

        Role role = roleService.getRoleByName(projectId, role1);
        assertTrue(role.getRolePrivileges().length == 1);
        roleService.removeAllPrivilegeOnObjectFromRole(projectId, role1, roleInput);
        role = roleService.getRoleByName(projectId, role1);
        assertTrue(role.getRolePrivileges().length == 0);

        roleService.dropRoleByName(projectId, role1);
    }

    @Test
    public void roleUserTest() {
        RoleInput roleInput = new RoleInput();
        roleInput.setRoleName(role1);
        roleInput.setOwnerUser(userId);
        roleService.createRole(projectId, roleInput);

        String[] users = {"xy12345", "pz12345"};
        roleInput.setUserId(users);
        roleService.addUserToRole(projectId, role1, roleInput);

        Role role = roleService.getRoleByName(projectId, role1);
        assertTrue(role.getToUsers().length == 2);

        roleService.removeUserFromRole(projectId, role1, roleInput);
        roleService.dropRoleByName(projectId, role1);
    }

    @Test
    public void delTablePrivilegeByPurgeTable() {
        RoleInput roleInput = new RoleInput();
        roleInput.setRoleName(role1);
        roleInput.setOwnerUser(userId);

        // add table privilege and delete table privilege, table privilege not exist
        roleService.createRole(projectId, roleInput);

        roleInput.setObjectType(ObjectType.TABLE.name());
        roleInput.setRoleName(role1);
        String objectName = catalogNameString + "." + databaseNameString + "." + tableNameString;
        roleInput.setObjectName(objectName);
        roleService.addAllPrivilegeOnObjectToRole(projectId, role1, roleInput);
        Role role = roleService.getRoleByName(projectId, role1);
        for (RolePrivilege rolePrivilege : role.getRolePrivileges()) {
            assertEquals(rolePrivilege.getName(), objectName);
            boolean flag = false;
            for (String privilege : tablePrivileges) {
                if (privilege.equals(rolePrivilege.getPrivilege())) {
                    flag = true;
                }
            }
            assertTrue(flag, "privilege not find : " + rolePrivilege.getPrivilege());
        }

        CatalogInnerObject catalogInnerObject = RolePrivilegeHelper.getCatalogObject(projectId,
            ObjectType.TABLE.name(), objectName);
        assertNotNull(catalogInnerObject);
        TableName tableName = StoreConvertor.tableName(projectId, catalogNameString, databaseNameString, tableNameString);
        tableService.dropTable(tableName, false, false, true);
        role = roleService.getRoleByName(projectId, role1);
        assertTrue(role.getRolePrivileges().length == 0);

        roleService.dropRoleByName(projectId, role1);
    }
}

