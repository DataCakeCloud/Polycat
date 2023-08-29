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

import java.util.ArrayList;
import java.util.List;

import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.model.MetaPolicyHistory;
import io.polycat.catalog.common.model.MetaPrivilegePolicy;
import io.polycat.catalog.common.model.Policy;
import io.polycat.catalog.common.model.Principal;
import io.polycat.catalog.common.model.PrincipalSource;
import io.polycat.catalog.common.model.PrincipalType;
import io.polycat.catalog.common.plugin.request.input.PolicyInput;
import io.polycat.catalog.common.plugin.request.input.RoleInput;
import io.polycat.catalog.service.api.NewRoleService;
import io.polycat.catalog.service.api.PolicyService;
import io.polycat.catalog.service.api.UserGroupService;
import io.polycat.catalog.store.fdb.record.RecordStoreHelper;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@Disabled
public class PolicyServiceImplTestUtil extends  TestUtil{

    @Autowired
    private NewRoleService newRoleService;

    @Autowired
    private PolicyService policyService;

    private static final String[] catalogPrivileges = {"DESC CATALOG", "DROP CATALOG", "ALTER CATALOG", "USE CATALOG",
        "CREATE BRANCH", "SHOW BRANCH", "CREATE DATABASE", "SHOW DATABASE", "SHOW_ACCESSSTATS CATALOG"};

    private static final String[] tablePrivileges = {"DROP TABLE", "ALTER TABLE", "UNDROP TABLE", "RESTORE TABLE",
        "SELECT TABLE", "INSERT TABLE", "DESC TABLE", "DESC_ACCESSSTATS TABLE", "SHOW_DATALINEAGE TABLE"};
    static boolean isFirstTest = true;

    private void beforeClass() {
        if (isFirstTest) {
            createCatalogBeforeClass();
            isFirstTest = false;
        }
    }

    @BeforeEach
    public void beforeEach() {
        beforeClass();
    }

    @Test
    public void alterOnePrivilegeTest() {
        String roleName = "role1";
        RoleInput roleInput = new RoleInput();
        roleInput.setRoleName(roleName);
        roleInput.setOwnerUser(userId);
        newRoleService.createRole(projectId, roleInput);


        PolicyInput policyInput = new PolicyInput();
        policyInput.setPrincipalType(PrincipalType.ROLE.name());
        policyInput.setPrincipalSource(PrincipalSource.IAM.name());
        policyInput.setPrincipalName(roleName);
        policyInput.setObjectType(ObjectType.CATALOG.name());
        List<Operation> operationList = new ArrayList<>();
        operationList.add(Operation.ALTER_CATALOG);
        policyInput.setOperationList(operationList);
        policyInput.setObjectName(catalogNameString);
        policyInput.setEffect(true);
        policyInput.setOwner(false);
        policyInput.setGrantAble(true);
        List<Policy> policyList = new ArrayList<>();
        policyService.addMetaPolicyToPrincipal(projectId, roleName, policyInput);
        policyList = policyService.showMetaPolicyFromPrincipal(projectId,
            policyInput.getPrincipalType(),policyInput.getPrincipalSource(), roleName );
        assertTrue(policyList.size() == 1);


        policyService.revokeMetaPolicyFromPrincipal(projectId, roleName, policyInput);
        policyList = policyService.showMetaPolicyFromPrincipal(projectId, policyInput.getPrincipalType(),policyInput.getPrincipalSource(),
            roleName);
        assertTrue(policyList.size() == 0);

        newRoleService.dropRoleByName(projectId, roleName);
    }

    @Test
    public void alterAllPrivilegeOnObjectTest() {
        String roleName = "role1";
        RoleInput roleInput = new RoleInput();
        roleInput.setRoleName(roleName);
        roleInput.setOwnerUser(userId);
        newRoleService.createRole(projectId, roleInput);


        PolicyInput policyInput = new PolicyInput();
        policyInput.setPrincipalType(PrincipalType.ROLE.name());
        policyInput.setPrincipalSource(PrincipalSource.IAM.name());
        policyInput.setPrincipalName(roleName);
        policyInput.setObjectType(ObjectType.CATALOG.name());
        List<Operation> operationList = new ArrayList<>();
        policyInput.setOperationList(operationList);
        policyInput.setObjectName(catalogNameString);
        policyInput.setEffect(true);
        policyInput.setOwner(false);
        policyInput.setGrantAble(true);
        List<Policy> policyList = new ArrayList<>();
        policyService.addAllPrivilegeOnObjectToPrincipal(projectId, roleName, policyInput);
        policyList = policyService.showMetaPolicyFromPrincipal(projectId, policyInput.getPrincipalType(), policyInput.getPrincipalSource(),
            roleName);
        assertTrue(policyList.size() == 9);


        policyService.removeAllPrivilegeOnObjectFromPrincipal(projectId, roleName, policyInput);
        policyList = policyService.showMetaPolicyFromPrincipal(projectId, policyInput.getPrincipalType(), policyInput.getPrincipalSource(),
            roleName);
        assertTrue(policyList.size() == 0);

        newRoleService.dropRoleByName(projectId, roleName);
    }

    @Test
    public void getModifiedPolicyByTime() {
        long time1 = RecordStoreHelper.getCurrentTime();
        String role1Name = "role1";
        String role2Name = "role2";
        RoleInput roleInput = new RoleInput();
        roleInput.setRoleName(role1Name);
        roleInput.setOwnerUser(userId);
        newRoleService.createRole(projectId, roleInput);
        roleInput.setRoleName(role2Name);
        newRoleService.createRole(projectId, roleInput);


        PolicyInput policyInput = new PolicyInput();
        policyInput.setPrincipalType(PrincipalType.ROLE.name());
        policyInput.setPrincipalSource(PrincipalSource.IAM.name());
        policyInput.setPrincipalName(role1Name);
        policyInput.setObjectType(ObjectType.CATALOG.name());
        List<Operation> operationList = new ArrayList<>();
        policyInput.setOperationList(operationList);
        policyInput.setObjectName(catalogNameString);
        policyInput.setEffect(true);
        policyInput.setOwner(false);
        policyInput.setGrantAble(true);
        policyService.addAllPrivilegeOnObjectToPrincipal(projectId, role1Name, policyInput);

        long time2 = RecordStoreHelper.getCurrentTime();
        policyInput.setPrincipalName(role1Name);
        policyService.addAllPrivilegeOnObjectToPrincipal(projectId, role2Name, policyInput);

        List<MetaPolicyHistory> policyHistoryList = new ArrayList<>();
        List<MetaPrivilegePolicy> privilegePolicyList = new ArrayList<>();
        policyHistoryList = policyService.getUpdatedMetaPolicyIdsByTime(projectId, time1);
        assertTrue(policyHistoryList.size() == 2 + 9 + 9);
        List<String> idList = new ArrayList<>();
        for (MetaPolicyHistory policyHistory : policyHistoryList) {
            if (policyHistory.getPrincipalType() == PrincipalType.USER) {
                idList.add(policyHistory.getPolicyId());
            }
        }
        privilegePolicyList = policyService.getUpdatedMetaPolicyByIdList(projectId,PrincipalType.USER.name(),idList);
        assertTrue(privilegePolicyList.size() == 2);


        policyHistoryList = policyService.getUpdatedMetaPolicyIdsByTime(projectId, time2);
        assertTrue(policyHistoryList.size() == 9);
        idList.clear();
        for (MetaPolicyHistory policyHistory : policyHistoryList) {
            if (policyHistory.getPrincipalType() == PrincipalType.ROLE) {
                idList.add(policyHistory.getPolicyId());
            }
        }
        privilegePolicyList = policyService.getUpdatedMetaPolicyByIdList(projectId,PrincipalType.ROLE.name(),idList);
        assertTrue(privilegePolicyList.size() == 9);

        newRoleService.dropRoleByName(projectId, role1Name);
        newRoleService.dropRoleByName(projectId, role2Name);
    }


    @Test
    public void listPolicyByPrincipal() {
        String role1Name = "role1";
        RoleInput roleInput = new RoleInput();
        roleInput.setRoleName(role1Name);
        roleInput.setOwnerUser(userId);
        newRoleService.createRole(projectId, roleInput);

        List<MetaPrivilegePolicy> privilegePolicyList = new ArrayList<>();
        List<Principal> principalList  = new ArrayList<>();
        Principal principal = new Principal();
        principal.setPrincipalType("USER");
        principal.setPrincipalSource("IAM");
        principal.setPrincipalId(userId);
        principalList.add(principal);

        privilegePolicyList = policyService.listMetaPolicyByPrincipal(projectId,principalList);
        assertTrue(privilegePolicyList.size() == 1 + 1);

        newRoleService.dropRoleByName(projectId, role1Name);
        privilegePolicyList = policyService.listMetaPolicyByPrincipal(projectId,principalList);
        assertTrue(privilegePolicyList.size() == 1 );

    }
}
