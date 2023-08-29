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
import java.util.UUID;

import io.polycat.catalog.authorization.policyDecisionPoint.AccessType;
import io.polycat.catalog.authorization.policyDecisionPoint.AuthRequest;
import io.polycat.catalog.authorization.policyDecisionPoint.AuthResult;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.model.MetaPrivilegePolicy;
import io.polycat.catalog.common.model.Principal;
import io.polycat.catalog.common.model.PrincipalSource;
import io.polycat.catalog.common.model.PrincipalType;
import io.polycat.catalog.common.model.PrivilegeType;

import org.junit.Test;
import org.junit.jupiter.api.Disabled;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.jupiter.api.Assertions.assertEquals;

@RunWith(MockitoJUnitRunner.class)
@Disabled
public class AuthorizationServiceTest {

    @InjectMocks
    private AuthorizationServiceImpl authorizationService;

    @Mock
    private PolicyServiceImpl policyService;

    @Test
    public void checkPrivilegeByOwner() {
        try {
            List<MetaPrivilegePolicy> policies = getPolicy();
            AuthRequest.Builder request = new AuthRequest.Builder();
            List<String> userGroups = new ArrayList<>();
            request.setAccessType(AccessType.TABLE_ACCESS.getNum());
            request.setUser("Owner");
            request.setUserGroup(userGroups);
            request.setUserSource("IAM");
            request.setObjectId("c1.d1.t1");
            request.setObjectType(ObjectType.TABLE.getNum());
            request.setOperation(Operation.DROP_TABLE.ordinal());
            request.setProjectId("CD_Storage");
            request.setUserRoles(new ArrayList<>());

            List<Principal> principals = new ArrayList<>();
            Principal userPrincipal = new Principal();
            userPrincipal.setPrincipalId("Owner");
            userPrincipal.setPrincipalType(PrincipalType.USER.toString());
            userPrincipal.setPrincipalSource("IAM");
            principals.add(userPrincipal);

            Mockito.when(policyService.listMetaPolicyByPrincipal("CD_Storage", principals))
                .thenReturn(policies);
            AuthResult result = authorizationService.checkPrivilege(request.build());
            assertEquals(result.isAllowed(), true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static List<MetaPrivilegePolicy> getPolicy() {
        List<MetaPrivilegePolicy> policies = new ArrayList<>();
        MetaPrivilegePolicy.Builder allowPolicy = new MetaPrivilegePolicy.Builder();
        allowPolicy.setPolicyId(UUID.randomUUID().toString().toLowerCase());
        allowPolicy.setProjectId("CD_Storage");
        allowPolicy.setEffect(true);
        allowPolicy.setObjectId("c1.d1.t1");
        allowPolicy.setObjectType(ObjectType.TABLE.getNum());
        allowPolicy.setPrivilege(PrivilegeType.DROP.getType());
        allowPolicy.setPrincipalSource(PrincipalSource.IAM.getNum());
        allowPolicy.setPrincipalType(PrincipalType.GROUP.getNum());
        allowPolicy.setPrincipalId("DataEngineer");
        allowPolicy.setGrantAble(true);
        policies.add(allowPolicy.build());

        MetaPrivilegePolicy.Builder ownerPolicy = new MetaPrivilegePolicy.Builder();
        ownerPolicy.setPolicyId(UUID.randomUUID().toString().toLowerCase());
        ownerPolicy.setProjectId("CD_Storage");
        ownerPolicy.setEffect(true);
        ownerPolicy.setObjectId("c1.d1.t1");
        ownerPolicy.setObjectType(ObjectType.TABLE.getNum());
        ownerPolicy.setPrivilege(PrivilegeType.OWNER.getType());
        ownerPolicy.setPrincipalSource(PrincipalSource.IAM.getNum());
        ownerPolicy.setPrincipalType(PrincipalType.GROUP.getNum());
        ownerPolicy.setPrincipalId("Owner");
        ownerPolicy.setGrantAble(true);
        policies.add(ownerPolicy.build());

        MetaPrivilegePolicy.Builder selectPolicy = new MetaPrivilegePolicy.Builder();
        selectPolicy.setPolicyId(UUID.randomUUID().toString().toLowerCase());
        selectPolicy.setProjectId("CD_Storage");
        selectPolicy.setEffect(true);
        selectPolicy.setObjectId("c1.d1.t1");
        selectPolicy.setObjectType(ObjectType.TABLE.getNum());
        selectPolicy.setPrivilege(PrivilegeType.OWNER.getType());
        selectPolicy.setPrincipalSource(PrincipalSource.IAM.getNum());
        selectPolicy.setPrincipalType(PrincipalType.GROUP.getNum());
        selectPolicy.setPrincipalId("DataEngineer");
        selectPolicy.setGrantAble(false);
        selectPolicy.setObligation("ROW_FILTER:(c1>2);DATA_MASK:c2:SHA2");
        policies.add(selectPolicy.build());

        MetaPrivilegePolicy.Builder denyPolicy = new MetaPrivilegePolicy.Builder();
        denyPolicy.setPolicyId(UUID.randomUUID().toString().toLowerCase());
        allowPolicy.setProjectId("CD_Storage");
        denyPolicy.setEffect(false);
        denyPolicy.setObjectId("c1.d1.t2");
        denyPolicy.setObjectType(ObjectType.TABLE.getNum());
        denyPolicy.setPrivilege(PrivilegeType.ALTER.getType());
        denyPolicy.setPrincipalSource(PrincipalSource.IAM.getNum());
        denyPolicy.setPrincipalType(PrincipalType.GROUP.getNum());
        denyPolicy.setPrincipalId("DataEngineer");
        denyPolicy.setGrantAble(false);
        policies.add(denyPolicy.build());

        return policies;
    }

    @Test
    public void checkPrivilegeByPolicyFail() {
        try {
            List<MetaPrivilegePolicy> policies = getPolicy();
            AuthRequest.Builder request = new AuthRequest.Builder();
            List<String> userGroups = new ArrayList<>();
            request.setAccessType(AccessType.TABLE_ACCESS.getNum());
            userGroups.add("DataAnalyzer");
            request.setUserGroup(userGroups);
            request.setUserSource("IAM");
            request.setObjectId("c1.d1.t2");
            request.setObjectType(ObjectType.TABLE.getNum());
            request.setOperation(Operation.ALTER_TABLE.ordinal());
            request.setProjectId("CD_Storage");
            request.setUserRoles(new ArrayList<>());
            request.setUser("Test");

            List<Principal> principals = new ArrayList<>();
            Principal userPrincipal = new Principal();
            userPrincipal.setPrincipalId("Test");
            userPrincipal.setPrincipalType(PrincipalType.USER.toString());
            userPrincipal.setPrincipalSource("IAM");
            principals.add(userPrincipal);

            for (String principal : userGroups) {
                Principal userGroup = new Principal();
                userGroup.setPrincipalType(PrincipalType.GROUP.toString());
                userGroup.setPrincipalSource("IAM");
                userGroup.setPrincipalId(principal);
                principals.add(userGroup);
            }

            Mockito.when(policyService.listMetaPolicyByPrincipal("CD_Storage", principals))
                .thenReturn(policies);
            AuthResult result = authorizationService.checkPrivilege(request.build());
            assertEquals(result.isAllowed(), false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void checkPrivilegeByPolicySuccess() {
        try {
            List<MetaPrivilegePolicy> policies = getPolicy();
            AuthRequest.Builder request = new AuthRequest.Builder();
            List<String> userGroups = new ArrayList<>();
            request.setAccessType(AccessType.TABLE_ACCESS.getNum());
            userGroups.add("DataEngineer");
            request.setUser("Test");
            request.setUserGroup(userGroups);
            request.setUserSource("IAM");
            request.setObjectId("c1.d1.t1");
            request.setObjectType(ObjectType.TABLE.getNum());
            request.setOperation(Operation.DROP_TABLE.ordinal());
            request.setProjectId("CD_Storage");
            request.setUserRoles(new ArrayList<>());

            List<Principal> principals = new ArrayList<>();
            Principal userPrincipal = new Principal();
            userPrincipal.setPrincipalId("Test");
            userPrincipal.setPrincipalType(PrincipalType.USER.toString());
            userPrincipal.setPrincipalSource("IAM");
            principals.add(userPrincipal);
            for (String principal : userGroups) {
                Principal userGroup = new Principal();
                userGroup.setPrincipalType(PrincipalType.GROUP.toString());
                userGroup.setPrincipalSource("IAM");
                userGroup.setPrincipalId(principal);
                principals.add(userGroup);
            }

            Mockito.when(policyService.listMetaPolicyByPrincipal("CD_Storage", principals))
                .thenReturn(policies);
            AuthResult result = authorizationService.checkPrivilege(request.build());
            assertEquals(result.isAllowed(), true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}