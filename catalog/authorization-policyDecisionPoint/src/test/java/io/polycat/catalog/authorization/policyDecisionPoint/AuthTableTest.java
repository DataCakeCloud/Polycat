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
package io.polycat.catalog.authorization.policyDecisionPoint;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.model.MetaPrivilegePolicy;
import io.polycat.catalog.common.model.PrincipalSource;
import io.polycat.catalog.common.model.PrincipalType;
import io.polycat.catalog.common.model.PrivilegeType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AuthTableTest {

    public static AuthTable authTable = new AuthTable();

    @BeforeAll
    public static void beforeClass() {
        List<MetaPrivilegePolicy> policies = new ArrayList<>();
        MetaPrivilegePolicy.Builder allowPolicy = new MetaPrivilegePolicy.Builder();
        allowPolicy.setPolicyId(UUID.randomUUID().toString().toLowerCase());
        allowPolicy.setProjectId("CD_Storage");
        allowPolicy.setEffect(true);
        allowPolicy.setObjectId("c1");
        allowPolicy.setObjectType(ObjectType.CATALOG.getNum());
        allowPolicy.setPrivilege(PrivilegeType.OWNER.getType());
        allowPolicy.setPrincipalSource(PrincipalSource.IAM.getNum());
        allowPolicy.setPrincipalType(PrincipalType.GROUP.getNum());
        allowPolicy.setPrincipalId("DataEngineer");
        allowPolicy.setGrantAble(true);
        policies.add(allowPolicy.build());

        MetaPrivilegePolicy.Builder selectPolicy = new MetaPrivilegePolicy.Builder();
        selectPolicy.setPolicyId(UUID.randomUUID().toString().toLowerCase());
        selectPolicy.setProjectId("CD_Storage");
        selectPolicy.setEffect(true);
        selectPolicy.setObjectId("c1.d1");
        selectPolicy.setObjectType(ObjectType.DATABASE.getNum());
        selectPolicy.setPrivilege(PrivilegeType.OWNER.getType());
        selectPolicy.setPrincipalSource(PrincipalSource.IAM.getNum());
        selectPolicy.setPrincipalType(PrincipalType.GROUP.getNum());
        selectPolicy.setPrincipalId("DataEngineer");
        selectPolicy.setGrantAble(true);
        policies.add(selectPolicy.build());

        MetaPrivilegePolicy.Builder denyPolicy = new MetaPrivilegePolicy.Builder();
        denyPolicy.setPolicyId(UUID.randomUUID().toString().toLowerCase());
        allowPolicy.setProjectId("CD_Storage");
        denyPolicy.setEffect(false);
        denyPolicy.setObjectId("c1.d1.t2");
        denyPolicy.setObjectType(ObjectType.DATABASE.getNum());
        denyPolicy.setPrivilege(PrivilegeType.ALTER.getType());
        denyPolicy.setPrincipalSource(PrincipalSource.IAM.getNum());
        denyPolicy.setPrincipalType(PrincipalType.GROUP.getNum());
        denyPolicy.setPrincipalId("DataEngineer");
        denyPolicy.setGrantAble(false);
        policies.add(denyPolicy.build());

        authTable.init(policies);
    }

    @Test
    public void isOwner() {
        AuthRequest.Builder request = new AuthRequest.Builder();
        request.setPrivilege(PrivilegeType.SELECT.getType());
        List<String> userGroups = new ArrayList<>();
        request.setAccessType(AccessType.TABLE_ACCESS.getNum());
        userGroups.add("DataEngineer");
        request.setUserGroup(userGroups);
        request.setObjectId("c1.d1.t1");
        request.setObjectType(ObjectType.TABLE.getNum());
        request.setOperation(Operation.SELECT_TABLE.ordinal());
        AuthResult result = authTable.isOwner(request.build());
        assertEquals(result.isAllowed(), true);
    }
}
