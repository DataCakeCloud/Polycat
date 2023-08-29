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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.model.MetaPrivilegePolicy;
import io.polycat.catalog.common.model.PrincipalSource;
import io.polycat.catalog.common.model.PrincipalType;
import io.polycat.catalog.common.model.PrivilegeType;
import io.polycat.catalog.common.model.ShareConsumer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AuthShareTest {

    @Test
    public void checkPrivilegeForAlterShare() {
        List<MetaPrivilegePolicy> policies = new ArrayList<>();
        MetaPrivilegePolicy.Builder allowPolicy = new MetaPrivilegePolicy.Builder();
        allowPolicy.setPolicyId(UUID.randomUUID().toString().toLowerCase());
        allowPolicy.setProjectId("CD_Storage");
        allowPolicy.setEffect(true);
        allowPolicy.setObjectId("s1");
        allowPolicy.setObjectType(ObjectType.SHARE.getNum());
        allowPolicy.setPrivilege(PrivilegeType.ALTER.getType());
        allowPolicy.setPrincipalSource(PrincipalSource.IAM.getNum());
        allowPolicy.setPrincipalType(PrincipalType.GROUP.getNum());
        allowPolicy.setPrincipalId("LakeAdmin");
        allowPolicy.setGrantAble(true);
        policies.add(allowPolicy.build());

        AuthShare authShare = new AuthShare();
        authShare.init(policies);

        AuthRequest.Builder request = new AuthRequest.Builder();
        List<String> userGroups = new ArrayList<>();
        request.setAccessType(AccessType.SHARE_ACCESS.getNum());
        userGroups.add("LakeAdmin");
        request.setUserGroup(userGroups);
        request.setObjectId("s1");
        request.setObjectType(ObjectType.SHARE.getNum());
        request.setOperation(Operation.ALTER_SHARE.ordinal());

        AuthResult result = authShare.checkPrivilege(request.build());
        assertEquals(true, result.isAllowed());
    }

    @Test
    public void checkPrivilegeForDropShare() {
        List<MetaPrivilegePolicy> policies = new ArrayList<>();
        MetaPrivilegePolicy.Builder denyPolicy = new MetaPrivilegePolicy.Builder();
        denyPolicy.setPolicyId(UUID.randomUUID().toString().toLowerCase());
        denyPolicy.setProjectId("CD_Storage");
        denyPolicy.setEffect(false);
        denyPolicy.setObjectId("s1");
        denyPolicy.setObjectType(ObjectType.SHARE.getNum());
        denyPolicy.setPrivilege(PrivilegeType.DROP.getType());
        denyPolicy.setPrincipalSource(PrincipalSource.IAM.getNum());
        denyPolicy.setPrincipalType(PrincipalType.GROUP.getNum());
        denyPolicy.setPrincipalId("DataEngineer");
        denyPolicy.setGrantAble(false);
        policies.add(denyPolicy.build());

        AuthShare authShare = new AuthShare();
        authShare.init(policies);

        AuthRequest.Builder request = new AuthRequest.Builder();
        List<String> userGroups = new ArrayList<>();
        request.setAccessType(AccessType.SHARE_ACCESS.getNum());
        userGroups.add("DataEngineer");
        request.setUserGroup(userGroups);
        request.setObjectId("s1");
        request.setObjectType(ObjectType.SHARE.getNum());
        request.setOperation(Operation.DROP_SHARE.ordinal());

        AuthResult result = authShare.checkPrivilege(request.build());
        assertEquals(false, result.isAllowed());
    }

    @Test
    public void checkPrivilegeForSelectShare() {
        List<MetaPrivilegePolicy> policies = new ArrayList<>();
        MetaPrivilegePolicy.Builder selectPolicy = new MetaPrivilegePolicy.Builder();
        selectPolicy.setPolicyId(UUID.randomUUID().toString().toLowerCase());
        selectPolicy.setProjectId("CD_Storage");
        selectPolicy.setEffect(true);
        selectPolicy.setObjectId("c1.d1.t1");
        selectPolicy.setObjectType(ObjectType.TABLE.getNum());
        selectPolicy.setPrivilege(PrivilegeType.SELECT.getType());
        selectPolicy.setPrincipalSource(PrincipalSource.LOCAL.getNum());
        selectPolicy.setPrincipalType(PrincipalType.SHARE.getNum());
        selectPolicy.setPrincipalId("s1");
        selectPolicy.setGrantAble(false);
        selectPolicy.setObligation("");
        policies.add(selectPolicy.build());

        AuthShare authShare = new AuthShare();
        authShare.init(policies);

        AuthRequest.Builder request = new AuthRequest.Builder();
        List<String> userGroups = new ArrayList<>();
        request.setAccessType(AccessType.SHARE_ACCESS_SELECT.getNum());
        userGroups.add("DataEngineer");
        request.setUserGroup(userGroups);
        request.setObjectId("p1.s1.d1.t1");
        request.setObjectType(ObjectType.SHARE.getNum());
        request.setOperation(Operation.SELECT_SHARE.ordinal());
        request.setUser("shareTest");
        request.setAccountId("consumer");
        request.setUserSource(PrincipalSource.IAM.toString());
        authShare.getCatalogId("c1");

        Map<String, String> users = new HashMap<String, String>();
        users.put("shareTest", PrincipalType.USER.toString() + ":" + PrincipalSource.IAM.toString());
        ShareConsumer shareConsumer = new ShareConsumer("p1", "s1",
            "consumer", "ShareAdmin", users);

        List<ShareConsumer> shareConsumers = new ArrayList<>();
        shareConsumers.add(shareConsumer);
        authShare.getShareConsumers(shareConsumers);
        authShare.getCatalogId("c1");

        AuthResult result = authShare.checkPrivilege(request.build());
        assertEquals(true, result.isAllowed());
    }
}