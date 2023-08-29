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
package io.polycat.catalog.client.authorization;

import java.util.ArrayList;
import java.util.List;

import io.polycat.catalog.authorization.policyDecisionPoint.AccessType;
import io.polycat.catalog.authorization.policyDecisionPoint.AuthRequest;
import io.polycat.catalog.authorization.policyDecisionPoint.AuthResult;
import io.polycat.catalog.authorization.policyDecisionPoint.AuthTable;
import io.polycat.catalog.client.PolyCatClient;
import io.polycat.catalog.common.model.MetaPrivilegePolicy;
import io.polycat.catalog.common.model.PrincipalType;

import org.apache.hadoop.conf.Configuration;

public class PrivilegeClientImpl implements PrivilegeClient {

    private PolicyCache policyCache;

    public void init(Configuration conf, PolyCatClient polyCatClient) {
        policyCache = PolicyCache.getInstance(conf, polyCatClient);
    }

    /**
     * checkPrivilege
     *
     * @param authRequest authRequest
     */
    @Override
    public AuthResult checkPrivilege(AuthRequest authRequest) {

        List<MetaPrivilegePolicy> privilegePolicies = getPrivilegePolicy(authRequest);
        if (authRequest.getAccessType() == AccessType.TABLE_ACCESS.ordinal()) {
            AuthTable authTable = new AuthTable();
            authTable.init(privilegePolicies);
            return authTable.checkPrivilege(authRequest);
        }

        AuthResult.Builder result = new AuthResult.Builder();
        result.setIsAllowed(false);
        result.setReason("The access type is not recognized!");
        return result.build();
    }

    private List<MetaPrivilegePolicy> getPrivilegePolicy(AuthRequest authRequest) {
        List<MetaPrivilegePolicy> policies = new ArrayList<>();
        //get user policies
        policies = policyCache.getPolicyByPrincipal(PrincipalType.USER.toString(), authRequest.getUser());
        //get UserGroups policies
        for (String userGroup : authRequest.getUserGroups()) {
            List<MetaPrivilegePolicy> userGroupsPolicies = new ArrayList<>();
            userGroupsPolicies = policyCache.getPolicyByPrincipal(PrincipalType.GROUP.toString(), userGroup);
            policies.addAll(userGroupsPolicies);
        }
        //get userRoles policies
        for (String userRole : authRequest.getUserRoles()){
            List<MetaPrivilegePolicy> rolesPolicies = new ArrayList<>();
            rolesPolicies = policyCache.getPolicyByPrincipal(PrincipalType.ROLE.toString(), userRole);
            policies.addAll(rolesPolicies);
        }
        return policies;
    }
}
