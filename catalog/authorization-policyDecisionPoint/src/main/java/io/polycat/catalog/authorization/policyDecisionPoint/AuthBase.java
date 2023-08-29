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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.model.MetaPrivilegePolicy;
import io.polycat.catalog.common.model.OperationPrivilege;
import io.polycat.catalog.common.model.PrivilegeType;
import io.polycat.catalog.common.ObjectType;

/*
Authentication base class
 */
public class AuthBase {

    protected PrivilegePolicyEva privilegePolicyEva;
    protected List<MetaPrivilegePolicy> ownerPolicies;

    protected static Map<Operation, OperationPrivilege> operationPrivilegeMap = new ConcurrentHashMap<>();

    public AuthBase() {
        ownerPolicies = new ArrayList<>();
        privilegePolicyEva = new PrivilegePolicyEva();

        //init operationPrivilegeMap
        operationPrivilegeMap.put(Operation.DESC_CATALOG,
            new OperationPrivilege(ObjectType.CATALOG,
                PrivilegeType.DESC.getType()));

        operationPrivilegeMap.put(Operation.DROP_CATALOG,
            new OperationPrivilege(ObjectType.CATALOG,
                PrivilegeType.DROP.getType()));

        operationPrivilegeMap.put(Operation.ALTER_CATALOG,
            new OperationPrivilege(ObjectType.CATALOG,
                PrivilegeType.ALTER.getType()));

        operationPrivilegeMap.put(Operation.USE_CATALOG,
            new OperationPrivilege(ObjectType.CATALOG,
                PrivilegeType.USE.getType()));

        operationPrivilegeMap.put(Operation.CREATE_BRANCH,
            new OperationPrivilege(ObjectType.CATALOG,
                PrivilegeType.CREATE_BRANCH.getType()));

        operationPrivilegeMap.put(Operation.USE_BRANCH,
            new OperationPrivilege(ObjectType.CATALOG,
                PrivilegeType.USE.getType()));

        operationPrivilegeMap.put(Operation.SHOW_BRANCH,
            new OperationPrivilege(ObjectType.CATALOG,
                PrivilegeType.SHOW_BRANCH.getType()));

        operationPrivilegeMap.put(Operation.MERGE_BRANCH,
            new OperationPrivilege(ObjectType.CATALOG,
                PrivilegeType.MERGE_BRANCH.getType()));

        operationPrivilegeMap.put(Operation.CREATE_DATABASE,
            new OperationPrivilege(ObjectType.CATALOG,
                PrivilegeType.CREATE_DATABASE.getType()));

        operationPrivilegeMap.put(Operation.SHOW_DATABASE,
            new OperationPrivilege(ObjectType.CATALOG,
                PrivilegeType.SHOW_DATABASE.getType()));

        operationPrivilegeMap.put(Operation.DESC_DATABASE,
            new OperationPrivilege(ObjectType.DATABASE,
                PrivilegeType.DESC.getType()));

        operationPrivilegeMap.put(Operation.DROP_DATABASE,
            new OperationPrivilege(ObjectType.DATABASE,
                PrivilegeType.DROP.getType()));

        operationPrivilegeMap.put(Operation.UNDROP_DATABASE,
            new OperationPrivilege(ObjectType.DATABASE,
                PrivilegeType.UNDROP.getType()));

        operationPrivilegeMap.put(Operation.ALTER_DATABASE,
            new OperationPrivilege(ObjectType.DATABASE,
                PrivilegeType.ALTER.getType()));

        operationPrivilegeMap.put(Operation.USE_DATABASE,
            new OperationPrivilege(ObjectType.DATABASE,
                PrivilegeType.USE.getType()));

        operationPrivilegeMap.put(Operation.CREATE_TABLE,
            new OperationPrivilege(ObjectType.DATABASE,
                PrivilegeType.CREATE_TABLE.getType()));

        operationPrivilegeMap.put(Operation.SHOW_TABLE,
            new OperationPrivilege(ObjectType.DATABASE,
                PrivilegeType.SHOW_TABLE.getType()));

        operationPrivilegeMap.put(Operation.CREATE_VIEW,
            new OperationPrivilege(ObjectType.DATABASE,
                PrivilegeType.CREATE_VIEW.getType()));

        operationPrivilegeMap.put(Operation.SHOW_VIEW,
            new OperationPrivilege(ObjectType.DATABASE,
                PrivilegeType.SHOW_VIEW.getType()));

        operationPrivilegeMap.put(Operation.DESC_TABLE,
            new OperationPrivilege(ObjectType.TABLE,
                PrivilegeType.DESC.getType()));

        operationPrivilegeMap.put(Operation.DROP_TABLE,
            new OperationPrivilege(ObjectType.TABLE,
                PrivilegeType.DROP.getType()));

        operationPrivilegeMap.put(Operation.PURGE_TABLE,
            new OperationPrivilege(ObjectType.TABLE,
                PrivilegeType.DROP.getType()));

        operationPrivilegeMap.put(Operation.ALTER_TABLE,
            new OperationPrivilege(ObjectType.TABLE,
                PrivilegeType.ALTER.getType()));

        operationPrivilegeMap.put(Operation.UNDROP_TABLE,
            new OperationPrivilege(ObjectType.TABLE,
                PrivilegeType.UNDROP.getType()));

        operationPrivilegeMap.put(Operation.RESTORE_TABLE,
            new OperationPrivilege(ObjectType.TABLE,
                PrivilegeType.RESTORE.getType()));

        operationPrivilegeMap.put(Operation.SELECT_TABLE,
            new OperationPrivilege(ObjectType.TABLE,
                PrivilegeType.SELECT.getType()));

        operationPrivilegeMap.put(Operation.INSERT_TABLE,
            new OperationPrivilege(ObjectType.TABLE,
                PrivilegeType.INSERT.getType()));

        operationPrivilegeMap.put(Operation.ALTER_VIEW,
            new OperationPrivilege(ObjectType.VIEW,
                PrivilegeType.ALTER.getType()));

        operationPrivilegeMap.put(Operation.DROP_VIEW,
            new OperationPrivilege(ObjectType.VIEW,
                PrivilegeType.DROP.getType()));

        operationPrivilegeMap.put(Operation.DESC_VIEW,
            new OperationPrivilege(ObjectType.VIEW,
                PrivilegeType.DESC.getType()));

        operationPrivilegeMap.put(Operation.ALTER_COLUMN,
            new OperationPrivilege(ObjectType.TABLE,
                PrivilegeType.CHANGE_SCHEMA.getType()));

        operationPrivilegeMap.put(Operation.SHOW_ACCESS_STATS_FOR_CATALOG,
            new OperationPrivilege(ObjectType.CATALOG,
                PrivilegeType.SHOW_ACCESSSTATS.getType()));

        operationPrivilegeMap.put(Operation.DESC_ACCESS_STATS_FOR_TABLE,
            new OperationPrivilege(ObjectType.TABLE,
                PrivilegeType.DESC_ACCESSSTATS.getType()));

        operationPrivilegeMap.put(Operation.SHOW_DATA_LINEAGE_FOR_TABLE,
            new OperationPrivilege(ObjectType.TABLE,
                PrivilegeType.SHOW_DATALINEAGE.getType()));

        operationPrivilegeMap.put(Operation.SET_PROPERTIES,
            new OperationPrivilege(ObjectType.TABLE,
                PrivilegeType.ALTER.getType()));

        operationPrivilegeMap.put(Operation.UNSET_PROPERTIES,
            new OperationPrivilege(ObjectType.TABLE,
                PrivilegeType.ALTER.getType()));

        operationPrivilegeMap.put(Operation.ADD_PARTITION,
            new OperationPrivilege(ObjectType.TABLE,
                PrivilegeType.INSERT.getType()));

        operationPrivilegeMap.put(Operation.DROP_PARTITION,
            new OperationPrivilege(ObjectType.TABLE,
                PrivilegeType.DELETE.getType()));

        operationPrivilegeMap.put(Operation.COPY_INTO,
            new OperationPrivilege(ObjectType.TABLE,
                PrivilegeType.COPY_INTO.getType()));

        operationPrivilegeMap.put(Operation.SHOW_CREATE_TABLE,
            new OperationPrivilege(ObjectType.TABLE,
                PrivilegeType.DESC.getType()));
    }

    public void init(List<MetaPrivilegePolicy> policies) {
        for (MetaPrivilegePolicy policy : policies) {
            if (policy.getPrivilege() == PrivilegeType.OWNER.getType()) {
                ownerPolicies.add(policy);
            }
        }
        privilegePolicyEva.init(policies);
    }

    /**
     * isOwner
     *
     * @param request request
     * @return AuthResult
     */
    public AuthResult isOwner(AuthRequest request) {
        AuthResult.Builder result = new AuthResult.Builder();
        for (MetaPrivilegePolicy policy : ownerPolicies) {
            if ((policy.getPrivilege() == PrivilegeType.OWNER.getType()) &&
                (policy.getObjectType() == request.getObjectType()) &&
                (policy.getObjectId().equals(request.getObjectId()))) {
                result.setIsAllowed(true);
                result.setPolicyId(policy.getPolicyId());
                result.setReason(request.getUser() + "is owner!");
                return result.build();
            }
        }
        result.setIsAllowed(false);
        return result.build();
    }

    /**
     * checkPrivilege
     *
     * @param request request
     * @return AuthResult
     */
    public AuthResult checkPrivilege(AuthRequest request) {
        AuthResult result = isOwner(request);
        if (result.isAllowed()) {
            return result;
        }
        return privilegePolicyEva.isMatch(setRequestPrivilege(request));
    }

    /**
     * setRequestPrivilegeb: Get the required privilege by operation.
     *
     * @param request request
     * @return AuthRequest
     */
    protected AuthRequest setRequestPrivilege(AuthRequest request) {
        AuthRequest.Builder authRequest = new AuthRequest.Builder();
        authRequest.setAccessType(request.getAccessType());
        authRequest.setProjectId(request.getProjectId());
        authRequest.setUser(request.getUser());
        authRequest.setAccountId(request.getAccountId());
        authRequest.setUserGroup(request.getUserGroups());
        authRequest.setUserRoles(request.getUserRoles());
        authRequest.setOperation(request.getOperation());
        authRequest.setObjectType(request.getObjectType());
        authRequest.setObjectId(request.getObjectId());
        authRequest.setObjectName(request.getObjectName());
        authRequest.setPrivilege(operationPrivilegeMap.get(Operation.values()[request.getOperation()]).getPrivilege());
        return authRequest.build();
    }
}