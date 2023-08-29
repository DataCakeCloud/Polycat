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

import io.polycat.catalog.common.model.ColumnFilter;
import io.polycat.catalog.common.model.DataMask;
import io.polycat.catalog.common.model.MaskType;
import io.polycat.catalog.common.model.MetaPrivilegePolicy;
import io.polycat.catalog.common.model.ObligationType;
import io.polycat.catalog.common.model.PrivilegeType;
import io.polycat.catalog.common.model.RowFilter;

public class PrivilegePolicyEva {

    /* obligations
   NONE;
   COLUMN_FILTER:(column_name, column_name,column_name):include;
   ROW_FILTER:() and () or ();
   DATA_MASK:column_name:mask;
   */
    private static final String policyObligationTypeRegex = ";";
    private static final String policyObligationRegex = ":";

    private List<MetaPrivilegePolicy> denyPolicyList;
    private List<MetaPrivilegePolicy> allowPolicyList;

    /**
     * init
     *
     * @param policyList policyList
     */
    public void init(List<MetaPrivilegePolicy> policyList) {
        denyPolicyList = new ArrayList<>();
        allowPolicyList = new ArrayList<>();
        for (MetaPrivilegePolicy policy : policyList) {
            if (policy.isEffect()) {
                allowPolicyList.add(policy);
            } else {
                denyPolicyList.add(policy);
            }
        }
    }

    /**
     * isMatch
     *
     * @param request request
     * @return AuthResult
     */
    public AuthResult isMatch(AuthRequest request) {
        AuthResult result = isDenyMatch(request);
        if (!result.isAllowed()) {
            return result;
        }
        return isAllowMatch(request);
    }

    private AuthResult isDenyMatch(AuthRequest request) {
        AuthResult.Builder result = new AuthResult.Builder();
        for (MetaPrivilegePolicy policy : denyPolicyList) {
            if ((policy.getPrivilege() == request.getPrivilege()) &&
                (policy.getObjectType() == request.getObjectType()) &&
                (policy.getObjectId().equals(request.getObjectId()))) {
                result.setIsAllowed(false);
                result.setPolicyId(policy.getPolicyId());
                result.setRequest(request);
                result.setReason("Deny " + policy.getPrincipalType() + ":" + policy.getPrincipalId()
                    + " " + policy.getObjectType() + ":" + policy.getObjectId() + " " + policy.getPrivilege());
                return result.build();
            }
        }

        result.setIsAllowed(true);
        return result.build();
    }

    private AuthResult isAllowMatch(AuthRequest request) {
        AuthResult.Builder result = new AuthResult.Builder();
        for (MetaPrivilegePolicy policy : allowPolicyList) {
            if ((policy.getPrivilege() == request.getPrivilege()) &&
                (policy.getObjectType() == request.getObjectType()) &&
                (policy.getObjectId().equals(request.getObjectId()))) {
                result.setIsAllowed(true);
                result.setPolicyId(policy.getPolicyId());
                result.setRequest(request);
                result.setReason("Allow " + policy.getPrincipalType() + ":" + policy.getPrincipalId()
                    + " " + policy.getObjectType() + ":" + policy.getObjectId() + " " + policy.getPrivilege());
                if ((policy.getObligation() != null)
                    && (policy.getPrivilege() == PrivilegeType.SELECT.getType())) {
                    AuthResult obligationResult = getObligation(policy.getObligation());
                    result.setColumnFilter(obligationResult.getColumnFilter());
                    result.setRowFilter(obligationResult.getRowFilter());
                    result.setDataMask(obligationResult.getDataMasks());
                }
                return result.build();
            }
        }
        result.setIsAllowed(false);
        return result.build();
    }

    /* obligations
    NONE;
    COLUMN_FILTER:(column_name, column_name,column_name):include
    ROW_FILTER:() and () or ();
    DATA_MASK:column_name:mask;
    */
    private AuthResult getObligation(String obligations) {
        AuthResult.Builder result = new AuthResult.Builder();
        RowFilter rowFilter = new RowFilter();
        ColumnFilter columnFilter = new ColumnFilter();
        List<DataMask> dataMasks = new ArrayList<>();
        String[] obligationList = obligations.split(policyObligationTypeRegex);
        for (String obligation : obligationList) {
            String[] obligationStr = obligation.split(policyObligationRegex);
            if (obligationStr.length < 1) {
                return result.build();
            }
            if (obligationStr[0].equals(ObligationType.NONE.toString())) {
                return result.build();
            }

            if ((obligationStr[0].equals(ObligationType.COLUMN_FILTER.toString())) &&
                (obligationStr.length == ObligationType.COLUMN_FILTER.getLength())) {
                columnFilter.setExpression(obligationStr[1]);
                columnFilter.setType(obligationStr[2]);
                result.setColumnFilter(columnFilter);
            }

            if ((obligationStr[0].equals(ObligationType.ROW_FILTER.toString())) &&
                (obligationStr.length == ObligationType.ROW_FILTER.getLength())) {
                rowFilter.setExpression(obligationStr[1]);
                result.setRowFilter(rowFilter);
            }

            if (obligationStr[0].equals(ObligationType.DATA_MASK.toString()) &&
                (obligationStr.length == ObligationType.DATA_MASK.getLength())) {
                DataMask dataMask = new DataMask();
                dataMask.maskType = MaskType.valueOf(obligationStr[2]);
                dataMask.columnName = obligationStr[1];
                dataMasks.add(dataMask);
            }
        }
        if (dataMasks.size() != 0) {
            result.setDataMask(dataMasks);
        }
        return result.build();
    }

    /**
     * isGrantAble
     *
     * @param request request
     * @return AuthResult
     */
    public AuthResult isGrantAble(AuthRequest request) {
        AuthResult.Builder result = new AuthResult.Builder();
        for (MetaPrivilegePolicy policy : allowPolicyList) {
            if ((policy.getPrivilege() == request.getPrivilege()) &&
                (policy.getObjectType() == request.getObjectType()) &&
                (policy.getObjectId().equals(request.getObjectId()))
                && (policy.isGrantAble())) {
                result.setIsAllowed(true);
                result.setPolicyId(policy.getPolicyId());
                result.setReason(request.getUser() + " have this permission and can be put away to others!");
                return result.build();
            }
        }
        result.setIsAllowed(false);
        return result.build();
    }
}
