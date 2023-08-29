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
package io.polycat.catalog.server.controller;

import java.util.List;

import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.model.BaseResponse;
import io.polycat.catalog.common.model.CatalogResponse;
import io.polycat.catalog.common.model.MetaPolicyHistory;
import io.polycat.catalog.common.model.MetaPrivilegePolicy;
import io.polycat.catalog.common.model.MetaPrivilegePolicyAggrData;
import io.polycat.catalog.common.model.PagedList;
import io.polycat.catalog.common.model.Policy;
import io.polycat.catalog.common.plugin.request.input.PolicyInput;
import io.polycat.catalog.server.util.BaseResponseUtil;
import io.polycat.catalog.server.util.ResponseUtil;
import io.polycat.catalog.service.api.PolicyService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Api(tags = "policy api")
@ApiResponses(value = {
    @ApiResponse(code = 400, message = "Bad Request"),
    @ApiResponse(code = 408, message = "Request Timeout"),
    @ApiResponse(code = 500, message = "Internal Server Error")
})
@RestController
@RequestMapping("/v1/projects/{projectId}/policy")
public class PolicyController extends BaseController {

    @Autowired
    private PolicyService policyService;

    @ApiOperation(value = "grant privilege to principal")
    @PatchMapping(value = "/grantPrivilege", produces = "application/json;charset=UTF-8")
    public BaseResponse grantPrivilegeToPrincipal(
        @ApiParam(value = "project id", required = true) @PathVariable("projectId") String projectId,
        @ApiParam(value = "principal type", required = true) @RequestParam("principalType") String principalType,
        @ApiParam(value = "principal source", required = true) @RequestParam("principalSource") String principalSource,
        @ApiParam(value = "principal name", required = true) @RequestParam("principalName") String principalName,
        @ApiParam(value = "policy input body", required = true) @Valid @RequestBody PolicyInput policyInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            if (policyInput.getOperationList().get(0) == Operation.ADD_ALL_OPERATION) {
                policyService.addAllPrivilegeOnObjectToPrincipal(projectId, principalName, policyInput);
            } else {
                policyService.addMetaPolicyToPrincipal(projectId, principalName, policyInput);
            }
            return BaseResponseUtil.responseSuccess();
        });
    }

    @ApiOperation(value = "revoke privilege to principal")
    @PatchMapping(value = "/revokePrivilege", produces = "application/json;charset=UTF-8")
    public BaseResponse revokePrivilegeFromPrincipal(
        @ApiParam(value = "project id", required = true) @PathVariable("projectId") String projectId,
        @ApiParam(value = "principal type", required = true) @RequestParam("principalType") String principalType,
        @ApiParam(value = "principal source", required = true) @RequestParam("principalSource") String principalSource,
        @ApiParam(value = "principal name", required = true) @RequestParam("principalName") String principalName,
        @ApiParam(value = "policy input body", required = true) @Valid @RequestBody PolicyInput policyInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            if (policyInput.getOperationList().get(0) == Operation.REVOKE_ALL_OPERATION_FROM_ROLE) {
                policyService.removeAllMetaPrivilegeFromPrincipal(projectId, principalName, policyInput);
            } else if (policyInput.getOperationList().get(0) == Operation.REVOKE_ALL_OPERATION) {
                policyService.removeAllPrivilegeOnObjectFromPrincipal(projectId, principalName, policyInput);
            } else {
                policyService.revokeMetaPolicyFromPrincipal(projectId, principalName, policyInput);
            }
            return BaseResponseUtil.responseSuccess();
        });
    }

    @ApiOperation(value = "show privilege of principal")
    @GetMapping(value = "/showPrivilege", produces = "application/json;charset=UTF-8")
    public CatalogResponse<PagedList<Policy>> showPrivilegeOfPrincipal(
        @ApiParam(value = "project id", required = true) @PathVariable("projectId") String projectId,
        @ApiParam(value = "principal type", required = true) @RequestParam("principalType") String principalType,
        @ApiParam(value = "principal source", required = true) @RequestParam("principalSource") String principalSource,
        @ApiParam(value = "principal name", required = true) @RequestParam("principalName") String principalName,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            PagedList<Policy> result = new PagedList<>();
            List<Policy> policies = policyService.showMetaPolicyFromPrincipal(projectId,
                principalType, principalSource, principalName);
            result.setObjects(policies.toArray(new Policy[0]));
            return ResponseUtil.responseSuccess(result);
        });
    }

    @ApiOperation(value = "get Updated Policy History by time")
    @GetMapping(value = "/listPolicyHistoryByTime", produces = "application/json;charset=UTF-8")
    public CatalogResponse<PagedList<MetaPolicyHistory>> listUpdatedMetaPolicyHistoryByTime(
        @ApiParam(value = "project id", required = true) @PathVariable("projectId") String projectId,
        @ApiParam(value = "updated time", required = true) @RequestParam("updatedTime") long updatedTime,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            PagedList<MetaPolicyHistory> result = new PagedList<>();
            List<MetaPolicyHistory> policies = policyService.getUpdatedMetaPolicyIdsByTime(projectId, updatedTime);
            result.setObjects(policies.toArray(new MetaPolicyHistory[0]));
            return ResponseUtil.responseSuccess(result);
        });
    }

    @ApiOperation(value = "get Updated Meta Policy by policy Id")
    @PostMapping(value = "/listPrivilegeOfId", produces = "application/json;charset=UTF-8")
    public CatalogResponse<MetaPrivilegePolicyAggrData> listUpdatedMetaPolicyOfId(
        @ApiParam(value = "project id", required = true) @PathVariable("projectId") String projectId,
        @ApiParam(value = "principal type", required = true) @RequestParam("principalType") String principalType,
        @ApiParam(value = "policy input body", required = true) @Valid @RequestBody PolicyInput policyInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            MetaPrivilegePolicyAggrData policyAggrData = new MetaPrivilegePolicyAggrData();
            List<MetaPrivilegePolicy> policies = policyService.getUpdatedMetaPolicyByIdList(projectId, principalType, policyInput.getPolicyIdList());
            policyAggrData.setPolicy_num(policies.size());
            policyAggrData.setPrivilegePolicyList(policies);
            return ResponseUtil.responseSuccess(policyAggrData);
        });
    }

    @ApiOperation(value = "export privilege")
    @GetMapping(value = "/exportPrivilege", produces = "application/json;charset=UTF-8")
    public BaseResponse exportPrivilegePolicies(
        @ApiParam(value = "project id", required = true) @PathVariable("projectId") String projectId,
        @ApiParam(value = "principal type", required = true) @RequestParam("principalType") String principalType,
        @ApiParam(value = "principal source", required = true) @RequestParam("principalSource") String principalSource,
        @ApiParam(value = "principal name", required = true) @RequestParam("principalName") String principalName,
        @ApiParam(value = "policy input body", required = true) @Valid @RequestBody PolicyInput policyInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            return BaseResponseUtil.responseSuccess();
        });
    }

    @ApiOperation(value = "import privilege")
    @PatchMapping(value = "/importPrivilege", produces = "application/json;charset=UTF-8")
    public BaseResponse importPrivilegePolicies(
        @ApiParam(value = "project id", required = true) @PathVariable("projectId") String projectId,
        @ApiParam(value = "principal type", required = true) @RequestParam("principalType") String principalType,
        @ApiParam(value = "principal source", required = true) @RequestParam("principalSource") String principalSource,
        @ApiParam(value = "principal name", required = true) @RequestParam("principalName") String principalName,
        @ApiParam(value = "policy input body", required = true) @Valid @RequestBody PolicyInput policyInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            return BaseResponseUtil.responseSuccess();
        });
    }

}
