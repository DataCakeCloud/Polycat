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

import java.util.Map;
import javax.ws.rs.QueryParam;

import io.polycat.catalog.common.model.BaseResponse;
import io.polycat.catalog.common.plugin.request.input.UserGroupInput;
import io.polycat.catalog.server.util.BaseResponseUtil;
import io.polycat.catalog.service.api.UserGroupService;


import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Api(tags = "user group api")
@ApiResponses(value = {
    @ApiResponse(code = 400, message = "Bad Request"),
    @ApiResponse(code = 408, message = "Request Timeout"),
    @ApiResponse(code = 500, message = "Internal Server Error")
})
@RestController
@RequestMapping("/v1/projects/{projectId}/userGroup")
public class UserGroupController extends BaseController {

    @Autowired
    private UserGroupService userGroupService;

    @ApiOperation(value = "sync user")
    @PostMapping(value = "/syncUser", produces = "application/json;charset=UTF-8")
    public BaseResponse syncUserInfo(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "projectId") String projectId,
        @ApiParam(value = "domain id", required = true) @QueryParam(value = "domain_id") String domainId,
        @ApiParam(value = "input body", required = true) @RequestBody UserGroupInput userInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            System.out.println("Start sync user info");
            userGroupService.syncUser(projectId, domainId, userInput);
            return BaseResponseUtil.responseSuccess(HttpStatus.CREATED);
        });
    }

    @ApiOperation(value = "sync group")
    @PostMapping(value = "/syncGroup", produces = "application/json;charset=UTF-8")
    public BaseResponse  syncGroupInfo(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "projectId") String projectId,
        @ApiParam(value = "domain id", required = true) @QueryParam(value = "domain_id") String domainId,
        @ApiParam(value = "input body", required = true) @RequestBody UserGroupInput groupInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            System.out.println("Start sync user info");
            userGroupService.syncGroup(projectId, domainId, groupInput);
            return BaseResponseUtil.responseSuccess(HttpStatus.CREATED);
        });
    }


    @ApiOperation(value = "sync user group user")
    @PostMapping(value = "/syncUserGroup", produces = "application/json;charset=UTF-8")
    public BaseResponse syncUserGroupInfo(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "projectId") String projectId,
        @ApiParam(value = "domain id", required = true) @QueryParam(value = "domain_id") String domainId,
        @ApiParam(value = "input body", required = true) @RequestBody Map<String, Object> groupUserIndex,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            System.out.println("Start sync user info");
            userGroupService.syncGroupUserIndex(projectId,domainId,groupUserIndex);
            return BaseResponseUtil.responseSuccess(HttpStatus.CREATED);
        });
    }
}
