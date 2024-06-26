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


import io.polycat.catalog.server.util.ResponseUtil;
import io.polycat.catalog.common.model.AuthorizationResponse;
import io.polycat.catalog.common.model.CatalogResponse;
import io.polycat.catalog.common.plugin.request.input.AuthorizationInput;

import io.polycat.catalog.service.api.PrivilegeService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Api(tags = "privilege api")
@ApiResponses(value = {
    @ApiResponse(code = 400, message = "Bad Request"),
    @ApiResponse(code = 408, message = "Request Timeout"),
    @ApiResponse(code = 500, message = "Internal Server Error")
})
@RestController
@RequestMapping("/v1/{project-id}/authentication")
public class PrivilegeController extends BaseController {

    @Autowired
    private PrivilegeService privilegeService;

    @ApiOperation(value = "data authentication")
    @PostMapping(value = "", produces = "application/json;charset=UTF-8")
    public CatalogResponse<AuthorizationResponse> sqlAuthentication(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "ignore unknown object", required = false) @RequestParam(value = "ignoreUnknownObj", required = false, defaultValue = "false") Boolean ignoreUnknownObj,
        @ApiParam(value = "authorization input list body", required = true) @Valid @RequestBody
            List<AuthorizationInput> authorizationInputList,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            AuthorizationResponse result = privilegeService.sqlAuthentication(projectId,
                authorizationInputList, ignoreUnknownObj);
            return ResponseUtil.responseSuccess(result);
        });
    }
}