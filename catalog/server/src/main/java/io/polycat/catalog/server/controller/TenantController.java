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

import io.polycat.catalog.audit.api.UserLog;
import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.model.Catalog;
import io.polycat.catalog.common.model.CatalogResponse;
import io.polycat.catalog.server.util.ResponseUtil;
import io.polycat.catalog.service.api.CatalogResourceService;
import io.swagger.annotations.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

@Api(tags = "tanant api")
@ApiResponses(value = {
        @ApiResponse(code = 400, message = "Bad Request"),
        @ApiResponse(code = 408, message = "Request Timeout"),
        @ApiResponse(code = 500, message = "Internal Server Error")
})
@RestController
@RequestMapping("/v1/{project-id}/tenants")
@Validated
public class TenantController extends BaseController {

    @Autowired
    private CatalogResourceService resourceService;

    @UserLog(operation = "create project", objectType = ObjectType.OTHERS)
    @ApiOperation(value = "create project")
    @PostMapping(value = "", produces = "application/json;charset=UTF-8")
    public CatalogResponse<Catalog> createProject(
            @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
            @ApiParam(value = "authorization") @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            if (!resourceService.doesExistResource(projectId)) {
                resourceService.createResource(projectId);
                return ResponseUtil.responseSuccess(HttpStatus.CREATED);
            } else {
                throw new CatalogServerException(ErrorCode.TENANT_PROJECT_ALREADY_EXIST, projectId);
            }
        });
    }
/*
    *//**
     * drop catalog By name
     *
     * @param projectId projectId
     * @return ResponseEntity
     *//*
    @ApiOperation(value = "drop project by name")
    @DeleteMapping(value = "/{project-name}", produces = "application/json;charset=UTF-8")
    public BaseResponse dropProject(
            @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
            @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            resourceService.dropResource(projectId);
            return BaseResponseUtil.responseSuccess();
        });
    }*/
}
