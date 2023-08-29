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

import java.util.Optional;

import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.model.CatalogResponse;
import io.polycat.catalog.common.model.MetaObjectName;
import io.polycat.catalog.server.util.ResponseUtil;

import io.polycat.catalog.service.api.ObjectNameMapService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.*;

@Api(tags = "object name map api")
@ApiResponses(value = {
    @ApiResponse(code = 400, message = "Bad Request"),
    @ApiResponse(code = 408, message = "Request Timeout"),
    @ApiResponse(code = 500, message = "Internal Server Error")
})
@RestController
@RequestMapping("/v1/{project-id}/objectNameMap")
public class ObjectNameMapController extends BaseController {

    @Autowired
    private ObjectNameMapService objectNameMapService;

    @ApiOperation(value = "get object")
    @GetMapping(value = "/getObject", produces = "application/json;charset=UTF-8")
    public CatalogResponse<MetaObjectName> getObject(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "database name", required = true) @RequestParam("databaseName") String databaseName,
        @ApiParam(value = "object name", required = true) @RequestParam("objectName") String objectName,
        @ApiParam(value = "object type", required = true) @RequestParam("objectType") Integer objectType,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            Optional<MetaObjectName> metaObjectName;
            String dbNameIgnCase = databaseName.toLowerCase();
            if (objectType == ObjectType.DATABASE.getNum()) {
                metaObjectName = objectNameMapService.getObjectFromNameMap(projectId, ObjectType.forNum(objectType).name(), dbNameIgnCase);
            } else {
                metaObjectName = objectNameMapService
                    .getObjectFromNameMap(projectId, ObjectType.forNum(objectType).name(), dbNameIgnCase, objectName);
            }

            if (!metaObjectName.isPresent()) {
                return ResponseUtil.responseFromErrorCode(ErrorCode.OBJECT_NAME_MAP_NOT_FOUND,
                    String.format("Object name map {%s.%s} not found", dbNameIgnCase, objectName));
            }

            return ResponseUtil.responseSuccess(metaObjectName.get());
        });
    }
}
