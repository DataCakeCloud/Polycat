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

import io.polycat.catalog.common.model.AcceleratorObject;
import io.polycat.catalog.common.model.BaseResponse;
import io.polycat.catalog.common.model.CatalogResponse;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.PagedList;
import io.polycat.catalog.common.plugin.request.input.AcceleratorInput;
import io.polycat.catalog.server.util.BaseResponseUtil;
import io.polycat.catalog.server.util.ResponseUtil;
import io.polycat.catalog.service.api.AcceleratorService;
import io.polycat.catalog.store.common.StoreConvertor;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Api(tags = "accelerator api")
@ApiResponses(value = {
    @ApiResponse(code = 400, message = "Bad Request"),
    @ApiResponse(code = 408, message = "Request Timeout"),
    @ApiResponse(code = 500, message = "Internal Server Error")
})
@RestController
@RequestMapping("/v1/{project-id}/catalogs/{catalog-name}/databases/{database-name}/accelerators")
public class AcceleratorController extends BaseController {

    @Autowired
    private AcceleratorService acceleratorService;

    @ApiOperation(value = "create accelerator")
    @PostMapping(value = "", produces = "application/json;charset=UTF-8")
    public BaseResponse createAccelerator(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "accelerator input body", required = true) @RequestBody AcceleratorInput acceleratorInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            DatabaseName database = StoreConvertor.databaseName(projectId, catalogName, databaseName);
            acceleratorService.createAccelerator(database, acceleratorInput);
            return BaseResponseUtil.responseSuccess();
        });
    }

    @ApiOperation(value = "show accelerators")
    @GetMapping(value = "", produces = "application/json;charset=UTF-8")
    public CatalogResponse<PagedList<AcceleratorObject>> showAccelerators(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable("database-name") String databaseName,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            DatabaseName database = StoreConvertor.databaseName(projectId, catalogName, databaseName);
            List<AcceleratorObject> tableList = acceleratorService.showAccelerators(database);
            PagedList<AcceleratorObject> result = new PagedList<>();
            result.setObjects(tableList.toArray(new AcceleratorObject[0]));
            return ResponseUtil.responseSuccess(result);
        });
    }

    @ApiOperation(value = "drop accelerator")
    @DeleteMapping(value = "/{accelerator-name}", produces = "application/json;charset=UTF-8")
    public BaseResponse dropAccelerator(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable("database-name") String databaseName,
        @ApiParam(value = "accelerator name", required = true) @PathVariable("acceleratorName") String acceleratorName,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            acceleratorService.dropAccelerators(projectId, catalogName, databaseName, acceleratorName);
            return BaseResponseUtil.responseSuccess();
        });
    }

    @ApiOperation(value = "alter accelerator")
    @PostMapping(value = "/{accelerator-name}/compile", produces = "application/json;charset=UTF-8")
    public BaseResponse alterAccelerator(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "accelerator name", required = true) @PathVariable(value = "acceleratorName") String acceleratorName,
        @ApiParam(value = "accelerator input body", required = true) @RequestBody AcceleratorInput acceleratorInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            acceleratorService.alterAccelerator(projectId, catalogName, databaseName, acceleratorName, acceleratorInput);
            return BaseResponseUtil.responseSuccess();
        });
    }
}
