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

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.model.BaseResponse;
import io.polycat.catalog.common.model.CatalogResponse;
import io.polycat.catalog.common.model.PagedList;
import io.polycat.catalog.common.plugin.request.input.FunctionInput;
import io.polycat.catalog.server.util.BaseResponseUtil;
import io.polycat.catalog.server.util.ResponseUtil;
import io.polycat.catalog.service.api.FunctionService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Api(tags = "Function api")
@ApiResponses(value = {
    @ApiResponse(code = 400, message = "Bad Request"),
    @ApiResponse(code = 408, message = "Request Timeout"),
    @ApiResponse(code = 500, message = "Internal Server Error")
})
@RestController
@RequestMapping("/v1/{project-id}/catalogs/{catalog-name}/databases/{database-name}/functions")
public class FunctionController extends BaseController {

    @Autowired
    private FunctionService functionService;

    @Api(tags = "All Functions api")
    @RestController
    @RequestMapping("/v1/{project-id}/catalogs/{catalog-name}/functions")
    public class GetAllFunctionController {

        @GetMapping(value = "", produces = "application/json;charset=UTF-8")
        public CatalogResponse<PagedList<FunctionInput>> getAllFunctions(
            @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
            @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
            @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
            return createResponse(token, () -> {
                PagedList<FunctionInput> result = new PagedList<>();
                result.setObjects(functionService.getAllFunctions(projectId, catalogName).toArray(new FunctionInput[0]));
                return ResponseUtil.responseSuccess(result);
            });
        }
    }

    @ApiOperation(value = "create function")
    @PostMapping(value = "", produces = "application/json;charset=UTF-8")
    public BaseResponse createFunction(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable("database-name") String databaseName,
        @ApiParam(value = "function input body", required = true) @Valid @RequestBody FunctionInput functionBody,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            functionService.createFunction(projectId, catalogName, databaseName, functionBody);
            return BaseResponseUtil.responseSuccess();
        });
    }

    @ApiOperation(value = "get function")
    @GetMapping(value = "/{functionName}", produces = "application/json;charset=UTF-8")
    public CatalogResponse<FunctionInput> getFunction(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable("database-name") String databaseName,
        @ApiParam(value = "function name", required = true) @PathVariable("functionName") String functionName,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            FunctionInput func = functionService.getFunction(projectId, catalogName, databaseName, functionName);
            if (func == null) {
                throw new CatalogServerException(ErrorCode.FUNCTION_NOT_FOUND, functionName);
            }
            return ResponseUtil.responseSuccess(func);
        });
    }

    @ApiOperation(value = "drop function")
    @DeleteMapping(value = "", produces = "application/json;charset=UTF-8")
    public BaseResponse dropFunction(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable("database-name") String databaseName,
        @ApiParam(value = "function name", required = true) @RequestParam("functionName") String functionName,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            functionService.dropFunction(projectId, catalogName, databaseName, functionName);
            return BaseResponseUtil.responseSuccess();
        });
    }

    @ApiOperation(value = "list functions")
    @GetMapping(value = "/list", produces = "application/json;charset=UTF-8")
    public CatalogResponse<PagedList<String>> listFunctions(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable("database-name") String databaseName,
        @ApiParam(value = "pattern") @RequestParam(value = "pattern", required = false) String pattern,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            PagedList<String> result = new PagedList<>();
            result.setObjects(functionService.listFunctions(projectId, catalogName, databaseName, pattern)
                .toArray(new String[0]));
            return ResponseUtil.responseSuccess(result);
        });
    }

    @ApiOperation(value = "alter function")
    @PostMapping(value = "/{functionName}", produces = "application/json;charset=UTF-8")
    public BaseResponse alterFunction(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable("database-name") String databaseName,
        @ApiParam(value = "function name", required = true) @PathVariable("functionName") String functionName,
        @ApiParam(value = "function input body", required = true) @Valid @RequestBody FunctionInput functionBody,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            functionService.alterFunction(projectId, catalogName, databaseName, functionName, functionBody);
            return BaseResponseUtil.responseSuccess();
        });
    }
}
