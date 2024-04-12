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

import io.polycat.catalog.server.util.BaseResponseUtil;
import io.polycat.catalog.common.model.BaseResponse;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.ViewName;
import io.polycat.catalog.common.plugin.request.input.ViewInput;
import io.polycat.catalog.service.api.ViewService;
import io.polycat.catalog.store.common.StoreConvertor;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Api(tags = "view api")
@ApiResponses(value = {
    @ApiResponse(code = 400, message = "Bad Request"),
    @ApiResponse(code = 408, message = "Request Timeout"),
    @ApiResponse(code = 500, message = "Internal Server Error")
})
@RestController
@RequestMapping("/v1/{project-id}/catalogs/{catalog-name}/databases/{database-name}/views")
public class ViewController extends BaseController {

    @Autowired
    private ViewService viewService;

    /**
     * create view
     *
     * @param projectId projectId
     * @return CatalogResponse
     */
    @ApiOperation(value = "create view")
    @PostMapping(value = "", produces = "application/json;charset=UTF-8")
    public BaseResponse createView(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "view input body", required = true) @RequestBody ViewInput viewInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            System.out.println("Start create view");
            DatabaseName name = StoreConvertor.databaseName(projectId, catalogName, databaseName);
            viewService.createView(name, viewInput);
            return BaseResponseUtil.responseSuccess(HttpStatus.CREATED);
        });
    }

    /**
     * drop view
     *
     * @param projectId projectId
     * @param viewName  viewName
     * @return CatalogResponse
     */
    @ApiOperation(value = "drop view")
    @DeleteMapping(value = "/{viewName}",
        produces = "application/json;charset=UTF-8")
    public BaseResponse dropView(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable("database-name") String databaseName,
        @ApiParam(value = "view name", required = true) @PathVariable("viewName") String viewName,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            ViewName name = StoreConvertor.viewName(projectId, catalogName, databaseName, viewName);
            viewService.dropView(name);
            return BaseResponseUtil.responseSuccess();
        });
    }

    /**
     * alter view
     *
     * @param projectId projectId
     * @param viewName  viewName
     * @return CatalogResponse
     */
    @ApiOperation(value = "alter view")
    @PatchMapping(value = "/{viewName}/rename",
        produces = "application/json;charset=UTF-8")
    public BaseResponse alterView(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable("database-name") String databaseName,
        @ApiParam(value = "view name", required = true) @PathVariable("viewName") String viewName,
        @ApiParam(value = "view input body", required = true) @Valid @RequestBody ViewInput viewInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            ViewName name = StoreConvertor.viewName(projectId, catalogName, databaseName, viewName);
            viewService.alterView(name, viewInput);
            return BaseResponseUtil.responseSuccess();
        });
    }

}