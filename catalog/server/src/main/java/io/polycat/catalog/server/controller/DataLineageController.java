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

import io.polycat.catalog.common.DataLineageType;
import io.polycat.catalog.common.model.DataLineage;
import io.polycat.catalog.common.model.BaseResponse;
import io.polycat.catalog.common.model.CatalogResponse;
import io.polycat.catalog.common.model.DataSource;
import io.polycat.catalog.common.model.PagedList;

import io.polycat.catalog.common.plugin.request.input.DataLineageInput;
import io.polycat.catalog.server.util.BaseResponseUtil;
import io.polycat.catalog.server.util.ResponseUtil;

import io.polycat.catalog.service.api.DataLineageService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Api(tags = "data lineage api")
@ApiResponses(value = {
    @ApiResponse(code = 400, message = "Bad Request"),
    @ApiResponse(code = 408, message = "Request Timeout"),
    @ApiResponse(code = 500, message = "Internal Server Error")
})
@RestController
@RequestMapping("/v1/{project-id}/datalineages")
public class DataLineageController extends BaseController {

    @Autowired
    private DataLineageService dataLineageService;

    @ApiOperation(value = "record data lineage")
    @PostMapping(value = "/recordDataLineage", produces = "application/json;charset=UTF-8")
    public BaseResponse recordDataLineage(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "data lineage input body", required = true) @RequestBody DataLineageInput dataLineageInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {

        return createBaseResponse(token, () -> {
            dataLineageService.recordDataLineage(dataLineageInput);
            return BaseResponseUtil.responseSuccess();
        });
    }

    /**
     * get sourceTables By tables
     *
     * @param projectId    projectId
     * @param catalogName  catalogName
     * @param databaseName databaseName
     * @param tableName    tableName
     * @return CatalogResponse
     */
    @ApiOperation(value = "get data lineage")
    @GetMapping(value = "/getDataLineagesByTable", produces = "application/json;charset=UTF-8")
    public CatalogResponse<PagedList<DataLineage>> getDataLineageByTable(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @RequestParam(value = "catalogName") String catalogName,
        @ApiParam(value = "database name", required = true) @RequestParam(value = "databaseName") String databaseName,
        @ApiParam(value = "table name", required = true) @RequestParam(value = "tableName") String tableName,
        @ApiParam(value = "lineage type", required = true) @RequestParam(value = "lineageType", required = false,
        defaultValue = "UPSTREAM") DataLineageType lineageType,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            List<DataLineage> sourceList = dataLineageService.getDataLineageByTable(projectId, catalogName, databaseName,
                tableName, lineageType);
            PagedList<DataLineage> result = new PagedList<>();
            result.setObjects(sourceList.toArray(new DataLineage[0]));
            return ResponseUtil.responseSuccess(result);
        });
    }
}
