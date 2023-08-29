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

import io.polycat.catalog.common.model.*;
import io.polycat.catalog.common.plugin.request.input.TableUsageProfileInput;
import io.polycat.catalog.common.plugin.request.input.TopTableUsageProfileInput;
import io.polycat.catalog.server.util.BaseResponseUtil;
import io.polycat.catalog.server.util.ResponseUtil;
import io.polycat.catalog.service.api.UsageProfileService;
import io.polycat.catalog.store.common.StoreConvertor;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@Api(tags = "usage profile api")
@ApiResponses(value = {
    @ApiResponse(code = 400, message = "Bad Request"),
    @ApiResponse(code = 408, message = "Request Timeout"),
    @ApiResponse(code = 500, message = "Internal Server Error")
})
@RestController
@RequestMapping("/v1/{project-id}/usageprofiles")
public class UsageProfileController extends BaseController {

    @Autowired
    private UsageProfileService usageProfileService;

    /**
     * Record Usage Profile
     *
     * @param projectId         projectId
     * @param usageProfileInput usageProfileInput
     * @return CatalogResponse
     */
    @ApiOperation(value = "record table usage profile")
    @PostMapping(value = "/recordTableUsageProfile", produces = "application/json;charset=UTF-8")
    public BaseResponse recordTableUsageProfile(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "table usage profile input body", required = true) @RequestBody
            TableUsageProfileInput usageProfileInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            usageProfileService.recordTableUsageProfile(usageProfileInput);
            return BaseResponseUtil.responseSuccess(HttpStatus.CREATED);
        });
    }

    /**
     * get top tables
     *
     * @param projectId        projectId
     * @param catalogName      catalogName
     * @param startTime        startTime
     * @param endTime          endTime
     * @param opTypes          opTypes
     * @param usageProfileType usageProfileType
     * @param topNum           topNum
     * @return CatalogResponse
     */
    @ApiOperation(value = "get top usage profile")
    @GetMapping(value = "/getTopUsageProfilesByCatalog", produces = "application/json;charset=UTF-8")
    public CatalogResponse<PagedList<TableUsageProfile>> getTopUsageProfilesByCatalog(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @RequestParam(value = "catalogName") String catalogName,
        @ApiParam(value = "start time") @RequestParam(value = "startTime", required = false, defaultValue = "0") long startTime,
        @ApiParam(value = "end time") @RequestParam(value = "endTime", required = false, defaultValue = "0") long endTime,
        @ApiParam(value = "user id") @RequestParam(value = "userId", required = false) String userId,
        @ApiParam(value = "task id") @RequestParam(value = "taskId", required = false) String taskId,
        @ApiParam(value = "op types", required = true) @RequestParam(value = "opTypes", required = true) String opTypes,
        @ApiParam(value = "top type", required = true) @RequestParam(value = "topType", required = true) int usageProfileType,
        @ApiParam(value = "top num") @RequestParam(value = "topNum", required = false, defaultValue = "10") int topNum,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        TopTableUsageProfileInput topTableUsageProfileInput = new TopTableUsageProfileInput(startTime, endTime, opTypes,
            topNum, usageProfileType, taskId, userId);
        return createResponse(token, () -> {
            List<TableUsageProfile> tableNameList = usageProfileService.getTopTablesByCount(projectId, catalogName,
                topTableUsageProfileInput);
            PagedList<TableUsageProfile> result = new PagedList<>();
            result.setObjects(tableNameList.toArray(new TableUsageProfile[0]));
            return ResponseUtil.responseSuccess(result);
        });
    }

    /**
     * get sourceTables By tables
     *
     * @param projectId    projectId
     * @param catalogName  catalogName
     * @param databaseName databaseName
     * @param tableName    tableName
     * @param startTime    startTime
     * @param endTime      endTime
     * @return CatalogResponse
     */
    @ApiOperation(value = "get usage profile")
    @GetMapping(value = "/getUsageProfilesByTable", produces = "application/json;charset=UTF-8")
    public CatalogResponse<PagedList<TableUsageProfile>> getUsageProfileByTable(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @RequestParam(value = "catalogName") String catalogName,
        @ApiParam(value = "database name", required = true) @RequestParam(value = "databaseName") String databaseName,
        @ApiParam(value = "table name", required = true) @RequestParam(value = "tableName") String tableName,
        @ApiParam(value = "start timestamp") @RequestParam(value = "startTime", required = false, defaultValue = "0") long startTime,
        @ApiParam(value = "end timestamp") @RequestParam(value = "endTime", required = false, defaultValue = "0") long endTime,
        @ApiParam(value = "user id") @RequestParam(value = "userId", required = false) String userId,
        @ApiParam(value = "task id") @RequestParam(value = "taskId", required = false) String taskId,
        @ApiParam(value = "tag name") @RequestParam(value = "tag", required = false) String tag,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            TableName table = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            List<TableUsageProfile> usageProfileList = usageProfileService.getUsageProfileByTable(table,
                startTime, endTime, userId, taskId, tag);
            PagedList<TableUsageProfile> result = new PagedList<>();
            result.setObjects(usageProfileList.toArray(new TableUsageProfile[0]));
            return ResponseUtil.responseSuccess(result);
        });
    }

    @ApiOperation(value = "get usage profile group by user")
    @GetMapping(value = "/getUsageProfileGroupByUser", produces = "application/json;charset=UTF-8")
    public CatalogResponse<List<TableUsageProfile>> getUsageProfileGroupByUser(
            @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
            @ApiParam(value = "catalog name", required = true) @RequestParam(value = "catalogName") String catalogName,
            @ApiParam(value = "database name", required = true) @RequestParam(value = "databaseName") String databaseName,
            @ApiParam(value = "table name", required = true) @RequestParam(value = "tableName") String tableName,
            @ApiParam(value = "start timestamp") @RequestParam(value = "startTime", required = false, defaultValue = "0") long startTime,
            @ApiParam(value = "end timestamp") @RequestParam(value = "endTime", required = false, defaultValue = "0") long endTime,
            @ApiParam(value = "op types", required = true, example = "WRITE;READ") @RequestParam(value = "opTypes", required = true) String opTypes,
            @ApiParam(value = "sort asc", required = false) @RequestParam(value = "sortAsc", required = false, defaultValue = "false") boolean sortAsc,
            @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            TableName tableNameObj = new TableName(projectId, catalogName, databaseName, tableName);
            List<TableUsageProfile> tableUsageProfiles = usageProfileService.getUsageProfileGroupByUser(tableNameObj, startTime, endTime, opTypes, sortAsc);
            return ResponseUtil.responseSuccess(tableUsageProfiles);
        });
    }

    @ApiOperation(value = "get table access user")
    @PostMapping(value = "/getTableAccessUsers", produces = "application/json;charset=UTF-8")
    public CatalogResponse<List<TableAccessUsers>> getTableAccessUsers(
            @RequestHeader("Authorization") String token,
            @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
            @ApiParam(value = "table list", required = true) @RequestBody List<TableSource> tableSources
    ) {
        return createResponse(token, () -> {
            List<TableAccessUsers> tableAccessUsers = usageProfileService.getTableAccessUsers(projectId, tableSources);
            return ResponseUtil.responseSuccess(tableAccessUsers);
        });
    }

    @ApiOperation(value = "get usage profile details")
    @GetMapping(value = "/getUsageProfileDetails", produces = "application/json;charset=UTF-8")
    public CatalogResponse<PagedList<TableUsageProfile>> getUsageProfileDetails(
            @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
            @ApiParam(value = "catalog name", required = true) @RequestParam(value = "catalogName") String catalogName,
            @ApiParam(value = "database name", required = true) @RequestParam(value = "databaseName") String databaseName,
            @ApiParam(value = "table name", required = true) @RequestParam(value = "tableName") String tableName,
            @ApiParam(value = "user id", required = false) @RequestParam(value = "userId", required = false) String userId,
            @ApiParam(value = "task id", required = false) @RequestParam(value = "taskId", required = false) String taskId,
            @ApiParam(value = "tag name", required = false) @RequestParam(value = "tag", required = false) String tag,
            @ApiParam(value = "row count", required = false, defaultValue = "100") @RequestParam(value = "rowCount", required = false, defaultValue = "100") int rowCount,
            @ApiParam(value = "start timestamp", required = false) @RequestParam(value = "startTime", required = false, defaultValue = "0") long startTime,
            @ApiParam(value = "end timestamp", required = false) @RequestParam(value = "endTime", required = false, defaultValue = "0") long endTime,
            @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            TableSource tableSource = new TableSource(projectId, catalogName, databaseName, tableName);
            List<TableUsageProfile> usageProfileList = usageProfileService.getUsageProfileDetailsByCondition(tableSource, startTime, endTime, userId, taskId, rowCount, tag);
            return ResponseUtil.responseSuccess(usageProfileList);
        });
    }


}

