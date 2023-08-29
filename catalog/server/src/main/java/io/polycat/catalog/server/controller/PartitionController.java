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

import io.polycat.catalog.common.model.BaseResponse;
import io.polycat.catalog.common.model.CatalogResponse;
import io.polycat.catalog.common.model.PagedList;
import io.polycat.catalog.common.model.Partition;
import io.polycat.catalog.common.model.TableName;
import io.polycat.catalog.common.model.TraverseCursorResult;
import io.polycat.catalog.common.model.stats.AggrStatisticData;
import io.polycat.catalog.common.model.stats.PartitionStatisticData;
import io.polycat.catalog.common.plugin.request.input.AddPartitionInput;
import io.polycat.catalog.common.plugin.request.input.AlterPartitionInput;
import io.polycat.catalog.common.plugin.request.input.ColumnStatisticsInput;
import io.polycat.catalog.common.plugin.request.input.DropPartitionByValuesInput;
import io.polycat.catalog.common.plugin.request.input.DropPartitionInput;
import io.polycat.catalog.common.plugin.request.input.DropPartitionsByExprsInput;
import io.polycat.catalog.common.plugin.request.input.FilterInput;
import io.polycat.catalog.common.plugin.request.input.GetPartitionColumnStaticsInput;
import io.polycat.catalog.common.plugin.request.input.GetPartitionInput;
import io.polycat.catalog.common.plugin.request.input.GetPartitionWithAuthInput;
import io.polycat.catalog.common.plugin.request.input.GetPartitionsByExprInput;
import io.polycat.catalog.common.plugin.request.input.GetPartitionsWithAuthInput;
import io.polycat.catalog.common.plugin.request.input.PartitionDescriptorInput;
import io.polycat.catalog.common.plugin.request.input.PartitionFilterInput;
import io.polycat.catalog.common.plugin.request.input.PartitionValuesInput;
import io.polycat.catalog.common.plugin.request.input.SetPartitionColumnStatisticsInput;
import io.polycat.catalog.common.plugin.request.input.TruncatePartitionInput;
import io.polycat.catalog.common.utils.GsonUtil;
import io.polycat.catalog.server.util.BaseResponseUtil;
import io.polycat.catalog.server.util.ResponseUtil;
import io.polycat.catalog.service.api.PartitionService;
import io.polycat.catalog.store.common.StoreConvertor;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Api(tags = "partition api")
@ApiResponses(value = {
    @ApiResponse(code = 400, message = "Bad Request"),
    @ApiResponse(code = 408, message = "Request Timeout"),
    @ApiResponse(code = 500, message = "Internal Server Error")
})
@RestController
@RequestMapping("/v1/{project-id}/catalogs/{catalog-name}/databases/{database-name}/tables/{table-name}/partitions")
public class PartitionController extends BaseController {

    @Autowired
    private PartitionService partitionService;

    @ApiOperation(value = "add partition")
    @PostMapping(value = "/add", produces = "application/json;charset=UTF-8")
    public BaseResponse addPartition(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
        @ApiParam(value = "partition input body", required = true) @RequestBody AddPartitionInput partitionInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        // client: addPartition(AddPartitionRequest request)
        return createBaseResponse(token, () -> {
            TableName tableNameObj = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            partitionService.addPartitions(tableNameObj, partitionInput);
            return BaseResponseUtil.responseSuccess();
        });
    }

    @ApiOperation(value = "drop partition by name")
    @PostMapping(value = "/dropByName", produces = "application/json;charset=UTF-8")
    public BaseResponse dropPartitionByName(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
        @ApiParam(value = "drop partition input body", required = true) @RequestBody DropPartitionInput dropPartitionInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        //client: dropPartition(DropPartitionRequest request)
        return createBaseResponse(token, () -> {
            TableName tableNameObj = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            partitionService.dropPartition(tableNameObj, dropPartitionInput);
            return BaseResponseUtil.responseSuccess();
        });
    }

    @ApiOperation(value = "drop partition by values (未实现，bridge无调用)")
    @PostMapping(params = "operate=dropByValue", produces = "application/json;charset=UTF-8")
    public BaseResponse dropPartitionByValue(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
        @ApiParam(value = "drop partition by value input body", required = true) @RequestBody DropPartitionByValuesInput input,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            // UnSupport
            TableName tableNameObj = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            partitionService.dropPartition(tableNameObj, input);
            return BaseResponseUtil.responseSuccess();
        });
    }

    @ApiOperation(value = "drop partitions by Expr (未实现，bridge无调用)")
    @PostMapping(params = "operate=dropPartitionsByExpr", produces = "application/json;charset=UTF-8")
    public CatalogResponse<Partition[]> dropPartitionsByExpr(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
        @ApiParam(value = "drop partitions by Expr input body", required = true) @RequestBody DropPartitionsByExprsInput input,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            // UnSupport
            TableName tableNameObj = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            Partition[] partitions = partitionService.dropPartitionsByExprs(tableNameObj, input);
            return ResponseUtil.responseSuccess(partitions);
        });
    }

    @ApiOperation(value = "get partition by name")
    @PostMapping(value = "/getByName", produces = "application/json;charset=UTF-8")
    public CatalogResponse<Partition> getPartitionByName(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
        @ApiParam(value = "partition name input body", required = true) @RequestBody GetPartitionInput getPartitionInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        //client: getPartition(GetPartitionRequest request)
        return createResponse(token, () -> {
            TableName tableNameObj = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            Partition partition = partitionService.getPartitionByName(tableNameObj, getPartitionInput.getPartName());
            return ResponseUtil.responseSuccess(partition);
        });
    }

    @ApiOperation(value = "get partition By value (未实现，bridge无调用)")
    @PostMapping(params = "operate=getByValue", produces = "application/json;charset=UTF-8")
    public CatalogResponse<Partition> getPartitionByValue(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
        @ApiParam(value = "partition values json form", required = true) @RequestParam(value = "partValues") String partValues,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            // UnSupport
            TableName tableNameObj = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            Partition partition = partitionService.getPartitionByValue(tableNameObj,
                GsonUtil.fromJson(partValues, List.class));
            return ResponseUtil.responseSuccess(partition);
        });
    }

    @ApiOperation(value = "add partitions (未实现，bridge无调用)")
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "Bad Request"),
        @ApiResponse(code = 408, message = "Request Timeout"),
        @ApiResponse(code = 500, message = "Internal Server Error")
    })
    @PostMapping(params = "operate=addParts", produces = "application/json;charset=UTF-8")
    public CatalogResponse<Partition[]> addPartitions(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
        @ApiParam(value = "partition input body", required = true) @RequestBody AddPartitionInput partitionInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        //UnSupport
        return createResponse(token, () -> {
            TableName tableNameObj = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            Partition[] partitions = partitionService.addPartitionsBackResult(tableNameObj, partitionInput);
            return ResponseUtil.responseSuccess(partitions);
        });
    }

    @ApiOperation(value = "alter partitions")
    @PostMapping(value = "/alterParts", produces = "application/json;charset=UTF-8")
    public BaseResponse alterPartitions(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
        @ApiParam(value = "partition input body", required = true) @RequestBody AlterPartitionInput alterPartitionInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        //client: alterPartitions(AlterPartitionRequest request)
        return createBaseResponse(token, () -> {
            TableName tableNameObj = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            partitionService.alterPartitions(tableNameObj, alterPartitionInput);
            return BaseResponseUtil.responseSuccess();
        });
    }

    @ApiOperation(value = "alter partitions")
    @PostMapping(params = "operate=rename", produces = "application/json;charset=UTF-8")
    public BaseResponse renamePartition(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
        @ApiParam(value = "partition input body", required = true) @RequestBody AlterPartitionInput alterPartitionInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            TableName tableNameObj = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            partitionService.renamePartition(tableNameObj, alterPartitionInput);
            return BaseResponseUtil.responseSuccess();
        });
    }

    @ApiOperation(value = "append partition (未实现，bridge无调用)")
    @PostMapping(params = "operate=append", produces = "application/json;charset=UTF-8")
    public CatalogResponse<Partition> appendPartition(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
        @ApiParam(value = "partition Descriptor", required = true) @RequestBody PartitionDescriptorInput descriptor,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            // UnSupport
            TableName tableNameObj = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            Partition partition = partitionService.appendPartition(tableNameObj, descriptor);
            return ResponseUtil.responseSuccess(partition);
        });
    }

    @ApiOperation(value = "get partitions by filter")
    @PostMapping(value = "/getByFilter", produces = "application/json;charset=UTF-8")
    public CatalogResponse<Partition[]> getPartitionsByFilter(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
        @ApiParam(value = "filter input body", required = true) @RequestBody PartitionFilterInput filterInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        //client: getPartitionsByFilter(ListPartitionByFilterRequest request)
        return createResponse(token, () -> {
            TableName tableNameParam = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            Partition[] partitions = partitionService.getPartitionsByFilter(tableNameParam, filterInput);
            return ResponseUtil.responseSuccess(partitions);
        });
    }

    @PostMapping(value = "/getPartitionsByNames", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "get partitions by names")
    public CatalogResponse<Partition[]> getPartitionsByNames(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
        @ApiParam(value = "filter input body", required = true) @RequestBody PartitionFilterInput filterInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        //client: getPartitionsByNames(GetTablePartitionsByNamesRequest request)
        return createResponse(token, () -> {
            TableName tableNameParam = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            Partition[] partitions = partitionService.getPartitionsByNames(tableNameParam, filterInput);
            return ResponseUtil.responseSuccess(partitions);
        });
    }

    @PostMapping(value = "/getWithAuth", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "get partition with auth")
    public CatalogResponse<Partition> getPartitionWithAuth(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
        @ApiParam(value = "partition and auth info", required = true) @RequestBody GetPartitionWithAuthInput partitionInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        //client: getPartitionWithAuth(GetPartitionWithAuthRequest request)
        return createResponse(token, () -> {
            TableName tableNameParam = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            Partition partition = partitionService.getPartitionWithAuth(tableNameParam, partitionInput);
            return ResponseUtil.responseSuccess(partition);
        });
    }

//    @PostMapping(value = "/names", params = "operate=list", produces = "application/json;charset=UTF-8")
//    @ApiOperation(value = "list partition names")
//    public CatalogResponse<String[]> listPartitionNames(
//        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
//        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
//        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
//        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
//        @ApiParam(value = "max partitions", required = true) @RequestBody short maxParts,
//        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
//        String apiMethod = Thread.currentThread().getStackTrace()[1].getMethodName();
//        return createResponse(token, () -> {
//            TableName tableNameParam = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
//            String[] partitionNames = partitionService.listPartitionNames(tableNameParam, maxParts);
//            return ResponseUtil.responseSuccess(partitionNames);
//        });
//    }

    //    @PostMapping(value = "/listPartitionNamesByFilter", produces = "application/json;charset=UTF-8")
    @PostMapping(value = "/listNamesByFilter", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "list partition names by filter")
    public CatalogResponse<String[]> listPartitionNamesByFilter(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
        @ApiParam(value = "filter input body", required = true) @RequestBody PartitionFilterInput filterInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        //client: listPartitionNamesByFilter(ListPartitionNamesByFilterRequest request)
        return createResponse(token, () -> {
            TableName tableNameParam = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            String[] partitionNames = partitionService.listPartitionNamesByFilter(tableNameParam, filterInput);
            return ResponseUtil.responseSuccess(partitionNames);
        });
    }

    @PostMapping(value = "/listByExpr", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "list partitions by expr")
    public CatalogResponse<Partition[]> listPartitionsByExpr(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
        @ApiParam(value = "expr input body", required = true) @RequestBody GetPartitionsByExprInput input,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        String apiMethod = Thread.currentThread().getStackTrace()[1].getMethodName();
        return createResponse(token, () -> {
            TableName tableNameParam = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            Partition[] partitions = partitionService.listPartitionsByExpr(tableNameParam, input);
            return ResponseUtil.responseSuccess(partitions);
        });
    }

    @PostMapping(params = "operate=listByValue", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "list partitions by value")
    public CatalogResponse<Partition[]> listPartitionsByValues(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
        @ApiParam(value = "filter input body", required = true) @RequestBody PartitionFilterInput input,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        //UnSupport
        String apiMethod = Thread.currentThread().getStackTrace()[1].getMethodName();
        return createResponse(token, () -> {
            TableName tableNameParam = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            Partition[] partitions = partitionService.listPartitions(tableNameParam, input);
            return ResponseUtil.responseSuccess(partitions);
        });
    }

    @PostMapping(value = "/listNames", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "list partition names")
    public CatalogResponse<String[]> listPartitionNames(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
        @ApiParam(value = "filter input body", required = true) @RequestBody PartitionFilterInput filterInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        //client: listPartitionNames(ListTablePartitionsRequest request)
        return createResponse(token, () -> {
            TableName tableNameParam = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            String[] partitionNames = partitionService.listPartitionNames(tableNameParam, filterInput.getMaxParts());
            return ResponseUtil.responseSuccess(partitionNames);
        });
    }

    @PostMapping(value = "/listPartitionNamesPs", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "list partitions ps")
    public CatalogResponse<String[]> listPartitionNamesPs(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
        @ApiParam(value = "get partition with auth info", required = true) @RequestBody PartitionFilterInput partitionInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        //client: listPartitionNamesPs(ListPartitionNamesPsRequest request)
        return createResponse(token, () -> {
            TableName tableNameParam = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            String[] partitionNames = partitionService.listPartitionNamesPs(tableNameParam, partitionInput);
            return ResponseUtil.responseSuccess(partitionNames);
        });
    }

    @PostMapping(value = "/listPartitionsPsWithAuth", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "list partitions ps with auth")
    public CatalogResponse<Partition[]> listPartitionsPsWithAuth(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
        @ApiParam(value = "get partition with auth info", required = true) @RequestBody GetPartitionsWithAuthInput partitionInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        //client: listPartitionsPsWithAuth(ListPartitionsWithAuthRequest request)
        return createResponse(token, () -> {
            TableName tableNameParam = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            Partition[] partitions = partitionService.listPartitionsPsWithAuth(tableNameParam, partitionInput);
            return ResponseUtil.responseSuccess(partitions);
        });
    }



    @PostMapping(params = "operate=truncate", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "truncate partition")
    public BaseResponse truncatePartition(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
        @ApiParam(value = "get partition with auth info", required = true) @RequestBody TruncatePartitionInput input,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        //UnSupport
        String apiMethod = Thread.currentThread().getStackTrace()[1].getMethodName();
        return createBaseResponse(token, () -> {
            TableName tableNameParam = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            partitionService.truncatePartitions(tableNameParam, input);
            return BaseResponseUtil.responseSuccess();
        });
    }

    @PostMapping(value = "/doesExists", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "does partition exists")
    public CatalogResponse<Boolean> doesPartitionExists(
            @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
            @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
            @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
            @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
            @ApiParam(value = "partition name input body", required = true) @RequestBody PartitionValuesInput input,
            @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        //client: doesPartitionExist(DoesPartitionExistsRequest request)
        return createResponse(token, () -> {
            TableName tableNameParam = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            return ResponseUtil.responseSuccess(partitionService.doesPartitionExists(tableNameParam, input));
        });
    }


    @PostMapping(value = "/showPartitions", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "show partitions")
    public CatalogResponse<PagedList<Partition>> showPartitions(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
        @ApiParam(value = "filter input body", required = true) @RequestBody FilterInput filterInput,
        @ApiParam(value = "max results", required = true) @RequestParam(value = "maxResults", required = false, defaultValue = "1000") int maxResults,
        @ApiParam(value = "net page token", required = true) @RequestParam(value = "nextPageToken", required = false) String pageToken,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        //UnSupport
        return createResponse(token, () -> {
            TableName tableNameParam = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            TraverseCursorResult<List<Partition>> partitions = partitionService.showTablePartition(tableNameParam,
                maxResults, pageToken, null);
            PagedList<Partition> result = new PagedList<>();
            result.setObjects(partitions.getResult().toArray(new Partition[0]));
            partitions.getContinuation().ifPresent(catalogToken -> result.setNextMarker(catalogToken.toString()));
            return ResponseUtil.responseSuccess(result);
        });
    }

    @PostMapping(value = "/columnStatistics", params = "operate=set", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "delete columns statistics")
    public BaseResponse setPartitionColumnStatistics(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
        @ApiParam(value = "partition statistic", required = true) @RequestBody SetPartitionColumnStatisticsInput statsInfo,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        //UnSupport
        return createBaseResponse(token, () -> {
            TableName tableNameParam = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            partitionService.setPartitionColumnStatistics(tableNameParam, statsInfo);
            return BaseResponseUtil.responseSuccess();
        });
    }

    @DeleteMapping(value = "/columnStatistics", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "delete columns statistics")
    public BaseResponse deleteColumnStatistics(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
        @ApiParam(value = "partition name", required = true) @RequestParam(value = "partName", required = false) String partName,
        @ApiParam(value = "column name", required = true) @RequestParam(value = "colName", required = false) String colName,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        //UnSupport
        return createBaseResponse(token, () -> {
            TableName tableNameParam = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            partitionService.deletePartitionColumnStatistics(tableNameParam, partName, colName);
            return BaseResponseUtil.responseSuccess();
        });
    }

    @PostMapping(value = "/columnStatistics", params = "operate=update", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "delete columns statistics")
    public BaseResponse updatePartitionColumnStatistics(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
        @ApiParam(value = "column statistic info", required = true) @RequestBody ColumnStatisticsInput statsInfo,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        //UnSupport
        return createResponse(token, () -> {
            TableName tableNameParam = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            partitionService.updatePartitionColumnStatistics(tableNameParam, statsInfo);
            return BaseResponseUtil.responseSuccess();
        });
    }

    @PostMapping(value = "/columnStatistics", params = "operate=getAggr", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "delete columns statistics")
    public CatalogResponse<AggrStatisticData> getAggregateColumnStatistics(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
        @ApiParam(value = "partition names and column names", required = true) @RequestBody GetPartitionColumnStaticsInput input,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        //UnSupport
        return createResponse(token, () -> {
            TableName tableNameParam = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            AggrStatisticData aggrStats = partitionService.getAggrColStatsFor(tableNameParam, input.getPartNames(),
                input.getColNames());
            return ResponseUtil.responseSuccess(aggrStats);
        });
    }

    @PostMapping(value = "/columnStatistics", params = "operate=get", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "delete columns statistics")
    public CatalogResponse<PartitionStatisticData> getColumnStatistics(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
        @ApiParam(value = "partition name", required = true) @RequestBody GetPartitionColumnStaticsInput input,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        //UnSupport
        return createResponse(token, () -> {
            TableName tableNameParam = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            PartitionStatisticData partStats = partitionService.getPartitionColumnStatistic(tableNameParam,
                input.getPartNames(), input.getColNames());
            return ResponseUtil.responseSuccess(partStats);
        });
    }
}
