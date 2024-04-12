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

import io.polycat.catalog.common.lineage.*;
import io.polycat.catalog.common.model.*;

import io.polycat.catalog.common.plugin.request.input.LineageInfoInput;
import io.polycat.catalog.server.util.BaseResponseUtil;
import io.polycat.catalog.server.util.ResponseUtil;

import io.polycat.catalog.service.api.DataLineageService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.springframework.beans.factory.annotation.Autowired;
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

    /*@ApiOperation(value = "record data lineage")
    @PostMapping(value = "/recordDataLineage", produces = "application/json;charset=UTF-8")
    public BaseResponse recordDataLineage(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "data lineage input body", required = true) @RequestBody DataLineageInput dataLineageInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {

        return createBaseResponse(token, () -> {
            dataLineageService.recordDataLineage(dataLineageInput);
            return BaseResponseUtil.responseSuccess();
        });
    }*/
    @ApiOperation(value = "update data lineage")
    @PostMapping(value = "/updateDataLineage", produces = "application/json;charset=UTF-8")
    public BaseResponse updateDataLineage(
            @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
            @ApiParam(value = "data lineage input body", required = true) @RequestBody LineageInfoInput lineageInput,
            @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {

        return createBaseResponse(token, projectId, () -> {
            dataLineageService.updateDataLineage(projectId, lineageInput);
            return BaseResponseUtil.responseSuccess();
        });
    }

    /**
     * search lineage graph
     * @param projectId projectId
     * @param dbType Lineage DB vendor type {@link EDbType}
     * @param objectType Lineage object type {@link ELineageObjectType}
     * @param qualifiedName qualified name TABLE: catalogName.databaseName.tableName
     * @param depth number of hops for lineage
     * @param direction UPSTREAM, DOWNSTREAM or BOTH {@link ELineageDirection}
     * @param lineageType lineage type {@link ELineageType}
     * @param startTime lineage start time position.
     * @param token auth
     * @return {@link LineageInfo}
     */
    @ApiOperation(value = "lineage search")
    @GetMapping(value = "lineageGraph", produces = "application/json;charset=UTF-8")
    public CatalogResponse<LineageInfo> getLineageGraph(
            @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
            @ApiParam(value = "Lineage DB vendor type", required = true) @RequestParam(value = "dbType", required = true) EDbType dbType,
            @ApiParam(value = "Lineage object type", required = true) @RequestParam(value = "objectType", required = true) ELineageObjectType objectType,
            @ApiParam(value = "qualified name", required = true) @RequestParam(value = "qualifiedName", required = true) String qualifiedName,
            @ApiParam(value = "depth", required = false, defaultValue = "1") @RequestParam(value = "depth", required = false) int depth,
            @ApiParam(value = "direction", required = false) @RequestParam(value = "direction", required = false) ELineageDirection direction,
            @ApiParam(value = "lineage type, " + "Lineage relationship type: the value and description are as follows：\n" +
                    " * FIELD_DEPEND_FIELD：insert into table new_t1 select a1 from t1;\n" +
                    " * TABLE_DEPEND_TABLE：insert into table new_t1 select a1 from t1;\n" +
                    " * FIELD_INFLU_TABLE：WHERE/GROUP BY/ORDER BY/JOIN influence table\n" +
                    " * FIELD_INFLU_FIELD：WHERE/GROUP BY/ORDER BY/JOIN influence field\n" +
                    " * TABLE_INFLU_FIELD：insert into table new_t1 select count(*) from t1;\n" +
                    " * FIELD_JOIN_FIELD：insert into table new_t1 select t1.a1 from t1 join t2 on t1.a1=t2.a14", required = false) @RequestParam(value = "lineageType", required = false) ELineageType lineageType,
            @ApiParam(value = "start time", required = false) @RequestParam(value = "startTime", required = false, defaultValue = "0") Long startTime,
            @ApiParam(value = "authorization") @RequestHeader("Authorization") String token) {
        return createResponse(token, projectId, () -> {
            LineageInfo lineageInfo = dataLineageService.getLineageGraph(projectId, dbType, objectType, qualifiedName, depth, direction, lineageType, startTime);
            return ResponseUtil.responseSuccess(lineageInfo);
        });
    }

    /**
     * lineage job fact
     *
     * @param projectId projectId
     * @param factId lineage job fact id
     * @param token auth
     * @return
     */
    @ApiOperation(value = "lineage job fact")
    @GetMapping(value = "getLineageFact", produces = "application/json;charset=UTF-8")
    public CatalogResponse<LineageFact> getLineageFact(
            @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
            @ApiParam(value = "lineage job fact id") @RequestParam(value = "factId", required = false) String factId,
            @ApiParam(value = "authorization") @RequestHeader("Authorization") String token) {
        return createResponse(token, projectId, () -> {
            LineageFact lineageFact = dataLineageService.getLineageJobFact(projectId, factId);
            return ResponseUtil.responseSuccess(lineageFact);
        });
    }


//    /**
//     * get sourceTables By tables
//     *
//     * @param projectId    projectId
//     * @param catalogName  catalogName
//     * @param databaseName databaseName
//     * @param tableName    tableName
//     * @return CatalogResponse
//     */
//    @ApiOperation(value = "get data lineage")
//    @GetMapping(value = "/getDataLineagesByTable", produces = "application/json;charset=UTF-8")
//    public CatalogResponse<PagedList<DataLineage>> getDataLineageByTable(
//        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
//        @ApiParam(value = "catalog name", required = true) @RequestParam(value = "catalogName") String catalogName,
//        @ApiParam(value = "database name", required = true) @RequestParam(value = "databaseName") String databaseName,
//        @ApiParam(value = "table name", required = true) @RequestParam(value = "tableName") String tableName,
//        @ApiParam(value = "lineage type", required = true) @RequestParam(value = "lineageType", required = false,
//        defaultValue = "UPSTREAM") DataLineageType lineageType,
//        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
//        return createResponse(token, () -> {
//            List<DataLineage> sourceList = dataLineageService.getDataLineageByTable(projectId, catalogName, databaseName,
//                tableName, lineageType);
//            PagedList<DataLineage> result = new PagedList<>();
//            result.setObjects(sourceList.toArray(new DataLineage[0]));
//            return ResponseUtil.responseSuccess(result);
//        });
//    }
}
