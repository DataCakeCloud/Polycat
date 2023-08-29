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

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.model.AuthTableObjParam;
import io.polycat.catalog.common.model.AuthorizationType;
import io.polycat.catalog.common.model.BaseResponse;
import io.polycat.catalog.common.model.CatalogResponse;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.IndexInfo;
import io.polycat.catalog.common.model.IndexName;
import io.polycat.catalog.common.model.PagedList;
import io.polycat.catalog.common.model.TableName;
import io.polycat.catalog.common.model.TraverseCursorResult;
import io.polycat.catalog.common.plugin.request.input.IndexInput;
import io.polycat.catalog.common.plugin.request.input.IndexRefreshInput;
import io.polycat.catalog.service.api.IndexService;
import io.polycat.catalog.server.util.BaseResponseUtil;
import io.polycat.catalog.server.util.ResponseUtil;
import io.polycat.catalog.store.common.StoreConvertor;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Api(tags = "mv api")
@ApiResponses(value = {
    @ApiResponse(code = 400, message = "Bad Request"),
    @ApiResponse(code = 408, message = "Request Timeout"),
    @ApiResponse(code = 500, message = "Internal Server Error")
})
@RestController
@RequestMapping("/v1/{project-id}/catalogs/{catalog-name}/databases/{database-name}/index/materializedView")
public class MaterializedViewController extends BaseController {

  @Autowired
  private IndexService materializedViewService;

  /**
   * create materialized view
   */
  @ApiOperation(value = "create materialized view")
  @PostMapping(value = "", produces = "application/json;charset=UTF-8")
  public BaseResponse createMaterializedView(
      @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
      @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
      @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
      @ApiParam(value = "materialized view input body", required = true) @RequestBody
          IndexInput materializedViewInput,
      @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
    return createBaseResponse(token, () -> {
      System.out.println("Start creating materialized view");
      DatabaseName name = StoreConvertor.databaseName(projectId, catalogName, databaseName);
      materializedViewService.createIndex(name, materializedViewInput);
      return BaseResponseUtil.responseSuccess(HttpStatus.CREATED);
    });
  }

  /**
   * drop materialized view
   *
   * @param projectId projectId
   * @param indexName  materialized view
   * @return CatalogResponse
   */
  @ApiOperation(value = "drop materializedView")
  @DeleteMapping(value = "/{materializedViewName}",
      produces = "application/json;charset=UTF-8")
  public BaseResponse dropMaterializedView(
      @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
      @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
      @ApiParam(value = "database name", required = true) @PathVariable("database-name") String databaseName,
      @ApiParam(value = "materializedView name", required = true) @PathVariable("materializedViewName") String indexName,
      @ApiParam(value = "hmsTab") @RequestParam(value = "hmsTab", required = false, defaultValue = "false") boolean hmsTab,
      @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
    return createBaseResponse(token, () -> {
      IndexName name = StoreConvertor.indexName(projectId, catalogName, databaseName, indexName);
      materializedViewService.dropIndex(name, hmsTab);
      return BaseResponseUtil.responseSuccess();
    });
  }

  @GetMapping(value = "/list", produces = "application/json;charset=UTF-8")
  @ApiOperation(value = "list indexes")
  public CatalogResponse<PagedList<IndexInfo>> listMaterializedViews(
      @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
      @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
      @ApiParam(value = "database name", required = true) @PathVariable("database-name") String databaseName,
      @ApiParam(value = "table name") @RequestParam(value = "tableName", required = false, defaultValue = "") String parentTableName,
      @ApiParam(value = "parent database name") @RequestParam(value = "parentDatabaseName", required = false, defaultValue = "") String parentDatabaseName,
      @ApiParam(value = "include dropped flag") @RequestParam(value = "includeDrop", required = false, defaultValue = "false") boolean includeDrop,
      @ApiParam(value = "max results") @RequestParam(value = "maxResults", required = false, defaultValue = "1000") Integer maxResults,
      @ApiParam(value = "page token") @RequestParam(value = "pageToken", required = false, defaultValue = "") String pageToken,
      @ApiParam(value = "hmsTab") @RequestParam(value = "hmsTab", required = false, defaultValue = "false") boolean hmsTab,
      @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
    return createResponse(token, () -> {
      TableName tableName = StoreConvertor.tableName(projectId, catalogName, parentDatabaseName, parentTableName);
      DatabaseName database = StoreConvertor.databaseName(projectId, catalogName, databaseName);
      PagedList<IndexInfo> result = new PagedList<>();
      TraverseCursorResult<List<IndexInfo>>
          indexes = materializedViewService.listIndexes(tableName, includeDrop,
          maxResults, pageToken, hmsTab, database);
      result.setObjects(indexes.getResult().toArray(new IndexInfo[0]));
      indexes.getContinuation().ifPresent(catalogToken -> result.setNextMarker(catalogToken.toString()));
      return ResponseUtil.responseSuccess(result);
    });
  }

  @GetMapping(value = "/getMVByName", produces = "application/json;charset=UTF-8")
  @ApiOperation(value = "get mv by name")
  public CatalogResponse<IndexInfo> getMVByName(
      @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
      @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
      @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
      @ApiParam(value = "mv name", required = true) @RequestParam("mvName") String mvName,
      @ApiParam(value = "share name") @RequestParam(value = "shareName", required = false) String shareName,
      @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
    return createResponse(token, () -> {
      String mvNameIgnCase = mvName.toLowerCase();
      AuthTableObjParam
          authTableObjParam = new AuthTableObjParam(projectId, catalogName, databaseName,
          mvNameIgnCase);
      if (shareName != null) {
        authTableObjParam.setShareName(shareName);
        authTableObjParam.setAuthorizationType(AuthorizationType.SELECT_FROM_SHARE);
      }
      IndexName indexName = StoreConvertor.indexName(projectId, catalogName, databaseName, mvNameIgnCase);
      IndexInfo indexInfo = materializedViewService.getIndexByName(indexName);
      if (indexInfo == null) {
        throw new CatalogServerException(ErrorCode.MV_NOT_FOUND, mvNameIgnCase);
      }
      return ResponseUtil.responseSuccess(indexInfo);
    });

  }

  @PatchMapping(value = "/{materializedViewName}", produces = "application/json;charset=UTF-8")
  @ApiOperation(value = "alter mv")
  public BaseResponse alterMVByName(
      @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
      @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
      @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
      @ApiParam(value = "materializedView name", required = true) @PathVariable(value = "materializedViewName") String materializedViewName,
      @ApiParam(value = "MV refresh input body", required = true) @RequestBody
          IndexRefreshInput indexRefreshInput,
      @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
    return createBaseResponse(token, () -> {
      String mvNameIgnCase = materializedViewName.toLowerCase();
      IndexName indexName =
          StoreConvertor.indexName(projectId, catalogName, databaseName, mvNameIgnCase);
      materializedViewService.alterIndexByName(indexName, indexRefreshInput);
      return BaseResponseUtil.responseSuccess();
    });

  }
}
