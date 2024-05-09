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
import io.polycat.catalog.server.util.ResponseUtil;
import io.polycat.catalog.common.model.BaseResponse;
import io.polycat.catalog.common.model.CatalogResponse;
import io.polycat.catalog.common.model.PagedList;
import io.polycat.catalog.common.model.TraverseCursorResult;
import io.polycat.catalog.common.model.discovery.CatalogTableCount;
import io.polycat.catalog.common.model.discovery.DiscoverySearchBase;
import io.polycat.catalog.common.model.discovery.ObjectCount;
import io.polycat.catalog.common.model.discovery.TableCategories;
import io.polycat.catalog.common.plugin.request.input.FilterConditionInput;
import io.polycat.catalog.service.api.DiscoveryService;
import io.swagger.annotations.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Api(tags = "discovery api")
@ApiResponses(value = {
        @ApiResponse(code = 400, message = "Bad Request"),
        @ApiResponse(code = 408, message = "Request Timeout"),
        @ApiResponse(code = 500, message = "Internal Server Error")
})
@RestController
@RequestMapping("/v1/{project-id}/discovery")
@Validated
public class DiscoveryController extends BaseController {

    @Autowired
    private DiscoveryService discoveryService;

    /**
     * discovery fulltext
     *
     * @param projectId projectId
     * @return ResponseEntity
     */
    @ApiOperation(value = "discovery fulltext")
    @PostMapping(value = "search", produces = "application/json;charset=UTF-8")
    public CatalogResponse<PagedList<DiscoverySearchBase>> fulltext(
            @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
            @ApiParam(value = "catalog name") @RequestParam(value = "catalogName", required = false) String catalogName,
            @ApiParam(value = "object type") @RequestParam(value = "objectType", required = false) String objectType,
            @ApiParam(value = "keyword") @RequestParam(value = "keyword", required = false) String keyword,
            @ApiParam(value = "owner") @RequestParam(value = "owner", required = false) String owner,
            @ApiParam(value = "category id") @RequestParam(value = "categoryId", required = false) Integer categoryId,
            @ApiParam(value = "logical operator") @RequestParam(value = "logicalOperator", required = false, defaultValue = "and") String logicalOperator,
            @ApiParam(value = "exact match") @RequestParam(value = "exactMatch", required = false, defaultValue = "false") boolean exactMatch,
            @ApiParam(value = "with categories") @RequestParam(value = "withCategories", required = false, defaultValue = "false") boolean withCategories,
            @ApiParam(value = "limit") @RequestParam(value = "limit", required = false, defaultValue = "100") Integer limit,
            @ApiParam(value = "filter json conditions") @RequestBody(required = false) FilterConditionInput filterConditionInput,
            @ApiParam(value = "pageToken") @RequestParam(value = "pageToken", required = false) String pageToken,
            @ApiParam(value = "authorization") @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            PagedList<DiscoverySearchBase> result = new PagedList<>();
            TraverseCursorResult<List<DiscoverySearchBase>> searchResult = discoveryService.searchUsingFullText(projectId, catalogName, objectType,
                    keyword, owner, categoryId, logicalOperator, exactMatch, withCategories, limit, pageToken, filterConditionInput.getConditions());
            result.setObjectList(searchResult.getResult());
            result.setPreviousMarker(searchResult.getPreviousTokenString());
            searchResult.getContinuation().ifPresent(catalogToken -> {
                result.setNextMarker(catalogToken.toString());
            });
            return ResponseUtil.responseSuccess(result);
        });
    }

    /**
     * match discovery qualified names
     *
     * @param projectId
     * @param catalogName
     * @param objectType
     * @param keyword
     * @param owner
     * @param limit
     * @param pageToken
     * @param token
     * @return
     */
    @ApiOperation(value = "match discovery names")
    @PostMapping(value = "matchListNames", produces = "application/json;charset=UTF-8")
    public CatalogResponse<PagedList<String>> matchListNames(
            @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
            @ApiParam(value = "catalog name") @RequestParam(value = "catalogName", required = false) String catalogName,
            @ApiParam(value = "object type") @RequestParam(value = "objectType", required = false) String objectType,
            @ApiParam(value = "qualified name keyword") @RequestParam(value = "keyword", required = false) String keyword,
            @ApiParam(value = "owner") @RequestParam(value = "owner", required = false) String owner,
            @ApiParam(value = "category id") @RequestParam(value = "categoryId", required = false) Integer categoryId,
            @ApiParam(value = "limit") @RequestParam(value = "limit", required = false, defaultValue = "100") Integer limit,
            @ApiParam(value = "pageToken") @RequestParam(value = "pageToken", required = false) String pageToken,
            @ApiParam(value = "filter json conditions", required = false) @RequestBody(required = false) FilterConditionInput filterConditionInput,
            @ApiParam(value = "authorization") @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            PagedList<String> result = new PagedList<>();
            TraverseCursorResult<List<String>> searchResult = discoveryService.matchListNames(projectId, catalogName, objectType,
                    keyword, owner, categoryId, limit, pageToken, filterConditionInput.getConditions());
            result.setObjectList(searchResult.getResult());
            result.setPreviousMarker(searchResult.getPreviousTokenString());
            searchResult.getContinuation().ifPresent(catalogToken -> {
                result.setNextMarker(catalogToken.toString());
            });
            return ResponseUtil.responseSuccess(result);
        });
    }

    @ApiOperation(value = "add category relation to discovery info")
    @PatchMapping(value = "addCategoryRelation", produces = "application/json;charset=UTF-8")
    public BaseResponse addCategoryRelation(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id", required = true) String projectId,
        @ApiParam(value = "category id", required = true) @RequestParam(value = "categoryId", required = true) Integer categoryId,
        @ApiParam(value = "qualified name", required = true) @RequestParam(value = "qualifiedName", required = true) String qualifiedName,
        @ApiParam(value = "authorization") @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            discoveryService.addCategoryRelation(projectId, qualifiedName, categoryId);
            return BaseResponseUtil.responseSuccess();
        });
    }

    @ApiOperation(value = "remove category relation from discovery info")
    @DeleteMapping(value = "removeCategoryRelation", produces = "application/json;charset=UTF-8")
    public BaseResponse removeCategoryRelation(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id", required = true) String projectId,
        @ApiParam(value = "category id", required = true) @RequestParam(value = "categoryId", required = true) Integer categoryId,
        @ApiParam(value = "qualified name", required = true) @RequestParam(value = "qualifiedName", required = true) String qualifiedName,
        @ApiParam(value = "authorization") @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            discoveryService.removeCategoryRelation(projectId, qualifiedName, categoryId);
            return BaseResponseUtil.responseSuccess();
        });
    }

    @ApiOperation(value = "get table categories")
    @GetMapping(value = "getTableCategories", produces = "application/json;charset=UTF-8")
    public CatalogResponse<TableCategories> getTableCategories(
            @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id", required = true) String projectId,
            @ApiParam(value = "qualified name", required = true) @RequestParam(value = "qualifiedName", required = true) String qualifiedName,
            @ApiParam(value = "authorization") @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> ResponseUtil.responseSuccess(discoveryService.getTableCategories(projectId, qualifiedName)));
    }
    @ApiOperation(value = "count by category with cascade")
    @PostMapping(value = "getObjectCountByCategory", produces = "application/json;charset=UTF-8")
    public CatalogResponse<ObjectCount> getObjectCountByCategory(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id", required = true) String projectId,
        @ApiParam(value = "categoryId", required = true) @RequestParam(value = "categoryId", required = true) Integer categoryId,
        @ApiParam(value = "catalog name", required = true) @RequestParam(value = "catalogName", required = false) String catalogName,
        @ApiParam(value = "object type", required = true) @RequestParam(value = "objectType", required = false) String objectType,
        @ApiParam(value = "qualified name keyword") @RequestParam(value = "keyword", required = false) String keyword,
        @ApiParam(value = "owner") @RequestParam(value = "owner", required = false) String owner,
        @ApiParam(value = "logical operator") @RequestParam(value = "logicalOperator", required = false, defaultValue = "and") String logicalOperator,
        @ApiParam(value = "exact match") @RequestParam(value = "exactMatch", required = false, defaultValue = "false") boolean exactMatch,
        @ApiParam(value = "filter json conditions") @RequestBody(required = false) FilterConditionInput filterConditionInput,
        @ApiParam(value = "authorization") @RequestHeader("Authorization") String token) {
        return createResponse(token, () ->
                ResponseUtil.responseSuccess(discoveryService.getObjectCountByCategory(projectId, catalogName, objectType,
                    keyword, owner,logicalOperator, exactMatch, categoryId, filterConditionInput.getConditions())));
    }

    @ApiOperation(value = "count by catalog")
    @PostMapping(value = "getTableCountByCatalog", produces = "application/json;charset=UTF-8")
    public CatalogResponse<CatalogTableCount[]> getTableCountByCatalog(
            @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id", required = true) String projectId,
            @ApiParam(value = "catalog name", required = true) @RequestParam(value = "catalogName", required = false) String catalogName,
            @ApiParam(value = "qualified name keyword") @RequestParam(value = "keyword", required = false) String keyword,
            @ApiParam(value = "owner") @RequestParam(value = "owner", required = false) String owner,
            @ApiParam(value = "logical operator") @RequestParam(value = "logicalOperator", required = false, defaultValue = "and") String logicalOperator,
            @ApiParam(value = "exact match") @RequestParam(value = "exactMatch", required = false, defaultValue = "false") boolean exactMatch,
            @ApiParam(value = "filter json conditions") @RequestBody(required = false) FilterConditionInput filterConditionInput,
            @ApiParam(value = "authorization") @RequestHeader("Authorization") String token) {

        return createResponse(token, () ->
                ResponseUtil.responseSuccess(discoveryService.getTableCountByCatalog(projectId, catalogName,
                        keyword, owner,logicalOperator, exactMatch, filterConditionInput.getConditions()).toArray())
                );

    }

    /**
     * Initialize discovery record data, currently only supported ObjectType=Table
     *
     * @param projectId
     * @param objectType
     * @param token
     */
    @ApiOperation(value = "init discovery")
    @GetMapping(value = "init", produces = "application/json;charset=UTF-8")
    public BaseResponse initDiscovery(
            @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id", required = true) String projectId,
            @ApiParam(value = "object type") @RequestParam(value = "objectType", required = false) String objectType,
            @ApiParam(value = "authorization") @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            discoveryService.initDiscovery(projectId, objectType);
            return BaseResponseUtil.responseSuccess();
        });
    }
}
