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

import io.polycat.catalog.audit.api.UserLog;
import io.polycat.catalog.audit.impl.UserLogAop;
import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.model.BaseResponse;
import io.polycat.catalog.common.model.Catalog;
import io.polycat.catalog.common.model.CatalogCommit;
import io.polycat.catalog.common.model.CatalogHistory;
import io.polycat.catalog.common.model.CatalogHistoryObject;
import io.polycat.catalog.common.model.CatalogIdent;
import io.polycat.catalog.common.model.CatalogName;
import io.polycat.catalog.common.model.CatalogResponse;
import io.polycat.catalog.common.model.PagedList;
import io.polycat.catalog.common.plugin.request.input.CatalogInput;
import io.polycat.catalog.common.plugin.request.input.MergeBranchInput;
import io.polycat.catalog.common.utils.ModelConvertor;
import io.polycat.catalog.server.util.BaseResponseUtil;
import io.polycat.catalog.server.util.ResponseUtil;
import io.polycat.catalog.service.api.CatalogService;
import io.polycat.catalog.store.common.StoreConvertor;

import com.sun.jersey.spi.StringReader.ValidateDefaultValue;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.Length;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.protobuf.ProtobufHttpMessageConverter;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Api(tags = "catalog api")
@ApiResponses(value = {
    @ApiResponse(code = 400, message = "Bad Request"),
    @ApiResponse(code = 408, message = "Request Timeout"),
    @ApiResponse(code = 500, message = "Internal Server Error")
})
@RestController
@RequestMapping("/v1/{project-id}/catalogs")
@Validated
public class CatalogController extends BaseController {

    @Autowired
    private CatalogService catalogService;

    @Bean
    ProtobufHttpMessageConverter protobufHttpMessageConverter() {
        return new ProtobufHttpMessageConverter();
    }

    /**
     * create catalog
     *
     * @param projectId    projectId
     * @param catalogInput catalog
     * @return ResponseEntity
     */
    @UserLog(operation = "create catalog", objectType = ObjectType.CATALOG)
    @ApiOperation(value = "create catalog")
    @PostMapping(value = "", produces = "application/json;charset=UTF-8")
    public CatalogResponse<Catalog> createCatalog(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog input body", required = true) @Valid @RequestBody CatalogInput catalogInput,
        @ApiParam(value = "authorization") @RequestHeader("Authorization") String token){
        return createResponse(token, () -> {
            //record auditlog objectName;
            UserLogAop.setAuditLogObjectName(catalogInput.getCatalogName());

            Catalog catalog = catalogService.createCatalog(projectId, catalogInput);
            //record auditlog objectID;
//            UserLogAop.setAuditLogObjectId(catalog.getCatalogId());
            return ResponseUtil.responseSuccess(catalog, HttpStatus.CREATED);
        });
    }

    /**
     * drop catalog By name
     *
     * @param projectId   projectId
     * @param catalogName catalogName
     * @return ResponseEntity
     */
    @ApiOperation(value = "drop catalog by name")
    @DeleteMapping(value = "/{catalog-name}", produces = "application/json;charset=UTF-8")
    public BaseResponse dropCatalog(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            CatalogName catalog = StoreConvertor.catalogName(projectId, catalogName);
            catalogService.dropCatalog(catalog);
            return BaseResponseUtil.responseSuccess();
        });
    }

    /**
     * alter catalog
     *
     * @param projectId    projectId
     * @param catalogName  catalogName
     * @param catalogInput catalogDTO
     * @return ResponseEntity
     */
    @ApiOperation(value = "alter catalog by Name")
    @PutMapping(value = "/{catalog-name}", produces = "application/json;charset=UTF-8")
    public BaseResponse alterCatalog(
            @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
            @ApiParam(value = "catalog Name", required = true) @PathVariable("catalog-name") String catalogName,
            @ApiParam(value = "catalog input body", required = true) @Valid @RequestBody CatalogInput catalogInput,
            @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            CatalogName name = StoreConvertor.catalogName(projectId, catalogName);
            catalogService.alterCatalog(name, catalogInput);
            return BaseResponseUtil.responseSuccess();
        });
    }

    /**
     * get Catalog by name
     *
     * @param projectId
     * @param catalogName
     * @return responseEntity
     */
    @UserLog(operation = "get catalog by name", objectType = ObjectType.CATALOG)
    @ApiOperation(value = "get catalog by name")
    @GetMapping(value = "/{catalog-name}", produces = "application/json;charset=UTF-8")
    public CatalogResponse<Catalog> getCatalog(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
        @ApiParam(value = "catalog version") @RequestParam(value = "version", required = false) String version,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        String apiMethod = Thread.currentThread().getStackTrace()[1].getMethodName();
        return createResponse(token, () -> {
            //record auditlog
            UserLogAop.setAuditLogObjectName(catalogName);
            CatalogName catalogNameObj = StoreConvertor.catalogName(projectId, catalogName);
            Catalog catalog = null;
            if (StringUtils.isNotBlank(version)) {
                catalog = ModelConvertor.toCatalog(catalogService.getCatalogByVersion(catalogNameObj, version));
            } else {
                catalog = catalogService.getCatalog(catalogNameObj);
            }
            if (catalog == null) {
                UserLogAop.setAuditLogDetail(ErrorCode.CATALOG_NOT_FOUND.buildErrorMessage());
                throw new CatalogServerException(ErrorCode.CATALOG_NOT_FOUND, catalogName);
            }

            return ResponseUtil.responseSuccess(catalog);
        });
    }

    /**
     * list catalogs by pattern
     *
     * @param projectId
     * @param pattern
     * @return responseEntity
     */
    @ApiOperation(value = "list catalogs")
    @GetMapping(value = "", produces = "application/json;charset=UTF-8")
    public CatalogResponse<PagedList<Catalog>> listCatalogs(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "limit", required = true) @RequestParam(value = "limit", required = false, defaultValue = "1000") @Max(1000) Integer limit,
        @ApiParam(value = "page token", required = true) @RequestParam(value = "pageToken", required = false, defaultValue = "") String pageToken,
        @ApiParam(value = "pattern", required = true) @RequestParam(value = "pattern", required = false, defaultValue = "") String pattern,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            PagedList<Catalog> result = new PagedList<>();
            result.setObjects(catalogService.getCatalogs(projectId, limit, pageToken, pattern)
                .getResult().toArray(new Catalog[0]));
            return ResponseUtil.responseSuccess(result);
        });
    }

    /**
     * list catalog commits
     *
     * @param projectId
     * @param catalogName
     * @return responseEntity
     */
    @ApiOperation(value = "show catalog commits")
    @GetMapping(value = "/{catalog-name}/commit-logs", produces = "application/json;charset=UTF-8")
    public CatalogResponse<PagedList<CatalogCommit>> listCatalogCommits(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
        @ApiParam(value = "begin date") @RequestParam(value = "begin_date", required = false) String begDate,
        @ApiParam(value = "end date") @RequestParam(value = "end_date",required = false) String endDate,
        @ApiParam(value = "begin version") @RequestParam(value = "begin_version",required = false) String begVersion,
        @ApiParam(value = "end version") @RequestParam(value = "end_version",required = false) String endVersion,
        @ApiParam(value = "limit", required = true) @RequestParam(value = "limit", defaultValue = "1000") Integer limit,
        @ApiParam(value = "marker id", required = true) @RequestParam(value = "marker", defaultValue = "") String marker,
        @ApiParam(value = "authorization") @RequestHeader("Authorization") String token) {

        return createResponse(token, () -> {
            CatalogName name = new CatalogName(projectId, catalogName);
            PagedList<CatalogCommit> result = new PagedList<>();
            result.setObjects(catalogService.getCatalogCommits(name, limit, marker)
                .getResult().toArray(new CatalogCommit[0]));
            return ResponseUtil.responseSuccess(result);
        });
    }

    /**
     * list catalog branches
     *
     * @param projectId
     * @return responseEntity
     */
    @ApiOperation(value = "list branches")
    @GetMapping(value = "/{catalog-name}/sub-branches", produces = "application/json;charset=UTF-8")
    public CatalogResponse<PagedList<Catalog>> showBranchesByCatalogName(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            CatalogName name = new CatalogName(projectId, catalogName);
            PagedList<Catalog> result = new PagedList<>();
            result.setObjects(catalogService.listSubBranchCatalogs(name)
                .toArray(new Catalog[0]));
            return ResponseUtil.responseSuccess(result);
        });
    }

    @ApiOperation(value = "merge branch")
    @PostMapping(value = "/merge", produces = "application/json;charset=UTF-8")
    public BaseResponse mergeBranchByName(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "merge branch input body", required = true) @Valid @RequestBody MergeBranchInput mergeBranchInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            catalogService.mergeBranch(projectId, mergeBranchInput);
            return BaseResponseUtil.responseSuccess();
        });
    }

}
