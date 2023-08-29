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
import io.polycat.catalog.common.model.*;
import io.polycat.catalog.common.model.CatalogName;
import io.polycat.catalog.common.model.Database;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;
import io.polycat.catalog.common.utils.GsonUtil;
import io.polycat.catalog.server.util.BaseResponseUtil;
import io.polycat.catalog.server.util.ResponseUtil;
import io.polycat.catalog.service.api.DatabaseService;
import io.polycat.catalog.store.common.StoreConvertor;

import com.google.api.Page;
import io.swagger.annotations.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Api(tags = "database api")
@ApiResponses(value = {
    @ApiResponse(code = 400, message = "Bad Request"),
    @ApiResponse(code = 408, message = "Request Timeout"),
    @ApiResponse(code = 500, message = "Internal Server Error")
})
@RestController
@RequestMapping("/v1/{project-id}/catalogs/{catalog-name}/databases")
public class DatabaseController extends BaseController {

    @Autowired
    private DatabaseService databaseService;

    /**
     * create database
     *
     * @param projectId     projectId
     * @param catalogName   catalogName
     * @param dataBaseInput dataBaseDTO
     * @return CatalogResponse
     */
    @UserLog(operation = "create database", objectType = ObjectType.DATABASE)
    @PostMapping(value = "", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "create database")
    public CatalogResponse<Database> createDatabase(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
        @ApiParam(value = "database input body", required = true) @RequestBody DatabaseInput dataBaseInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            UserLogAop.setAuditLogObjectName(catalogName + "." + dataBaseInput.getDatabaseName());
            CatalogName catalog = StoreConvertor.catalogName(projectId, catalogName);
            Database database = databaseService.createDatabase(catalog, dataBaseInput);
            //record auditlog
            UserLogAop.setAuditLogDetail(database.getCatalogName() + "." + database.getDatabaseName() + " create success");
            return ResponseUtil.responseSuccess(database, HttpStatus.CREATED);
        });
    }

    /**
     * drop/delete database
     *
     * @param projectId    projectId
     * @param catalogName  catalogName
     * @param databaseName databaseName
     * @param cascade      cascade
     * @return CatalogResponse
     */
    @DeleteMapping(value = "", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "drop database")
    public BaseResponse dropDatabase(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @RequestParam(value = "databaseName") String databaseName,
        @ApiParam(value = "ignore unknown database") @RequestParam(value = "ignoreUnknownObj", required = false, defaultValue = "false") Boolean ignoreUnknownObj,
        @ApiParam(value = "delete data in database") @RequestParam(value = "deleteData", required = false, defaultValue = "false") Boolean deleteData,
        @ApiParam(value = "cascade") @RequestParam(value = "cascade", required = false, defaultValue = "false") String cascade,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            String dbNameIgnCase = databaseName.toLowerCase();
            DatabaseName name = StoreConvertor.databaseName(projectId, catalogName, dbNameIgnCase);
            databaseService.dropDatabase(name, ignoreUnknownObj, deleteData, cascade);
            return BaseResponseUtil.responseSuccess();
        });
    }

    /**
     * undrop DB
     *
     * @param projectId   projectId
     * @param catalogName catalogId
     * @param databaseId  databaseId
     * @param rename      rename
     * @return CatalogResponse
     */
    @PutMapping(value = "/{database-name}/undrop", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "undrop database")
    public BaseResponse undropDatabase(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable("database-name") String databaseName,
        @ApiParam(value = "database id") @RequestParam(value = "databaseId", required = false, defaultValue = "") String databaseId,
        @ApiParam(value = "new name") @RequestParam(value = "rename", required = false, defaultValue = "") String rename,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            DatabaseName databaseNameParam = StoreConvertor.databaseName(projectId, catalogName, databaseName);
            databaseService.undropDatabase(databaseNameParam, databaseId, rename);
            return BaseResponseUtil.responseSuccess();
        });
    }

    /**
     * alter DB
     *
     * @param projectId the projectId
     * @param catalogName catalog Name
     * @param databaseName databaseName
     * @param databaseInput dataBaseDTO
     * @return CatalogResponse
     */
    @PatchMapping(value = "", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "alter database")
    public BaseResponse alterDatabase(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @RequestParam(value = "databaseName") String databaseName,
        @ApiParam(value = "database input body", required = true) @RequestBody DatabaseInput databaseInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            String catNameIgnCase = catalogName.toLowerCase();
            String dbNameIgnCase = databaseName.toLowerCase();
            DatabaseName database = StoreConvertor.databaseName(projectId, catNameIgnCase, dbNameIgnCase);
            databaseService.alterDatabase(database, databaseInput);
            return BaseResponseUtil.responseSuccess();
        });
    }

    /**
     * list DBs
     *
     * @param projectId   projectId
     * @param catalogName
     * @param includeDrop "true" to see DBs including the deleted ones
     * @param maxResults  number of DBs limits in one response
     * @param pageToken   pageToken is the next consecutive key to begin with in the LIST-Request
     * @param pattern     db pattern expression
     * @return CatalogResponse
     */
    @GetMapping(value = "/names", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "get database names with pattern")
    public CatalogResponse<PagedList<String>> getDatabases(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
        @ApiParam(value = "include dropped flag") @RequestParam(value = "includeDrop", required = false, defaultValue = "false") boolean includeDrop,
        @ApiParam(value = "max results") @RequestParam(value = "maxResults", required = false, defaultValue = "1000") Integer maxResults,
        @ApiParam(value = "page token") @RequestParam(value = "pageToken", required = false, defaultValue = "") String pageToken,
        @ApiParam(value = "pattern") @RequestParam(value = "pattern", required = false, defaultValue = "") String pattern,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            CatalogName catalog = StoreConvertor.catalogName(projectId, catalogName);
            TraverseCursorResult<List<String>> databases = databaseService.getDatabaseNames(catalog, includeDrop,
                maxResults, pageToken, pattern);
            PagedList<String> result = new PagedList<>();

            result.setObjects(databases.getResult().toArray(new String[0]));
            databases.getContinuation().ifPresent(catalogToken -> result.setNextMarker(catalogToken.toString()));
            return ResponseUtil.responseSuccess(result);
        });
    }

    /**
     * rename DB
     *
     * @param projectId
     * @param catalogName
     * @param oldName
     * @param newName String
     * @return CatalogResponse
     */
    @PatchMapping(value = "/rename", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "alter database")
    public BaseResponse renameDatabase(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
        @ApiParam(value = "origin database name", required = true) @RequestParam(value = "oldName") String oldName,
        @ApiParam(value = "new database name", required = true) @RequestParam(value = "newName") String newName,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            String catNameIgnCase = catalogName.toLowerCase();
            String dbNameIgnCase = oldName.toLowerCase();
            DatabaseName database = StoreConvertor.databaseName(projectId, catNameIgnCase, dbNameIgnCase);
            databaseService.renameDatabase(database, newName);
            return BaseResponseUtil.responseSuccess();
        });
    }

    /**
     * list DBs
     *
     * @param projectId   projectId
     * @param catalogName
     * @param includeDrop "true" to see DBs including the deleted ones
     * @param maxResults  number of DBs limits in one response
     * @param pageToken   pageToken is the next consecutive key to begin with in the LIST-Request
     * @param filter      filter expression
     * @return CatalogResponse
     */
    @GetMapping(value = "/list", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "list databases")
    public CatalogResponse<PagedList<Database>> listDatabases(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
        @ApiParam(value = "include dropped flag") @RequestParam(value = "includeDrop", required = false, defaultValue = "false") boolean includeDrop,
        @ApiParam(value = "max results") @RequestParam(value = "maxResults", required = false, defaultValue = "1000") Integer maxResults,
        @ApiParam(value = "page token") @RequestParam(value = "pageToken", required = false, defaultValue = "") String pageToken,
        @ApiParam(value = "filter") @RequestParam(value = "filter", required = false, defaultValue = "") String filter,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            CatalogName catalog = StoreConvertor.catalogName(projectId, catalogName);
            PagedList<Database> result = new PagedList<>();
            TraverseCursorResult<List<Database>> databases = databaseService.listDatabases(catalog, includeDrop,
                maxResults, pageToken, filter);
            result.setObjects(databases.getResult().toArray(new Database[0]));
            databases.getContinuation().ifPresent(catalogToken -> result.setNextMarker(catalogToken.toString()));
            return ResponseUtil.responseSuccess(result);
        });
    }


    @UserLog(operation = "get database by name", objectType = ObjectType.DATABASE)
    @GetMapping(value = "/getDatabaseByName", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "get database by name")
    public CatalogResponse<Database> getDatabaseByName(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @RequestParam("databaseName") String dbName,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        String apiMethod = Thread.currentThread().getStackTrace()[1].getMethodName();

        return createResponse(token, () -> {
            String catNameIgnCase = catalogName.toLowerCase();
            String dbNameIgnCase = dbName.toLowerCase();
            //record auditlog
            UserLogAop.setAuditLogObjectName(catNameIgnCase + "." + dbNameIgnCase);
            DatabaseName databaseName = StoreConvertor.databaseName(projectId, catNameIgnCase, dbNameIgnCase);
            Database database = databaseService.getDatabaseByName(databaseName);
            if (database == null) {
                //record auditlog
                throw new CatalogServerException(ErrorCode.DATABASE_NOT_FOUND, dbNameIgnCase);
            }
            //record auditlog
            UserLogAop.setAuditLogDetail(database.getCatalogName() + "." + database.getDatabaseName() + "success");
            return ResponseUtil.responseSuccess(database);
        });
    }

    @PatchMapping(value = "/metadata", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "get tables meta data")
    public CatalogResponse<PagedList<TableBrief>> getTableMetaData(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
        @ApiParam(value = "database pattern", required = true) @PathVariable("dbPattern") String dbPattern,
        @ApiParam(value = "table pattern", required = true) @RequestParam("pattern") String tblPattern,
        @ApiParam(value = "table types need list", required = true) @RequestParam("tableType") String tableType,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            CatalogName cName = StoreConvertor.catalogName(projectId, catalogName);
            TraverseCursorResult<TableBrief[]> tableBriefList = databaseService.getTableFuzzy(cName, dbPattern,
                tblPattern, GsonUtil.fromJson(tableType, List.class));
            PagedList<TableBrief> result = new PagedList<>();
            result.setObjects(tableBriefList.getResult());
            tableBriefList.getContinuation().ifPresent(t -> result.setNextMarker(t.toString()));
            return ResponseUtil.responseSuccess(result);
        });
    }
}
