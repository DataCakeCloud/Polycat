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

import io.polycat.catalog.audit.api.UserLog;
import io.polycat.catalog.audit.impl.UserLogAop;
import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.Constants;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.model.BaseResponse;
import io.polycat.catalog.common.model.AuthTableObjParam;
import io.polycat.catalog.common.model.AuthorizationType;
import io.polycat.catalog.common.model.Constraint;
import io.polycat.catalog.common.model.ConstraintType;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.ForeignKey;
import io.polycat.catalog.common.model.PagedList;
import io.polycat.catalog.common.model.PrimaryKey;
import io.polycat.catalog.common.model.Table;
import io.polycat.catalog.common.model.TableCommit;
import io.polycat.catalog.common.model.Partition;
import io.polycat.catalog.common.model.TableStats;
import io.polycat.catalog.common.model.TraverseCursorResult;
import io.polycat.catalog.common.model.CatalogResponse;
import io.polycat.catalog.common.model.stats.ColumnStatistics;
import io.polycat.catalog.common.model.stats.ColumnStatisticsObj;
import io.polycat.catalog.common.plugin.request.input.AddConstraintsInput;
import io.polycat.catalog.common.plugin.request.input.AddForeignKeysInput;
import io.polycat.catalog.common.plugin.request.input.AddPrimaryKeysInput;
import io.polycat.catalog.common.plugin.request.input.AlterTableInput;
import io.polycat.catalog.common.plugin.request.input.ColumnChangeInput;
import io.polycat.catalog.common.plugin.request.input.FilterInput;
import io.polycat.catalog.common.plugin.request.input.SetTablePropertyInput;
import io.polycat.catalog.common.plugin.request.input.TableInput;
import io.polycat.catalog.common.plugin.request.input.UnsetTablePropertyInput;
import io.polycat.catalog.common.utils.CatalogStringUtils;
import io.polycat.catalog.common.utils.GsonUtil;
import io.polycat.catalog.common.utils.ModelConvertor;
import io.polycat.catalog.server.util.BaseResponseUtil;
import io.polycat.catalog.server.util.ResponseUtil;
import io.polycat.catalog.service.api.PartitionService;
import io.polycat.catalog.service.api.TableService;
import io.polycat.catalog.store.common.StoreConvertor;
import io.polycat.catalog.common.model.TableName;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import jakarta.validation.Valid;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Api(tags = "table api")
@ApiResponses(value = {
    @ApiResponse(code = 400, message = "Bad Request"),
    @ApiResponse(code = 408, message = "Request Timeout"),
    @ApiResponse(code = 500, message = "Internal Server Error")
})
@RestController
@RequestMapping("/v1/{project-id}/catalogs/{catalog-name}/databases/{database-name}/tables")
public class TableController extends BaseController {

    @Autowired
    private TableService tableService;

    @Autowired
    private PartitionService partitionService;

    @UserLog(operation = "create table", objectType = ObjectType.TABLE)
    @PostMapping(value = "", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "create table")
    public CatalogResponse<Table> createTable(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table input body", required = true) @RequestBody TableInput tableInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            //record auditlog
            UserLogAop.setAuditLogObjectName(catalogName + "." + databaseName + "." + tableInput.getTableName());
            DatabaseName database = StoreConvertor.databaseName(projectId, catalogName, databaseName);
            CatalogStringUtils.databaseNameNormalize(database);
            CatalogStringUtils.tableInputNormalize(tableInput);
            tableService.createTable(database, tableInput);
            Table tableResponse = ModelConvertor.toTableModel(catalogName, databaseName, tableInput.getTableName());
            //record auditlog
            //UserLogAop.setAuditLogObjectId(tableId);
            return ResponseUtil.responseSuccess(tableResponse, HttpStatus.CREATED);
        });
    }

    @GetMapping(value = "/{table-name}/listTableCommits", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "list table commits")
    public CatalogResponse<PagedList<TableCommit>> listTableCommits(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
        @ApiParam(value = "max results") @RequestParam(value = "maxResults", required = false, defaultValue = "1000") int maxResults,
        @ApiParam(value = "next page token") @RequestParam(value = "nextPageToken", required = false) String pageToken,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            TableName table = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            PagedList<TableCommit> result = new PagedList<>();
            TraverseCursorResult<List<TableCommit>> tableCommits = tableService.listTableCommits(table, maxResults,
                pageToken);
            result.setObjects(tableCommits.getResult().toArray(new TableCommit[0]));
            tableCommits.getContinuation().ifPresent(catalogToken -> result.setNextMarker(catalogToken.toString()));
            return ResponseUtil.responseSuccess(result);
        });
    }

    @GetMapping(value = "/{table-name}", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "get table by name")
    public CatalogResponse<Table> getTableByName(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable("table-name") String tableName,
        @ApiParam(value = "share name") @RequestParam(value = "shareName", required = false) String shareName,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            TableName tableNameParam = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            CatalogStringUtils.tableNameNormalize(tableNameParam);
            AuthTableObjParam authTableObjParam = new AuthTableObjParam(projectId, catalogName, databaseName,
                    tableNameParam.getTableName());
            if (shareName != null) {
                authTableObjParam.setShareName(shareName);
                authTableObjParam.setAuthorizationType(AuthorizationType.SELECT_FROM_SHARE);
            }
            Table table = tableService.getTableByName(tableNameParam);
            if (table == null) {
                throw new CatalogServerException(ErrorCode.TABLE_NOT_FOUND, tableNameParam.getTableName());
            }
            return ResponseUtil.responseSuccess(table);
        });
    }

    @PostMapping(value = "/{table-name}/listPartitions", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "list partitions")
    public CatalogResponse<Partition[]> listPartitions(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
        @ApiParam(value = "share name") @RequestParam(value = "shareName", required = false) String shareName,
        @ApiParam(value = "filter input body, expressionGson不生效", required = true) @RequestBody FilterInput filterInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        //client: listPartitions(ListFileRequest request) filterInput.expressionGson不生效
        return createResponse(token, () -> {
            AuthTableObjParam tableParam = new AuthTableObjParam(projectId, catalogName, databaseName, tableName);
            if (shareName != null) {
                //project.share.db.table
                tableParam.setShareName(shareName);
                tableParam.setAuthorizationType(AuthorizationType.SELECT_FROM_SHARE);
            }
            TableName tableNameParam = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            Partition[] partitions = partitionService.listPartitions(tableNameParam, filterInput);
            return ResponseUtil.responseSuccess(partitions);
        });
    }

    @GetMapping(value = "/{table-name}/stats", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "get table stats")
    public CatalogResponse<TableStats> getTableStats(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
        @ApiParam(value = "share name") @RequestParam(value = "shareName", required = false) String shareName,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            AuthTableObjParam authTableObjParam = new AuthTableObjParam(projectId, catalogName, databaseName,
                tableName);
            if (shareName != null) {
                authTableObjParam.setShareName(shareName);
                authTableObjParam.setAuthorizationType(AuthorizationType.SELECT_FROM_SHARE);
            } else {
                authTableObjParam.setAuthorizationType(AuthorizationType.NORMAL_OPERATION);
            }
            TableName tableNameParam = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            TableStats stat = partitionService.getTableStats(tableNameParam);
            return ResponseUtil.responseSuccess(stat);
        });
    }


    @GetMapping(value = "/{table-name}/latestVersion", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "get table latest version")
    public CatalogResponse<TableCommit> getTableLatestVersion(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            TableName tableNameParam = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            TableCommit tableCommit = tableService.getLatestTableCommit(tableNameParam);
            return ResponseUtil.responseSuccess(tableCommit);
        });
    }

    @GetMapping(value = "", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "list tables")
    public CatalogResponse<PagedList<Table>> listTables(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable("database-name") String databaseName,
        @ApiParam(value = "include dropped flag", required = false) @RequestParam(value = "includeDrop", required = false, defaultValue = "false") Boolean includeDrop,
        @ApiParam(value = "max results", required = false) @RequestParam(value = "maxResults", required = false, defaultValue = "100") Integer maxResults,
        @ApiParam(value = "page token", required = false) @RequestParam(value = "pageToken", required = false, defaultValue = "") String pageToken,
        @ApiParam(value = "filter", required = false) @RequestParam(value = "filter", required = false, defaultValue = "") String filter,
        @ApiParam(value = "table names") @RequestParam(value = "tblNames", required = false) String tblNamesJson,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            DatabaseName databaseFullName = StoreConvertor.databaseName(projectId, catalogName, databaseName);
            TraverseCursorResult<List<Table>> tableList;
            if (StringUtils.isNotBlank(tblNamesJson)) {
                //tableList = new TraverseCursorResult<>(tableService.getTableObjectsByName(databaseFullName, GsonUtil.fromJson(tblNamesJson, List.class)), null);
                throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getTableObjectsByName");
            } else {
                tableList = tableService.listTable(databaseFullName, includeDrop, maxResults, pageToken, filter);
            }
            PagedList<Table> result = new PagedList<>();
            result.setObjects(tableList.getResult().toArray(new Table[0]));
            tableList.getContinuation().ifPresent(catalogToken -> result.setNextMarker(catalogToken.toString()));
            return ResponseUtil.responseSuccess(result);
        });
    }

    @GetMapping(value = "/listTableNames", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "list table names")
    public CatalogResponse<PagedList<String>> listTableNames(
            @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
            @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
            @ApiParam(value = "database name", required = true) @PathVariable("database-name") String databaseName,
            @ApiParam(value = "include dropped flag", required = false) @RequestParam(value = "includeDrop", required = false, defaultValue = "false") Boolean includeDrop,
            @ApiParam(value = "max results", required = false) @RequestParam(value = "maxResults", required = false, defaultValue = "100") Integer maxResults,
            @ApiParam(value = "page token", required = false) @RequestParam(value = "pageToken", required = false, defaultValue = "") String pageToken,
            @ApiParam(value = "filter", required = false) @RequestParam(value = "filter", required = false, defaultValue = "") String filter,
            @ApiParam(value = "table names") @RequestParam(value = "tblNames", required = false) String tblNamesJson,
            @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            DatabaseName databaseFullName = StoreConvertor.databaseName(projectId, catalogName, databaseName);
            PagedList<String> tableList;
            if (StringUtils.isNotBlank(tblNamesJson)) {
                //tableList = new TraverseCursorResult<>(tableService.getTableObjectsByName(databaseFullName, GsonUtil.fromJson(tblNamesJson, List.class)), null);
                throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getTableObjectsByName");
            } else {
                tableList = tableService.listTableNames(databaseFullName, includeDrop, maxResults, pageToken, filter);
            }
            return ResponseUtil.responseSuccess(tableList);
        });
    }

    @GetMapping(value = "/names", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "get table names")
    public CatalogResponse<PagedList<String>> getTableNames(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable("database-name") String databaseName,
        @ApiParam(value = "filterType", required = true) @RequestParam(value = Constants.PATTERN, required = false) String pattern,
        @ApiParam(value = "filter", required = true) @RequestParam(value = Constants.FILTER, required = false) String filter,
        @ApiParam(value = "tableType", required = true) @RequestParam(value = Constants.TABLE_TYPE, required = false) String tableType,
        @ApiParam(value = "max results", required = true) @RequestParam(value = "maxResults", required = false, defaultValue = "2147483647") Integer maxResults,
        @ApiParam(value = "page token", required = true) @RequestParam(value = "pageToken", required = false, defaultValue = "") String pageToken,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            DatabaseName databaseFullName = StoreConvertor.databaseName(projectId, catalogName, databaseName);
            TraverseCursorResult<List<String>> tableList;
            if (pattern != null) {
                tableList = tableService.getTableNames(databaseFullName, tableType, Integer.valueOf(maxResults),
                    pageToken, pattern);
            } else if (filter != null) {
                tableList = tableService.listTableNamesByFilter(databaseFullName, filter, Integer.valueOf(maxResults),
                    pageToken);
            } else {
                throw new CatalogServerException(ErrorCode.INVALID_OPERATION, "Get table names request illegal");
            }

            PagedList<String> result = new PagedList<>();
            result.setObjects(tableList.getResult().toArray(new String[0]));
            tableList.getContinuation().ifPresent(catalogToken -> result.setNextMarker(catalogToken.toString()));
            return ResponseUtil.responseSuccess(result);
        });
    }

    @DeleteMapping(value = "/{table-name}", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "drop table")
    public BaseResponse dropTable(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable("database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable("table-name") String tableName,
        @ApiParam(value = "purge") @RequestParam(value = "purge", required = false, defaultValue = "false") Boolean purgeFlag,
        @ApiParam(value = "ignoreUnknownObj") @RequestParam(value = "ignoreUnknownObj", required = false, defaultValue = "false") Boolean ignoreUnknownObj,
        @ApiParam(value = "deleteData") @RequestParam(value = "deleteData", required = false, defaultValue = "false") Boolean deleteData,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            TableName tableNameParam = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            tableService.dropTable(tableNameParam, deleteData, ignoreUnknownObj, purgeFlag);
            return BaseResponseUtil.responseSuccess();
        });
    }

    @DeleteMapping(value = "/{table-name}/purge", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "purge table")
    public BaseResponse purgeTable(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable("database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable("table-name") String tableName,
        @ApiParam(value = "table id", required = true) @RequestParam(value = "tableId", required = false, defaultValue = "") String tableId,
        @ApiParam(value = "exist flag", required = true) @RequestParam(value = "ifExist", required = false, defaultValue = "false") Boolean ifExist,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            try {
                TableName tableNameParam = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
                tableService.purgeTable(tableNameParam, tableId);
            } catch (CatalogServerException e) {
                if (e.getErrorCode() == ErrorCode.TABLE_NOT_FOUND && ifExist) {
                    return ResponseUtil.responseSuccess(HttpStatus.OK);
                }
                throw e;
            }

            return BaseResponseUtil.responseSuccess();
        });
    }

    @PutMapping(value = "/{table-name}/undrop", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "undrop table")
    public BaseResponse undropTable(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable("database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable("table-name") String tableName,
        @ApiParam(value = "table id", required = true) @RequestParam(value = "tableId", required = false, defaultValue = "") String tableId,
        @ApiParam(value = "rename", required = true) @RequestParam(value = "rename", required = false, defaultValue = "") String rename,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            TableName tableParam = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            tableService.undropTable(tableParam, tableId, rename);
            return BaseResponseUtil.responseSuccess();
        });
    }

    @PutMapping(value = "/{table-name}/restore", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "restore table")
    public BaseResponse restoreTable(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable("database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable("table-name") String tableName,
        @ApiParam(value = "version", required = true) @RequestParam(value = "version", required = true) String version,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            TableName tableNameParam = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            tableService.restoreTable(tableNameParam, version);
            return BaseResponseUtil.responseSuccess();
        });
    }

    @PatchMapping(value = "/{table-name}", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "alter table")
    public BaseResponse alterTable(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable("database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable("table-name") String tableName,
        @ApiParam(value = "operator", required = true) @RequestParam("operate") String operator,
        @ApiParam(value = "table input body", required = true) @Valid @RequestBody(required = false) AlterTableInput alterInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            TableName tableNameParam = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            CatalogStringUtils.tableNameNormalize(tableNameParam);
            CatalogStringUtils.tableInputNormalize(alterInput.getTable());
            if (operator.equals(Constants.TRUNCATE)) {
                tableService.truncateTable(tableNameParam);
            } else if (operator.equals(Constants.ALTER)) {
                tableService.alterTable(tableNameParam, alterInput.getTable(), alterInput.getAlterParams());
            } else {
                throw new CatalogServerException(ErrorCode.INVALID_OPERATION, operator + " illegal");
            }
            return BaseResponseUtil.responseSuccess();
        });
    }

    @PatchMapping(value = "/{table-name}/setProperties", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "set properties")
    public BaseResponse setProperties(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable("database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable("table-name") String tableName,
        @ApiParam(value = "set table property input body", required = true) @Valid @RequestBody SetTablePropertyInput input,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            TableName tableNameParam = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            tableService.setProperties(tableNameParam, input.getSetProperties());

            return BaseResponseUtil.responseSuccess();
        });
    }

    @PatchMapping(value = "/{table-name}/unsetProperties", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "unset properties")
    public BaseResponse unsetProperties(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable("database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable("table-name") String tableName,
        @ApiParam(value = "unset table property input body", required = true) @Valid @RequestBody UnsetTablePropertyInput input,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            TableName tableNameParam = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            tableService.unsetProperties(tableNameParam, input.getUnsetProperties());

            return BaseResponseUtil.responseSuccess();
        });
    }

    @PatchMapping(value = "/{table-name}/alterColumn", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "alter column")
    public BaseResponse alterColumn(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable("database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable("table-name") String tableName,
        @ApiParam(value = "column change input body", required = true) @Valid @RequestBody ColumnChangeInput columnChangeInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            TableName tableNameParam = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            tableService.alterColumn(tableNameParam, columnChangeInput);
            return BaseResponseUtil.responseSuccess();
        });
    }

    @DeleteMapping(value = "/{table-name}/columnStatistics", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "delete table column statistics")
    public BaseResponse deleteTableColumnStatistics(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable("database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable("table-name") String tableName,
        @ApiParam(value = "column name", required = true) @RequestParam("colName") String columnName,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            TableName tableNameParam = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            tableService.deleteTableColumnStatistics(tableNameParam, columnName);
            return BaseResponseUtil.responseSuccess();
        });
    }

    @GetMapping(value = "/{table-name}/columnStatistics", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "get table column statistics")
    public CatalogResponse<PagedList<ColumnStatisticsObj>> getTableColumnStatistics(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable("catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable("database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable("table-name") String tableName,
        @ApiParam(value = "column name", required = true) @RequestParam("columns") String columnName,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            TableName tableNameParam = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            PagedList<ColumnStatisticsObj> statsList = new PagedList<>();
            statsList.setObjects(tableService.getTableColumnStatistics(tableNameParam, GsonUtil.fromJson(columnName, List.class)));
            return ResponseUtil.responseSuccess(statsList);
        });
    }

    @PatchMapping(value = "/{table-name}/columnStatistics", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "update table column statistics")
    public BaseResponse updateTableColumnStatistics(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "new column statistics object", required = true) @RequestBody() ColumnStatistics newStatObj,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            tableService.updateTableColumnStatistics(projectId, newStatObj);
            return BaseResponseUtil.responseSuccess();
        });
    }

    // constraint
    @ApiOperation(value = "add constraint")
    @PostMapping(value = "/{table-name}/constraints", produces = "application/json;charset=UTF-8")
    public BaseResponse addConstraint(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
        @ApiParam(value = "partition input body", required = true) @RequestBody AddConstraintsInput input,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            tableService.addConstraint(projectId, input.getConstraints());
            return BaseResponseUtil.responseSuccess();
        });
    }

    @ApiOperation(value = "delete constraint")
    @DeleteMapping(value = "/{table-name}/constraints/{constraint-name}", produces = "application/json;charset=UTF-8")
    public BaseResponse deleteConstraint(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
        @ApiParam(value = "constraint name", required = true) @PathVariable(value = "constraint-name") String constraintName,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            TableName tableNameObj = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            tableService.dropConstraint(tableNameObj, constraintName);
            return BaseResponseUtil.responseSuccess();
        });
    }

    @ApiOperation(value = "get constraints")
    @GetMapping(value = "/{table-name}/constraints", produces = "application/json;charset=UTF-8")
    public CatalogResponse<List<Constraint>> getConstraint(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
        @ApiParam(value = "constraint type", required = true) @RequestParam(value = "type") String constraintType,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            TableName tableNameObj = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            PagedList<Constraint> result = new PagedList<>();
            result.setObjects(tableService.getConstraints(tableNameObj, ConstraintType.valueOf(constraintType))
                .toArray(new Constraint[0]));
            return ResponseUtil.responseSuccess(result);
        });
    }

    @ApiOperation(value = "add primary key")
    @PostMapping(value = "/{table-name}/primaryKeys", produces = "application/json;charset=UTF-8")
    public BaseResponse addPrimaryKey(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
        @ApiParam(value = "partition input body", required = true) @RequestBody AddPrimaryKeysInput input,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            tableService.addPrimaryKey(projectId, input.getPrimaryKeys());
            return BaseResponseUtil.responseSuccess();
        });
    }

    @ApiOperation(value = "add primary key")
    @PostMapping(value = "/{table-name}/foreignKeys", produces = "application/json;charset=UTF-8")
    public BaseResponse addForeignKey(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
        @ApiParam(value = "partition input body", required = true) @RequestBody AddForeignKeysInput input,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            tableService.addForeignKey(projectId, input.getForeignKeys());
            return BaseResponseUtil.responseSuccess();
        });
    }

    @ApiOperation(value = "get primary keys")
    @GetMapping(value = "/{table-name}/primaryKeys", produces = "application/json;charset=UTF-8")
    public CatalogResponse<PagedList<PrimaryKey>> getPrimaryKeys(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            TableName tableNameObj = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            PagedList<PrimaryKey> result = new PagedList<>();
            result.setObjects(tableService.getPrimaryKeys(tableNameObj).toArray(new PrimaryKey[0]));
            return ResponseUtil.responseSuccess(result);
        });
    }

    @ApiOperation(value = "get foreign keys")
    @GetMapping(value = "/{table-name}/foreignKeys", produces = "application/json;charset=UTF-8")
    public CatalogResponse<PagedList<ForeignKey>> getForeignKeys(
        @ApiParam(value = "project id", required = true) @PathVariable(value = "project-id") String projectId,
        @ApiParam(value = "catalog name", required = true) @PathVariable(value = "catalog-name") String catalogName,
        @ApiParam(value = "database name", required = true) @PathVariable(value = "database-name") String databaseName,
        @ApiParam(value = "table name", required = true) @PathVariable(value = "table-name") String tableName,
        @ApiParam(value = "parent db name", required = true) @RequestParam(value = "parentDbName") String parentDbName,
        @ApiParam(value = "parent tbl name", required = true) @RequestParam(value = "parentTblName") String parentTblName,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            TableName tableNameObj = StoreConvertor.tableName(projectId, catalogName, databaseName, tableName);
            TableName parentTableNameObj = StoreConvertor.tableName(projectId, catalogName, parentDbName,
                parentTblName);
            PagedList<ForeignKey> result = new PagedList<>();
            result.setObjects(tableService.getForeignKeys(parentTableNameObj, tableNameObj).toArray(new ForeignKey[0]));
            return ResponseUtil.responseSuccess(result);
        });
    }
    // end constraint
}
