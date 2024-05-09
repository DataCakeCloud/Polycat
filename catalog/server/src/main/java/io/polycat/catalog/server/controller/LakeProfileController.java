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


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.polycat.catalog.server.util.ResponseUtil;
import io.polycat.catalog.common.model.Catalog;
import io.polycat.catalog.common.model.CatalogName;
import io.polycat.catalog.common.model.CatalogResponse;
import io.polycat.catalog.common.model.Database;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.LakeProfile;
import io.polycat.catalog.common.model.Table;
import io.polycat.catalog.common.model.TraverseCursorResult;
import io.polycat.catalog.common.model.UserProfileContext;
import io.polycat.catalog.service.api.CatalogService;
import io.polycat.catalog.service.api.DatabaseService;
import io.polycat.catalog.service.api.TableService;
import io.polycat.catalog.store.common.StoreConvertor;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@ApiResponses(value = {
    @ApiResponse(code = 400, message = "Bad Request"),
    @ApiResponse(code = 408, message = "Request Timeout"),
    @ApiResponse(code = 500, message = "Internal Server Error")
})
@RestController
@RequestMapping("/v1/{project-id}/lakeprofile")
public class LakeProfileController extends BaseController{

    @Autowired
    private CatalogService catalogService;

    @Autowired
    private DatabaseService databaseService;

    @Autowired
    private TableService tableService;

    /**
     *
     *
     * @param projectId   projectId
     * @return CatalogResponse
     */
    @ApiOperation(value = "get lake profile")
    @GetMapping(value = "", produces = "application/json;charset=UTF-8")
    public CatalogResponse<LakeProfile> getLakeProfileInfo(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            TraverseCursorResult<List<Catalog>> result = catalogService.getCatalogs(projectId,
                0, null, null);
            List<Catalog> catalogList = result.getResult();
            List<Database> databaseList = getDatabasesByCatalogs(projectId, catalogList);
            List<Table> tableList = getTablesByDatabases(projectId, databaseList);
            return ResponseUtil.responseSuccess(convertLakeProfile(projectId, databaseList, tableList));
        });
    }

    private List<Database> traverseAllDatabases(CatalogName catalogName) {
        List<Database> databaseList = new ArrayList<>();
        String pageToken = null;
        while(true) {
            TraverseCursorResult<List<Database>> result = databaseService.listDatabases(catalogName,
                false, 1000, pageToken, null);
            pageToken = result.getContinuation().toString();
            databaseList.addAll(result.getResult());
            if (!result.getContinuation().isPresent()) {
                break;
            }
        }
        return databaseList;
    }

    private List<Database> getDatabasesByCatalogs(String projectId, List<Catalog> catalogList) {
        List<Database> databaseList = new ArrayList<>();
        for (Catalog catalog : catalogList) {
            CatalogName catalogName = StoreConvertor.catalogName(projectId, catalog.getCatalogName());
            List<Database> databases = traverseAllDatabases(catalogName);
            databaseList.addAll(databases);
        }
        return databaseList;
    }

    private List<Table> traverseAllTables(DatabaseName databaseName) {
        List<Table> tableList = new ArrayList<>();
        String pageToken = null;
        while(true) {
            TraverseCursorResult<List<Table>> result = tableService.listTable(databaseName,
                false, 1000, pageToken, null);
            pageToken = result.getContinuation().toString();
            tableList.addAll(result.getResult());
            if (!result.getContinuation().isPresent()) {
                break;
            }
        }
        return tableList;
    }

    private List<Table> getTablesByDatabases(String projectId, List<Database> databaseList) {
        List<Table> tableList = new ArrayList<>();
        for (Database database : databaseList) {
            DatabaseName databaseName = StoreConvertor.databaseName(projectId, database.getCatalogName(), database.getDatabaseName());
            List<Table> tables = traverseAllTables(databaseName);
            tableList.addAll(tables);
        }
        return tableList;
    }

    private LakeProfile convertLakeProfile(String projectId, List<Database> databaseList, List<Table> tableList) {
        List<UserProfileContext> userProfileContexts = convertUserProfiles(databaseList, tableList);
        LakeProfile lakeProfile = LakeProfile.builder()
            .projectId(projectId).userProfileContexts(userProfileContexts).build();
        return lakeProfile;
    }

    private List<UserProfileContext> convertUserProfiles(List<Database> databaseList, List<Table> tableList) {
        Map<String, UserProfileContext> userProfileMap = new HashMap<>();
        for (Database database : databaseList) {
            StringBuffer buffer = new StringBuffer();
            buffer.append(database.getAuthSourceType()).append(database.getAccountId())
                .append(database.getOwnerType()).append(database.getOwner());
            UserProfileContext userProfileContext = userProfileMap.get(buffer.toString());
            if (userProfileContext == null) {
                userProfileContext = UserProfileContext.builder()
                    .authSourceType(database.getAuthSourceType()).accountId(database.getAccountId())
                    .ownerType(database.getOwnerType()).owner(database.getOwner()).totalDatabase(1).totalTable(0).build();
                userProfileMap.put(buffer.toString(), userProfileContext);
            } else {
                userProfileContext.setTotalDatabase(userProfileContext.getTotalDatabase()+1);
            }
        }

        for (Table table : tableList) {
            StringBuffer buffer = new StringBuffer();
            buffer.append(table.getAuthSourceType()).append(table.getAccountId())
                .append(table.getOwnerType()).append(table.getOwner());
            UserProfileContext userProfileContext = userProfileMap.get(buffer.toString());
            if (userProfileContext == null) {
                userProfileContext = UserProfileContext.builder()
                    .authSourceType(table.getAuthSourceType()).accountId(table.getAccountId())
                    .ownerType(table.getOwnerType()).owner(table.getOwner()).totalDatabase(0).totalTable(1).build();
                userProfileMap.put(buffer.toString(), userProfileContext);
            } else {
                userProfileContext.setTotalTable(userProfileContext.getTotalTable()+1);
            }
        }
        return userProfileMap.values().stream().collect(Collectors.toList());
    }
}
