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
package io.polycat.probe.trino;

import io.polycat.catalog.client.PolyCatClient;
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.http.CatalogClientHelper;
import io.polycat.catalog.common.http.HttpMethodName;
import io.polycat.catalog.common.model.AuthorizationResponse;
import io.polycat.catalog.common.model.AuthorizationType;
import io.polycat.catalog.common.model.CatalogInnerObject;
import io.polycat.catalog.common.plugin.request.AuthenticationRequest;
import io.polycat.catalog.common.plugin.request.GetTableRequest;
import io.polycat.catalog.common.plugin.request.input.AuthorizationInput;
import io.polycat.catalog.common.plugin.response.CatalogWebServiceResult;
import io.polycat.probe.PolyCatClientUtil;

import io.trino.sql.tree.Statement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrinoTableAuthorization {
    private static final Logger LOG = LoggerFactory.getLogger(TrinoTableAuthorization.class);
    private final String CLOUD_RESOURCE_REGION = "clustermanager.cloudresource.region";
    private final String CLUSTER_MANAGER_CHECK_PATH_URL = "clustermanager.checkpath.url";

    private PolyCatClient polyCatClient;
    private String polyCatCatalog;
    private Configuration configuration;
    private ExecutorService executorService = Executors.newFixedThreadPool(1);

    public TrinoTableAuthorization(Configuration conf, String polyCatCatalog) {
        this.polyCatClient = PolyCatClientUtil.buildPolyCatClientFromConfig(conf);
        this.polyCatCatalog = polyCatCatalog;
        this.configuration = conf;
    }

    public List<AuthorizationInput> parseSqlAndCheckAuthorization(Statement statement)
            throws CatalogException {
        List<AuthorizationInput> authorizationInputs = new ArrayList<>();
        List<AuthorizationInput> authorizationNotAllowedList = new ArrayList<>();

        Set<String> targetTable = Sets.newHashSet();
        Set<String> queryTable = Sets.newHashSet();
        Set<String> tempTable = Sets.newHashSet();
        // parse sql and extract table name
        TrinoSqlParserHelper.extractTablesFromStatement(
                statement, targetTable, queryTable, tempTable);
        // remove temporary table
        queryTable =
                queryTable.stream()
                        .filter(tb -> !tempTable.contains(tb))
                        .collect(Collectors.toSet());
        // create AuthorizationInput
        authorizationInputs.addAll(
                targetTable.stream()
                        .map(table -> createAuthorizationInput(table, Operation.INSERT_TABLE))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()));
        authorizationInputs.addAll(
                queryTable.stream()
                        .map(table -> createAuthorizationInput(table, Operation.SELECT_TABLE))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()));
        // permission check
        if (!authorizationInputs.isEmpty()) {
            for (AuthorizationInput authorization : authorizationInputs) {
                LOG.info(String.format("authorization info:%s\n", authorization));
                try {
                    Future<AuthorizationResponse> submit =
                            executorService.submit(
                                    () ->
                                            polyCatClient.authenticate(
                                                    new AuthenticationRequest(
                                                            polyCatClient.getProjectId(),
                                                            Arrays.asList(authorization))));
                    AuthorizationResponse response = submit.get(1, TimeUnit.SECONDS);
                    if (!response.getAllowed()) {
                        LOG.info(
                                String.format(
                                        "object [%s] no permission [%s] \n",
                                        authorization.getCatalogInnerObject().getObjectName(),
                                        authorization.getOperation().getPrintName()));
                        authorizationNotAllowedList.add(authorization);
                    }
                } catch (CatalogException e) {
                    if (e.getStatusCode() == 404) {
                        String msg =
                                String.format(
                                        "table [%s] not found error!",
                                        authorization.getCatalogInnerObject().getObjectName());
                        LOG.error(msg, e);
                        throw new CatalogException(msg, e);
                    } else {
                        throw new CatalogException("table authenticate error!", e);
                    }
                } catch (ExecutionException | InterruptedException | TimeoutException e) {
                    LOG.info("polycat request future get exception:", e);
                }
            }
        }

        executorService.shutdownNow();
        return authorizationNotAllowedList;
    }

    public Map<String, String> parseSqlAndCheckLocationPermission(Statement statement) {
        Set<String> targetTable = Sets.newHashSet();
        Set<String> queryTable = Sets.newHashSet();
        Set<String> tempTable = Sets.newHashSet();
        // parse sql and extract table name
        TrinoSqlParserHelper.extractTablesFromStatement(
                statement, targetTable, queryTable, tempTable);
        // remove temporary table
        queryTable =
                queryTable.stream()
                        .filter(tb -> !tempTable.contains(tb))
                        .collect(Collectors.toSet());
        targetTable.addAll(queryTable);
        // <tableName, location>
        Map<String, String> tableAndLocation =
                targetTable.stream()
                        .map(this::createGetTableRequest)
                        .filter(Objects::nonNull)
                        .map(
                                request -> {
                                    try {
                                        return polyCatClient.getTable(request);
                                    } catch (Exception e) {
                                        LOG.warn(
                                                "table "
                                                        + request.getDatabaseName()
                                                        + "."
                                                        + request.getTableName()
                                                        + " not found!");
                                        return null;
                                    }
                                })
                        .filter(Objects::nonNull)
                        .collect(
                                Collectors.toMap(
                                        table ->
                                                table.getDatabaseName()
                                                        + "."
                                                        + table.getTableName(),
                                        table -> table.getStorageDescriptor().getLocation()));
        return filterNotAccessTableStory(tableAndLocation);
    }

    public Map<String, String> filterNotAccessTableStory(Map<String, String> tableAndLocation) {
        Map<String, String> unauthorizedTableAndLocation = new HashMap<>();
        if (tableAndLocation.isEmpty()) {
            return unauthorizedTableAndLocation;
        }
        String checkPathUrl = configuration.get(CLUSTER_MANAGER_CHECK_PATH_URL);
        String cloudResourceRegion = configuration.get(CLOUD_RESOURCE_REGION);
        if (StringUtils.isEmpty(checkPathUrl) || StringUtils.isEmpty(cloudResourceRegion)) {
            throw new RuntimeException(
                    "when check table path access `clustermanager.checkpath.url` and `clustermanager.cloudresource.region` param is required!");
        }
        LOG.info(
                String.format(
                        "table path check url:%s,  cloud region:%s",
                        checkPathUrl, cloudResourceRegion));

        String requestTablePath =
                tableAndLocation.values().stream().collect(Collectors.joining(","));
        if (StringUtils.isEmpty(requestTablePath)) {
            return unauthorizedTableAndLocation;
        }

        CatalogWebServiceResult response = null;
        Map<String, String> head = new HashMap<>();
        head.put("tenantName", polyCatClient.getProjectId());
        head.put(
                "userName",
                StringUtils.isEmpty(polyCatClient.getUserName())
                        ? "project1"
                        : polyCatClient.getUserName());

        Map<String, String> params = new HashMap<>();
        params.put("path", requestTablePath);
        params.put("cloudResourceRegion", cloudResourceRegion);
        try {
            response = CatalogClientHelper.access(checkPathUrl, head, params, HttpMethodName.POST);
        } catch (IOException e) {
            throw new RuntimeException(String.format("request [%s] error!", checkPathUrl));
        }

        String entity = response.getEntity();
        JSONObject jsonObject = JSONObject.parseObject(entity);
        if (jsonObject.getInteger("code") == 500) {
            throw new RuntimeException(
                    String.format("cluster manager server error, %s", jsonObject.get("message")));
        }
        JSONArray unauthorizedTablePath = jsonObject.getJSONArray("data");
        if (!unauthorizedTablePath.isEmpty()) {
            unauthorizedTableAndLocation =
                    tableAndLocation.entrySet().stream()
                            .filter(kv -> unauthorizedTablePath.contains(kv.getValue()))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }
        return unauthorizedTableAndLocation;
    }

    public GetTableRequest createGetTableRequest(String identifier) {
        String[] split = identifier.split("\\.");
        String databaseName = "default";
        String tableName = "";

        if (split.length == 1) {
            tableName = split[0];
        } else if (split.length > 1) {
            databaseName = split[split.length - 2];
            tableName = split[split.length - 1];
        }
        return new GetTableRequest(
                polyCatClient.getProjectId(), polyCatCatalog, databaseName, tableName);
    }

    private AuthorizationInput createAuthorizationInput(String identifier, Operation operation) {
        String[] split = identifier.split("\\.");
        String databaseName = "default";
        String tableName = "";

        if (split.length == 1) {
            tableName = split[0];
        } else if (split.length > 1) {
            databaseName = split[split.length - 2];
            tableName = split[split.length - 1];
        }

        CatalogInnerObject catalogObject =
                new CatalogInnerObject(
                        polyCatClient.getProjectId(), polyCatCatalog, databaseName, tableName);
        AuthorizationInput authorizationInput = new AuthorizationInput();
        authorizationInput.setAuthorizationType(AuthorizationType.NORMAL_OPERATION);
        authorizationInput.setOperation(operation);
        authorizationInput.setCatalogInnerObject(catalogObject);
        authorizationInput.setGrantObject(null);
        authorizationInput.setUser(polyCatClient.getContext().getUser());
        authorizationInput.setToken(polyCatClient.getContext().getToken());
        return authorizationInput;
    }
}
