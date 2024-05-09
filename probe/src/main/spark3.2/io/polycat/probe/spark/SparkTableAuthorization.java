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
package io.polycat.probe.spark;

import io.polycat.catalog.client.PolyCatClient;
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.model.AuthorizationResponse;
import io.polycat.catalog.common.model.AuthorizationType;
import io.polycat.catalog.common.model.CatalogInnerObject;
import io.polycat.catalog.common.plugin.request.AuthenticationRequest;
import io.polycat.catalog.common.plugin.request.input.AuthorizationInput;
import io.polycat.probe.PolyCatClientUtil;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.SparkSqlParser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;


import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Perform polyCat authentication on the spark SQL table name
 *
 * @author renjianxu
 * @date 2023/3/28
 */
public class SparkTableAuthorization {
    private static final Logger LOG = LoggerFactory.getLogger(SparkTableAuthorization.class);

    private PolyCatClient polyCatClient;
    private String polyCatCatalog;
    private String defaultDbName;

    public SparkTableAuthorization(
            Configuration conf, String polyCatCatalog, String defaultDbName) {
        this.polyCatClient = PolyCatClientUtil.buildPolyCatClientFromConfig(conf);
        this.polyCatCatalog = polyCatCatalog;
        this.defaultDbName = defaultDbName;
    }

    public List<AuthorizationInput> parseSqlAndCheckAuthorization(String sqlText)
            throws CatalogException {
        List<AuthorizationInput> authorizationInputs = new ArrayList<>();
        List<AuthorizationInput> authorizationNotAllowedList = new ArrayList<>();
        // key: tableType,  value: tableName
        Map<String, Set<String>> tables = Maps.newHashMap();
        LogicalPlan plan = null;
        try {
            plan = new SparkSqlParser().parsePlan(sqlText);
            SparkSqlParserHelper.visitedLogicalPlan(tables, plan, plan);
        } catch (Exception e) {
            LOG.error("", e);
            throw e;
        }
        // remove temp table
        SparkSqlParserHelper.getRealIOTableMap(tables);
        Set<String> selectTable =
                tables.getOrDefault(SparkSqlParserHelper.INPUT_TABLE, new HashSet<>());
        Set<String> insertTable =
                tables.getOrDefault(SparkSqlParserHelper.OUTPUT_TABLE, new HashSet<>());
        LOG.info(
                String.format(
                        "parsed spark sql selectTable:%s, insertTable:%s",
                        String.join(",", selectTable), String.join(",", insertTable)));
        // create AuthorizationInput
        authorizationInputs.addAll(
                insertTable.stream()
                        .map(table -> createAuthorizationInput(table, Operation.INSERT_TABLE))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()));
        authorizationInputs.addAll(
                selectTable.stream()
                        .map(table -> createAuthorizationInput(table, Operation.SELECT_TABLE))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()));
        // permission check
        if (!authorizationInputs.isEmpty()) {
            for (AuthorizationInput authorization : authorizationInputs) {
                LOG.info(String.format("authorization info:%s\n", authorization));
                try {
                    AuthorizationResponse response =
                            polyCatClient.authenticate(
                                    new AuthenticationRequest(
                                            polyCatClient.getProjectId(),
                                            Arrays.asList(authorization)));
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
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return authorizationNotAllowedList;
    }

    private AuthorizationInput createAuthorizationInput(String identifier, Operation operation) {
        String[] split = identifier.split("\\.");
        String databaseName = defaultDbName;
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
