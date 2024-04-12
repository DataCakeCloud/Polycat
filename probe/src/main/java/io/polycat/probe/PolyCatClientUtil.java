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
package io.polycat.probe;

import io.polycat.catalog.client.CatalogUserInformation;
import io.polycat.catalog.client.PolyCatClient;
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.model.AuthorizationType;
import io.polycat.catalog.common.model.CatalogInnerObject;
import io.polycat.catalog.common.model.TableSource;
import io.polycat.catalog.common.model.TableUsageProfile;
import io.polycat.catalog.common.model.User;
import io.polycat.catalog.common.plugin.CatalogContext;
import io.polycat.catalog.common.plugin.request.UsageProfileOpType;
import io.polycat.catalog.common.plugin.request.input.AuthorizationInput;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.polycat.catalog.common.Operation.CREATE_DATABASE;
import static io.polycat.catalog.common.Operation.CREATE_TABLE;
import static io.polycat.probe.ProbeConstants.*;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PolyCatClientUtil {
    private static final Logger LOG = LoggerFactory.getLogger(PolyCatClientUtil.class);

    public static PolyCatClient buildPolyCatClientFromConfig(Configuration conf) {
        PolyCatClient polyCatClient =
                new PolyCatClient(
                        conf.get(POLYCAT_CLIENT_HOST), conf.getInt(POLYCAT_CLIENT_PORT, 80));
        Pair<String, String> projectIdAndUserName = getProjectIdAndUserName(conf);
        String projectId = projectIdAndUserName.getLeft();
        String polyCatUserName = projectIdAndUserName.getRight();
        String token = conf.get(POLYCAT_CLIENT_TOKEN);
        String password = conf.get(POLYCAT_CLIENT_PASSWORD);
        String tenantName = conf.get(POLYCAT_CLIENT_TENANT_NAME);
        CatalogContext catalogContext = null;

        if (StringUtils.isNotEmpty(password)) {
            catalogContext = CatalogUserInformation.doAuth(polyCatUserName, password);
            catalogContext.setProjectId(projectId);
            catalogContext.setTenantName(tenantName);
        } else if (StringUtils.isNotEmpty(token)) {
            catalogContext = new CatalogContext(projectId, polyCatUserName, tenantName, token);
        } else {
            throw new CatalogException(
                    "param [polycat.client.password] or [polycat.client.token] is required!");
        }
        polyCatClient.setContext(catalogContext);
        LOG.info("current polyCat userName:{}", polyCatClient.getUserName());
        return polyCatClient;
    }

    public static Pair<String, String> getProjectIdAndUserName(Configuration conf) {
        String projectId = conf.get(POLYCAT_CLIENT_PROJECT_ID);
        String username = conf.get(POLYCAT_CLIENT_USERNAME);
        if (!StringUtils.isEmpty(username)) {
            return new ImmutablePair<>(projectId, username);
        }
        try {
            final String ugiUserName = UserGroupInformation.getCurrentUser().getUserName();
            final String[] project2User = ugiUserName.split("#");
            if (project2User.length == 2) {
                projectId = project2User[0];
                username = project2User[1];
            } else {
                username = ugiUserName;
            }
        } catch (IOException e) {
            LOG.error("get userName from UGI error!", e);
        }
        return new ImmutablePair<>(projectId, username);
    }

    public static List<TableUsageProfile> createTableUsageProfileByOperationObjects(
            String catalogName,
            String defaultDbName,
            String command,
            Configuration conf,
            Map<Operation, Set<String>> tablesOperation) {
        Pair<String, String> projectIdAndUserName = getProjectIdAndUserName(conf);
        String userName = projectIdAndUserName.getRight();
        String userGroup = conf.get(ProbeConstants.CATALOG_PROBE_USER_GROUP, userName);
        String projectId = projectIdAndUserName.getLeft();
        String taskId = conf.get(ProbeConstants.CATALOG_PROBE_TASK_ID);

        List<TableUsageProfile> tableUsageProfiles = new ArrayList<>();
        for (Map.Entry<Operation, Set<String>> entry : tablesOperation.entrySet()) {
            Operation operation = entry.getKey();
            if (operation == Operation.ALTER_TABLE) {
                // alter table is parent, will be ignore
                continue;
            }
            for (String table : entry.getValue()) {
                String[] split = table.split("\\.");
                String databaseName = defaultDbName;
                String tableName = "";

                if (split.length == 1) {
                    tableName = split[0];
                } else if (split.length > 1) {
                    databaseName = split[split.length - 2];
                    tableName = split[split.length - 1];
                } else {
                    return tableUsageProfiles;
                }
                TableUsageProfile tableUsageProfile = new TableUsageProfile();
                TableSource tableSource = new TableSource();
                tableSource.setTableName(tableName);
                tableSource.setDatabaseName(databaseName);
                tableSource.setCatalogName(catalogName);
                tableSource.setProjectId(projectId);
                tableUsageProfile.setTable(tableSource);
                tableUsageProfile.setSumCount(BigInteger.ONE);
                tableUsageProfile.setStatement(command);
                tableUsageProfile.setUserId(userName);
                tableUsageProfile.setUserGroup(userGroup);
                tableUsageProfile.setTaskId(taskId);

                Calendar calendar = Calendar.getInstance();
                calendar.set(Calendar.HOUR_OF_DAY, 0);
                calendar.set(Calendar.MINUTE, 0);
                calendar.set(Calendar.SECOND, 0);
                calendar.set(Calendar.MILLISECOND, 0);
                tableUsageProfile.setCreateDayTimestamp(calendar.getTimeInMillis());
                tableUsageProfile.setCreateTimestamp(System.currentTimeMillis());

                switch (operation) {
                    case INSERT_TABLE:
                        tableUsageProfile.setOpTypes(
                                Collections.singletonList(UsageProfileOpType.WRITE.name()));
                        break;
                    case SELECT_TABLE:
                        tableUsageProfile.setOpTypes(
                                Collections.singletonList(UsageProfileOpType.READ.name()));
                        break;
                    default:
                        tableUsageProfile.setOriginOpTypes(
                                Collections.singletonList(operation.name()));
                }
                tableUsageProfiles.add(tableUsageProfile);
            }
        }
        return tableUsageProfiles;
    }

    public static List<AuthorizationInput> createAuthorizationInputByOperationObjects(
            String projectId,
            String polyCatCatalog,
            User user,
            String token,
            String defaultDbName,
            Map<Operation, Set<String>> operationObjects) {
        return operationObjects.entrySet().stream()
                .flatMap(
                        entry ->
                                entry.getValue().stream()
                                        .map(
                                                object ->
                                                        createAuthorizationInput(
                                                                projectId,
                                                                polyCatCatalog,
                                                                user,
                                                                token,
                                                                defaultDbName,
                                                                object,
                                                                entry.getKey())))
                .collect(Collectors.toList());
    }

    private static AuthorizationInput createAuthorizationInput(
            String projectId,
            String polyCatCatalog,
            User user,
            String token,
            String defaultDbName,
            String identifier,
            Operation operation) {
        operation = Operation.convertToParentOperation(operation);
        String[] split = identifier.split("\\.");
        String databaseName = defaultDbName;
        String objectName = "";
        if (split.length == 1) {
            objectName = split[0];
        } else if (split.length > 1) {
            databaseName = split[split.length - 2];
            objectName = split[split.length - 1];
        }
        // database operation
        if (isDatabaseOperation(operation)) {
            databaseName = objectName;
        }
        // create database operation object is catalog
        if (operation.name().equals(CREATE_DATABASE.name())) {
            objectName = polyCatCatalog;
        }
        // create table operation object is  database
        if (operation.name().equals(CREATE_TABLE.name())) {
            objectName = databaseName;
        }
        CatalogInnerObject catalogObject =
                new CatalogInnerObject(projectId, polyCatCatalog, databaseName, objectName);
        AuthorizationInput authorizationInput = new AuthorizationInput();
        authorizationInput.setAuthorizationType(AuthorizationType.NORMAL_OPERATION);
        authorizationInput.setOperation(operation);
        authorizationInput.setCatalogInnerObject(catalogObject);
        authorizationInput.setGrantObject(null);
        authorizationInput.setUser(user);
        authorizationInput.setToken(token);
        return authorizationInput;
    }

    private static boolean isDatabaseOperation(Operation operation) {
        switch (operation) {
            case ALTER_DATABASE:
            case CREATE_DATABASE:
            case DROP_DATABASE:
                return true;
        }
        return false;
    }
}
