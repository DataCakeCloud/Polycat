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
package io.polycat.probe.hive;

import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.model.TableUsageProfile;
import io.polycat.probe.EventProcessor;
import io.polycat.probe.ProbeConstants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.polycat.catalog.common.Operation.CREATE_DATABASE;
import static io.polycat.catalog.common.Operation.CREATE_TABLE;
import static io.polycat.probe.ProbeConstants.CATALOG_PROBE_TASK_ID;
import static io.polycat.probe.ProbeConstants.CATALOG_PROBE_USER_ID;
import static io.polycat.probe.ProbeConstants.HIVE_AUTHORIZATION_ENABLE_CONFIG;
import static io.polycat.probe.ProbeConstants.HIVE_PUSH_USAGE_PROFILE_ENABLE_CONFIG;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.HiveDriverRunHook;
import org.apache.hadoop.hive.ql.HiveDriverRunHookContext;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Access external services for authentication
 *
 * @author renjianxu
 * @date 2023/9/4
 */
public class HiveTableAuthorizationByDSHook implements HiveDriverRunHook {
    private static final Logger LOG =
            LoggerFactory.getLogger(HiveTableAuthorizationByDSHook.class.getName());

    private static final String POLYCAT_USER_PROJECT_ID = "polycat.user.project";
    private static final String DS_DO_AUTH_URL_CONFIG = "hive.sql.authorization.url";
    private static final ObjectMapper objMapper = new ObjectMapper();

    private String dsDoAuthUrl = "";

    @Override
    public void preDriverRun(HiveDriverRunHookContext hookContext) throws Exception {
        HiveConf hiveConf = (HiveConf) hookContext.getConf();
        if (!(hiveAuthorizationEnable(hiveConf) || hiveUsageProfileEnable(hiveConf))) {
            return;
        }
        String defaultDbName = SessionState.get().getCurrentDatabase();
        String command = hookContext.getCommand();
        String catalog = hiveConf.get(ProbeConstants.HIVE_HMS_BRIDGE_CATALOG_NAME);
        String projectId = hiveConf.get(POLYCAT_USER_PROJECT_ID);
        String userId = parseUserIdFromUGI();
        LOG.info("current db:{}", defaultDbName);
        if (StringUtils.isEmpty(catalog)) {
            throw new RuntimeException("`hive.hmsbridge.defaultCatalogName` is required!");
        }
        if (StringUtils.isEmpty(projectId)) {
            throw new RuntimeException("`polycat.user.project` is required!");
        }
        Map<Operation, Set<String>> operationObjects =
                new HiveSqlParserHelper().parseSqlGetOperationObjects(command);
        // do authentication
        if (hiveAuthorizationEnable(hiveConf)) {
            dsDoAuthUrl = hiveConf.get(DS_DO_AUTH_URL_CONFIG);
            if (StringUtils.isEmpty(dsDoAuthUrl)) {
                throw new RuntimeException("`ds.doAuth.url` is required!");
            }
            // build  AuthenticationReq list by operation type
            List<AuthenticationReq> authenticationReqList = new ArrayList<>();
            for (Map.Entry<Operation, Set<String>> entry : operationObjects.entrySet()) {
                List<AuthenticationReq> ddlAuthenticationReq =
                        entry.getValue().stream()
                                .map(
                                        identifier ->
                                                createAuthenticationReq(
                                                        identifier,
                                                        entry.getKey(),
                                                        defaultDbName,
                                                        catalog,
                                                        projectId,
                                                        userId))
                                .collect(Collectors.toList());
                authenticationReqList.addAll(ddlAuthenticationReq);
            }
            // authorization and get not allowed list
            List<AuthenticationReq> notAuthorizationInputs =
                    doAuthorizationAndGetNotAllowedList(authenticationReqList);
            if (!notAuthorizationInputs.isEmpty()) {
                String errorMsg =
                        notAuthorizationInputs.stream()
                                .map(
                                        input ->
                                                String.format(
                                                        "Principal user [%s] no permission [%s] for object:[%s] ",
                                                        input.getUserId(),
                                                        input.getOperation(),
                                                        input.getQualifiedName()
                                                                .replace(catalog + ".", "")))
                                .collect(Collectors.joining("\n"));
                throw new RuntimeException(errorMsg);
            }
        }
        // push usage profile
        if (hiveUsageProfileEnable(hiveConf)) {
            Map<String, String> customConfig = getCustomConfig(hiveConf);
            String taskId = customConfig.get(CATALOG_PROBE_TASK_ID);
            String execUserId = customConfig.get(CATALOG_PROBE_USER_ID);

            LOG.info("current db:{}", defaultDbName);
            List<TableUsageProfile> tableUsageProfiles =
                    new HiveUsageProfileExtracter(hiveConf, catalog, defaultDbName, command)
                            .extractTableUsageProfile(operationObjects);
            // replace or create taskId and userId
            tableUsageProfiles.forEach(
                    tableUsageProfile -> {
                        if (StringUtils.isNotEmpty(taskId)) {
                            tableUsageProfile.setTaskId(taskId);
                        }
                        if (StringUtils.isNotEmpty(execUserId)) {
                            tableUsageProfile.setUserId(execUserId);
                        }
                    });
            EventProcessor eventProcessor = new EventProcessor(hiveConf);
            eventProcessor.pushTableUsageProfile(tableUsageProfiles);
        }
    }

    private Map<String, String> getCustomConfig(HiveConf conf) {
        Map<String, String> customConfig = new HashMap<>();
        String paraString = HiveConf.getVar(conf, HiveConf.ConfVars.NEWTABLEDEFAULTPARA);
        if (paraString != null && !paraString.isEmpty()) {
            for (String keyValuePair : paraString.split(",")) {
                String[] keyValue = keyValuePair.split("=", 2);
                if (keyValue.length != 2) {
                    continue;
                }
                if (!customConfig.containsKey(keyValue[0])) {
                    customConfig.put(keyValue[0], keyValue[1]);
                }
            }
        }
        return customConfig;
    }

    private String parseUserIdFromUGI() throws IOException {
        String userName = UserGroupInformation.getCurrentUser().getUserName();
        LOG.info("current ugi userName:{}", userName);
        //    hive/ip-xx@EXAMPLE.COM or hive@EXAMPLE.COM
        String primaryAndInstance = StringUtils.substringBefore(userName, "@");
        String primary = StringUtils.substringBefore(primaryAndInstance, "/");
        LOG.info("parsed userId from ugi:{}", primary);
        return primary;
    }

    private List<AuthenticationReq> doAuthorizationAndGetNotAllowedList(
            List<AuthenticationReq> authenticationReqList) throws JsonProcessingException {
        List<AuthenticationReq> authorizationNotAllowedList = new ArrayList<>();
        if (!authenticationReqList.isEmpty()) {
            for (AuthenticationReq authenticationReq : authenticationReqList) {
                LOG.info(String.format("authorizationReq info:%s\n", authenticationReq));
                if (!doPost(dsDoAuthUrl, objMapper.writeValueAsString(authenticationReq))) {
                    authorizationNotAllowedList.add(authenticationReq);
                }
            }
        }
        return authorizationNotAllowedList;
    }

    private AuthenticationReq createAuthenticationReq(
            String identifier,
            Operation operation,
            String defaultDbName,
            String catalog,
            String projectId,
            String userId) {
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
        // database operation, build catalog.dbname
        if (isDatabaseOperation(operation)) {
            databaseName = objectName;
            objectName = "";
        }
        // create database operation object is catalog, only use catalog
        if (operation.name().equals(CREATE_DATABASE.name())) {
            objectName = "";
            databaseName = "";
        }
        // create table operation object is database
        if (operation.name().equals(CREATE_TABLE.name())) {
            objectName = "";
        }
        AuthenticationReq authenticationReq = new AuthenticationReq();
        authenticationReq.setProjectId(projectId);
        authenticationReq.setCatalogName(catalog);
        authenticationReq.setOperation(operation.name());
        authenticationReq.setUserId(userId);
        authenticationReq.setRegion(catalog);
        authenticationReq.setQualifiedName(buildQualifiedName(catalog, databaseName, objectName));
        return authenticationReq;
    }

    private String buildQualifiedName(String catalog, String databaseName, String objectName) {
        String dbName = StringUtils.isEmpty(databaseName) ? "" : "." + databaseName;
        String obName = StringUtils.isEmpty(objectName) ? "" : "." + objectName;
        return String.format("%s%s%s", catalog, dbName, obName);
    }

    private boolean isDatabaseOperation(Operation operation) {
        switch (operation) {
            case ALTER_DATABASE:
            case CREATE_DATABASE:
            case DROP_DATABASE:
                return true;
        }
        return false;
    }

    @Override
    public void postDriverRun(HiveDriverRunHookContext hiveDriverRunHookContext) throws Exception {}

    private boolean hiveAuthorizationEnable(HiveConf hiveConf) {
        return hiveConf.get(HIVE_AUTHORIZATION_ENABLE_CONFIG, "false").equalsIgnoreCase("true");
    }

    private boolean hiveUsageProfileEnable(HiveConf hiveConf) {
        return hiveConf.get(HIVE_PUSH_USAGE_PROFILE_ENABLE_CONFIG, "false")
                .equalsIgnoreCase("true");
    }

    public static boolean doPost(String url, String json) {
        RequestConfig requestConfig =
                RequestConfig.custom()
                        .setSocketTimeout(100000)
                        .setConnectTimeout(100000)
                        .setConnectionRequestTimeout(100000)
                        .build();
        CloseableHttpClient closeableHttpClient = HttpClients.createDefault();
        ContentType contentType = ContentType.create("text/plain", "UTF-8");
        CloseableHttpResponse httpResponse = null;

        HttpPost httpPost = new HttpPost(url);
        httpPost.setConfig(requestConfig);
        httpPost.setHeader(new BasicHeader("Content-Type", "application/json;charset=utf-8"));
        try {
            httpPost.setEntity(new StringEntity(json, contentType));
            httpResponse = closeableHttpClient.execute(httpPost);
            HttpEntity entity = httpResponse.getEntity();
            int statusCode = httpResponse.getStatusLine().getStatusCode();
            if (statusCode == 200) {
                if (entity != null) {
                    Map<String, Object> map =
                            objMapper.readValue(EntityUtils.toString(entity), Map.class);
                    LOG.info("response result:{}", objMapper.writeValueAsString(map));
                    Object data = map.get("data");
                    if (null != data && StringUtils.equalsIgnoreCase("true", data.toString())) {
                        return true;
                    }
                }
            } else {
                throw new RuntimeException(
                        String.format("request dsDoAuthUrl error, statusCode:%s", statusCode));
            }
        } catch (Exception e) {
            LOG.error("doPost error!", e);
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            try {
                if (httpResponse != null) {
                    httpResponse.close();
                }
                if (closeableHttpClient != null) {
                    closeableHttpClient.close();
                }
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }
        }
        return false;
    }

    static class AuthenticationReq {
        private String projectId;
        private String catalogName;
        private String operation;
        private String qualifiedName;
        private String userId;
        private String region;

        public String getProjectId() {
            return projectId;
        }

        public void setProjectId(String projectId) {
            this.projectId = projectId;
        }

        public String getCatalogName() {
            return catalogName;
        }

        public void setCatalogName(String catalogName) {
            this.catalogName = catalogName;
        }

        public String getOperation() {
            return operation;
        }

        public void setOperation(String operation) {
            this.operation = operation;
        }

        public String getQualifiedName() {
            return qualifiedName;
        }

        public void setQualifiedName(String qualifiedName) {
            this.qualifiedName = qualifiedName;
        }

        public String getUserId() {
            return userId;
        }

        public void setUserId(String userId) {
            this.userId = userId;
        }

        public String getRegion() {
            return region;
        }

        public void setRegion(String region) {
            this.region = region;
        }

        @Override
        public String toString() {
            return "AuthenticationReq{"
                    + "projectId='"
                    + projectId
                    + '\''
                    + ", catalogName='"
                    + catalogName
                    + '\''
                    + ", operation='"
                    + operation
                    + '\''
                    + ", qualifiedName='"
                    + qualifiedName
                    + '\''
                    + ", userId='"
                    + userId
                    + '\''
                    + ", region='"
                    + region
                    + '\''
                    + '}';
        }
    }
}
