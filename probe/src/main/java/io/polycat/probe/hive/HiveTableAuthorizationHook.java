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
import io.polycat.catalog.common.plugin.request.input.AuthorizationInput;
import io.polycat.probe.EventProcessor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.polycat.probe.ProbeConstants.CATALOG_PROBE_TASK_ID;
import static io.polycat.probe.ProbeConstants.CATALOG_PROBE_USER_GROUP;
import static io.polycat.probe.ProbeConstants.HIVE_AUTHORIZATION_ENABLE_CONFIG;
import static io.polycat.probe.ProbeConstants.HIVE_DEFAULT_DB_NAME_CONFIG;
import static io.polycat.probe.ProbeConstants.HIVE_PUSH_USAGE_PROFILE_ENABLE_CONFIG;
import static io.polycat.probe.ProbeConstants.POLYCAT_CATALOG;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.HiveDriverRunHook;
import org.apache.hadoop.hive.ql.HiveDriverRunHookContext;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * @author renjianxu
 * @date 2023/9/4
 */
public class HiveTableAuthorizationHook implements HiveDriverRunHook {
    @Override
    public void preDriverRun(HiveDriverRunHookContext hookContext) throws Exception {
        HiveConf hiveConf = (HiveConf) hookContext.getConf();
        if (!(hiveAuthorizationEnable(hiveConf) || hiveUsageProfileEnable(hiveConf))) {
            return;
        }
        SessionState ss = new SessionState(hiveConf);
        String command = hookContext.getCommand();
        String catalog = hiveConf.get(POLYCAT_CATALOG);
        String defaultDbName = hiveConf.get(HIVE_DEFAULT_DB_NAME_CONFIG, ss.getCurrentDatabase());
        if (StringUtils.isEmpty(catalog)) {
            throw new RuntimeException("`polycat.catalog.name` is required!");
        }
        Map<Operation, Set<String>> operationObjects =
                new HiveSqlParserHelper().parseSqlGetOperationObjects(command);
        // auth
        if (hiveAuthorizationEnable(hiveConf)) {
            List<AuthorizationInput> notAuthorizationInputs =
                    new HiveTableAuthorization(hiveConf, catalog, defaultDbName)
                            .checkAuthorization(operationObjects);
            if (!notAuthorizationInputs.isEmpty()) {
                String errorMsg =
                        notAuthorizationInputs.stream()
                                .map(
                                        input ->
                                                String.format(
                                                        "user [%s] no permission [%s] for object:[%s.%s] ",
                                                        input.getUser(),
                                                        input.getOperation().getPrintName(),
                                                        input.getCatalogInnerObject()
                                                                .getDatabaseName(),
                                                        input.getCatalogInnerObject()
                                                                .getObjectName()))
                                .collect(Collectors.joining("\n"));
                throw new RuntimeException(errorMsg);
            }
        }
        // push usage profile
        if (hiveUsageProfileEnable(hiveConf)) {
            Map<String, String> customConfig = getCustomConfig(hiveConf);
            String taskId = customConfig.get(CATALOG_PROBE_TASK_ID);
            String userGroup = customConfig.get(CATALOG_PROBE_USER_GROUP);
            List<TableUsageProfile> tableUsageProfiles =
                    new HiveUsageProfileExtracter(hiveConf, catalog, defaultDbName, command)
                            .extractTableUsageProfile(operationObjects);
            // fill taskId and userGroup
            tableUsageProfiles.forEach(
                    tableUsageProfile -> {
                        if (StringUtils.isNotEmpty(taskId)) {
                            tableUsageProfile.setTaskId(taskId);
                        }
                        if (StringUtils.isNotEmpty(userGroup)) {
                            tableUsageProfile.setUserGroup(userGroup);
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

    private boolean hiveAuthorizationEnable(HiveConf hiveConf) {
        return hiveConf.get(HIVE_AUTHORIZATION_ENABLE_CONFIG, "false").equalsIgnoreCase("true");
    }

    private boolean hiveUsageProfileEnable(HiveConf hiveConf) {
        return hiveConf.get(HIVE_PUSH_USAGE_PROFILE_ENABLE_CONFIG, "false")
                .equalsIgnoreCase("true");
    }

    @Override
    public void postDriverRun(HiveDriverRunHookContext hiveDriverRunHookContext) throws Exception {}
}
