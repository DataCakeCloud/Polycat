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

import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.model.TableSource;
import io.polycat.catalog.common.model.TableUsageProfile;
import io.polycat.catalog.common.plugin.request.UsageProfileOpType;
import io.polycat.probe.PolyCatClientUtil;
import io.polycat.probe.ProbeConstants;
import io.polycat.probe.UsageProfileExtracter;
import io.polycat.probe.model.CatalogOperationObject;
import io.polycat.probe.spark.parser.SparkPlanParserHelper;

import org.apache.spark.sql.execution.QueryExecution;

import java.math.BigInteger;
import java.util.*;


import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;

public class SparkUsageProfileExtracter implements UsageProfileExtracter<QueryExecution> {
    private Configuration conf;
    private String command;
    private String catalogName;

    public SparkUsageProfileExtracter(Configuration conf, String command) {
        this.conf = conf;
        this.command = command;
        this.catalogName = conf.get(ProbeConstants.POLYCAT_CATALOG);
    }

    @Override
    public List<TableUsageProfile> extractTableUsageProfile(QueryExecution queryExecution) {
        List<TableUsageProfile> tableUsageProfiles = new ArrayList<>();
        Pair<String, String> projectIdAndUserName = PolyCatClientUtil.getProjectIdAndUserName(conf);
        String userName = projectIdAndUserName.getRight();
        String projectId = projectIdAndUserName.getLeft();
        String taskId = conf.get(ProbeConstants.CATALOG_PROBE_TASK_ID, "");
        String userGroup = conf.get(ProbeConstants.CATALOG_PROBE_USER_GROUP, userName);
        Set<CatalogOperationObject> catalogOperationObjects =
                new SparkPlanParserHelper(projectId, catalogName)
                        .parsePlan(queryExecution.analyzed());

        for (CatalogOperationObject catalogOperationObject : catalogOperationObjects) {
            Operation operation = catalogOperationObject.getOperation();

            TableUsageProfile tableUsageProfile = new TableUsageProfile();
            TableSource tableSource = new TableSource();
            tableSource.setTableName(catalogOperationObject.getCatalogObject().getObjectName());
            tableSource.setDatabaseName(
                    catalogOperationObject.getCatalogObject().getDatabaseName());
            tableSource.setCatalogName(catalogName);
            tableSource.setProjectId(projectId);
            tableUsageProfile.setTable(tableSource);
            tableUsageProfile.setSumCount(BigInteger.ONE);
            tableUsageProfile.setTaskId(taskId);
            tableUsageProfile.setUserGroup(userGroup);
            tableUsageProfile.setUserId(userName);
            tableUsageProfile.setStatement(command);

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
                    tableUsageProfile.setOriginOpTypes(Collections.singletonList(operation.name()));
            }
            tableUsageProfiles.add(tableUsageProfile);
        }
        return tableUsageProfiles;
    }
}
