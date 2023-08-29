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
import io.polycat.probe.EventProcessor;
import io.polycat.probe.UsageProfileExtracter;
import io.polycat.probe.model.CatalogOperationObject;
import io.polycat.probe.spark.parser.SparkPlanParserHelper;

import org.apache.spark.sql.execution.QueryExecution;

import java.math.BigInteger;
import java.util.*;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkUsageProfileExtracter implements UsageProfileExtracter<QueryExecution> {
    private static final Logger LOG = LoggerFactory.getLogger(EventProcessor.class);

    private String catalogName;

    public SparkUsageProfileExtracter(String catalogName) {
        this.catalogName = catalogName;
    }

    @Override
    public List<TableUsageProfile> extractTableUsageProfile(QueryExecution queryExecution) {
        List<TableUsageProfile> tableUsageProfiles = new ArrayList<>();
        SparkPlanParserHelper sparkPlanParserHelper = new SparkPlanParserHelper("", catalogName);
        Set<CatalogOperationObject> catalogOperationObjects =
                sparkPlanParserHelper.parsePlan(queryExecution.analyzed());

        for (CatalogOperationObject catalogOperationObject : catalogOperationObjects) {
            LOG.info(
                    String.format(
                            "extract TableUsageProfile catalogOperationObject:%s \n",
                            catalogOperationObject));
            if (!(catalogOperationObject.getOperation().equals(Operation.INSERT_TABLE)
                    || catalogOperationObject.getOperation().equals(Operation.SELECT_TABLE))) {
                continue;
            }

            TableUsageProfile tableUsageProfile = new TableUsageProfile();
            TableSource tableSource = new TableSource();
            tableSource.setTableName(catalogOperationObject.getCatalogObject().getObjectName());
            tableSource.setDatabaseName(
                    catalogOperationObject.getCatalogObject().getDatabaseName());
            tableSource.setCatalogName(catalogName);
            // todo: fill project id
            tableUsageProfile.setTable(tableSource);
            tableUsageProfile.setSumCount(BigInteger.ONE);

            Calendar calendar = Calendar.getInstance();
            calendar.set(Calendar.HOUR_OF_DAY, 0);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            calendar.set(Calendar.MILLISECOND, 0);
            tableUsageProfile.setCreateDayTimestamp(calendar.getTimeInMillis());
            tableUsageProfile.setCreateTimestamp(System.currentTimeMillis());

            String opType =
                    catalogOperationObject.getOperation() == Operation.INSERT_TABLE
                            ? UsageProfileOpType.WRITE.name()
                            : UsageProfileOpType.READ.name();
            tableUsageProfile.setOpTypes(Arrays.asList(opType));
            tableUsageProfiles.add(tableUsageProfile);
        }
        return tableUsageProfiles;
    }
}
