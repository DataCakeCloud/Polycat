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

import io.polycat.catalog.common.model.TableSource;
import io.polycat.catalog.common.model.TableUsageProfile;
import io.polycat.catalog.common.plugin.request.UsageProfileOpType;
import io.polycat.probe.UsageProfileExtracter;

import io.trino.sql.tree.Statement;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Sets;

public class TrinoUsageProfileExtracter implements UsageProfileExtracter<Statement> {
    protected final String DEFAULT_DATABASE_NAME = "default";
    protected final String catalogName;

    public TrinoUsageProfileExtracter(String catalogName) {
        this.catalogName = catalogName;
    }

    @Override
    public List<TableUsageProfile> extractTableUsageProfile(Statement statement) {
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

        return buildTableUsageProfilesByTableName(queryTable, targetTable);
    }

    public List<TableUsageProfile> buildTableUsageProfilesByTableName(
            Set<String> queryTable, Set<String> targetTable) {
        List<TableUsageProfile> tableUsageProfiles = Lists.newArrayList();
        List<String> mergeTable = Lists.newArrayList();
        mergeTable.addAll(queryTable);
        mergeTable.addAll(targetTable);

        for (String table : mergeTable) {
            String[] split = table.split("\\.");
            String databaseName = DEFAULT_DATABASE_NAME;
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
                    targetTable.contains(table)
                            ? UsageProfileOpType.WRITE.name()
                            : UsageProfileOpType.READ.name();
            tableUsageProfile.setOpTypes(Arrays.asList(opType));
            tableUsageProfiles.add(tableUsageProfile);
        }
        return tableUsageProfiles;
    }
}
