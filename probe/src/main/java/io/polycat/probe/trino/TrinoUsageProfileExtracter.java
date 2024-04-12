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

import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.model.TableUsageProfile;
import io.polycat.probe.PolyCatClientUtil;
import io.polycat.probe.UsageProfileExtracter;

import java.util.List;
import java.util.Map;
import java.util.Set;


import org.apache.hadoop.conf.Configuration;

public class TrinoUsageProfileExtracter
        implements UsageProfileExtracter<Map<Operation, Set<String>>> {
    private String catalogName;
    private String defaultDbName;
    private Configuration conf;
    private String command;

    public TrinoUsageProfileExtracter(String catalogName, String defaultDbName) {
        this.catalogName = catalogName;
        this.defaultDbName = defaultDbName;
    }

    public TrinoUsageProfileExtracter(
            Configuration conf, String catalogName, String defaultDbName, String command) {
        this.conf = conf;
        this.catalogName = catalogName;
        this.defaultDbName = defaultDbName;
        this.command = command;
    }

    @Override
    public List<TableUsageProfile> extractTableUsageProfile(
            Map<Operation, Set<String>> operationAndTablesName) {
        return PolyCatClientUtil.createTableUsageProfileByOperationObjects(
                catalogName, defaultDbName, command, conf, operationAndTablesName);
    }
}
