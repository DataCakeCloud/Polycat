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
import io.polycat.catalog.common.plugin.request.input.AuthorizationInput;
import io.polycat.probe.model.SqlInfo;
import io.polycat.probe.parser.ParserInterface;

import java.util.List;
import java.util.Map;
import java.util.Set;


import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrinoSqlInfoParser implements ParserInterface {
    private static final Logger LOG = LoggerFactory.getLogger(TrinoSqlInfoParser.class);
    private static final String TRINO_DEFAULT_DB_NAME_CONFIG = "trino.sql.default.dbName";

    private final Configuration configuration;
    private String defaultDbName;

    public TrinoSqlInfoParser(Configuration conf) {
        this.configuration = conf;
        this.defaultDbName =
                StringUtils.isEmpty(configuration.get(TRINO_DEFAULT_DB_NAME_CONFIG))
                        ? "default"
                        : configuration.get(TRINO_DEFAULT_DB_NAME_CONFIG);
    }

    @Override
    public SqlInfo parse(String sqlText, String defaultCatalog) throws Exception {
        SqlInfo sqlInfo = new SqlInfo();
        sqlInfo.setCatalog(defaultCatalog);
        Map<Operation, Set<String>> operationAndTablesName =
                new TrinoSqlParserHelper().parseSqlGetOperationObjects(sqlText);
        // auth
        List<AuthorizationInput> authorizationInputs =
                new TrinoTableAuthorization(configuration, defaultCatalog, defaultDbName)
                        .parseSqlAndCheckAuthorization(operationAndTablesName);
        if (!authorizationInputs.isEmpty()) {
            sqlInfo.setPermit(false);
            sqlInfo.setAuthorizationNowAllowList(authorizationInputs);
        }
        // set tableUsageProfiles
        List<TableUsageProfile> tableUsageProfiles =
                new TrinoUsageProfileExtracter(
                                configuration, defaultCatalog, defaultDbName, sqlText)
                        .extractTableUsageProfile(operationAndTablesName);
        sqlInfo.setTableUsageProfiles(tableUsageProfiles);
        return sqlInfo;
    }
}
