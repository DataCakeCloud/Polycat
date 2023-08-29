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

import io.polycat.catalog.common.model.TableUsageProfile;
import io.polycat.catalog.common.plugin.request.input.AuthorizationInput;
import io.polycat.probe.UsageProfileExtracter;
import io.polycat.probe.model.SqlInfo;
import io.polycat.probe.parser.ParserInterface;

import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Statement;

import java.util.List;
import java.util.Map;

import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrinoSqlInfoParser implements ParserInterface {
    private static final Logger LOG = LoggerFactory.getLogger(TrinoSqlInfoParser.class);

    private final Configuration configuration;

    public TrinoSqlInfoParser(Configuration conf) {
        this.configuration = conf;
    }

    @Override
    public SqlInfo parse(String sqlText, String defaultCatalog) throws Exception {
        SqlParser sqlParser = new SqlParser();
        SqlInfo sqlInfo = new SqlInfo();
        sqlInfo.setCatalog(defaultCatalog);
        Statement statement = null;
        try {
            statement = sqlParser.createStatement(sqlText, new ParsingOptions(AS_DECIMAL));
        } catch (Exception e) {
            LOG.error(
                    "the trino sql syntax verification failed, please check the sql statement!", e);
            throw e;
        }
        TrinoTableAuthorization trinoTableAuthorization =
                new TrinoTableAuthorization(configuration, defaultCatalog);
        List<AuthorizationInput> authorizationInputs =
                trinoTableAuthorization.parseSqlAndCheckAuthorization(statement);
        if (!authorizationInputs.isEmpty()) {
            sqlInfo.setPermit(false);
            sqlInfo.setAuthorizationNowAllowList(authorizationInputs);
        }

        UsageProfileExtracter<Statement> trinoUsageProfileExtracter =
                new TrinoUsageProfileExtracter(defaultCatalog);
        List<TableUsageProfile> tableUsageProfiles =
                trinoUsageProfileExtracter.extractTableUsageProfile(statement);
        sqlInfo.setTableUsageProfiles(tableUsageProfiles);

        return sqlInfo;
    }

    @Deprecated
    public Map<String, String> parseSqlAndCheckLocationPermission(
            String sqlText, String defaultCatalog) {
        SqlParser sqlParser = new SqlParser();
        Statement statement = sqlParser.createStatement(sqlText, new ParsingOptions(AS_DECIMAL));
        TrinoTableAuthorization trinoTableAuthorization =
                new TrinoTableAuthorization(configuration, defaultCatalog);
        Map<String, String> unauthorizedTable =
                trinoTableAuthorization.parseSqlAndCheckLocationPermission(statement);
        return unauthorizedTable;
    }
}
