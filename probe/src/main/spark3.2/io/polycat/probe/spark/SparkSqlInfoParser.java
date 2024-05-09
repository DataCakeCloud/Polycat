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

import io.polycat.catalog.common.plugin.request.input.AuthorizationInput;
import io.polycat.probe.model.SqlInfo;
import io.polycat.probe.parser.ParserInterface;

import org.apache.spark.sql.catalyst.parser.ParseException;

import java.util.List;


import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

public class SparkSqlInfoParser implements ParserInterface {
    private static final String SPARK_DEFAULT_DB_NAME_CONFIG = "spark.sql.default.dbName";

    private Configuration configuration;
    private String defaultDbName;

    public SparkSqlInfoParser(Configuration configuration) {
        this.configuration = configuration;
        this.defaultDbName =
                StringUtils.isEmpty(configuration.get(SPARK_DEFAULT_DB_NAME_CONFIG))
                        ? "default"
                        : configuration.get(SPARK_DEFAULT_DB_NAME_CONFIG);
    }

    @Override
    public SqlInfo parse(String sqlText, String defaultCatalog) throws ParseException {
        SqlInfo sqlInfo = new SqlInfo();
        sqlInfo.setCatalog(defaultCatalog);
        // authorization
        SparkTableAuthorization sparkTableAuthorization =
                new SparkTableAuthorization(configuration, defaultCatalog, defaultDbName);
        List<AuthorizationInput> authorizationInputs =
                sparkTableAuthorization.parseSqlAndCheckAuthorization(sqlText);
        if (!authorizationInputs.isEmpty()) {
            sqlInfo.setPermit(false);
            sqlInfo.setAuthorizationNowAllowList(authorizationInputs);
        }
        return sqlInfo;
    }
}
