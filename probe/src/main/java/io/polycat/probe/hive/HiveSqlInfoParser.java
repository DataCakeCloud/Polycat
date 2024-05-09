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

import io.polycat.catalog.client.PolyCatClient;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.model.Database;
import io.polycat.catalog.common.model.Table;
import io.polycat.catalog.common.model.TableUsageProfile;
import io.polycat.catalog.common.plugin.request.AlterDatabaseRequest;
import io.polycat.catalog.common.plugin.request.AlterRoleRequest;
import io.polycat.catalog.common.plugin.request.AlterTableRequest;
import io.polycat.catalog.common.plugin.request.GetDatabaseRequest;
import io.polycat.catalog.common.plugin.request.GetTableRequest;
import io.polycat.catalog.common.plugin.request.input.AlterTableInput;
import io.polycat.catalog.common.plugin.request.input.AuthorizationInput;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;
import io.polycat.catalog.common.plugin.request.input.RoleInput;
import io.polycat.catalog.common.plugin.request.input.TableInput;
import io.polycat.probe.PolyCatClientUtil;
import io.polycat.probe.model.SqlInfo;
import io.polycat.probe.parser.ParserInterface;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static io.polycat.catalog.common.Operation.CREATE_DATABASE;
import static io.polycat.catalog.common.Operation.CREATE_TABLE;

import com.esotericsoftware.minlog.Log;
import org.apache.arrow.util.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;

/**
 * build SqlInfo by parse sql,
 *
 * @author renjianxu
 * @date 2023/7/10
 */
public class HiveSqlInfoParser implements ParserInterface {
    private static final Logger LOG = LoggerFactory.getLogger(HiveSqlInfoParser.class);
    private static final String HIVE_DEFAULT_DB_NAME_CONFIG = "hive.sql.default.dbName";

    private Configuration configuration;
    private String defaultDbName;

    public HiveSqlInfoParser(Configuration configuration) {
        this.configuration = configuration;
        this.defaultDbName =
                StringUtils.isEmpty(configuration.get(HIVE_DEFAULT_DB_NAME_CONFIG))
                        ? "default"
                        : configuration.get(HIVE_DEFAULT_DB_NAME_CONFIG);
    }

    @Override
    public SqlInfo parse(String sqlText, String defaultCatalog) throws Exception {
        SqlInfo sqlInfo = new SqlInfo();
        sqlInfo.setCatalog(defaultCatalog);
        Map<Operation, Set<String>> operationObjects =
                new HiveSqlParserHelper().parseSqlGetOperationObjects(sqlText);
        // authorization
        List<AuthorizationInput> notAuthorizationInputs =
                new HiveTableAuthorization(configuration, defaultCatalog, defaultDbName)
                        .checkAuthorization(operationObjects);
        if (!notAuthorizationInputs.isEmpty()) {
            sqlInfo.setPermit(false);
            sqlInfo.setAuthorizationNowAllowList(notAuthorizationInputs);
        }
        // build usage profile
        List<TableUsageProfile> tableUsageProfiles =
                new HiveUsageProfileExtracter(configuration, defaultCatalog, defaultDbName, sqlText)
                        .extractTableUsageProfile(operationObjects);
        sqlInfo.setTableUsageProfiles(tableUsageProfiles);
        return sqlInfo;
    }

    /**
     * filter out the create table statement from operationObjects and modify the table owner
     *
     * @param operationObjects
     * @param catalog
     * @param owner
     * @param roleName
     */
    public void alterTableOrDatabaseOwner(
            Map<Operation, Set<String>> operationObjects,
            String catalog,
            String owner,
            String roleName) {
        if (Objects.isNull(operationObjects)
                || operationObjects.isEmpty()
                || !(operationObjects.keySet().contains(CREATE_TABLE)
                        || operationObjects.keySet().contains(CREATE_DATABASE))) {
            LOG.info("operationObjects no create table or create database!");
            return;
        }
        PolyCatClient polyCatClient = PolyCatClientUtil.buildPolyCatClientFromConfig(configuration);
        Set<String> createTables = operationObjects.getOrDefault(CREATE_TABLE, new HashSet<>());
        Set<String> createDatabases =
                operationObjects.getOrDefault(CREATE_DATABASE, new HashSet<>());
        // alter db owner
        for (String dbName : createDatabases) {
            LOG.info(
                    String.format(
                            "alter database owner catalog:%s, database:%s, owner:%s, roleName:%s",
                            catalog, dbName, owner, roleName));
            Preconditions.checkArgument(
                    StringUtils.isNoneBlank(roleName),
                    "alter database privilege role is required!");
            try {
                Database database =
                        polyCatClient.getDatabase(
                                new GetDatabaseRequest(
                                        polyCatClient.getProjectId(), catalog, dbName));
                DatabaseInput databaseInput = new DatabaseInput();
                BeanUtils.copyProperties(database, databaseInput);
                databaseInput.setOwner(owner);
                polyCatClient.alterDatabase(
                        new AlterDatabaseRequest(
                                polyCatClient.getProjectId(), catalog, dbName, databaseInput));
            } catch (Exception e) {
                Log.error("hive alter database owner error!", e);
            }
            //             grant privilege to role
            try {
                AlterRoleRequest alterRoleRequest = new AlterRoleRequest();
                RoleInput roleInput = new RoleInput();
                roleInput.setObjectType(ObjectType.DATABASE.name());
                roleInput.setObjectName(catalog + "." + dbName);
                roleInput.setRoleName(roleName);
                roleInput.setOwnerUser(owner);

                alterRoleRequest.setProjectId(polyCatClient.getProjectId());
                alterRoleRequest.setInput(roleInput);
                polyCatClient.grantAllObjectPrivilegeToRole(alterRoleRequest);
            } catch (Exception e) {
                Log.error("hive grant privilege to role error!", e);
            }
        }
        // alter table owner
        for (String table : createTables) {
            LOG.info(
                    String.format(
                            "alter table owner catalog:%s, tableName:%s, owner:%s",
                            catalog, table, owner));
            String[] split = table.split("\\.");
            String databaseName = defaultDbName;
            String tableName = "";
            if (split.length == 1) {
                tableName = split[0];
            } else if (split.length > 1) {
                databaseName = split[split.length - 2];
                tableName = split[split.length - 1];
            }
            try {
                Table tableInfo =
                        polyCatClient.getTable(
                                new GetTableRequest(
                                        polyCatClient.getProjectId(),
                                        catalog,
                                        databaseName,
                                        tableName));
                AlterTableRequest request = new AlterTableRequest();
                request.setCatalogName(tableInfo.getCatalogName());
                request.setDatabaseName(tableInfo.getDatabaseName());
                request.setTableName(tableInfo.getTableName());
                request.setProjectId(polyCatClient.getProjectId());

                TableInput tableInput = new TableInput();
                BeanUtils.copyProperties(tableInfo, tableInput);
                tableInput.setOwner(owner);
                request.setInput(new AlterTableInput(tableInput, null));
                polyCatClient.alterTable(request);
            } catch (Exception e) {
                LOG.error("hive alter table owner error! ", e);
            }
        }
    }
}
