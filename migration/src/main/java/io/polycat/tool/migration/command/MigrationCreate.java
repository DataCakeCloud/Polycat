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
package io.polycat.tool.migration.command;

import io.polycat.catalog.common.Logger;
import io.polycat.tool.migration.common.MigrationStatus;
import io.polycat.tool.migration.common.Tools;
import io.polycat.tool.migration.entity.Migration;
import io.polycat.tool.migration.service.MigrationSerivce;
import org.apache.commons.lang3.RandomStringUtils;
import picocli.CommandLine;

import java.util.Date;

@CommandLine.Command(
        name = "create",
        description = "create migration",
        sortOptions = false,
        mixinStandardHelpOptions = true
)
public class MigrationCreate implements Runnable {
    private final static Logger logger = Logger.getLogger(MigrationCreate.class);

    @CommandLine.Option(
            names = {"--s_url"},
            description = "源地址信息, eg: thrift://*.*.*.*:9083.",
            required = true
    )
    private String sourceURI;

    @CommandLine.Option(
            names = {"--s_user"},
            description = "源端认证用户名",
            required = false
    )
    private String sourceUser;

    @CommandLine.Option(
            names = {"--s_password"},
            description = "源端认证密码",
            required = false
    )
    private String sourcePassword;

    @CommandLine.Option(
            names = {"--d_url"},
            description = "目的端PolyCat地址信息, eg: 127.0.0.1:8082.",
            required = true
    )
    private String destURI;

    @CommandLine.Option(
            names = {"--d_user"},
            description = "PolyCat端认证用户名",
            required = true
    )
    private String destUser;

    @CommandLine.Option(
            names = {"--d_password"},
            description = "PolyCat端认证密码",
            required = true
    )
    private String destPassword;

    @CommandLine.Option(
            names = {"--project"},
            description = "PolyCat端Project ID",
            required = true
    )
    private String projectId;

    @CommandLine.Option(
            names = {"--tenant"},
            description = "PolyCat端Tenant Name",
            required = true
    )
    private String tenantName;

    @Override
    public void run() {
        logger.debug("begin create migration");
        MigrationSerivce migrationService = Tools.getMigrationServiceBean();

        try {
            // TODO: 加密保存password
            Migration migration = new Migration();
            migration.setName("migration-" + RandomStringUtils.randomAlphabetic(8));
            migration.setSourceUri(sourceURI);
            migration.setSourceUser(sourceUser);
            migration.setSourcePassword(sourcePassword);
            migration.setDestUri(destURI);
            migration.setDestUser(destUser);
            migration.setDestPassword(destPassword);
            migration.setProjectId(projectId);
            migration.setTenantName(tenantName);
            migration.setStatus(MigrationStatus.PREPARED.ordinal());
            migration.setCreateTime(new Date());
            migrationService.insertMigration(migration);
            logger.info("create migration {}", migration.getName());
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }
}