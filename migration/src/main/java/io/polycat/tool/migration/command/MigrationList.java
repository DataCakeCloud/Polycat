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
import picocli.CommandLine;

import java.util.List;

@CommandLine.Command(
        subcommands = {},
        name = "list",
        description = "list migration",
        sortOptions = false,
        mixinStandardHelpOptions = true
)
public class MigrationList implements Runnable {
    private final static Logger logger = Logger.getLogger(MigrationList.class);

    @Override
    public void run() {
        logger.debug("begin create migration");
        MigrationSerivce migrationService = Tools.getMigrationServiceBean();

        List<Migration> migrations = migrationService.listMigration();
        MigrationStatus[] values = MigrationStatus.values();

        System.out.println("\n迁移列表信息");
        System.out.println("ID   名称    HMS地址   PolyCat地址  状态");

        for (Migration migration: migrations) {
            System.out.println(migration.getId() + " " + migration.getName() + " "
                    + migration.getSourceUri() + " " + migration.getDestUri()
                    + " " + values[migration.getStatus()]);
        }
    }
}
