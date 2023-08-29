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
import io.polycat.tool.migration.common.MigrationType;
import io.polycat.tool.migration.common.Tools;
import io.polycat.tool.migration.entity.Migration;
import io.polycat.tool.migration.entity.MigrationItem;
import io.polycat.tool.migration.service.MigrationItemService;
import io.polycat.tool.migration.service.MigrationSerivce;
import lombok.SneakyThrows;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import picocli.CommandLine;

import java.util.List;

@CommandLine.Command(
        subcommands = {},
        name = "prepare",
        description = "prepare for migration",
        sortOptions = false,
        mixinStandardHelpOptions = true
)
public class MigrationPrepare implements Runnable {
    private final static Logger logger = Logger.getLogger(MigrationPrepare.class);

    @CommandLine.Option(
            names = {"--name"},
            description = "migration名称, eg: migration-****.",
            required = true
    )
    private String migrationName;


    @Override
    public void run() {
        logger.debug("begin prepare migration {}", migrationName);
        MigrationSerivce migrationService = Tools.getMigrationServiceBean();
        MigrationItemService migrationItemService = Tools.getMigrationItemServiceBean();

        Migration migration = migrationService.getMigrationByName(migrationName);

        if (migration == null) {
            logger.error("migration {} not exists", migration.getName());
            return;
        }

        if (migration.getStatus() != MigrationStatus.INITIALIZATION.ordinal()) {
            logger.warn("Incremental synchronization is not supported");
            return;
        }

        migration = prepare(migration, migrationItemService);

        migration.setStatus(MigrationStatus.PREPARED.ordinal());
        migrationService.updateMigrationById(migration);
    }

    @SneakyThrows
    private Migration prepare(Migration migration, MigrationItemService migrationItemService) {
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.uris", migration.getSourceUri());
        IMetaStoreClient hiveClient = RetryingMetaStoreClient.getProxy(hiveConf, true);

        List<String> dbs = hiveClient.getAllDatabases();

        for (String db : dbs) {
            MigrationItem item = new MigrationItem();
            item.setStatus(MigrationStatus.INITIALIZATION.ordinal());
            item.setItemName(db);
            item.setMigrationId(migration.getId());
            item.setType(MigrationType.DATABASE.ordinal());
            migrationItemService.insertMigrationItem(item);

            List<String> tables = hiveClient.getTables(db, ".*", TableType.MANAGED_TABLE);
            tables.addAll(hiveClient.getTables(db, ".*", TableType.EXTERNAL_TABLE));

            for (String table : tables) {
                item = new MigrationItem();
                item.setStatus(MigrationStatus.INITIALIZATION.ordinal());
                item.setItemName(db + "." + table);
                item.setMigrationId(migration.getId());
                item.setType(MigrationType.TABLE.ordinal());
                migrationItemService.insertMigrationItem(item);
            }
        }

        return migration;
    }
}
