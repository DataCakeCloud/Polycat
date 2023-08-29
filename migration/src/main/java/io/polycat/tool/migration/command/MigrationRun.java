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
import io.polycat.catalog.hms.hive2.CatalogStore;
import io.polycat.tool.migration.common.MigrationStatus;
import io.polycat.tool.migration.common.MigrationType;
import io.polycat.tool.migration.common.Tools;
import io.polycat.tool.migration.entity.Migration;
import io.polycat.tool.migration.entity.MigrationItem;
import io.polycat.tool.migration.service.MigrationItemService;
import io.polycat.tool.migration.service.MigrationSerivce;
import lombok.SneakyThrows;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import picocli.CommandLine;

import java.util.Date;
import java.util.List;

@CommandLine.Command(
        subcommands = {},
        name = "migrate",
        description = "run migration",
        sortOptions = false,
        mixinStandardHelpOptions = true
)
public class MigrationRun implements Runnable {
    private final static Logger logger = Logger.getLogger(MigrationRun.class);

    @CommandLine.Option(
            names = {"--name"},
            description = "migration名称, eg: migration-****.",
            required = true
    )
    private String migrationName;

    @CommandLine.Option(
            names = {"--with-partition"},
            defaultValue = "false",
            description = "是否迁移表的partition"
    )
    private boolean migratePartitions;

    @Override
    public void run() {
        logger.debug("begin to migrate");
        MigrationSerivce migrationService = Tools.getMigrationServiceBean();
        MigrationItemService migrationItemService = Tools.getMigrationItemServiceBean();

        Migration migration = migrationService.getMigrationByName(migrationName);
        logger.info("create catalog {} if not exists in LMS.", migration.getTenantName());
        Tools.createCatalog(migration);

        logger.info("begin to migrate database.");
        migrateDatabase(migration, migrationItemService);
        logger.info("begin to migrate table.");
        migrateTable(migration, migrationItemService);
        logger.debug("end to migrate");
    }

    /**
     * migrate database from HMS to LMS.
     *
     * @param migration
     * @param service
     */
    @SneakyThrows
    private void migrateDatabase(Migration migration, MigrationItemService service) {
        List<MigrationItem> items = service.getUnFinishedMigrationItemByType(migration.getId(), MigrationType.DATABASE);

        CatalogStore catalogStore = Tools.buildCatalogStore(migration);
        IMetaStoreClient hiveClient = Tools.buildHiveClient(migration);

        for (MigrationItem item : items) {
            logger.info("migrate database {}.", item.getItemName());
            item.setStartTime(new Date());
            try {
                Database database = hiveClient.getDatabase(item.getItemName());
                database.getParameters().put("lms_name", migration.getTenantName());
                catalogStore.createDatabase(database);
                item.setExtra("");
                item.setStatus(MigrationStatus.SUCCESS.ordinal());
            } catch (Exception e) {
                item.setExtra("error " + e.getLocalizedMessage());
                item.setStatus(MigrationStatus.FAILED.ordinal());
            }
            item.setEndTime(new Date());
            service.updateMigrationItem(item);
        }
    }

    /**
     * migrate table and partitions from HMS to LMS.
     *
     * @param migration
     * @param service
     */
    @SneakyThrows
    private void migrateTable(Migration migration, MigrationItemService service) {
        List<MigrationItem> items = service.getUnFinishedMigrationItemByType(migration.getId(), MigrationType.TABLE);

        CatalogStore catalogStore = Tools.buildCatalogStore(migration);
        IMetaStoreClient hiveClient = Tools.buildHiveClient(migration);

        for (MigrationItem item : items) {
            logger.info("migrate table {}.", item);
            item.setStartTime(new Date());
            Table table = null;
            try {
                String[] name = item.getItemName().split("\\.");
                table = hiveClient.getTable(name[0], name[1]);
                table.getParameters().put("lms_name", migration.getTenantName());
                catalogStore.createTable(table);
                item.setExtra("");
                item.setStatus(MigrationStatus.SUCCESS.ordinal());
            } catch (Exception e) {
                item.setExtra("error " + e.getLocalizedMessage());
                item.setStatus(MigrationStatus.FAILED.ordinal());
            }
            if (migratePartitions && table != null) {
                item = Tools.migratePartitions(catalogStore, hiveClient, migration, table, item);
            }
            item.setEndTime(new Date());
            service.updateMigrationItem(item);
        }
    }
}
