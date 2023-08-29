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
package io.polycat.tool.migration;

import io.polycat.tool.migration.common.MigrationType;
import io.polycat.tool.migration.common.Tools;
import io.polycat.tool.migration.entity.Migration;
import io.polycat.tool.migration.entity.MigrationItem;
import io.polycat.tool.migration.migrate.DatabaseMigration;
import io.polycat.tool.migration.migrate.TableMigration;
import io.polycat.tool.migration.service.MigrationItemService;
import io.polycat.tool.migration.service.MigrationSerivce;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.List;

/**
 * migrate from hms to lms distribute.
 */
public class Migrate {
    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(SparkSession.builder().getOrCreate().sparkContext());

        MigrationSerivce migrationService = Tools.getMigrationServiceBean();
        MigrationItemService migrationItemService = Tools.getMigrationItemServiceBean();

        Migration migration = migrationService.getMigrationByName(args[0]);

        // 1. create catalog if not exists;
        Tools.createCatalog(migration);

        // 2. migrate database
        List<MigrationItem> items = migrationItemService.getMigrationItemByType(migration.getId(), MigrationType.DATABASE);
        List<MigrationItem> result = new DatabaseMigration(migration, items, sc).run();
        for (MigrationItem item: result) {
            migrationItemService.updateMigrationItem(item);
        }

        // 3. migrate table
        items = migrationItemService.getMigrationItemByType(migration.getId(), MigrationType.TABLE);
        result = new TableMigration(migration, items, sc).run();
        for (MigrationItem item: result) {
            migrationItemService.updateMigrationItem(item);
        }
    }
}
