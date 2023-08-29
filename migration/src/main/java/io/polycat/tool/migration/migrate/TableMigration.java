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
package io.polycat.tool.migration.migrate;

import io.polycat.catalog.hms.hive2.CatalogStore;
import io.polycat.tool.migration.common.MigrationStatus;
import io.polycat.tool.migration.common.Tools;
import io.polycat.tool.migration.entity.Migration;
import io.polycat.tool.migration.entity.MigrationItem;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class TableMigration extends BaseMigration implements Serializable {

    public TableMigration(Migration migration, List<MigrationItem> items, JavaSparkContext jsc) {
        this.migration = migration;
        this.items = items;
        this.spark = jsc;
    }

    public List<MigrationItem> run() {
        JavaRDD<MigrationItem> rdd = spark.parallelize(items);

        Migration migration = this.migration;

        List<MigrationItem> collect = rdd.mapPartitions(new FlatMapFunction<Iterator<MigrationItem>, MigrationItem>() {
            @Override
            public Iterator<MigrationItem> call(Iterator<MigrationItem> itemIterator) throws Exception {
                CatalogStore catalogStore = Tools.buildCatalogStore(migration);
                IMetaStoreClient hiveClient = Tools.buildHiveClient(migration);

                List<MigrationItem> result = new ArrayList<>();
                while (itemIterator.hasNext()) {
                    MigrationItem item = itemIterator.next();
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
                        item.setExtra("error " + e.getMessage());
                        item.setStatus(MigrationStatus.FAILED.ordinal());
                    }
                    if (table != null) {
                        item = Tools.migratePartitions(catalogStore, hiveClient, migration, table, item);
                    }
                    item.setEndTime(new Date());
                    result.add(item);
                }
                return result.iterator();
            }
        }).collect();

        return collect;
    }
}
