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
package io.polycat.tool.migration.common;

import com.alibaba.fastjson.JSONObject;
import io.polycat.catalog.client.CatalogUserInformation;
import io.polycat.catalog.common.PolyCatConf;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.hms.hive2.CatalogStore;
import io.polycat.tool.migration.entity.Migration;
import io.polycat.tool.migration.entity.MigrationItem;
import io.polycat.tool.migration.service.MigrationItemService;
import io.polycat.tool.migration.service.MigrationSerivce;
import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.List;

public class Tools {
    private static final Logger logger = Logger.getLogger(Tools.class);

    public static IMetaStoreClient buildHiveClient(Migration migration) throws MetaException {
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.uris", migration.getSourceUri());
        IMetaStoreClient hiveClient = RetryingMetaStoreClient.getProxy(hiveConf, true);
        return hiveClient;
    }

    public static CatalogStore buildCatalogStore(Migration migration) {
        String[] uri = migration.getDestUri().split(":");

        Configuration conf = new Configuration();
        conf.set(CatalogUserInformation.POLYCAT_USER_NAME, migration.getDestUser());
        conf.set(CatalogUserInformation.POLYCAT_USER_PASSWORD, migration.getDestPassword());
        conf.set(CatalogUserInformation.POLYCAT_USER_PROJECT, migration.getProjectId());
        conf.set(CatalogUserInformation.POLYCAT_USER_TENANT, migration.getTenantName());
        conf.set(PolyCatConf.CATALOG_HOST, uri[0]);
        conf.set(PolyCatConf.CATALOG_PORT, uri[1]);
        CatalogStore catalogStore = new CatalogStore(conf);
        return catalogStore;
    }

    public static MigrationSerivce getMigrationServiceBean() {
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("classpath:spring.xml");
        MigrationSerivce migrationService = (MigrationSerivce) context.getBean("migrationService");
        return migrationService;
    }

    public static MigrationItemService getMigrationItemServiceBean() {
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("classpath:spring.xml");
        MigrationItemService migrationItemService = (MigrationItemService) context.getBean("migrationItemService");

        return migrationItemService;
    }

    /**
     * Use tenant name to create catalog if not exists.
     *
     * @param migration
     */
    @SneakyThrows
    public static void createCatalog(Migration migration) {
        logger.debug("begin create catalog {} ", migration.getTenantName());
        CatalogStore catalogStore = buildCatalogStore(migration);

        List<String> catalogs = catalogStore.getCatalogs();

        if (catalogs.contains(migration.getTenantName())) {
            logger.info("catalog {} already exists, no need to create.", migration.getTenantName());
            return;
        }

        try {
            catalogStore.createCatalog(migration.getTenantName());
        } catch (Exception e) {
            logger.error("create catalog failed {}", e.getMessage());
            throw new Exception(String.format("Create catalog {} failed, Skip migrate.", migration.getTenantName()));
        }
        logger.debug("end create catalog {} ", migration.getTenantName());
    }

    private static List<String> syncPartitions(CatalogStore store, IMetaStoreClient client,
                                                 String catalogName, Table table) throws TException {
        List<String> partitionNames = client.listPartitionNames(table.getDbName(), table.getTableName(), (short) -1);

        // 分批加载partition, 避免hive OOM.
        for (int i = 0; i < partitionNames.size(); i += Constants.NUM_OF_GROUPS) {
            int end = i + Constants.NUM_OF_GROUPS;
            if (end > partitionNames.size()) {
                end = partitionNames.size();
            }
            List<Partition> partitions = client.getPartitionsByNames(table.getDbName(),
                    table.getTableName(), partitionNames.subList(i, end));
            store.addPartitions(catalogName, table.getDbName(), table.getTableName(), table, partitions);
        }
        return partitionNames;
    }

    public static MigrationItem migratePartitions(CatalogStore catalogStore, IMetaStoreClient hiveClient,
                                                  Migration migration, Table table, MigrationItem item) {
        logger.debug("begin to migrate partitions for table {}.", table.getTableName());
        try {
            List<String> partitions = syncPartitions(catalogStore, hiveClient,
                    migration.getTenantName(), table);
            JSONObject object;
            if (item.getProperties() != null) {
                object = JSONObject.parseObject(item.getProperties());
            } else {
                object = new JSONObject();
            }
            object.put("partition_num", partitions.size());
            item.setProperties(object.toString());
        } catch (Exception e) {
            logger.error("migrate partition for table {} failed: {}", table.getTableName(), e.getMessage());
            item.setExtra("error:" + e.getMessage());
        }
        return item;
    }
}
