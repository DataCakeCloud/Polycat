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
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.subspace.Subspace;

import io.polycat.catalog.client.PolyCatClient;
import io.polycat.catalog.hms.hive2.HMSBridgeStore;
import io.polycat.catalog.server.CatalogApplication;
import io.polycat.catalog.store.api.StoreBase;
import io.polycat.catalog.store.fdb.record.RecordStoreHelper;

import io.polycat.catalog.store.fdb.record.impl.StoreImpl;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class HMSBridgeReadonlyTest {

    private static ConfigurableApplicationContext catalogServer;
    public static PolyCatClient catalogClient;
    public static HMSBridgeStore bridgeStore;
    public static Configuration configuration;
    public static String warehouse;
    public static String targetPath;
    public static StoreBase storeBase;

    @BeforeAll
    public static void setup() throws Exception {
        storeBase = new StoreImpl();
        catalogServer = SpringApplication.run(CatalogApplication.class);
        targetPath = new File("/tmp/hive2bridge/readonly").getCanonicalPath();
        warehouse = targetPath + "/warehouse";
        bridgeStore = new HMSBridgeStore();
        configuration = new HiveConf();
        configuration.set("javax.jdo.option.ConnectionURL",
            "jdbc:derby:;databaseName=" + targetPath + "/metastore_db;create=true");
        configuration.set("hive.hmsbridge.readonly", "true");
        configuration.set("datanucleus.schema.autoCreateAll", "true");
        bridgeStore.setConf(configuration);
        catalogClient = PolyCatClient.getInstance(configuration);
    }

    @AfterAll
    public static void afterAll() {
        try {
            if (catalogServer != null) {
                try {
                    catalogServer.close();
                } catch (Exception ignored) {

                }
            }
        } finally {
            clean(targetPath + "/spark-warehouse", targetPath + "/metastore_db");
        }
    }

    public static void clean(String warehouse, String metastore) {
        File sparkWarehouse = new File(warehouse);
        if (sparkWarehouse.exists()) {
            try {
                FileUtils.deleteDirectory(sparkWarehouse);
            } catch (IOException e) {

            }
        }
        if (bridgeStore != null) {
            bridgeStore.shutdown();
        }
        File hiveMetastore = new File(metastore);
        if (hiveMetastore.exists()) {
            try {
                FileUtils.deleteDirectory(hiveMetastore);
            } catch (IOException e) {

            }
        }

        storeBase.clearDB();
    }

    @Test
    public void testDatabaseOps() {
        Map emptyMap = Collections.EMPTY_MAP;
        Database db1 = new Database("db1", "description", "locationurl", null);
        assertThrows(MetaException.class, () -> bridgeStore.createDatabase(db1),
            "Cannot create database in readonly mode");
        assertThrows(MetaException.class, () -> bridgeStore.dropDatabase("db1"),
            "Cannot drop database in readonly mode");
        assertThrows(MetaException.class, () -> bridgeStore.alterDatabase("db1", db1),
            "Cannot alter database in readonly mode");
    }

    @Test
    public void testTableOps() {
        Map emptyMap = Collections.EMPTY_MAP;
        StorageDescriptor sd = new StorageDescriptor(null, "location", null, null, false, 0,
            new SerDeInfo("SerDeName", "serializationLib", null), null, null, null);
        Table tbl1 = new Table("tb1", "tb1", "owner", 1, 2, 3, sd, null, emptyMap, null, null, "MANAGED_TABLE");
        assertThrows(MetaException.class, () -> bridgeStore.createTable(tbl1), "Cannot create table in readonly mode");
        assertThrows(MetaException.class, () -> bridgeStore.alterTable("db1", "tb1", tbl1),
            "Cannot alter table in readonly mode");
        assertThrows(MetaException.class, () -> bridgeStore.dropTable("db1", "tb1"),
            "Cannot drop table in readonly mode");
    }

    @Test
    public void testPartitionOps() {
        Map emptyMap = Collections.EMPTY_MAP;
        List<String> value1 = Arrays.asList("US", "CA");
        StorageDescriptor sd = new StorageDescriptor(null, "location", null, null, false, 0,
            new SerDeInfo("SerDeName", "serializationLib", null), null, null, null);
        Partition part1 = new Partition(value1, "db1", "tb1", 111, 111, sd, emptyMap);
        assertThrows(MetaException.class, () -> bridgeStore.addPartition(part1),
            "Cannot add partition in readonly mode");
        assertThrows(MetaException.class, () -> bridgeStore.dropPartition("db1", "tb1", value1),
            "Cannot drop partition in readonly mode");
        assertThrows(MetaException.class, () -> bridgeStore.alterPartition("db1", "tb1", value1, part1),
            "Cannot alter partition in readonly mode");

        List<Partition> partitions = Collections.singletonList(part1);
        List<List<String>> values = Collections.singletonList(value1);
        assertThrows(MetaException.class, () -> bridgeStore.addPartitions("db1", "tb1", partitions),
            "Cannot add partitions in readonly mode");
        assertThrows(MetaException.class, () -> bridgeStore.alterPartitions("db1", "tb1", values, partitions),
            "Cannot alter partitions in readonly mode");
        assertThrows(MetaException.class, () -> bridgeStore.dropPartitions("db1", "tb1", value1),
            "Cannot drop partitions in readonly mode");
    }
}
