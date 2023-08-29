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
import io.polycat.catalog.client.CatalogUserInformation;
import io.polycat.catalog.client.PolyCatClient;
import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.plugin.request.CreateCatalogRequest;
import io.polycat.catalog.common.plugin.request.GetDatabaseRequest;
import io.polycat.catalog.common.plugin.request.GetTableRequest;
import io.polycat.catalog.common.plugin.request.ListFileRequest;
import io.polycat.catalog.common.plugin.request.input.CatalogInput;
import io.polycat.catalog.hms.hive2.HMSBridgeStore;
import io.polycat.catalog.server.CatalogApplication;
import io.polycat.catalog.store.api.StoreBase;

import io.polycat.catalog.store.fdb.record.impl.StoreImpl;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class HMSBridgeDelegateOnlyTest extends HMSBridgeTestEnv{

    private static ConfigurableApplicationContext catalogServer;
    public static PolyCatClient catalogClient;
    public static HMSBridgeStore bridgeStore;
    public static RawStore delegatedStore;
    public static Configuration configuration;
    public static String defaultCatalogName = "lms";
    public static String defaultDbName = "default";
    public static String userName = "test";
    public static String targetPath;
    public static String warehouse;
    public static StoreBase storeBase;

    @BeforeAll
    public static void setup() throws Exception {
        storeBase = new StoreImpl();
        catalogServer = SpringApplication.run(CatalogApplication.class);
        targetPath = new File("/tmp/hive2bridge/delegate").getCanonicalPath();
        warehouse = targetPath + "/warehouse";
        bridgeStore = new HMSBridgeStore();
        configuration = new HiveConf();
        configuration.set("javax.jdo.option.ConnectionURL",
            "jdbc:derby:;databaseName=" + targetPath + "/metastore_db;create=true");
        configuration.set("hive.hmsbridge.delegateOnly", "true");
        configuration.set("datanucleus.schema.autoCreateAll", "true");
        configuration.set(CatalogUserInformation.POLYCAT_USER_NAME, userName);
        configuration.set(CatalogUserInformation.POLYCAT_USER_PASSWORD, "dash");
        bridgeStore.setConf(configuration);
        delegatedStore = bridgeStore.getDelegatedStore();
        catalogClient = PolyCatClient.getInstance(configuration);
        CreateCatalogRequest createCatalogRequest = new CreateCatalogRequest();
        CatalogInput catalogInput = new CatalogInput();
        catalogInput.setCatalogName(defaultCatalogName);
        createCatalogRequest.setInput(catalogInput);
        createCatalogRequest.setProjectId(catalogClient.getProjectId());
        createCatalogRequest.getInput().setOwner(userName);
        catalogClient.createCatalog(createCatalogRequest);
        createDatabase(defaultDbName, null);
        delegatedStore.createDatabase(new Database("default", "", "", Collections.EMPTY_MAP));
    }

//    public static void createDatabase(String dbName, Map<String, String> properties)
//        throws InvalidObjectException, MetaException {
//        CreateDatabaseRequest createDatabaseRequest = new CreateDatabaseRequest();
//        DatabaseInput databaseInput = new DatabaseInput();
//        databaseInput.setDatabaseName(dbName);
//        databaseInput.setLocationUri(warehouse + "/" + dbName + ".db");
//        databaseInput.setParameters(properties);
//        createDatabaseRequest.setInput(databaseInput);
//        createDatabaseRequest.setProjectId(catalogClient.getProjectId());
//        createDatabaseRequest.getInput().setUserId(userName);
//        createDatabaseRequest.setCatalogName(defaultCatalogName);
//        catalogClient.createDatabase(createDatabaseRequest);
//        delegatedStore.createDatabase(new Database("default", "", "", Collections.EMPTY_MAP));
//    }

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

    private GetDatabaseRequest makeGetDatabaseRequest(String dbName) {
        GetDatabaseRequest request = new GetDatabaseRequest();
        request.setProjectId(catalogClient.getProjectId());
        request.setCatalogName(defaultCatalogName);
        request.setDatabaseName(dbName);
        return request;
    }

    @Test
    public void testDatabaseOps() throws Exception {

        HashMap<String, String> params = new HashMap<>();
        params.put("lms_name", "lms");
        String dbName = "db1";
        Database db1 = new Database(dbName, "description", "locationurl", params);
        db1.setOwnerType(PrincipalType.USER);
        bridgeStore.createDatabase(db1);

        Database db = delegatedStore.getDatabase(dbName);
        assertEquals(db1, db);
        assertThrows(CatalogException.class, () -> catalogClient.getDatabase(makeGetDatabaseRequest(dbName)));

        Database db2 = db1.deepCopy();
        String ownerName = "test";
        db2.setOwnerName(ownerName);
        bridgeStore.alterDatabase(dbName, db2);
        Database newDb = delegatedStore.getDatabase(dbName);
        assertEquals(db2, newDb);
        assertThrows(CatalogException.class, () -> catalogClient.getDatabase(makeGetDatabaseRequest(dbName)));

        bridgeStore.dropDatabase(dbName);
        assertThrows(NoSuchObjectException.class, () -> delegatedStore.getDatabase(dbName));
        assertThrows(CatalogException.class, () -> catalogClient.getDatabase(makeGetDatabaseRequest(dbName)));
    }

    @Test
    public void testTableOps() throws Exception {
        String tableName = "tb1";
        List emptyList = Collections.EMPTY_LIST;
        Map emptyMap = Collections.EMPTY_MAP;
        StorageDescriptor sd = new StorageDescriptor(emptyList, "location", null, null, false, 0,
            new SerDeInfo("SerDeName", "serializationLib", emptyMap), emptyList, emptyList, emptyMap);
        HashMap<String, String> params = new HashMap<String, String>();
        params.put("lms_name", "lms");
        Table tbl1 = new Table(tableName, defaultDbName, "owner", 1, 2, 3, sd, null, params, null, null,
            "MANAGED_TABLE");
        bridgeStore.createTable(tbl1);
        Table table = delegatedStore.getTable(defaultDbName, tableName);
        assertEquals(tableName, table.getTableName());
        GetTableRequest getTableRequest = new GetTableRequest(catalogClient.getProjectId(), defaultCatalogName,
            defaultDbName, tableName);
        assertThrows(CatalogException.class, () -> catalogClient.getTable(getTableRequest));

        params.put("k1", "v1");
        bridgeStore.alterTable(defaultDbName, tableName, tbl1);
        assertEquals(params, delegatedStore.getTable(defaultDbName, tableName).getParameters());
        assertThrows(CatalogException.class, () -> catalogClient.getTable(getTableRequest));

        bridgeStore.dropTable(defaultDbName, tableName);
        assertNull(delegatedStore.getTable(defaultDbName, tableName));
        assertThrows(CatalogException.class, () -> catalogClient.getTable(getTableRequest));
    }

    @Test
    public void testPartitionOps() throws Exception {
        String tableName = "tb1";
        List emptyList = Collections.EMPTY_LIST;
        Map emptyMap = Collections.EMPTY_MAP;
        String location = targetPath + "/tb1";
        FileUtils.forceMkdir(new File(location + "/Country=US/State=CA"));
        FieldSchema partitionKey1 = new FieldSchema("Country", serdeConstants.STRING_TYPE_NAME, "");
        FieldSchema partitionKey2 = new FieldSchema("State", serdeConstants.STRING_TYPE_NAME, "");
        FieldSchema col1 = new FieldSchema("c1", serdeConstants.STRING_TYPE_NAME, "");
        StorageDescriptor tblSd = new StorageDescriptor(Arrays.asList(col1), location, null, null, false, 0,
            new SerDeInfo("SerDeName", "serializationLib", emptyMap), emptyList, emptyList, emptyMap);
        StorageDescriptor sd = new StorageDescriptor(Arrays.asList(col1), location + "/Country=US/State=CA", null, null,
            false, 0, new SerDeInfo("SerDeName", "serializationLib", emptyMap), emptyList, emptyList, emptyMap);
        sd.setStoredAsSubDirectories(false);
        sd.setSkewedInfo(new SkewedInfo(emptyList, emptyList, emptyMap));
        HashMap<String, String> params = new HashMap<String, String>();
        params.put("lms_name", "lms");

        Table tbl1 = new Table(tableName, defaultDbName, "owner", 1, 2, 3, tblSd,
            Arrays.asList(partitionKey1, partitionKey2), params, null, null, "MANAGED_TABLE");
        bridgeStore.createTable(tbl1);
        List<String> value1 = Arrays.asList("US", "CA");
        Partition part1 = new Partition(value1, defaultDbName, tableName, 111, 111, sd, emptyMap);
        bridgeStore.addPartition(part1);

        Partition partition = delegatedStore.getPartition(defaultDbName, tableName, value1);
        assertEquals(part1, partition);
        ListFileRequest listFileRequest = new ListFileRequest(catalogClient.getProjectId(), defaultCatalogName,
            defaultDbName, tableName);
        assertThrows(CatalogException.class, () -> catalogClient.listPartitions(listFileRequest));

        bridgeStore.dropPartition(defaultDbName, tableName, value1);
        assertThrows(NoSuchObjectException.class, () -> delegatedStore.getPartition(defaultDbName, tableName, value1));
        assertThrows(CatalogException.class, () -> catalogClient.listPartitions(listFileRequest));

    }

}
