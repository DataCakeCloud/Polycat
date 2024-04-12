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
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import io.polycat.catalog.client.CatalogUserInformation;
import io.polycat.catalog.client.PolyCatClient;
import io.polycat.catalog.common.PolyCatConf;
import io.polycat.catalog.common.model.record.Record;
import io.polycat.catalog.common.plugin.request.CreateCatalogRequest;
import io.polycat.catalog.common.plugin.request.CreateDatabaseRequest;
import io.polycat.catalog.common.plugin.request.input.CatalogInput;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;
import io.polycat.catalog.hms.hive2.HMSBridgeStore;
import io.polycat.catalog.server.CatalogApplication;
import io.polycat.catalog.store.api.StoreBase;
import io.polycat.catalog.store.fdb.record.impl.StoreImpl;
import io.polycat.common.RowBatch;

import DynamicPackage.PackageUtil;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

public class HMSBridgeTestEnv {
    public static final String OWNER_TYPE = "USER";
    public static final String AUTH_SOURCE_TYPE = "iam";
    protected static final Map<String, String> PARAMS = new HashMap<>();
    public static final String ACCOUNT_ID = "test";

    private static ConfigurableApplicationContext catalogServer;
    public static SparkSession sparkSession;
    public static HMSBridgeStore bridgeStore;
    public static PolyCatClient catalogClient;
    public static String userName;
    public static String targetPath;
    public static File csvFolder;
    public static String warehouse;
    public static String defaultCatalogName = "lms";
    public static String defaultDbName = "default";
    public static StoreBase storeBase = new StoreImpl();

    @BeforeAll
    public static void setup() throws Exception {
        catalogServer = SpringApplication.run(CatalogApplication.class);
        targetPath = new File("/tmp/hive2bridge").getCanonicalPath();
        warehouse = targetPath + "/spark-warehouse";
        clean(targetPath + "/spark-warehouse", targetPath + "/metastore_db");
        userName = "test";
        sparkSession = SparkSession
            .builder()
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.metrics.staticSources.enabled", "false")
            .config("spark.hadoop.hive.metastore.rawstore.impl",
                "HMSBridgeStore")
            .config("spark.sql.warehouse.dir", warehouse)
            .config("spark.hadoop.javax.jdo.option.ConnectionURL",
                "jdbc:derby:;databaseName=" + targetPath + "/metastore_db;create=true")
            .config("spark.hadoop." + CatalogUserInformation.POLYCAT_USER_NAME, userName)
            .config("spark.hadoop." + CatalogUserInformation.POLYCAT_USER_PASSWORD, "dash")
            .config("spark.hadoop." + PolyCatConf.POLYCAT_CONFI_DIR, "../../conf")
            .config("IS_SCHEMA_EVOLUTION_CASE_SENSITIVE", "false")
            .config("hive.exec.dynamic.partition", "true")
            .config("hive.exec.dynamic.partition.mode", "nonstrict")
            .enableHiveSupport()
            .getOrCreate();
        sparkSession.sparkContext().setLogLevel("INFO");

        Configuration configuration = sparkSession.sessionState().newHadoopConf();
        bridgeStore = new HMSBridgeStore(configuration);
        catalogClient = PolyCatClient.getInstance(configuration);


        // create catalog in lms
        CreateCatalogRequest createCatalogRequest = new CreateCatalogRequest();
        CatalogInput catalogInput = new CatalogInput();
        catalogInput.setCatalogName(defaultCatalogName);
        createCatalogRequest.setInput(catalogInput);
        createCatalogRequest.setProjectId(catalogClient.getProjectId());
        createCatalogRequest.getInput().setOwner(userName);
        catalogClient.createCatalog(createCatalogRequest);

        createDatabase(defaultDbName, null);

        makeCSVDataFiles();
        PackageUtil.runPackage();
    }

    public static void createDatabase(String dbName, Map<String, String> properties) {
        CreateDatabaseRequest createDatabaseRequest = new CreateDatabaseRequest();
        DatabaseInput databaseInput = getDatabaseInput(defaultCatalogName, dbName,
            warehouse + "/" + dbName + ".db", userName);
        databaseInput.setParameters(properties);
        createDatabaseRequest.setInput(databaseInput);
        createDatabaseRequest.setProjectId(catalogClient.getProjectId());
        createDatabaseRequest.setCatalogName(defaultCatalogName);
        catalogClient.createDatabase(createDatabaseRequest);
    }

    @AfterAll
    public static void afterAll() {
        try {
            if (sparkSession != null) {
                try {
                    sparkSession.close();
                } catch (Exception e) { }
            }
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
        File hiveMetastore = new File(metastore);
        if (hiveMetastore.exists()) {
            try {
                FileUtils.deleteDirectory(hiveMetastore);
            } catch (IOException e) {

            }
        }

        storeBase.clearDB();
    }

    private static void makeCSVDataFiles() throws IOException {
        String dataRoot = "target/data_dir";
        csvFolder = new File(dataRoot);
        csvFolder.mkdir();

        FileWriter fw = new FileWriter(dataRoot + "/user.csv");
        RowBatch dataRowBach = batch(row(1, "obs"), row(2, "s3"), row(3, "azure"));
        writeBatch(fw, dataRowBach);
        fw.close();
    }

    private static Record row(Object... values) {
        return new Record(values);
    }

    private static RowBatch batch(Record... records) {
        return new RowBatch(Arrays.asList(records));
    }

    private static void writeBatch(FileWriter fw, RowBatch batchRecords) throws IOException {
        for (Record record : batchRecords.getRecords()) {
            String line = record.fields.stream().map(Object::toString)
                .collect(Collectors.joining(","));
            fw.write(line + "\n");
        }
    }

    protected static DatabaseInput getDatabaseInput(String catalogName, String databaseName, String location,
        String owner) {
        DatabaseInput databaseInput = new DatabaseInput();
        databaseInput.setCatalogName(catalogName);
        databaseInput.setDatabaseName(databaseName);
        databaseInput.setDescription("");
        databaseInput.setLocationUri(location);
        databaseInput.setParameters(PARAMS);
        databaseInput.setAuthSourceType(AUTH_SOURCE_TYPE);
        databaseInput.setAccountId(ACCOUNT_ID);
        databaseInput.setOwner(owner);
        databaseInput.setOwnerType(OWNER_TYPE);
        return databaseInput;
    }
}
