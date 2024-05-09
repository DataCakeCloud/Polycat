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
package io.polycat.catalog.spark;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import io.polycat.catalog.client.CatalogUserInformation;
import io.polycat.catalog.client.PolyCatClient;
import io.polycat.catalog.common.GlobalConfig;
import io.polycat.catalog.common.model.Table;
import io.polycat.catalog.common.model.record.Field;
import io.polycat.catalog.common.model.record.IntWritable;
import io.polycat.catalog.common.model.record.Record;
import io.polycat.catalog.common.model.record.StringWritable;
import io.polycat.catalog.common.plugin.CatalogPlugin;
import io.polycat.catalog.common.plugin.request.GetTableRequest;
import io.polycat.catalog.server.CatalogApplication;
import io.polycat.catalog.store.api.StoreBase;
import io.polycat.catalog.store.fdb.record.impl.StoreImpl;
import io.polycat.common.RowBatch;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

public class SparkCatalogTestEnv implements BeforeAllCallback {

    private static boolean started = false;
    private static ConfigurableApplicationContext catalogServer;
    public static SparkSession spark;
    public static PolyCatClient catalogClient;
    public static String userName;
    public static String targetPath;
    public static String PROJECT_ID = "shenzhen";
    public static String PASSWORD = "dash";
    private static String TENANT = "tenantA";
    private static StoreBase storeBase = new StoreImpl();

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        if (!started) {
            started = true;
            GlobalConfig.set(GlobalConfig.CONF_DIR, "../../conf");
            targetPath = new File(
                SparkCatalogTest.class.getResource("/").getPath() + "/../").getCanonicalPath();
            clearFDB();
            extensionContext.getRoot().getStore(ExtensionContext.Namespace.GLOBAL)
                .put("my_report", new CloseableOnlyOnceResource());

            catalogServer = SpringApplication.run(CatalogApplication.class);

            userName = "test";
            spark = SparkSession.builder()
                .master("local[2]")
                .config("spark.sql.extensions", "org.apache.spark.sql.PolyCatExtensions")
                .config(CatalogUserInformation.POLYCAT_USER_NAME, userName)
                .config(CatalogUserInformation.POLYCAT_USER_PASSWORD, "dash")
                .config("spark.polycat.iceberg.enabled", "false")
                .config("spark.sql.warehouse.dir", targetPath + "/spark-warehouse")
                .config("spark.hadoop.javax.jdo.option.ConnectionURL",
                    "jdbc:derby:;databaseName=" + targetPath + "/metastore_db;create=true")
                .getOrCreate();
            catalogClient = PolyCatClient.getInstance(spark.sessionState().newHadoopConf());

        }
    }

    private static class CloseableOnlyOnceResource implements ExtensionContext.Store.CloseableResource {
        @Override
        public void close() throws Throwable {
            catalogServer.close();
            spark.close();
            clearFDB();
        }
    }

    public static void clearFDB() {
        storeBase.clearDB();
    }

    public static void shouldExistTableInLMS(String catalogName, String databaseName, String tableName) {
        GetTableRequest getTableRequest = new GetTableRequest(catalogClient.getProjectId(), catalogName, databaseName,
            tableName);
        Table table = catalogClient.getTable(getTableRequest);
        Assertions.assertNotNull(table);
    }

    protected static void checkAnswer(Dataset<Row> query, RowBatch batch) {
        List<Row> result = query.collectAsList();
        assertThat(result.size()).isEqualTo(batch.size());
        int i = 0;
        for (Record record : batch) {
            int numColumns = record.fields.size();
            Row row = result.get(i);
            assertThat(row.size()).isEqualTo(numColumns);
            for (int j = 0; j < numColumns; j++) {
                Field field = record.getField(j);
                if (field instanceof StringWritable) {
                    assertThat(row.getString(j)).isEqualTo(field.getString());
                } else if (field instanceof IntWritable) {
                    assertThat(row.getInt(j)).isEqualTo(field.getInteger());
                }
            }
            i++;
        }
    }


    public static Record row(Object... values) {
        return new Record(values);
    }

    public static RowBatch batch(Record... records) {
        return new RowBatch(Arrays.asList(records));
    }

    public static Dataset<Row> sql(String sqlText) {
        return spark.sql(sqlText);
    }

    public static String getUUIDName() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    public static String getProjectId() {
        return PROJECT_ID;
    }

    public static String getTenant() {
        return TENANT;
    }

    public static CatalogPlugin getCatalog() {
        CatalogUserInformation.login(userName, PASSWORD);
        return new PolyCatClient();
    }

    public static String getUser() {
        return userName;
    }

    public static boolean hasTitle(Row[] rows, String title) {
        for (Row r : rows) {
            if (r.getString(0).equalsIgnoreCase(title)) {
                return true;
            }
        }
        return false;
    }

    public static String getRecordValueOfTitle(Row[] rows, String title) {
        for (Row r : rows) {
            if (r.getString(0).equalsIgnoreCase(title)) {
                return r.getString(1);
            }
        }
        return null;
    }

    public static String createCatalog() {
        String catalogName = "catalogName_".toLowerCase() + getUUIDName();
        sql("create catalog " + catalogName);
        return catalogName;
    }

    public static String createNamespace(String catalogName) {
        String namespace = "namespace_".toLowerCase() + getUUIDName();
        sql("use catalog " + catalogName);
        sql("create namespace " + namespace);
        return namespace;
    }

    public static String createDatabase(String catalogName) {
        String databaseName = "database_".toLowerCase() + getUUIDName();
        sql("use catalog " + catalogName);
        sql("create database " + databaseName);
        return databaseName;
    }

    public static String createBranchOn(String catalogName) {
        String branchName = "branchName_".toLowerCase() + getUUIDName();
        sql("CREATE BRANCH " + branchName + " ON " + catalogName);
        return branchName;
    }

    public static String createTable(String catalogName, String databaseName, String fileFormat) {
        String tableName = "tableName_".toLowerCase() + getUUIDName();
        sql("use catalog " + catalogName);
        sql("create table " + databaseName + "." + tableName
            + "(c1 string, c2 int) using " + fileFormat);
        return tableName;
    }
}
