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
package io.polycat.catalog.iceberg;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import io.polycat.catalog.client.CatalogUserInformation;
import io.polycat.catalog.client.PolyCatClient;
import io.polycat.catalog.common.GlobalConfig;
import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.model.Table;
import io.polycat.catalog.common.model.record.Field;
import io.polycat.catalog.common.model.record.IntWritable;
import io.polycat.catalog.common.model.record.Record;
import io.polycat.catalog.common.model.record.StringWritable;
import io.polycat.catalog.common.plugin.request.GetTableRequest;
import io.polycat.catalog.server.CatalogApplication;
import io.polycat.catalog.store.api.StoreBase;
import io.polycat.catalog.store.fdb.record.RecordStoreHelper;
import io.polycat.catalog.store.fdb.record.impl.StoreImpl;
import io.polycat.common.RowBatch;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.subspace.Subspace;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PolyCatIcebergCatalogTest {

    private static ConfigurableApplicationContext catalogServer;
    private static SparkSession spark;
    private static PolyCatClient catalogClient;
    private static String userName;
    private static StoreBase storeBase;
    static String targetPath = new File(
        PolyCatIcebergCatalogTest.class.getResource("/").getPath() + "/../").getAbsolutePath();

    @BeforeAll
    static void beforeAll() throws IOException {
        storeBase = new StoreImpl();
        GlobalConfig.set(GlobalConfig.CONF_DIR, "../../conf");
        clearFDB();
        catalogServer = SpringApplication.run(CatalogApplication.class);
        userName = "test";
        spark = SparkSession.builder()
            .master("local[2]")
            .config("spark.sql.extensions",
                "org.apache.spark.sql.PolyCatExtensions, org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.polycat.iceberg.enabled", "true")
            .config(CatalogUserInformation.POLYCAT_USER_NAME, userName)
            .config(CatalogUserInformation.POLYCAT_USER_PASSWORD, "dash")
            .config("spark.sql.warehouse.dir", targetPath + "/spark-warehouse")
            .config("spark.hadoop.javax.jdo.option.ConnectionURL",
                "jdbc:derby:;databaseName=" + targetPath + "/metastore_db;create=true")
            .getOrCreate();

        catalogClient = PolyCatClient.getInstance(spark.sessionState().newHadoopConf());

    }

    @AfterAll
    static void afterAll() {
        sql("drop table if exists spark_catalog.db1.t1");
        sql("drop table if exists iceberg.db1.t1");

        if (catalogServer != null) {
            catalogServer.close();
        }
        if (spark != null) {
            spark.close();
        }
        clearFDB();
    }

    @Test
    void smoke_iceberg_table() {
        sql("create catalog iceberg");
        sql("use catalog iceberg");
        sql("create namespace db1");
        sql("use namespace db1");
        sql("create table t1 (c1 string, c2 int) using iceberg");
        sql("insert into t1 values('a', 1)");
        sql("insert into t1 values('b', 2)");

        checkAnswer(sql("select * from t1 order by c1"),
            batch(row("a", 1), row("b", 2)));

        shouldExistTableInLMS("iceberg", "db1", "t1");
    }

    @Test
    void smoke_iceberg_cdc() {
        sql("create catalog iceberg_cdc");
        sql("use catalog iceberg_cdc");
        sql("create namespace db1");
        sql("use namespace db1");
        sql("create table t1 (c1 string, c2 int) using iceberg");
        sql("insert into t1 values('a', 1),('b', 2),('c', 3)");
        sql("create table t2 (c1 string, c2 int, op string) using iceberg");
        sql("insert into t2 values('a', 11, 'U'),('b', 2, 'D'),('d', 44, 'I')");

        sql("merge into t1 t using t2 s on t.c1 = s.c1 " +
            "when matched and s.op = 'U' then update set t.c2 = s.c2 " +
            "when matched and s.op = 'D' then delete " +
            "when not matched and s.op = 'I' then insert (c1, c2) values (s.c1, s.c2)");

        checkAnswer(sql("select * from t1 order by c1"),
            batch(row("a", 11), row("c", 3), row("d", 44)));

        sql("delete from t1 where c1 = 'a'");

        checkAnswer(sql("select * from t1 order by c1"),
            batch(row("c", 3), row("d", 44)));

        shouldExistTableInLMS("iceberg_cdc", "db1", "t1");
    }

    @Test
    void smoke_iceberg_version_management() {
        sql("create catalog iceberg_version");
        sql("use catalog iceberg_version");
        sql("create namespace db1");
        sql("use namespace db1");
        sql("create table t1 (c1 string, c2 int) using iceberg");
        sql("insert into t1 values('a', 1)");
        sql("insert into t1 values('b', 2)");
        sql("insert into t1 values('c', 3)");

        checkAnswer(sql("select * from t1 order by c1"), batch(row("a", 1), row("b", 2), row("c", 3)));

        List<Row> commits = sql("show history for table t1").collectAsList();

        assertThat(commits.size()).isEqualTo(4);
        assertThat(commits.get(3).getString(5)).isEqualTo("[(operation=DDL_CREATE_TABLE, fileCount=0, addedRows=0)]");
        assertThat(commits.get(2).getString(5)).isEqualTo("[(operation=DML_INSERT, fileCount=0, addedRows=0)]");
        assertThat(commits.get(1).getString(5)).isEqualTo("[(operation=DML_INSERT, fileCount=0, addedRows=0)]");
        assertThat(commits.get(0).getString(5)).isEqualTo("[(operation=DML_INSERT, fileCount=0, addedRows=0)]");

        sql("restore table t1 version as of '" + commits.get(2).getString(3) + "'");

        checkAnswer(sql("select * from t1 order by c1"), batch(row("a", 1)));

        sql("restore table t1 version as of '" + commits.get(1).getString(3) + "'");

        checkAnswer(sql("select * from t1 order by c1"), batch(row("a", 1), row("b", 2)));
    }

    @Test
    void smoke_non_iceberg_table_without_hive_client_implement() {
        sql("create catalog spark_catalog");
        sql("use catalog spark_catalog");
        sql("create namespace db1");
        sql("use namespace db1");
        sql("create table t1 (c1 string, c2 int) using parquet");
        sql("insert into t1 values('a', 1)");
        sql("insert into t1 values('b', 2)");

        checkAnswer(sql("select * from t1 order by c1"),
            batch(row("a", 1), row("b", 2)));

        assertThrows(CatalogException.class, () -> shouldExistTableInLMS("spark_catalog", "db1", "t1"));
    }

    private void shouldExistTableInLMS(String catalogName, String databaseName, String tableName) {
        GetTableRequest getTableRequest = new GetTableRequest(catalogClient.getProjectId(), catalogName, databaseName,
            tableName);
        Table table = catalogClient.getTable(getTableRequest);
        Assertions.assertNotNull(table);
    }

    private static void checkAnswer(Dataset<Row> query, RowBatch batch) {
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

    private static Dataset<Row> sql(String sqlText) {
        return spark.sql(sqlText);
    }

    public static void clearFDB() {
        storeBase.clearDB();
    }
}
