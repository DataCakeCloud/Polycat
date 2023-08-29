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
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;

import io.polycat.catalog.client.CatalogUserInformation;
import io.polycat.catalog.client.PolyCatClient;
import io.polycat.catalog.common.GlobalConfig;
import io.polycat.catalog.common.PolyCatConf;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.exception.CarbonSqlException;
import io.polycat.catalog.common.plugin.CatalogPlugin;
import io.polycat.catalog.common.plugin.request.CreateCatalogRequest;
import io.polycat.catalog.common.plugin.request.CreateDatabaseRequest;
import io.polycat.catalog.common.plugin.request.input.CatalogInput;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;
import io.polycat.catalog.server.CatalogApplication;
import io.polycat.catalog.store.api.StoreBase;
import io.polycat.catalog.store.fdb.record.RecordStoreHelper;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.subspace.Subspace;
import io.polycat.catalog.store.fdb.record.impl.StoreImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.launcher.InProcessLauncher;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkAppHandle.Listener;
import org.apache.spark.launcher.SparkAppHandle.State;
import org.apache.spark.launcher.SparkLauncher;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Disabled
public class JDBCAuthenticationTest {

    private static final Logger logger = Logger.getLogger(JDBCAuthenticationTest.class);

    private static ConfigurableApplicationContext catalogServer;
    private static SparkAppHandle sparkAppHandle;
    private static StoreBase storeBase = new StoreImpl();

    @BeforeAll
    static void beforeAll() throws InterruptedException {
        clearFDB();
        catalogServer = SpringApplication.run(CatalogApplication.class);
        initCatalogAndDatabase();
        createSparkThriftServer();
    }

    private static void initCatalogAndDatabase() throws InterruptedException {
        GlobalConfig.set(GlobalConfig.CONF_DIR, "../../conf");
        GlobalConfig.set(GlobalConfig.WAREHOUSE, "target/lakehouse");
        CatalogUserInformation.login("test", "dash");
        CatalogPlugin client = PolyCatClient.getInstance(new Configuration());
        CreateCatalogRequest request = new CreateCatalogRequest();
        CatalogInput input = new CatalogInput();
        input.setCatalogName("lms");
        request.setInput(input);
        request.initContext(client.getContext());
        client.createCatalog(request);

        CreateDatabaseRequest request2 = new CreateDatabaseRequest();
        DatabaseInput input2 = new DatabaseInput();
        input2.setDatabaseName("default");
        request2.setInput(input2);
        request2.setCatalogName("lms");
        request2.initContext(client.getContext());
        client.createDatabase(request2);
    }

    private static void createSparkThriftServer() {
        try {
            CountDownLatch countDownLatch = new CountDownLatch(1);
            String classPath = Objects.requireNonNull(JDBCAuthenticationTest.class.getResource("/")).getPath();
            String targetPath = new File(classPath + "/../").getCanonicalPath();
            sparkAppHandle = new InProcessLauncher()
                .setMainClass("org.apache.spark.sql.hive.thriftserver.HiveThriftServer2")
                .setAppName("Thrift JDBC/ODBC Server")
                .setAppResource("target/spark/spark-hive-thriftserver_2.12-3.0.1.jar")
                .setMaster("local")
                .setConf("spark.ui.enabled", "false")
                .setConf("spark.metrics.staticSources.enabled", "false")
                .setConf("spark.sql.catalog.lms", "io.polycat.catalog.spark.SparkCatalog")
                .setConf("spark.sql.defaultCatalog", "lms")
                .setConf("spark.sql.extensions", "org.apache.spark.sql.PolyCatExtensions")
                .setConf("spark.polycat.iceberg.enabled", "false")
                .setConf("spark.sql.warehouse.dir", targetPath + "/spark-warehouse2")
                .setConf("spark.hadoop.javax.jdo.option.ConnectionURL",
                    "jdbc:derby:;databaseName=" + targetPath + "/metastore_db2;create=true")
                .setConf("spark.hadoop.hive.metastore.rawstore.impl",
                    "io.polycat.catalog.hms.hive2.CatalogStore")
                .setConf("spark.hadoop." + CatalogUserInformation.POLYCAT_USER_NAME, "test")
                .setConf("spark.hadoop." + CatalogUserInformation.POLYCAT_USER_PASSWORD, "dash")
                .setConf("spark.hadoop." + PolyCatConf.POLYCAT_CONFI_DIR, "../../conf")
                .setConf("spark.driver.ip", "localhost")
                .setConf("spark.hadoop.hive.server2.thrift.port", "9856")
                .setConf("spark.hadoop.hive.server2.thrift.bind.host", "localhost")
                .setConf("spark.hadoop.hive.server2.thrift.min.worker.threads", "1")
                .setConf("spark.hadoop.hive.server2.thrift.max.worker.threads", "5")
                .setConf("spark.cores.max", "1")
                .setConf(SparkLauncher.DRIVER_MEMORY, "256m")
                .addSparkArg("SPARK_LOCAL_HOSTNAME", "localhost")
                .startApplication(new Listener() {
                    @Override
                    public void stateChanged(SparkAppHandle handle) {
                        State state = handle.getState();
                        if (state == State.LOST) {
                            countDownLatch.countDown();
                        }
                    }

                    @Override
                    public void infoChanged(SparkAppHandle handle) {
                    }
                });
            countDownLatch.await();
        } catch (IOException | InterruptedException e) {
            logger.error(e.getMessage(), e);
            throw new CarbonSqlException("Failed to start Spark Application for test");
        }
    }

    @Test
    void smoke() throws SQLException {
        // use test user to create table and grant privilege to user david
        Connection connection = DriverManager.getConnection("jdbc:hive2://localhost:9856/", "test", "dash");
        Statement statement = connection.createStatement();
        statement.execute("use catalog lms");
        statement.execute("create namespace db1");
        statement.execute("use namespace db1");

        // t1
        statement.execute("create table t1 (c1 string, c2 int) using parquet");
        statement.execute("insert into t1 values('a', 1), ('b', 2)");

        checkQueryResult(statement);

        // grant privilege
        statement.execute("CREATE ROLE role1");
        statement.execute("GRANT ROLE role1 TO USER david");
        statement.execute("GRANT DESC ON TABLE lms.db1.t1 TO ROLE role1");
        statement.execute("GRANT USE ON CATALOG lms TO ROLE role1");
        statement.execute("GRANT USE ON DATABASE lms.default TO ROLE role1");
        statement.execute("GRANT SELECT ON TABLE lms.db1.t1 TO ROLE role1");

        // t2
        statement.execute("create table t2 (c1 string, c2 int) using parquet");
        statement.execute("insert into t2 values('a', 1), ('b', 2)");

        statement.close();
        connection.close();

        // switch to user david
        Connection connection2 = DriverManager.getConnection("jdbc:hive2://localhost:9856/", "david", "dash");
        Statement statement2 = connection2.createStatement();
        checkQueryResult(statement2);

        assertThrows(SQLException.class, () -> statement2.executeQuery("select * from db1.t2"),
            "Error running query: io.polycat.catalog.common.exception.CatalogException: No permission. SELECT TABLE");

    }

    private void checkQueryResult(Statement statement) throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from db1.t1");
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getString(1)).isEqualTo("a");
        assertThat(resultSet.getInt(2)).isEqualTo(1);
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getString(1)).isEqualTo("b");
        assertThat(resultSet.getInt(2)).isEqualTo(2);
    }

    @AfterAll
    static void afterAll() {
        if (catalogServer != null){
            catalogServer.close();
        }
        if (sparkAppHandle != null) {
            sparkAppHandle.kill();
        }
        clearFDB();
    }

    public static void clearFDB() {
        storeBase.clearDB();
    }
}
