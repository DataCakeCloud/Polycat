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
package io.polycat.integration.spark;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import io.polycat.catalog.authentication.Authentication;
import io.polycat.catalog.authentication.model.AuthenticationResult;
import io.polycat.catalog.authentication.model.LocalIdentity;
import io.polycat.catalog.client.PolyCatClient;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.plugin.CatalogContext;
import io.polycat.catalog.common.plugin.request.CreateCatalogRequest;
import io.polycat.catalog.common.plugin.request.input.CatalogInput;
import io.polycat.catalog.server.CatalogApplication;
import io.polycat.catalog.store.api.StoreBase;
import io.polycat.catalog.store.fdb.record.impl.StoreImpl;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SparkEnv {
    private static final Logger logger = Logger.getLogger(SparkEnv.class);

    static public SparkSession sparkSession;
    private static ConfigurableApplicationContext polycatServerApp;
    public static PolyCatClient catalogClient;

    public static final StoreBase storeBase = new StoreImpl();
    static public String targetPath;
    static public String warehouse;
    static public String metahouse;
    public static String userName = "test";
    public static String defaultCatalogName = "hive";

    public static String INTEGRATION_SPARK_JAR = "/polycat-integration-spark-0.1-SNAPSHOT.jar";

    @BeforeAll
    public static void setup() throws Exception {
        targetPath = new File(SparkEnv.class.getResource("/").getPath() + "/../").getCanonicalPath();
        warehouse = targetPath + "/spark-warehouse";
        metahouse = targetPath + "/metastore_db";

        String hiveTargetPath = new File(targetPath + "/../../spark/target").getCanonicalPath();
        assertHiveJarsExists(hiveTargetPath);
        String hiveJars = hiveTargetPath + INTEGRATION_SPARK_JAR + ", "
            + hiveTargetPath + "/dependency-lib/hive-standalone-metastore-3.1.2.jar, "
            + hiveTargetPath + "/dependency-lib/*.jar";

        sparkSession = SparkSession.builder()
            .appName("plugin hive client")
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.metrics.staticSources.enabled", "false")
            .config("spark.sql.warehouse.dir", warehouse)
            .config("spark.hadoop.javax.jdo.option.ConnectionURL",
                "jdbc:derby:;databaseName=" + targetPath + "/metastore_db;create=true")
            .config("IS_SCHEMA_EVOLUTION_CASE_SENSITIVE", "false")
            .config("hive.exec.dynamic.partition", "true")
            .config("hive.exec.dynamic.partition.mode", "nonstrict")
            .config("spark.sql.hive.metastore.version", "3.1.2")
            .config("spark.sql.hive.metastore.jars", "path")
            .config("spark.sql.hive.metastore.jars.path", hiveJars)
            .enableHiveSupport()
            .getOrCreate();
        SparkEnv.sparkSession.sparkContext().setLogLevel("INFO");

        storeBase.clearDB();
        clean(warehouse);
        clean(metahouse);
        polycatServerApp = SpringApplication.run(CatalogApplication.class);
        initCatalogClient();
        createCatalog();

        System.out.println("SparkEnv setup success");
    }

    @AfterAll
    public static void afterAll() {
        storeBase.clearDB();
        try {
            if (sparkSession != null) {
                try {
                    sparkSession.close();
                } catch (Exception e) { }
            }
            if (polycatServerApp != null) {
                try {
                    polycatServerApp.close();
                } catch (Exception ignored) {

                }
            }
        } finally {
            clean(warehouse);
            clean(metahouse);
        }
    }

    private static void assertHiveJarsExists(String hiveTargetPath) {
        String errorMsg = "polycat Hive dependency jars not found. Please package related module first by maven "
            + "command: [mvn clean package -DskipTests -pl :polycat-integration-test -am] ";

        File file = new File(hiveTargetPath);
        assertTrue(file.isDirectory(), errorMsg);

        file = new File(hiveTargetPath + INTEGRATION_SPARK_JAR);
        assertTrue(file.exists(), errorMsg);

        file = new File(hiveTargetPath + "/dependency-lib");
        assertTrue(file.exists(), errorMsg);
        assertTrue(file.isDirectory(), errorMsg);
        assertNotNull(file.listFiles(), errorMsg);
    }

    private static String getJarsInPath(String path) throws Exception {
        try {
            File file = new File(path);
            if (file.isDirectory()) {
                List<File> files = Arrays.stream(file.listFiles()).filter(f -> {
                        try {
                            return f.getCanonicalPath().endsWith(".jar");
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        return false;
                    })
                    .collect(Collectors.toList());

                StringBuilder jars = new StringBuilder();
                for (File f : files) {
                    jars.append(f.getCanonicalPath()).append(",");
                }
                jars.deleteCharAt(jars.length() - 1);
                return jars.toString();
            } else {
                return path;
            }
        } catch (Exception e) {
            logger.error("polycat Hive dependency jars not found. Please package related module first by maven "
                + "command: [mvn clean package -DskipTests -pl :polycat-integration-test -am] ");
            throw e;
        }
    }

    private static void initCatalogClient() {
        AuthenticationResult response = doAuthentication("test", "dash");
        String token = response.getToken();
        CatalogContext context = new CatalogContext("shenzhen", "test", "tenantA", token, "hive");
        catalogClient = new PolyCatClient();
        catalogClient.setContext(context);
    }

    private static AuthenticationResult doAuthentication(String userName, String password) {
        String[] tenant = password.split(":");
        if (tenant.length == 2 && tenant[0].equals(tenant[1])) {
            LocalIdentity localIdentity = new LocalIdentity();
            localIdentity.setAccountId(tenant[0]);
            localIdentity.setIdentityOwner("IMCAuthenticator");
            return Authentication.authAndCreateToken(localIdentity);
        }
        LocalIdentity localIdentity = new LocalIdentity();
        localIdentity.setUserId(userName);
        localIdentity.setPasswd(password);
        localIdentity.setIdentityOwner("LocalAuthenticator");
        return Authentication.authAndCreateToken(localIdentity);
    }

    private static String createCatalog() {
        CreateCatalogRequest createCatalogRequest = new CreateCatalogRequest();
        CatalogInput catalogInput = new CatalogInput();
        catalogInput.setCatalogName(defaultCatalogName);
        createCatalogRequest.setInput(catalogInput);
        createCatalogRequest.setProjectId(catalogClient.getProjectId());
        createCatalogRequest.getInput().setOwner(userName);
        return catalogClient.createCatalog(createCatalogRequest).getCatalogName();
    }

    private static void clean(String path) {
        File f = new File(path);
        if (f.exists()) {
            try {
                FileUtils.deleteDirectory(f);
            } catch (IOException e) {

            }
        }
    }
}
