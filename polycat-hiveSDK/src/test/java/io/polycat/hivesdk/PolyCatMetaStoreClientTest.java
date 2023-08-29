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
package io.polycat.hivesdk;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.polycat.catalog.authentication.Authentication;
import io.polycat.catalog.authentication.model.AuthenticationResult;
import io.polycat.catalog.authentication.model.LocalIdentity;
import io.polycat.catalog.common.plugin.CatalogContext;
import io.polycat.catalog.server.CatalogApplication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.client.builder.CatalogBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Disabled("some Hive feature, Catalog not support yet.")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class PolyCatMetaStoreClientTest {

    private static final Logger LOG = LoggerFactory.getLogger(PolyCatMetaStoreClientTest.class);
    public static final String LOCATION = "/location/uri";
    public static final String TBL_TEST_LOCATION = "/location/uri";
    public static final String TEST_DESCRIPTION = "alter description";
    public static final String SERDE_LIB = "hive.serde.lib";
    public static final String SERDE_NAME = "";
    public static final SerDeInfo SERDE = new SerDeInfo(SERDE_NAME, SERDE_LIB, new HashMap<>());
    public static final String OUTPUT_FORMAT = "output.format";
    public static final String INPUT_FORMAT = "input.format";
    public static final String TEST_COL1 = "col1";
    public static final String COL1_TYPE = "string";
    public static final String COL1_COMMENT = "empty";
    public static final String TEST_COL2 = "col2";
    public static final String COL2_TYPE = "string";
    public static final String COL2_COMMENT = "empty";
    public static final ArrayList<FieldSchema> COLS = new ArrayList<FieldSchema>() {{
        add(new FieldSchema(TEST_COL1, COL1_TYPE, COL1_COMMENT));
        add(new FieldSchema(TEST_COL2, COL2_TYPE, COL2_COMMENT));
    }};
    public static final String OWNER = "owner";
    public static final String DEFAULT_CATALOG_NAME = "hive";
    protected static IMetaStoreClient client;
    protected static Configuration conf;
    protected static Warehouse warehouse;

    private static final String CATALOG_TEST_NAME = "testc1";
    private static final String DB_TEST_NAME = "testdb1";
    private static final String TBL_TEST_NAME = "testtbl1";
    private static final String CATALOG_NAME = "c1";
    private static final String DB1_NAME = "db1";
    private static final String DB2_NAME = "db2";
    private static final String DEFAULT_DB = "default";
    private static final String TBL1_NAME = "tbl1";
    private static final String TBL2_NAME = "tbl2";
    private static final String DESCRIPTION = "description";
    private static final String DB1_LOCATION = LOCATION + "/" + DB1_NAME;
    private static final String DB_TEST_LOCATION = LOCATION + "/" + DB_TEST_NAME;
    private static final String DB2_LOCATION = LOCATION + "/" + DB2_NAME;


    private static final int DEFAULT_LIMIT_PARTITION_REQUEST = 100;
    private static ConfigurableApplicationContext catalogApp;
    public static String token;
    public static CatalogContext context;

    public static final String ACCOUNT_ID = "test";
    public static final String PROJECT_ID = "shenzhen";
    public static final String USER_ID = "test";
    public static final String PASSWORD = "dash";
    private static final String TENANT = "tenantA";

    public static void runCatalogApp() {

        // run catalog application
        catalogApp = SpringApplication.run(CatalogApplication.class);
        try {
            sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        AuthenticationResult response = doAuthentication(USER_ID, PASSWORD);
        token = response.getToken();
        context = new CatalogContext(PROJECT_ID, USER_ID, TENANT, token, DEFAULT_CATALOG_NAME);
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

    @AfterAll
    public static void stopAndClear() throws TException {
        // stop catalog application
        catalogApp.close();
        try {
            sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @AfterEach
    public void clearEnv() throws TException {
        client.dropDatabase(CATALOG_TEST_NAME, DB_TEST_NAME, false, true, true);
        client.dropDatabase(CATALOG_TEST_NAME, DB1_NAME, false, true, true);
        client.dropDatabase(CATALOG_TEST_NAME, DB2_NAME, false, true, true);
        client.dropDatabase(DB1_NAME, false, true, true);
        client.dropDatabase(DB2_NAME, false, true, true);
        client.dropCatalog(CATALOG_TEST_NAME);
    }

    @BeforeEach
    public void buildEnv() {
        try {
            Catalog catalog = new CatalogBuilder().setName(CATALOG_TEST_NAME).setDescription("").setLocation(LOCATION)
                .build();
            client.createCatalog(catalog);

            Database database = new Database(DB_TEST_NAME, DESCRIPTION, DB_TEST_LOCATION, new HashMap<>());
            database.setCatalogName(CATALOG_TEST_NAME);
            client.createDatabase(database);
            client.createTable(generateTable(CATALOG_TEST_NAME, TBL_TEST_NAME));
        } catch (TException e) {
            e.printStackTrace();
        }
    }

    @BeforeEach
    public void cleanUp() {
        try {
            client.dropCatalog(CATALOG_NAME);
        } catch (Exception e) {
        }
        try {
            String catalogName1 = CATALOG_NAME + "1";
            String catalogName2 = CATALOG_NAME + "2";
            deleteCatalogs(catalogName1, catalogName2);
        } catch (Exception e) {
        }
    }

    private Table generateTable(String catName, String tableName) {
        List<FieldSchema> partitionKeys = new ArrayList<>();
        Map<String, String> params = new HashMap<>();
        StorageDescriptor sd = new StorageDescriptor(COLS, TBL_TEST_LOCATION, INPUT_FORMAT, OUTPUT_FORMAT,
            false, 0, SERDE, Collections.emptyList(), Collections.emptyList(), new HashMap<>());
        Table table = new Table(tableName, DB_TEST_NAME, OWNER, 0, 0, 0, sd, partitionKeys, params, "", "",
            TableType.MANAGED_TABLE.name());
        table.setCatName(catName);
        return table;
    }

    @Test
    public void should_create_catalog_success() throws TException {
        try {
            Catalog except = new CatalogBuilder().setName(CATALOG_NAME).setDescription("").setLocation(LOCATION)
                .build();
            client.createCatalog(except);

            Catalog actual = client.getCatalog(CATALOG_NAME);

            assertEquals(except.getName(), actual.getName());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            client.dropCatalog(CATALOG_NAME);
        }
    }

    @Test
    public void multi_create_catalog_success() throws InterruptedException {

        Runnable runnable = () -> {
            String catalog_name = CATALOG_NAME + Thread.currentThread().getId();
            try {
                Catalog except = new CatalogBuilder().setName(catalog_name).setDescription("").setLocation(LOCATION)
                    .build();
                client.createCatalog(except);

                Catalog actual = client.getCatalog(catalog_name);

                assertEquals(except.getName(), actual.getName());
            } catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
            } finally {
                try {
                    client.dropCatalog(catalog_name);
                } catch (TException ignored) {
                }
            }
        };

        PolyCatTestUtils.multiThreadExecute(runnable, "createCatalog", 20);
    }

    @Test
    public void should_drop_catalog_success() {
        try {
            Catalog except = new CatalogBuilder().setName(CATALOG_NAME).setDescription("").setLocation(LOCATION)
                .build();
            client.createCatalog(except);
            Catalog actual = client.getCatalog(CATALOG_NAME);
            assertEquals(except.getName(), actual.getName());

            // drop catalog test begin
            client.dropCatalog(CATALOG_NAME);
            assertThrows(NoSuchObjectException.class, () -> client.getCatalog(CATALOG_NAME));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void should_alter_catalog_success() throws TException {
        try {
            Catalog catalog = new CatalogBuilder().setName(CATALOG_NAME).setDescription("").setLocation(LOCATION)
                .build();
            client.createCatalog(catalog);

            catalog.setDescription(TEST_DESCRIPTION);
            client.alterCatalog(CATALOG_NAME, catalog);

            Catalog actual = client.getCatalog(CATALOG_NAME);

            assertEquals(TEST_DESCRIPTION, actual.getDescription());

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            client.dropCatalog(CATALOG_NAME);
        }
    }

    @Test
    public void should_get_catalogs_success() throws TException {
        String catalogName1 = CATALOG_NAME + "1";
        String catalogName2 = CATALOG_NAME + "2";
        try {
            deleteCatalogs(catalogName1, catalogName2);
            List<String> expect = client.getCatalogs();
            expect.add(catalogName1);
            expect.add(catalogName2);
            Catalog catalog1 = new CatalogBuilder().setName(catalogName1).setDescription("").setLocation(LOCATION)
                .build();
            client.createCatalog(catalog1);

            Catalog catalog2 = new CatalogBuilder().setName(catalogName2).setDescription("").setLocation(LOCATION)
                .build();
            client.createCatalog(catalog2);

            List<String> actual = client.getCatalogs();

            assertTrue(actual.contains(catalogName1));
            assertTrue(actual.contains(catalogName2));
            assertEquals(expect.size(), actual.size());
            for (int i = 0; i < expect.size(); i++) {
                assertTrue(expect.contains(actual.get(i)));
            }

        } catch (Exception e) {
            fail(e.getMessage());
        } finally {
            client.dropCatalog(catalogName1);
            client.dropCatalog(catalogName2);
        }
    }

    @Test
    public void should_create_database_success() {
        try {
            Database except = new Database(DB1_NAME, DESCRIPTION, DB1_LOCATION, new HashMap<>());
            except.setCatalogName(CATALOG_TEST_NAME);
            client.createDatabase(except);
            Database actual = client.getDatabase(CATALOG_TEST_NAME, DB1_NAME);
            assertEquals(except.getName(), actual.getName());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void should_create_database_default_catalog_success() {
        try {
            Database except = new Database(DB1_NAME, DESCRIPTION, DB1_LOCATION, new HashMap<>());
            client.createDatabase(except);
            Database actual = client.getDatabase(DB1_NAME);
            assertEquals(except.getName(), actual.getName());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void should_alter_database_default_catalog_success() {
        try {
            Database except = new Database(DB1_NAME, DESCRIPTION, DB1_LOCATION, new HashMap<>());
            client.createDatabase(except);
            except.setDescription(TEST_DESCRIPTION);
            client.alterDatabase(DB1_NAME, except);
            Database actual = client.getDatabase(DB1_NAME);
            assertEquals(except.getDescription(), actual.getDescription());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void should_alter_database_success() {
        try {
            Database except = new Database(DB1_NAME, DESCRIPTION, DB1_LOCATION, new HashMap<>());
            except.setCatalogName(CATALOG_TEST_NAME);
            client.createDatabase(except);
            except.setDescription(TEST_DESCRIPTION);
            client.alterDatabase(CATALOG_TEST_NAME, DB1_NAME, except);

            Database actual = client.getDatabase(CATALOG_TEST_NAME, DB1_NAME);
            assertEquals(except.getDescription(), actual.getDescription());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void should_get_databases_success() {
        try {
            Database db1 = new Database(DB1_NAME, DESCRIPTION, DB1_LOCATION, new HashMap<>());
            db1.setCatalogName(CATALOG_TEST_NAME);
            client.createDatabase(db1);
            Database db2 = new Database(DB2_NAME, DESCRIPTION, DB2_LOCATION, new HashMap<>());
            db2.setCatalogName(CATALOG_TEST_NAME);
            client.createDatabase(db2);

            List<String> dbs = client.getDatabases(CATALOG_TEST_NAME, "db*");
            assertEquals(2, dbs.size());
            assertEquals(DB1_NAME, dbs.get(0));
            assertEquals(DB2_NAME, dbs.get(1));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void should_get_databases_default_catalog_success() {
        try {
            Database db1 = new Database(DB1_NAME, DESCRIPTION, DB1_LOCATION, new HashMap<>());
            client.createDatabase(db1);
            Database db2 = new Database(DB2_NAME, DESCRIPTION, DB2_LOCATION, new HashMap<>());
            client.createDatabase(db2);

            List<String> dbs = client.getDatabases("db*");
            assertTrue(dbs.contains(DB1_NAME));
            assertTrue(dbs.contains(DB2_NAME));
            for (String dbName : dbs) {
                dbName.contains("db");
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void should_drop_databases_default_catalog_success() {
        try {
            Database db1 = new Database(DB1_NAME, DESCRIPTION, DB1_LOCATION, new HashMap<>());
            client.createDatabase(db1);
            Database db2 = new Database(DB2_NAME, DESCRIPTION, DB2_LOCATION, new HashMap<>());
            client.createDatabase(db2);

            client.dropDatabase(DB1_NAME);
            client.dropDatabase(DB2_NAME, false, false, false);

            assertThrows(NoSuchObjectException.class, () -> client.getDatabase(DB1_NAME));
            assertThrows(NoSuchObjectException.class, () -> client.getDatabase(DB2_NAME));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void should_get_all_databases_default_catalog_success() {
        try {
            Database db1 = new Database(DB1_NAME, DESCRIPTION, DB1_LOCATION, new HashMap<>());
            db1.setCatalogName(CATALOG_TEST_NAME);
            client.createDatabase(db1);
            Database db2 = new Database(DB2_NAME, DESCRIPTION, DB2_LOCATION, new HashMap<>());
            db2.setCatalogName(CATALOG_TEST_NAME);
            client.createDatabase(db2);

            List<String> dbs = client.getAllDatabases(CATALOG_TEST_NAME);
            assertEquals(4, dbs.size());
            assertEquals(DB1_NAME, dbs.get(0));
            assertEquals(DB2_NAME, dbs.get(1));
            assertEquals(DEFAULT_DB, dbs.get(2));
            assertEquals(DB_TEST_NAME, dbs.get(3));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void should_alter_table_success() {
        try {
            Table table = generateTable(CATALOG_TEST_NAME, TBL1_NAME);
            client.createTable(table);
            Table actual1 = client.getTable(CATALOG_TEST_NAME, DB_TEST_NAME, TBL1_NAME);
            Map<String, String> newParams = new HashMap<String, String>() {{
                put("columnCnt", "0");
            }};
            table.setParameters(newParams);
            client.alter_table(CATALOG_TEST_NAME, DB_TEST_NAME, TBL1_NAME, table);
            Table actual2 = client.getTable(CATALOG_TEST_NAME, DB_TEST_NAME, TBL1_NAME);
            assertEquals(actual1.getParameters().size() + 1, actual2.getParameters().size());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    private void deleteCatalogs(String... catalogName1) {
        for (String catName : catalogName1) {
            try {
                client.dropCatalog(catName);
            } catch (Exception e) {
            }
        }
    }

    @Test
    public void should_get_table_names_success() {
        try {
            Table table1 = generateTable(CATALOG_TEST_NAME, TBL1_NAME);
            client.createTable(table1);
            Table table2 = generateTable(CATALOG_TEST_NAME, TBL2_NAME);
            client.createTable(table2);

            List<String> actual = client.getAllTables(CATALOG_TEST_NAME, DB_TEST_NAME);
            assertEquals(3, actual.size());
            assertEquals(TBL1_NAME, actual.get(0));
            assertEquals(TBL2_NAME, actual.get(1));

            actual = client.getTables(CATALOG_TEST_NAME, DB_TEST_NAME, "tbl*");
            assertEquals(2, actual.size());
            assertEquals(TBL1_NAME, actual.get(0));
            assertEquals(TBL2_NAME, actual.get(1));

            actual = client.getTables(CATALOG_TEST_NAME, DB_TEST_NAME, "tbl*", TableType.EXTERNAL_TABLE);
            assertEquals(0, actual.size());

            List<String> tableNames = client.listTableNamesByFilter(CATALOG_TEST_NAME, DB_TEST_NAME,
                "hive_filter_field_owner__=\"" + OWNER + "\"", 1);

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void otherCatalogs() throws TException {
        String[] tableNames = new String[4];
        for (int i = 0; i < tableNames.length; i++) {
            tableNames[i] = "table_in_other_catalog_" + i;
            TableBuilder builder = new TableBuilder()
                .setCatName(CATALOG_TEST_NAME)
                .setDbName(DB_TEST_NAME)
                .setTableName(tableNames[i])
                .addCol("col1_" + i, ColumnType.STRING_TYPE_NAME)
                .addCol("col2_" + i, ColumnType.INT_TYPE_NAME);
            if (i == 0) {
                builder.addTableParam("the_key", "the_value");
            }
            builder.create(client, conf);
        }

        String filter = hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS + "the_key=\"the_value\"";
        List<String> fetchedNames = client.listTableNamesByFilter(CATALOG_TEST_NAME, DB_TEST_NAME, filter, (short) -1);
        Assert.assertEquals(1, fetchedNames.size());
        Assert.assertEquals(tableNames[0], fetchedNames.get(0));
    }
}
