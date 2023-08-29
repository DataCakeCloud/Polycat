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

import java.util.List;
import java.util.UUID;

import io.polycat.catalog.client.CatalogUserInformation;
import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.model.record.Record;
import io.polycat.catalog.common.plugin.CatalogPlugin;
import io.polycat.common.ExecutionResult;

import io.grpc.StatusRuntimeException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static io.polycat.catalog.spark.SparkCatalogTestEnv.batch;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.checkAnswer;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.row;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.spark;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.sql;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith({SparkCatalogTestEnv.class})
public class PrivilegeTest {

    private static String userNameOri = "test";
    private static String passwordOri = "dash";
    private static String userNameNew = "david";
    private static String passwordNew = "dash";

    public static Dataset<Row> newSql(String sqlText) {
        CatalogPlugin client = PolyCatClientHelper.getClientFromSession(spark);
        client.setContext(CatalogUserInformation.doAuth(userNameNew, passwordNew));
        return spark.sql(sqlText);
    }

    public static Dataset<Row> oldSql(String sqlText) {
        CatalogPlugin client = PolyCatClientHelper.getClientFromSession(spark);
        client.setContext(CatalogUserInformation.doAuth(userNameOri, passwordOri));
        return spark.sql(sqlText);
    }

    @BeforeEach
    void beforeEach() {
        CatalogPlugin client = PolyCatClientHelper.getClientFromSession(spark);
        client.setContext(CatalogUserInformation.doAuth("test", "dash"));
    }

    @Test
    @Disabled
    public void smoke() {
        sql("create catalog privilege");
        sql("use catalog privilege");
        sql("create database db1");
        sql("use db1");
        String[] tables = {"t1", "t2"};
        for (String t : tables) {
            sql("create table " + t + " (c1 string, c2 int) using parquet");
            sql("insert into " + t + " values('a', 1)");
            sql("insert into " + t + " values('b', 2)");
            checkAnswer(sql("select * from " + t + " order by c1"),
                batch(row("a", 1), row("b", 2)));
        }

        sql("CREATE ROLE role1");
        sql("GRANT ROLE role1 TO USER david");
        sql("GRANT DESC ON TABLE privilege.db1.t1 TO ROLE role1");
        sql("GRANT SELECT ON TABLE privilege.db1.t1 TO ROLE role1");

        CatalogPlugin client = PolyCatClientHelper.getClientFromSession(spark);
        client.setContext(CatalogUserInformation.doAuth("david", "dash"));

        checkAnswer(sql("select * from t1 order by c1"),
            batch(row("a", 1), row("b", 2)));

        assertThrows(CatalogException.class, () -> sql("select * from t2 order by c1"), "No permission. SELECT TABLE");
    }

    @Test
    @Disabled
    public void ddlPrivilegeTest() {
        String catalogName = "catalog_" + UUID.randomUUID().toString().substring(0, 8);
        String testRole1 = "role_" + UUID.randomUUID().toString().substring(0, 8);
        String databaseName = "database_" + UUID.randomUUID().toString().substring(0, 8);
        String tableName = "table_" + UUID.randomUUID().toString().substring(0, 8);
        String tableNameR = "tableR_" + UUID.randomUUID().toString().substring(0, 8);

        String databaseNameNew = "databaseNew_" + UUID.randomUUID().toString().substring(0, 8);
        String tableNameNew = "tableNew_" + UUID.randomUUID().toString().substring(0, 8);

        sql("CREATE CATALOG " + catalogName);
        sql("USE CATALOG " + catalogName);
        sql("CREATE DATABASE " + databaseName);
        sql("USE DATABASE " + databaseName);
        sql("CREATE TABLE " + tableName + " (c1 string, c2 int) using parquet");

        sql("CREATE ROLE " + testRole1);
        sql("GRANT ROLE " + testRole1 + " TO USER " + userNameNew);

        CatalogPlugin client = PolyCatClientHelper.getClientFromSession(spark);

        //use catalog
        assertThrows(CatalogException.class, () -> newSql("USE CATALOG " + catalogName), "No permission. USE CATALOG");

        oldSql("GRANT USE ON CATALOG " + catalogName + " TO ROLE " + testRole1);

        newSql("USE CATALOG " + catalogName).collect();
        assertThrows(CatalogException.class, () -> newSql("USE CATALOG wrongNameCat"), "No permission. USE CATALOG");

        //desc catalog
        oldSql("GRANT DESC ON CATALOG " + catalogName + " TO ROLE " + testRole1);

        newSql("DESC CATALOG " + catalogName).collect();

        //list catalogs
        newSql("SHOW CATALOGS").collect();

        //list catalog commits use desc catalog
        oldSql("GRANT DESC ON CATALOG " + catalogName + " TO ROLE " + testRole1);
        newSql("SHOW HISTORY FOR CATALOG " + catalogName).collect();

        //todo alter catalog

        //use database
        assertThrows(CatalogException.class, () -> newSql("USE DATABASE " + databaseName), "No permission. USE DATABASE");

        oldSql("GRANT USE ON DATABASE " + catalogName + "." + databaseName + " TO ROLE " + testRole1);

        newSql("USE DATABASE " + databaseName).collect();

        //desc database
        oldSql("GRANT DESC ON DATABASE " + catalogName + "." + databaseName + " TO ROLE " + testRole1);

        newSql("DESC DATABASE " + databaseName).collect();

        //list database
        assertThrows(CatalogException.class, () -> newSql("SHOW DATABASES"), "No permission. SHOW DATABASE");

        oldSql("GRANT SHOW ON CATALOG " + catalogName + " TO ROLE " + testRole1);

        newSql("SHOW DATABASES ").collect();

        //create database
        newSql("USE CATALOG " + catalogName);
        assertThrows(CatalogException.class, () -> sql("CREATE DATABASE "  + databaseNameNew), "No permission. CREATE DATABASE");

        oldSql("GRANT CREATE ON CATALOG " + catalogName + " TO ROLE " + testRole1);

        newSql("USE CATALOG " + catalogName);
        sql("CREATE DATABASE " +  databaseNameNew).collect();

        //Switch User
        sql("USE DATABASE " + databaseName).collect();

        //desc table
        assertThrows(CatalogException.class, () -> sql("DESC TABLE " + tableName), "No permission. DESC TABLE");

        oldSql("GRANT DESC ON TABLE " + catalogName + "." + databaseName + "." + tableName + " TO ROLE " + testRole1);

        newSql("USE CATALOG " + catalogName);
        sql("USE DATABASE " + databaseName);
        Row[] rows = (Row[]) sql("DESC TABLE EXTENDED " + tableName).collect();
        String provider = null;
        for (Row row : rows) {
            if (row.getString(0).equalsIgnoreCase("Provider")) {
                provider = row.getString(1);
            }
        }
        assertEquals(provider, "parquet");
    }

    @AfterAll
    public static void afterAll() {
        CatalogPlugin client = PolyCatClientHelper.getClientFromSession(spark);
        client.setContext(CatalogUserInformation.doAuth("test", "dash"));
    }
}
