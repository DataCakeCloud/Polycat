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

import io.polycat.catalog.client.CatalogUserInformation;
import io.polycat.catalog.common.plugin.CatalogPlugin;

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static io.polycat.catalog.spark.SparkCatalogTestEnv.batch;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.checkAnswer;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.getProjectId;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.getRecordValueOfTitle;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.getUUIDName;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.row;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.spark;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.sql;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

@ExtendWith({SparkCatalogTestEnv.class})
public class ShareCommandTest {

    private final String testAccount1 = "'testAccount1:testAccount1'";
    private final String testAccount2 = "'testAccount2:testAccount2'";

    //The authentication information used in the test must be the same as that in the configuration file.
    private static String projectIdNew = "chengdu";
    private static String passwordNew = "huawei";
    private static String userLucy = "lucy";
    private static String userLilei = "lilei";
    private static String tenantNew = "tenantB";


    @AfterEach
    void clearTestEnv() {
        CatalogPlugin client = PolyCatClientHelper.getClientFromSession(spark);
        client.setContext(CatalogUserInformation.doAuth("test", "dash"));
    }

    @Test
    public void createShareSuccess() {
        String shareName1 = "share_" + getUUIDName().substring(0, 8);
        String shareName2 = "share_" + getUUIDName().substring(0, 8);

        sql("CREATE SHARE " + shareName1);
        sql("CREATE SHARE " + shareName2);
        sql("DROP SHARE " + shareName1);
        sql("DROP SHARE " + shareName2);
    }

    @Test
    public void descShareSuccess() {
        String shareName1 = "share_" + getUUIDName().substring(0, 8);

        sql("CREATE SHARE " + shareName1);
        Row[] rows = (Row[]) sql("DESC SHARE " + shareName1).collect();
        assertEquals(shareName1, getRecordValueOfTitle(rows, "share name"));
        assertNotEquals("", getRecordValueOfTitle(rows, "create time"));
        sql("DROP SHARE " + shareName1);
    }

    @Test
    public void showSharesSuccess() {

        String shareName1 = "share_" + getUUIDName().substring(0, 8);
        String shareName2 = "share_" + getUUIDName().substring(0, 8);

        sql("CREATE SHARE " + shareName1);
        sql("CREATE SHARE " + shareName2);
        sql("SHOW SHARES");
        sql("DROP SHARE " + shareName1);
        sql("DROP SHARE " + shareName2);
    }

    @Test
    public void addAccountToShareSuccess() {

        String shareName1 = "share_" + getUUIDName().substring(0, 8);

        sql("CREATE SHARE " + shareName1);
        sql("ALTER SHARE " + shareName1 + " ADD ACCOUNTS = " + testAccount1);
        sql("ALTER SHARE " + shareName1 + " REMOVE ACCOUNTS = " + "'testAccount1'");
        sql("DROP SHARE " + shareName1);
    }

    @Test
    public void grantPrivilegeToShareSuccess() {

        String catalogName = "catalog_" + getUUIDName().substring(0, 8);
        String databaseName = "database_" + getUUIDName().substring(0, 8);
        String tableName = "table_" + getUUIDName().substring(0, 8);
        String shareName = "share_" + getUUIDName().substring(0, 8);

        sql("CREATE CATALOG " + catalogName);
        sql("USE CATALOG " + catalogName);
        sql("CREATE DATABASE " + databaseName);
        sql("USE DATABASE " + catalogName + "." + databaseName);
        sql("CREATE TABLE " + tableName + "(c1 string, c2 int)  using parquet");
        sql("CREATE SHARE " + shareName);
        sql("GRANT SELECT ON TABLE " + catalogName + "." + databaseName + "." + tableName +
            " TO SHARE " + shareName);
        sql("ALTER SHARE " + shareName + " ADD ACCOUNTS = " + testAccount1);
        sql("ALTER SHARE " + shareName + " ADD ACCOUNTS = " + testAccount2);
        sql("SHOW SHARES");
        sql("REVOKE SELECT ON TABLE " + catalogName + "." + databaseName + "." + tableName +
            " FROM SHARE " + shareName);

    }


    @Test
    public void selectFromShareSuccess() {

        String catalogName = "catalog_" + getUUIDName().substring(0, 8);
        String databaseName = "database_" + getUUIDName().substring(0, 8);
        String tableName = "table_" + getUUIDName().substring(0, 8);
        String shareName = "share_" + getUUIDName().substring(0, 8);

        sql("CREATE CATALOG " + catalogName);
        sql("USE CATALOG " + catalogName);
        sql("CREATE DATABASE " + catalogName + "." + databaseName);
        sql("USE DATABASE " + catalogName + "." + databaseName);
        sql("CREATE TABLE " + tableName + "(c1 string, c2 int) using parquet");
        sql("insert into " + tableName + " values('ab', 1)");
        sql("CREATE SHARE " + shareName);
        sql("GRANT SELECT ON TABLE " + catalogName + "." + databaseName + "." + tableName +
            " TO SHARE " + shareName);

        sql("select c1,c2 from " + getProjectId() + "." + shareName + "." + databaseName + "." + tableName).show();
        checkAnswer(sql("select c1,c2 from " + getProjectId() + "." + shareName + "." + databaseName + "." + tableName),
            batch(row("ab", 1)));
        checkAnswer(
            sql("select count(*) from " + getProjectId() + "." + shareName + "." + databaseName + "." + tableName),
            batch(row(1L)));
    }

    @Test
    public void removeAccountFromShareSuccess() {

        String shareName1 = "share_" + getUUIDName().substring(0, 8);
        String shareName2 = "share_" + getUUIDName().substring(0, 8);

        sql("CREATE SHARE " + shareName1);
        sql("CREATE SHARE " + shareName2);
        sql("ALTER SHARE " + shareName1 + " ADD ACCOUNTS = " + testAccount1);
        sql("ALTER SHARE " + shareName2 + " ADD ACCOUNTS = " + testAccount1);
        sql("ALTER SHARE " + shareName1 + " REMOVE ACCOUNTS = " + "'testAccount1'");
        sql("ALTER SHARE " + shareName2 + " REMOVE ACCOUNTS = " + "'testAccount1'");
        sql("DROP SHARE " + shareName1);
        sql("DROP SHARE " + shareName2);
    }

    @Test
    public void dropShareSuccess() {

        String shareName1 = "share_" + getUUIDName().substring(0, 8);
        String shareName2 = "share_" + getUUIDName().substring(0, 8);

        sql("CREATE SHARE " + shareName1);
        sql("CREATE SHARE " + shareName2);
        sql("DROP SHARE " + shareName1);
        sql("DROP SHARE " + shareName2);
    }


    @Test
    public void shareIntegrationForTableTest() {
        //tenant A create table1 insert table
        String catalogName = "catalog_" + getUUIDName().substring(0, 8);
        String shareName = "share_" + getUUIDName().substring(0, 8);
        String databaseName = "database_" + getUUIDName().substring(0, 8);
        String tableName = "table_" + getUUIDName().substring(0, 8);

        sql("CREATE CATALOG " + catalogName);
        sql("USE CATALOG " + catalogName);
        sql("CREATE DATABASE " + databaseName);
        sql("USE DATABASE " + databaseName);
        sql("CREATE TABLE " + tableName + " (c1 string, c2 int)  using parquet");

        sql("insert into " + tableName + " values('ab', 1)");

        //tenant A create share to tenant B : lucy
        sql("CREATE SHARE " + shareName);

        sql("GRANT SELECT ON TABLE " + catalogName + "." + databaseName + "." + tableName +
            " TO SHARE " + shareName);
        sql("ALTER SHARE " + shareName + " ADD ACCOUNTS = " + "'tenantB:lucy'");

        //tenant B : lucy select table success
        CatalogPlugin client = PolyCatClientHelper.getClientFromSession(spark);
        client.setContext(CatalogUserInformation.doAuth(userLucy, passwordNew));
        checkAnswer(
            sql("select c1,c2 from " + getProjectId() + "." + shareName + "." + databaseName + "." + tableName),
            batch(row("ab", 1)));

        checkAnswer(sql("select count(*) from " + getProjectId() + "." + shareName + "." + databaseName +
                "." + tableName),
            batch(row(1L)));

        //tenant B : lucy grant share to lilei
        checkAnswer(sql("GRANT SHARE " + getProjectId() + "." + shareName + " TO USER " + userLilei),
            batch());

        //tenant B : lilei select table success
        client.setContext(CatalogUserInformation.doAuth(userLilei, passwordNew));
        checkAnswer(
            sql("select c1,c2 from " + getProjectId() + "." + shareName + "." + databaseName + "." + tableName),
            batch(row("ab", 1)));
    }

}
