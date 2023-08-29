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

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static io.polycat.catalog.spark.SparkCatalogTestEnv.createCatalog;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.createDatabase;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.createTable;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.getRecordValueOfTitle;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.sql;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

@ExtendWith({SparkCatalogTestEnv.class})
public class RoleCommandTest {

    private final String testRole1 = "testrole1";
    private final String testRole2 = "testrole2";
    private final String testAccount1 = "'testAccount1'";
    private final String testUser1 = "testUser1";
    private final String testCatalog = "testCatalog";
    private final String testDatabase = "testDatabase";
    private final String testTable = "testTable";

    @Test
    public void createRoleSuccess() {
        sql("CREATE ROLE " + testRole1);
        sql("CREATE ROLE " + testRole2);
        sql("DROP ROLE " + testRole1);
        sql("DROP ROLE " + testRole2);
    }

    @Test
    public void descRoleSuccess() {
        sql("CREATE ROLE " + testRole1);
        Row[] rows = (Row[]) sql("DESC ROLE " + testRole1).collect();
        assertEquals(testRole1, getRecordValueOfTitle(rows, "Role name"));
        assertNotEquals("", getRecordValueOfTitle(rows, "Create time"));
        sql("DROP ROLE " + testRole1);
    }

    @Test
    public void grantRoleToUserSuccess() {
        sql("CREATE ROLE " + testRole1);
        sql("GRANT ROLE " + testRole1 + " TO USER " + testUser1);
        sql("REVOKE ROLE " + testRole1 + " FROM USER " + testUser1);
        sql("DROP ROLE " + testRole1);
    }

    @Test
    public void showRolesSuccess() {
        sql("CREATE ROLE " + testRole1);
        sql("GRANT ROLE " + testRole1 + " TO USER " + testUser1);
        sql("SHOW ROLES").show();
        sql("REVOKE ROLE " + testRole1 + " FROM USER " + testUser1);
        sql("DROP ROLE " + testRole1);
    }

    @Test
    public void testforcreatetbale() {
        sql("create catalog c1");
        sql("use catalog c1");
        sql("create database db1");
        sql("use database db1");
        sql("create table t1  (c1 string, c2 int) using parquet");
    }

    @Test
    public void grantPrivilegeToRoleSuccess() {
        sql("CREATE CATALOG " + testCatalog);
        sql("USE CATALOG " + testCatalog);
        sql("CREATE DATABASE " + testDatabase);
        sql("USE  DATABASE " +  testDatabase);
        sql("CREATE TABLE " + testTable  + " (c1 string, c2 int) using parquet");
        sql("CREATE ROLE " + testRole1);
        sql("GRANT create ON CATALOG " + testCatalog + " TO ROLE " + testRole1);
        sql("GRANT select ON TABLE " +
            testCatalog + "." + testDatabase + "." + testTable + " TO ROLE " + testRole1);
        sql("SHOW ROLES");
        sql("SHOW GRANTS TO ROLE " + testRole1);
        sql("REVOKE select ON TABLE " +
            testCatalog + "." + testDatabase + "." + testTable + " FROM ROLE " + testRole1);
        sql("DROP ROLE " + testRole1);
    }


    @Test
    public void revokeRoleFromUserSuccess() {
        sql("CREATE ROLE " + testRole1);
        sql("GRANT ROLE " + testRole1 + " TO USER " + testUser1);
        sql("REVOKE ROLE " + testRole1 + " FROM USER " + testUser1);
        sql("DROP ROLE " + testRole1);
    }

    @Test
    public void dropRoleSuccess() {
        sql("CREATE ROLE " + testRole1);
        sql("CREATE ROLE " + testRole2);
        sql("DROP ROLE " + testRole1);
        sql("DROP ROLE " + testRole2);
    }
}
