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

import java.util.UUID;

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static io.polycat.catalog.spark.SparkCatalogTestEnv.createBranchOn;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.createCatalog;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.createDatabase;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.createTable;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.sql;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith({SparkCatalogTestEnv.class})
public class BranchCommandTest {

    @Test
    public void createBranchSuccess() {
        String catalogName = createCatalog();
        String branchName = UUID.randomUUID().toString().replace("-", "");
        sql("CREATE BRANCH " + branchName + " ON " + catalogName);
    }

    @Test
    public void showBranchSuccess() {
        String catalogName = createCatalog();
        String branchName = createBranchOn(catalogName);
        sql("SHOW BRANCHES ON " + catalogName);
    }

    @Test
    public void createTableOnBranchSuccess() {
        String catalogName = createCatalog();
        String databaseName = createDatabase(catalogName);
        String branchName = createBranchOn(catalogName);
        String tableName = createTable(branchName, databaseName, "parquet");

        sql("use catalog " + branchName);
        sql("use database " + databaseName);

        Row[] rows = (Row[])sql("desc table EXTENDED " + tableName).collect();
        String provider = null;
        for (Row row : rows) {
            if (row.getString(0).equalsIgnoreCase("Provider")) {
                provider = row.getString(1);
            }
        }
        assertEquals(provider, "parquet");
    }

    @Test
    public void undropTableOnBranchSuccess() {
        String catalogName = createCatalog();
        String databaseName = createDatabase(catalogName);
        String branchName = createBranchOn(catalogName);
        String tableName = createTable(branchName, databaseName, "parquet");

        sql("use catalog " + branchName);
        sql("use database " + databaseName);

        // describe table extended
        Row[] rows = (Row[])sql("desc table EXTENDED " + tableName).collect();
        String provider = null;
        for (Row row : rows) {
            if (row.getString(0).equalsIgnoreCase("Provider")) {
                provider = row.getString(1);
            }
        }
        assertEquals(provider, "parquet");

        sql("drop table " + tableName);
        //assertThrows(ClassCastException.class, () -> sql("desc table EXTENDED " + tableName));

        sql("undrop table " + tableName);
        Row[] rows1 = (Row[])sql("desc table EXTENDED " + tableName).collect();
        for (Row row : rows) {
            if (row.getString(0).equalsIgnoreCase("Provider")) {
                provider = row.getString(1);
            }
        }
        assertEquals(provider, "parquet");
    }

    @Test
    public void restoreTableOnBranchSuccess() {
        String catalogName = createCatalog();
        String databaseName = createDatabase(catalogName);
        String branchName = createBranchOn(catalogName);
        String tableName = createTable(branchName, databaseName, "csv");

        sql("use catalog " + branchName);
        sql("use database " + databaseName);

        Row[] rows = (Row[])sql("SHOW HISTORY FOR TABLE " + tableName).collect();
        assertEquals(rows.length, 1);
        String version = rows[0].getString(3);

        sql("RESTORE TABLE " + tableName + " VERSION AS OF '" + version + "'");
        Row[] rows1 = (Row[])sql("SHOW HISTORY FOR TABLE " + tableName).collect();
        assertEquals(rows1.length, 2);
    }

    @Test
    public void showTablesOnBranch() {
        String catalogName = createCatalog();
        String databaseName = createDatabase(catalogName);

        // create table on catalog.db
        sql("use catalog " + catalogName);
        sql("use database " + databaseName);
        String tableName1 = createTable(catalogName,databaseName,"parquet");
        String sqlStatement = "insert into " + tableName1 + " values('aaa', 1), ('bbb', 2) ";
        sql(sqlStatement);
        Row[] rows = (Row[])sql("show tables").collect();
        assertEquals(1, rows.length);

        // create table on branch.db
        String branchName = createBranchOn(catalogName);
        sql("use branch " + branchName);
        sql("use database " + databaseName);
        String tableName2 = createTable(branchName, databaseName, "parquet");
        Row[] rows1 = (Row[])sql("show tables").collect();
        assertEquals(2, rows1.length);
    }

}
