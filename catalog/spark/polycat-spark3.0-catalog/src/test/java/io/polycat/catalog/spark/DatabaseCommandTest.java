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

import java.util.Collections;

import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.model.record.Record;
import io.polycat.common.RowBatch;

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static io.polycat.catalog.spark.SparkCatalogTestEnv.createCatalog;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.getUUIDName;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.sql;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith({SparkCatalogTestEnv.class})
public class DatabaseCommandTest {
    private static String catalogName;
    private static String databaseNamePrefix;

    @BeforeEach
    public void databaseCommandBeforeEach() {
        catalogName = createCatalog();
        databaseNamePrefix = "databaseName".toLowerCase();
        sql("USE CATALOG " + catalogName);
    }

    @AfterEach
    public void databaseCommandAfterEach() {
        catalogName = null;
    }

    @Test
    public void createDatabaseSuccess() {
        String databaseName = databaseNamePrefix + getUUIDName();
        sql("CREATE DATABASE " + databaseName);
    }

    @Test
    public void dropDatabaseSuccess() {
        String databaseName = databaseNamePrefix + getUUIDName();
        sql("CREATE DATABASE " + databaseName);

        // describe database extended
        Row[] rows = (Row[]) sql("DESC DATABASE " + databaseName).collect();
        assertEquals(rows.length, 6);
        for (Row row : rows) {
            if (row.getString(0).equals("Catalog name")) {
                assertEquals(row.getString(1), catalogName);
            } else if (row.getString(0).equals("Database name")) {
                assertEquals(row.getString(1), databaseName);
            }
        }
        //check result
        sql("DROP DATABASE " +  databaseName);
        CatalogException e = assertThrows(CatalogException.class, () ->
            sql("DESC DATABASE " +   databaseName));
        assertEquals(e.getMessage(), String.format("Database [%s] not found", databaseName));
    }

    @Test
    public void undropDatabaseSuccess() {
        String databaseName = databaseNamePrefix + getUUIDName();
        sql("CREATE DATABASE "  + databaseName);

        sql("DROP DATABASE " + databaseName);
        CatalogException exception = assertThrows(CatalogException.class,
            () -> sql("DESC DATABASE "  + databaseName));
        assertEquals(exception.getMessage(), String.format("Database [%s] not found", databaseName));

        sql("UNDROP DATABASE "  + databaseName);
        Row[] rows = (Row[])  sql("DESC DATABASE " + databaseName).collect();
        assertEquals(rows.length, 6);
        for (Row row : rows) {
            if (row.getString(0).equals("Catalog Name")) {
                assertEquals(row.getString(1), catalogName);
            } else if (row.getString(0).equals("Database name")) {
                assertEquals(row.getString(1), databaseName);
            }
        }
    }

    @Test
    public void descDatabaseWithCatalogSuccess() {
        String databaseName = databaseNamePrefix +  getUUIDName();
        sql("CREATE DATABASE " + databaseName);

        Row[] rows = (Row[]) sql("DESC DATABASE " + databaseName).collect();
        assertEquals(rows.length, 6);
        for (Row row : rows) {
            if (row.getString(0).equals("Catalog Name")) {
                assertEquals(row.getString(1), catalogName);
            } else if (row.getString(0).equals("Database name")) {
                assertEquals(row.getString(1), databaseName);
            }
        }
    }

    @Test
    public void descDatabaseSuccess() {
        String databaseName = databaseNamePrefix +  getUUIDName();
        sql("CREATE DATABASE " + databaseName);

        sql("use catalog " + catalogName);

        Row[] rows = (Row[]) sql("DESC DATABASE " + databaseName).collect();
        assertEquals(rows.length, 6);
        for (Row row : rows) {
            if (row.getString(0).equals("Catalog Name")) {
                assertEquals(row.getString(1), catalogName);
            } else if (row.getString(0).equals("Database name")) {
                assertEquals(row.getString(1), databaseName);
            }
        }
    }

    @Test
    public void useDatabaseWithCatalogSuccess() {
        String databaseNme = databaseNamePrefix +  getUUIDName();
        sql("CREATE DATABASE "  + databaseNme);
        sql("USE DATABASE " + databaseNme);
    }

    @Test
    public void useDatabaseSuccess() {
        String databaseNme = databaseNamePrefix +  getUUIDName();
        sql("CREATE DATABASE "  + databaseNme);
        sql("USE CATALOG " + catalogName);
        sql("USE DATABASE " + databaseNme);
    }

    @Test
    public void showDatabasesSuccess() {
        String databaseName1 = databaseNamePrefix +  getUUIDName();
        sql("CREATE DATABASE "  + databaseName1);
        sql("USE CATALOG " + catalogName);

        Row[] rows = (Row[])sql("SHOW DATABASES ").collect();
        RowBatch expectedListRecords = new RowBatch(Collections.singletonList(
            new Record(catalogName, databaseName1)
        ));

        String databaseName2 = databaseNamePrefix +  getUUIDName();
        sql("CREATE DATABASE " + databaseName2);

        Row[] rows1 = (Row[])sql("SHOW DATABASES ").collect();
        assertEquals(2, rows1.length);
        for (Row row : rows1) {
            assertTrue(databaseName1.equals(row.getString(1))
                || databaseName2.equals(row.getString(1)));
        }
    }

}
