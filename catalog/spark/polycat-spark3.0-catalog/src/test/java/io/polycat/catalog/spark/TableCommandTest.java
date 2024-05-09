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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;


import static io.polycat.catalog.spark.SparkCatalogTestEnv.batch;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.checkAnswer;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.createCatalog;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.createDatabase;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.createTable;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.getRecordValueOfTitle;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.getUUIDName;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.hasTitle;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.row;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.sql;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith({SparkCatalogTestEnv.class})
public class TableCommandTest {
    private static String catalogName;
    private static String databaseName;

    @BeforeEach
    public void databaseCommandBeforeEach() {
        catalogName = createCatalog();
        sql("use catalog " + catalogName);

        databaseName = createDatabase(catalogName);
        sql("use database " + databaseName);
    }

    @AfterEach
    public void databaseCommandAfterEach() {
        catalogName = null;
        databaseName = null;
    }

    @Test
    public void dropTableSuccess() {
        String tableName = createTable(catalogName, databaseName,"parquet");

        // describe table extended
        Row[] rows = (Row[]) sql("desc table EXTENDED " + tableName).collect();
        String provider = null;
        for (Row row : rows) {
            if (row.getString(0).equalsIgnoreCase("Provider")) {
                provider = row.getString(1);
            }
        }
        assertEquals(provider, "parquet");

        sql("drop table " + tableName);

        //check result
        //assertThrows(ClassCastException.class, () ->
        //    sql("desc table EXTENDED " +   tableName), "Table [" + tableName + "] not found");
    }

    @Test
    public void dropTableRepeat() {
        String tableName = createTable(catalogName, databaseName, "parquet");

        // describe table extended
        Row[] rows = (Row[]) sql("desc table EXTENDED " + tableName).collect();
        String provider = null;
        for (Row row : rows) {
            if (row.getString(0).equalsIgnoreCase("Provider")) {
                provider = row.getString(1);
            }
        }
        assertEquals(provider, "parquet");

        sql("drop table " + tableName);

        /*
        //check result
        assertThrows(ClassCastException.class, () ->
           sql("desc table EXTENDED " +   tableName), "Table [" + tableName + "] not found");
        // repeat drop table
        if(SparkVersionUtil.isSpark30()) {
            assertThrows(CatalogException.class, () ->
                sql("drop table " + tableName), "Table [" + tableName + "] not found");
        } else {
            assertThrows(ClassCastException.class, () ->
                sql("drop table " +   tableName), "Table [" + tableName + "] not found");
        }
        */
    }

    @Test
    public void purgeTableSuccess() {
        String tableName = createTable(catalogName, databaseName, "parquet");

        // describe table extended
        Row[] rows = (Row[]) sql("desc table EXTENDED " + tableName).collect();
        String provider = null;
        for (Row row : rows) {
            if (row.getString(0).equalsIgnoreCase("Provider")) {
                provider = row.getString(1);
            }
        }
        assertEquals(provider, "parquet");

        sql("drop table " + tableName);
        sql("purge table " + tableName);

        /*
        //check result
        assertThrows(ClassCastException.class, () ->
            sql("desc table EXTENDED " +   tableName), "Table [" + tableName + "] not found");

        CatalogException  exception = assertThrows(CatalogException.class,
            () -> sql("undrop table " + tableName));
        assertEquals(String.format("Table [%s] not found", tableName), exception.getMessage());
         */
    }

    @Test
    public void undropTableSuccess() {
        String tableName = createTable(catalogName, databaseName, "parquet");

        // describe table extended
        Row[] rows = (Row[]) sql("desc table EXTENDED " + tableName).collect();
        assertTrue(rows.length > 0);

        sql("drop table " + tableName);

        /*
        //check result
        assertThrows(ClassCastException.class, () ->
            sql("desc table EXTENDED " +   tableName), "Table [" + tableName + "] not found");
        */

        sql("undrop table " + tableName);

        //check result
        Row[] rows1 = (Row[]) sql("desc table EXTENDED " + tableName).collect();
        assertTrue(rows1.length > 0);
    }

    @Test
    public void restoreTableSuccess() {
        String tableName = createTable(catalogName, databaseName, "csv");

        Row[] rows = (Row[]) sql("SHOW HISTORY FOR TABLE " + tableName).collect();
        assertEquals(rows.length, 1);
        String version = rows[0].getString(3);

        sql("RESTORE TABLE " + tableName + " VERSION AS OF '" + version + "'");

        //check result
        Row[] rows1 = (Row[])sql("SHOW HISTORY FOR TABLE " + tableName).collect();
        assertEquals(rows1.length, 2);
    }

    @Test
    public void listTableCommitsSuccess() {
        sql("CREATE CATALOG COMMITS_C3");
        sql("USE CATALOG COMMITS_C3");
        sql("CREATE DATABASE COMMITS_DB3");
        sql("USE DATABASE COMMITS_DB3");
        sql("CREATE TABLE COMMITS_T3 (C1 STRING, C2 INT) USING PARQUET");
        sql("SHOW HISTORY FOR TABLE COMMITS_T3");
    }

    @Test
    public void should_desc_and_list_tables_success() {
        String catalogName = "list_c1";
        String databaseName = "db1";
        String tableName = "t1";
        sql("CREATE CATALOG " + catalogName);
        sql("USE CATALOG " + catalogName);
        sql("CREATE DATABASE " + databaseName);
        sql("USE DATABASE " + databaseName);
        sql("CREATE TABLE " + tableName + " (c1 string) using parquet");
        sql("DESC TABLE " + tableName);

        sql("DESC TABLE EXTENDED " + tableName).show();
        Row[] rows = (Row[])sql("DESC TABLE EXTENDED " + tableName).collect();
        assertTrue(hasTitle(rows, "Name"));
        assertTrue(hasTitle(rows, "Provider"));

        Row[] rows1 = (Row[])sql("SHOW ALL TABLES LIMIT=10").collect();
        assertEquals(1, rows1.length);
        assertEquals(catalogName, rows1[0].getString(0));
        assertEquals(databaseName, rows1[0].getString(1));
        assertEquals(tableName, rows1[0].getString(2));
        assertNotNull(rows1[0].getString(3));
        assertNotNull(rows1[0].getString(4));
    }

    @Test
    public void show_table_partitions_success() {
        String catalogName = "show_partitions_c1";
        String databaseName = "show_partitions_db1";
        String tableName = "show_partitions_t1";
        sql("CREATE CATALOG " + catalogName);
        sql("USE CATALOG " + catalogName);
        sql("CREATE DATABASE " + databaseName);
        sql("USE DATABASE " + databaseName);
        sql("create table " + tableName
            + " (c1 string, c2 int, c3 string) using parquet partitioned by (c2, c3)"
            + " ");

        sql("INSERT INTO " + tableName + " PARTITION (c2 = 2, c3 = 'country') VALUES ('china'), ('india')");
        checkAnswer(sql("SELECT c1, c2, c3 FROM " + tableName + " order by c1 ASC"),
            batch(row("china", 2, "country"), row("india", 2, "country")));

        Row[] rows = (Row[]) sql("SHOW PARTITIONS " + tableName).collect();
        assertEquals(0, rows.length);
    }

    @Test
    public void alter_tables_name_success1() {
        String tableName = createTable(catalogName, databaseName, "parquet");
        sql("USE CATALOG " + catalogName);
        sql("USE DATABASE " + databaseName);
        String tableNameNew = "tableName_new_".toLowerCase() + getUUIDName();

        Row[] rows = (Row[]) sql("DESC TABLE EXTENDED " + tableName).collect();
        assertEquals("parquet", getRecordValueOfTitle(rows, "Provider"));

        sql("ALTER TABLE " + tableName + " RENAME TO " + tableNameNew);

        rows = (Row[])sql("DESC TABLE EXTENDED " + tableNameNew).collect();
        assertEquals("parquet", getRecordValueOfTitle(rows, "Provider"));

        sql("ALTER TABLE " + tableNameNew + " RENAME TO " + tableName);
        rows = (Row[]) sql("DESC TABLE EXTENDED " + tableName).collect();
        assertEquals("parquet", getRecordValueOfTitle(rows, "Provider"));

    }

    @Test
    public void should_desc_tables_with_properties_success() {
        String catalogName = "property_c1";
        String databaseName = "db1";
        String tableName = "t1";
        sql("CREATE CATALOG " + catalogName);
        sql("USE CATALOG " + catalogName);
        sql("CREATE DATABASE " + databaseName);
        sql("USE DATABASE " + databaseName);
        sql("CREATE TABLE " + tableName + " (c1 string) using parquet  TBLPROPERTIES ('key'='value', 'key1' = 'value1')");

        Row[]  rows = (Row[])sql("DESC TABLE EXTENDED " + tableName).collect();
        assertEquals("[catalog_name=property_c1,database_name=db1,key=value,key1=value1,table_name=t1]", getRecordValueOfTitle(rows, "Table Properties"));
    }

    @Test
    public void should_set_unset_tables_properties_success() {
        String catalogName = "set_property_c1";
        String databaseName = "set_property_db1";
        String tableName = "t1";
        sql("CREATE CATALOG " + catalogName);
        sql("USE CATALOG " + catalogName);
        sql("CREATE DATABASE " + databaseName);
        sql("USE DATABASE " + databaseName);
        sql("CREATE TABLE " + tableName + " (c1 string) using parquet TBLPROPERTIES ('key'='value', 'key1' = 'value1')");

        Row[]  rows = (Row[]) sql("DESC TABLE EXTENDED " + tableName).collect();
        assertEquals("[catalog_name=set_property_c1,database_name=set_property_db1,key=value,key1=value1,table_name=t1]",
            getRecordValueOfTitle(rows, "Table Properties"));

        sql("ALTER TABLE " + tableName + " SET TBLPROPERTIES ('key2'='value2', 'key1' = 'value1-1')");

        sql("DESC TABLE EXTENDED " + tableName).show();
        rows = (Row[])sql("DESC TABLE EXTENDED " + tableName).collect();
        assertEquals("[catalog_name=set_property_c1,database_name=set_property_db1,key=value,key1=value1-1,key2=value2,table_name=t1]",
            getRecordValueOfTitle(rows, "Table Properties"));

        sql("ALTER TABLE " + tableName+ " UNSET TBLPROPERTIES ('key')");

        sql("DESC TABLE EXTENDED " + tableName).show();
        rows = (Row[])sql("DESC TABLE EXTENDED " + tableName).collect();
        assertEquals("[catalog_name=set_property_c1,database_name=set_property_db1,key1=value1-1,key2=value2,table_name=t1]", getRecordValueOfTitle(rows, "Table Properties"));
    }

    @Test
    public void part_col_at_any_pos_should_success() {
        String tableName = "set_first_col_as_part";
        sql("create table " + tableName
            + " (c1 string, c2 string) using parquet partitioned by (c1) ");

        sql("INSERT INTO " + tableName + " PARTITION (c1 = 'city') VALUES ('chengdu'), ('shenzhen')");
        checkAnswer(sql("SELECT c2, c1 FROM " + tableName + " order by c2 ASC"),
            batch(row("chengdu", "city"), row("shenzhen", "city")));
    }

    @Test
    public void drop_column_should_success() {
        String tableName = "table_with_pt";
        sql("create table " + tableName
            + " (c3 string, c2 string, c1 string) using parquet partitioned by (c1)");
        sql("INSERT INTO " + tableName + " PARTITION (c1 = 'city') VALUES ('sichuan', 'chengdu'), ('guangdong', 'shenzhen')");
        sql("ALTER TABLE " + tableName + " DROP COLUMN c3").show();
        sql("DESC TABLE " + tableName).show();
        checkAnswer(sql("SELECT * FROM " + tableName + " order by c2 ASC"),
            batch(row("chengdu", "city"), row("shenzhen", "city")));
    }

    @Test
    void should_success_query_with_arithmetic_expression() {
        sql("CREATE TABLE expression (c1 int, c2 int) using parquet");
        sql("insert into expression values(10, 2), (10, 3)");
        checkAnswer(sql("select c1 + c2 as res from expression order by res"), batch(row(12), row(13)));
        checkAnswer(sql("select c1 - c2 as res from expression order by res"), batch(row(7), row(8)));
        checkAnswer(sql("select c1 * c2 as res from expression order by res"), batch(row(20), row(30)));
        checkAnswer(sql("select c1 / c2 as res from expression order by res"),
            batch(row(3.3333333333333335d), row(5d)));
        checkAnswer(sql("select c1 % c2 as res from expression order by res"), batch(row(0), row(1)));
    }
}
