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
import static io.polycat.catalog.spark.SparkCatalogTestEnv.getRecordValueOfTitle;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.getUUIDName;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.sql;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

@ExtendWith({SparkCatalogTestEnv.class})
public class CatalogCommandTest {

    @Test
    public void createCatalogSuccess() {
        String catalogName = getUUIDName();
        sql("CREATE CATALOG " + catalogName);
    }

    @Test
    public void descCatalogSuccess() {
        String catalogName = createCatalog();
        Row[] rows = (Row[])sql("DESC CATALOG " + catalogName).collect();
        sql("DESC CATALOG " + catalogName).show();
        assertEquals(catalogName, getRecordValueOfTitle(rows, "Catalog name"));
        assertNotEquals("", getRecordValueOfTitle(rows, "Create time"));
    }

    @Test
    public void useCatalogSuccess() {
        String catalogName = createCatalog();
        sql("USE CATALOG " + catalogName);
    }

    @Test
    public void showCatalogsSuccess() {
        sql("SHOW CATALOGS");
    }

    @Test
    public void dropCatalogSuccess() {
        String catalogName = createCatalog();
        sql("DROP CATALOG " + catalogName);
    }

    @Test
    public void listCatalogCommitsSuccess() {
        sql("CREATE CATALOG COMMITS_C2");
        sql("USE CATALOG COMMITS_C2");
        sql("CREATE DATABASE COMMITS_DB2");
        sql("USE DATABASE COMMITS_DB2");
        sql("CREATE TABLE COMMITS_T2 (C1 STRING, C2 INT) using parquet");
        Row[] rows = (Row[])sql("SHOW HISTORY FOR CATALOG COMMITS_C2").collect();
        assertEquals(rows.length, 3);
        assertEquals(rows[0].getString(4), "CREATE TABLE");
        assertEquals(rows[1].getString(4), "CREATE DATABASE");
        assertEquals(rows[2].getString(4), "CREATE CATALOG");
    }

}
