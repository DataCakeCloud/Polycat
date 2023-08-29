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
import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.model.StorageDescriptor;
import io.polycat.catalog.common.model.Table;
import io.polycat.catalog.common.plugin.request.CreateTableRequest;
import io.polycat.catalog.common.plugin.request.DeleteTableRequest;
import io.polycat.catalog.common.plugin.request.GetTableRequest;
import io.polycat.catalog.common.plugin.request.ListFileRequest;
import io.polycat.catalog.common.plugin.request.base.DatabaseRequestBase;
import io.polycat.catalog.common.plugin.request.input.TableInput;
import io.polycat.catalog.common.model.Column;
import io.polycat.catalog.common.types.DataTypes;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TableTest extends HMSBridgeTestEnv{
    private final static String catalogName = "lms";
    private final static String dbName = "db1";

    private DatabaseRequestBase buildDbRequestBase(DatabaseRequestBase request) {
        request.setCatalogName(catalogName);
        request.setDatabaseName(dbName);
        request.setProjectId(catalogClient.getProjectId());
        return request;
    }

    private Table getTableFromCLIByName(String name) {
        GetTableRequest request = new GetTableRequest(name);
        request = (GetTableRequest) buildDbRequestBase(request);
        Table table = null;
        try {
            table = catalogClient.getTable(request);
        } catch (Exception e) {
        }
        return table;
    }

    private Table createTableFromCLI(String name) {
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("c1", DataTypes.STRING.getName()));
        columns.add(new Column("c2", DataTypes.INT.getName()));
        StorageDescriptor si = new StorageDescriptor();
        si.setLocation(targetPath);
        si.setSourceShortName("parquet");
        si.setColumns(columns);

        TableInput tableInput = new TableInput();
        tableInput.setTableName(name);
        tableInput.setStorageDescriptor(si);
        tableInput.setOwner("dash");
        Map<String, String> prop = new HashMap<String, String>()  {{
            put("lms_name", catalogName);
        }};
        tableInput.setParameters(prop);
        tableInput.setTableType("EXTERNAL_TABLE");

        CreateTableRequest request = new CreateTableRequest();
        request.setInput(tableInput);
        request = (CreateTableRequest) buildDbRequestBase(request);
        Table table = catalogClient.createTable(request);
        return table;
    }

    @BeforeAll
    public static void createDatabase () {
        sparkSession.sql(String.format("CREATE DATABASE %s;", dbName));
        Map<String, String> dbproperties = new HashMap<String, String>() {{
                put("lms_name", catalogName);
        }};
        createDatabase(dbName, dbproperties);
        sparkSession.sql(String.format("USE %s;", dbName));
    }

    @Test
    public void create_lms_table_and_show_tables_test() {
        sparkSession.sql(String.format("CREATE TABLE IF NOT EXISTS tb1 (c1 string, c2 int) " +
                "LOCATION '%s' TBLPROPERTIES('lms_name'='lms');", targetPath));
        createTableFromCLI("tb2");
        Assertions.assertEquals(2L, sparkSession.sql("SHOW TABLES;").count());
        sparkSession.sql("DROP TABLE tb2;").collect();
        sparkSession.sql("DROP TABLE tb1;").collect();
    }

    @Test
    public void create_and_drop_table_from_beeline_test() {
        sparkSession.sql(String.format("CREATE TABLE IF NOT EXISTS tb1 (c1 string, c2 int) " +
                "LOCATION '%s' TBLPROPERTIES('lms_name'='lms');", targetPath));

        Row[] rows = (Row[]) sparkSession.sql("DESCRIBE TABLE EXTENDED tb1;").collect();
        Assertions.assertEquals("c1", rows[0].getString(0));
        Assertions.assertNotNull(getTableFromCLIByName("tb1"));

        sparkSession.sql("DROP TABLE tb1;").collect();
        Assertions.assertNull(getTableFromCLIByName("tb1"));
    }

    @Test
    public void show_tables_from_db_test() {
        sparkSession.sql(String.format("CREATE TABLE IF NOT EXISTS tb1 (c1 string, c2 int) " +
                "LOCATION '%s' TBLPROPERTIES('lms_name'='lms');", targetPath));
        Row[] rows = (Row[]) sparkSession.sql(String.format("SHOW TABLES FROM %s", dbName)).collect();
        Assertions.assertEquals("db1", rows[0].getString(0));
        Assertions.assertEquals("tb1", rows[0].getString(1));
    }

    @Test
    public void create_and_drop_table_from_cli_test() {
        createTableFromCLI("tb1");
        Assertions.assertNotNull(getTableFromCLIByName("tb1"));

        DeleteTableRequest request = new DeleteTableRequest();
        request.setPurgeFlag(true);
        request.setTableName("tb1");
        request = (DeleteTableRequest) buildDbRequestBase(request);
        catalogClient.deleteTable(request);

        Assertions.assertNull(getTableFromCLIByName("tb1"));
    }

    @Test
    void alter_table_name_test() {
        // create table by using 'lms_name' in spark
        sparkSession.sql("create table t4 (c1 bigint, c2 long, lms_commit string) using parquet"
                + " partitioned by (lms_commit) tblproperties('lms_name'='lms')").collect();

        sparkSession.sql("ALTER TABLE t4 RENAME TO t5");

        Row[] rowsTable = (Row[]) sparkSession.sql("DESC TABLE t5").collect();
        Assertions.assertEquals(6, rowsTable.length);
        Assertions.assertEquals("c1", rowsTable[0].getString(0));
        Assertions.assertEquals("bigint", rowsTable[0].getString(1));
        Assertions.assertEquals("c2", rowsTable[1].getString(0));
        Assertions.assertEquals("bigint", rowsTable[1].getString(1));
        Assertions.assertEquals("lms_commit", rowsTable[2].getString(0));
        Assertions.assertEquals("string", rowsTable[2].getString(1));

        // check metadata in lms
        GetTableRequest getTableRequestOld = new GetTableRequest(catalogClient.getProjectId(), catalogName, dbName,  "t4");
        CatalogException exception = assertThrows(CatalogException.class,
                () -> catalogClient.getTable(getTableRequestOld));
        Assertions.assertEquals(String.format("Table [%s] not found", "t4"), exception.getMessage());
        GetTableRequest getTableRequest = new GetTableRequest(catalogClient.getProjectId(), catalogName, dbName, "t5");
        Table table = catalogClient.getTable(getTableRequest);
        Assertions.assertNotNull(table);

        Assertions.assertEquals("t5", table.getTableName());

        sparkSession.sql("drop table t5").collect();
    }

    @Test
    void create_table_as_select_and_insert_into_should_success() {
        // create table by using 'lms_name' in spark
        sparkSession.sql("create table t2 (c1 string, c2 int, lms_commit string) using parquet"
                + " partitioned by (lms_commit) tblproperties('lms_name'='lms')").collect();

        String part = UUID.randomUUID().toString();
        sparkSession.sql("insert into t2 partition(lms_commit='" + part + "')" +
                " values ('a', 1),('b',2)").collect();

        // create table by using 'lms_name' in spark
        sparkSession.sql("create table t3 using parquet partitioned by (lms_commit)"
                        + " tblproperties('lms_name'='lms') as select c1 as d1,c2 as d2,lms_commit from t2")
                .collect();

        String part1 = UUID.randomUUID().toString();
        sparkSession.sql("insert into t3 partition(lms_commit='" + part1 + "')"
                + " values ('a', 1),('b',2)").collect();

        // check metadata in lms
        GetTableRequest getTableRequest1 = new GetTableRequest(catalogClient.getProjectId(), catalogName, dbName, "t3");
        Table table1 = catalogClient.getTable(getTableRequest1);
        Assertions.assertNotNull(table1);

        GetTableRequest getTableRequest = new GetTableRequest(catalogClient.getProjectId(), catalogName, dbName,  "t3");
        Table table2 = catalogClient.getTable(getTableRequest);
        Assertions.assertNotNull(table2);

        ListFileRequest listFileRequest = new ListFileRequest(catalogClient.getProjectId(), catalogName, dbName,  "t3");
        List<String> dataFiles = catalogClient.listFiles(listFileRequest);
        Assertions.assertNotNull(dataFiles);

        // check data from spark
        Row[] rows = (Row[]) sparkSession.sql("select d1,d2 from t3 order by d1").collect();
        Assertions.assertEquals(4, rows.length);
        Assertions.assertEquals("a", rows[0].getString(0));
        Assertions.assertEquals(1, rows[0].getInt(1));
        Assertions.assertEquals("a", rows[1].getString(0));
        Assertions.assertEquals(1, rows[1].getInt(1));

        sparkSession.sql("drop table t2").collect();
        sparkSession.sql("drop table t3").collect();
    }

    @Test
    public void insert_into_array_type_should_success() {
        String tblName = "insert_array_type";
        sparkSession.sql("create table " + tblName + " (c1 array<string>) tblproperties('lms_name'='lms')").collect();
        sparkSession.sql("insert into " + tblName + " select array('a', 'b', 'c')").collect();
        Row[] rows = (Row[]) sparkSession.sql("select * from " + tblName).collect();
        assertEquals(1, rows.length);
        assertEquals("WrappedArray(a, b, c)", rows[0].get(0).toString());
    }

    @Test
    public void alter_tblproperties_should_success() {
        String tblName = "alter_tblproperties";
        sparkSession.sql("create table " + tblName + " (c1 array<string>) tblproperties('lms_name'='lms', 'k1'='v1')").collect();
        Row[] rows = (Row[]) sparkSession.sql("show tblproperties " + tblName).collect();
        assertEquals(3, rows.length);
        assertEquals("v1", rows[2].get(1));

        sparkSession.sql("alter table " + tblName + " set tblproperties('k1'='v2')").collect();
        rows = (Row[]) sparkSession.sql("show tblproperties " + tblName).collect();
        assertEquals(3, rows.length);
        assertEquals("v2", rows[2].get(1));
    }

    @ParameterizedTest
    @ValueSource(strings = {"csv", "parquet", "orc"})
    public void set_partition_location_should_success(String source) {
        String tblName = "set_partition_location_" + source;
        sparkSession.sql(String.format("create table %s (c1 string, c2 string) using %s"
                + " partitioned by (c2) tblproperties('lms_name'='lms')", tblName, source)).collect();

        String loc = warehouse + File.separator + dbName + ".db/user_def_part_" + source;
        sparkSession.sql(String.format("alter table %s add partition (c2='9') location '%s'", tblName, loc)).collect();
        sparkSession.sql(String.format("alter table %s add partition (c2='10')", tblName)).collect();
        sparkSession.sql(String.format("insert into %s partition (c2='20') values ('sz')", tblName)).collect();
        sparkSession.sql(String.format("insert into %s partition (c2='9') values ('cd')", tblName)).collect();

        // check by show partitions
        Row[] rows = (Row[]) sparkSession.sql(String.format("show partitions %s", tblName)).collect();
        assertEquals(3, rows.length);
        assertEquals("c2=10", rows[0].getString(0));
        assertEquals("c2=20", rows[1].getString(0));
        assertEquals("c2=9", rows[2].getString(0));

        // check by select data
        rows = (Row[]) sparkSession.sql(String.format("select * from %s", tblName)).collect();
        assertEquals(2, rows.length);
        assertEquals("[cd,9]", rows[0].toString());
        assertEquals("[sz,20]", rows[1].toString());

        // check directory and files
        File fp9 = new File(loc);
        assertTrue(fp9.isDirectory());
        assertNotEquals(0, fp9.listFiles().length);

        String tblLoc = warehouse + File.separator + dbName + ".db/" + tblName;
        File fp10 = new File(tblLoc + "/c2=10");
        assertTrue(fp10.isDirectory());
        assertEquals(0, fp10.listFiles().length);

        File fp20 = new File(tblLoc + "/c2=20");
        assertTrue(fp20.isDirectory());
        assertNotEquals(0, fp20.listFiles().length);
    }
}
