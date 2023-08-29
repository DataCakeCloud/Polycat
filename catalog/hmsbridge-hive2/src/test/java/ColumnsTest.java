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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.model.Table;
import io.polycat.catalog.common.plugin.request.AlterColumnRequest;
import io.polycat.catalog.common.plugin.request.GetTableRequest;
import io.polycat.catalog.common.plugin.request.input.ColumnChangeInput;
import io.polycat.catalog.common.model.Column;

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ColumnsTest extends HMSBridgeTestEnv {
    
    private AlterColumnRequest makeAddColumnRequest(String tblName) {
        AlterColumnRequest request = new AlterColumnRequest();
        request.setProjectId(catalogClient.getProjectId());
        request.setCatalogName(defaultCatalogName);
        request.setDatabaseName(defaultDbName);
        request.setTableName(tblName);

        ColumnChangeInput input = new ColumnChangeInput();
        input.setChangeType(Operation.ADD_COLUMN);
        List<Column> addCols = new ArrayList<>();
        Column newCol = new Column();
        newCol.setColumnName("a1");
        newCol.setColType("string");
        newCol.setComment("new col a1");
        addCols.add(newCol);
        input.setColumnList(addCols);
        request.setInput(input);
        return request;
    }

    private GetTableRequest makeGetTableRequest(String tableName) {
        GetTableRequest request = new GetTableRequest(tableName);
        request.setProjectId(catalogClient.getProjectId());
        request.setCatalogName(defaultCatalogName);
        request.setDatabaseName(defaultDbName);
        return request;
    }

    @Test
    public void add_columns_should_success() {
        // Spark3 syntax: ALTER TABLE table_name ADD COLUMNS (col_spec[, col_spec ...])
        String tableName = "addColTable";
        sparkSession.sql("create table " + tableName + " (c1 string)" +
            " tblproperties('lms_name'='lms', ID=001)").collect();
        sparkSession.sql("insert into " + tableName + " values ('amy'), ('bob');").collect();

        // add column from bridge side
        sparkSession.sql("alter table " + tableName + " add columns (c2 string comment 'new col')").collect();

        // add column from polycat client side
        AlterColumnRequest alterColumnRequest = makeAddColumnRequest(tableName.toLowerCase());
        catalogClient.alterColumn(alterColumnRequest);

        // check columns through desc table
        Row[] rows = (Row[]) sparkSession.sql("desc table " + tableName).collect();
        assertEquals(3, rows.length);
        assertEquals("[c1,string,]", rows[0].toString());
        assertEquals("[c2,string,new col]", rows[1].toString());
        assertEquals("[a1,string,new col a1]", rows[2].toString());

        // check columns through show columns
        rows = (Row[]) sparkSession.sql("show columns in default." + tableName).collect();
        assertEquals("c1", rows[0].getString(0));
        assertEquals("c2", rows[1].getString(0));
        assertEquals("a1", rows[2].getString(0));

        // check columns through polycat client
        GetTableRequest getTableRequest = makeGetTableRequest(tableName);
        Table table = catalogClient.getTable(getTableRequest);
        assertNotNull(table);
        assertEquals(3, table.getStorageDescriptor().getColumns().size());
        assertEquals("c1", table.getStorageDescriptor().getColumns().get(0).getColumnName());
        assertEquals("c2", table.getStorageDescriptor().getColumns().get(1).getColumnName());
        assertEquals("a1", table.getStorageDescriptor().getColumns().get(2).getColumnName());

        // check data operations
        rows = (Row[]) sparkSession.sql("select * from " + tableName).collect();
        List<String> lines = Arrays.stream(rows).map(Row::toString).collect(Collectors.toList());
        Collections.sort(lines);
        assertEquals(2, lines.size());
        assertEquals("[amy,null,null]", lines.get(0));
        assertEquals("[bob,null,null]", lines.get(1));
        assertDoesNotThrow(
            () -> sparkSession.sql("insert into " + tableName + " values ('cindy', 'c2 val', 'a1 val');").collect());
        rows = (Row[]) sparkSession.sql("select * from " + tableName).collect();
        lines = Arrays.stream(rows).map(Row::toString).collect(Collectors.toList());
        Collections.sort(lines);
        assertEquals(3, lines.size());
        assertEquals("[amy,null,null]", lines.get(0));
        assertEquals("[bob,null,null]", lines.get(1));
        assertEquals("[cindy,c2 val,a1 val]", lines.get(2));
    }

    @ParameterizedTest
    @ValueSource(strings = {"csv"})
//    @ValueSource(strings = {"parquet", "orc", "csv"})
    public void alter_non_part_table_columns_should_success(String source) {
        // Spark3 syntax: ALTER TABLE table_identifier { ALTER | CHANGE } [ COLUMN ] col_spec alterColumnAction
        String tableName = "alterColTable_" + source;
        sparkSession.sql(
            "create table " + tableName + " (c1 string comment 'comment 1', c2 string comment 'comment 2')"
                + " using " + source
                + " tblproperties('lms_name'='lms')").collect();

        sparkSession.sql("alter table " + tableName + " alter column c2 comment 'new comment 2.1'").collect();

        // check columns through desc table
        Row[] rows = (Row[]) sparkSession.sql("desc table " + tableName).collect();
        assertEquals(2, rows.length);
        assertEquals("[c1,string,comment 1]", rows[0].toString());
        assertEquals("[c2,string,new comment 2.1]", rows[1].toString());

        // check columns through polycat client
        GetTableRequest getTableRequest = makeGetTableRequest(tableName);
        Table table = catalogClient.getTable(getTableRequest);
        assertEquals(2, table.getStorageDescriptor().getColumns().size());
        assertEquals("c1", table.getStorageDescriptor().getColumns().get(0).getColumnName());
        assertEquals("STRING", table.getStorageDescriptor().getColumns().get(0).getColType());
        assertEquals("comment 1", table.getStorageDescriptor().getColumns().get(0).getComment());
        assertEquals("c2", table.getStorageDescriptor().getColumns().get(1).getColumnName());
        assertEquals("STRING", table.getStorageDescriptor().getColumns().get(1).getColType());
        assertEquals("new comment 2.1", table.getStorageDescriptor().getColumns().get(1).getComment());
    }

    @ParameterizedTest
    @ValueSource(strings = {"parquet", "orc", "csv"})
    public void alter_part_table_columns_should_success(String source) {
        String tableName = "alterPartTableCol_" + source;
        sparkSession.sql(
            "create table " + tableName + " (c1 string comment 'comment 1', c2 string comment 'comment 2',"
                + " c3 string comment 'part col 3', c4 string comment 'part col 4')"
                + " using " + source
                + " partitioned by (c3, c4)"
                + " tblproperties('lms_name'='lms')").collect();

        // add column
        sparkSession.sql("alter table " + tableName + " add columns (a1 string comment 'new col 5')").collect();
        Row[] rows1 = (Row[]) sparkSession.sql("desc table " + tableName).collect();
        assertEquals(9, rows1.length);
        assertEquals("[c1,string,comment 1]", rows1[0].toString());
        assertEquals("[c2,string,comment 2]", rows1[1].toString());
        assertEquals("[a1,string,new col 5]", rows1[2].toString());
        assertEquals("[c3,string,part col 3]", rows1[3].toString());
        assertEquals("[c4,string,part col 4]", rows1[4].toString());


        // alter column
        sparkSession.sql("alter table " + tableName + " alter column c2 comment 'new comment 2.1'").collect();
        Row[] rows2 = (Row[]) sparkSession.sql("desc table " + tableName).collect();
        assertEquals(9, rows2.length);
        assertEquals(rows1[0].toString(), rows2[0].toString());
        assertEquals("[c2,string,new comment 2.1]", rows2[1].toString());
        assertEquals(rows1[2].toString(), rows2[2].toString());
        assertEquals(rows1[3].toString(), rows2[3].toString());
        assertEquals(rows1[4].toString(), rows2[4].toString());
    }

    @Test
    public void alter_non_part_textfile_table_column_should_success() {
        String tableName = "alter_textfile_table_column";
        sparkSession.sql(
            "create table " + tableName + " (c1 string comment 'comment 1', c2 string comment 'comment 2')"
                + " row format delimited fields terminated by ','"
                + " stored as textfile"
                + " tblproperties('lms_name'='lms')").collect();

        sparkSession.sql("alter table " + tableName + " add columns (a1 string comment 'new col 3')").collect();
        Row[] rows1 = (Row[]) sparkSession.sql("desc table " + tableName).collect();
        assertEquals(3, rows1.length);
        assertEquals("[c1,string,comment 1]", rows1[0].toString());
        assertEquals("[c2,string,comment 2]", rows1[1].toString());
        assertEquals("[a1,string,new col 3]", rows1[2].toString());

        sparkSession.sql("alter table " + tableName + " alter column c2 comment 'new comment 2.1'").collect();
        Row[] rows2 = (Row[]) sparkSession.sql("desc table " + tableName).collect();
        assertEquals(3, rows2.length);
        assertEquals(rows1[0].toString(), rows2[0].toString());
        assertEquals("[c2,string,new comment 2.1]", rows2[1].toString());
        assertEquals(rows1[2].toString(), rows2[2].toString());
    }

    @Test
    public void alter_part_textfile_table_column_should_success() {
        String tableName = "alter_partition_textfile_table_column";
        sparkSession.sql(
            "create table " + tableName + " (c1 string comment 'comment 1', c2 string comment 'comment 2')"
                + " partitioned by (c3 string comment 'part col 3')"
                + " row format delimited fields terminated by ','"
                + " stored as textfile"
                + " tblproperties('lms_name'='lms')").collect();

        sparkSession.sql("alter table " + tableName + " add columns (a1 string comment 'new col 4')").collect();
        Row[] rows1 = (Row[]) sparkSession.sql("desc table " + tableName).collect();
        assertEquals(7, rows1.length);
        assertEquals("[c1,string,comment 1]", rows1[0].toString());
        assertEquals("[c2,string,comment 2]", rows1[1].toString());
        assertEquals("[a1,string,new col 4]", rows1[2].toString());
        assertEquals("[c3,string,part col 3]", rows1[3].toString());

        sparkSession.sql("alter table " + tableName + " alter column a1 comment 'new comment 4.1'").collect();
        Row[] rows2 = (Row[]) sparkSession.sql("desc table " + tableName).collect();
        assertEquals(7, rows2.length);
        assertEquals(rows1[0].toString(), rows2[0].toString());
        assertEquals(rows1[1].toString(), rows2[1].toString());
        assertEquals("[a1,string,new comment 4.1]", rows2[2].toString());
        assertEquals(rows1[3].toString(), rows2[3].toString());
    }
}
