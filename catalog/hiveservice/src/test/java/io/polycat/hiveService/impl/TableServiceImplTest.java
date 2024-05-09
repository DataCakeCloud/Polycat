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
package io.polycat.hiveService.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import io.polycat.catalog.common.model.CatalogName;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.SerDeInfo;
import io.polycat.catalog.common.model.StorageDescriptor;
import io.polycat.catalog.common.model.Table;
import io.polycat.catalog.common.model.TableName;
import io.polycat.catalog.common.model.TableBrief;
import io.polycat.catalog.common.model.TraverseCursorResult;
import io.polycat.catalog.common.model.stats.ColumnStatistics;
import io.polycat.catalog.common.model.stats.ColumnStatisticsDesc;
import io.polycat.catalog.common.model.stats.ColumnStatisticsObj;
import io.polycat.catalog.common.model.stats.Decimal;
import io.polycat.catalog.common.model.stats.DecimalColumnStatsData;
import io.polycat.catalog.common.model.stats.StringColumnStatsData;
import io.polycat.catalog.common.plugin.request.input.TableInput;
import io.polycat.catalog.common.model.Column;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class TableServiceImplTest extends TestUtil{

    public static final List<Column> COLUMNS = new ArrayList<>();
    public static final List<Column> PARTITIONS = new ArrayList<>();
    public static final StorageDescriptor STORAGE_INPUT = new StorageDescriptor();
    public static final String SERDE = "mock serde";
    public static final SerDeInfo SERDE_INFO = new SerDeInfo("", SERDE, Collections.emptyMap());

    public static final String COLUMN_1 = "column1";
    public static final String COLUMN_2 = "column2";

    public static final String COLUMN_COMMENT = "";
    public static final String TABLE_TYPE = "MANAGED_TABLE";
    public static final String STORAGE_OUTPUT_FORMAT = "";
    public static final String STORAGE_INPUT_FORMAT = "";
    public static final String MOCK_LOCATION = "mock location";

    public static final String HIVE_STR_TYPE = "string";
    public static final String HIVE_DECIMAL_TYPE = "decimal";
    private static final String DATABASE_PATTERN = "*";
    private static final String TABLE_PATTERN = "t*";

    static {
        Column column = new Column();
        column.setColumnName(COLUMN_1);
        column.setColType(HIVE_STR_TYPE);
        column.setComment(COLUMN_COMMENT);
        Column column2 = new Column();
        column2.setColumnName(COLUMN_2);
        column2.setColType(HIVE_STR_TYPE);
        column2.setComment(COLUMN_COMMENT);
        COLUMNS.add(column);
        COLUMNS.add(column2);

        PARTITIONS.add(column);
        PARTITIONS.add(column2);

        fillInStorageInput();
    }

    private static void fillInStorageInput() {
        STORAGE_INPUT.setInputFormat(STORAGE_INPUT_FORMAT);
        STORAGE_INPUT.setOutputFormat(STORAGE_OUTPUT_FORMAT);
        STORAGE_INPUT.setLocation(MOCK_LOCATION);
        STORAGE_INPUT.setCompressed(false);
        STORAGE_INPUT.setSerdeInfo(SERDE_INFO);
        STORAGE_INPUT.setNumberOfBuckets(0);
        STORAGE_INPUT.setParameters(new HashMap<>());
    }

    @Test
    public void should_create_table_success() {
        try {
            DatabaseName databaseName = new DatabaseName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME);
            TableInput tableInput = createTableInput(TABLE_NAME);
            tableService.createTable(databaseName, tableInput);
            TableName tableName = new TableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, TABLE_NAME);
            Table actual = tableService.getTableByName(tableName);

            assertEquals(TABLE_NAME, actual.getTableName());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void should_drop_table_success() {
        try {
            DatabaseName databaseName = new DatabaseName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME);
            TableInput tableInput = createTableInput(TABLE_NAME);
            tableService.createTable(databaseName, tableInput);
            TableName tableName = new TableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, TABLE_NAME);

            tableService.dropTable(tableName, false, false, false);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void should_get_tables_by_names_success() {
        try {
            DatabaseName databaseName = new DatabaseName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME);
            TableInput tableInput = createTableInput(TABLE_NAME + "1");
            tableService.createTable(databaseName, tableInput);
            tableInput = createTableInput(TABLE_NAME + "2");
            tableService.createTable(databaseName, tableInput);

            TraverseCursorResult<List<String>> tableNames = tableService.getTableNames(databaseName, null,
                Integer.MAX_VALUE, null, "t*");

            List<Table> tables = tableService.getTableObjectsByName(databaseName, tableNames.getResult());

            assertEquals(2, tableNames.getResult().size());
            assertEquals(2, tables.size());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void should_list_tables_by_filter_success() {
        try {
            DatabaseName databaseName = new DatabaseName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME);
            TableInput tableInput = createTableInput(TABLE_NAME + "1");
            tableService.createTable(databaseName, tableInput);
            tableInput = createTableInput(TABLE_NAME + "2");
            tableService.createTable(databaseName, tableInput);

            List<String> tableNames = tableService.listTableNamesByFilter(databaseName, "hive_filter_field_owner__=\"" + OWNER+"\"", 1,
                null).getResult();
            assertEquals(1, tableNames.size());

            tableNames = tableService.listTableNamesByFilter(databaseName, "hive_filter_field_owner__=\"" + OWNER+"\"", 2,
                null).getResult();
            assertEquals(2, tableNames.size());

            tableNames = tableService.listTableNamesByFilter(databaseName, "hive_filter_field_owner__=\"T" + OWNER+"\"", 1,
                null).getResult();
            assertEquals(0, tableNames.size());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void should_alter_table_success() {
        try {
            DatabaseName databaseName = new DatabaseName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME);
            TableInput tableInput = createTableInput(TABLE_NAME);
            tableService.createTable(databaseName, tableInput);
            TableName tableName = new TableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, TABLE_NAME);

            tableInput.setOwner(OWNER + "1");
            tableService.alterTable(tableName, tableInput, null);

            Table actual = tableService.getTableByName(tableName);
            assertEquals(OWNER + "1", actual.getOwner());
            tableService.dropTable(tableName, false, false, false);

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void should_truncate_table_success() {
        try {
            DatabaseName databaseName = new DatabaseName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME);
            TableInput tableInput = createTableInput(TABLE_NAME);
            tableService.createTable(databaseName, tableInput);

            TableName tableName = new TableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, TABLE_NAME);
            tableService.truncateTable(tableName);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void should_get_column_stats_success() {
        try {
            DatabaseName databaseName = new DatabaseName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME);
            TableInput tableInput = createTableInput(TABLE_NAME);
            tableService.createTable(databaseName, tableInput);

            TableName tableName = new TableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, TABLE_NAME);

            // Add ColumnStatistics for tbl to metastore DB via ObjectStore
            ColumnStatistics stats = new ColumnStatistics();
            ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc(true, CATALOG_NAME, DATABASE_NAME, TABLE_NAME);
            List<ColumnStatisticsObj> colStatObjs = new ArrayList<>();

            // Col1
            StringColumnStatsData data1 = new StringColumnStatsData();
            ColumnStatisticsObj col1Stats = new ColumnStatisticsObj();
            data1.setAvgColLen(10);
            data1.setMaxColLen(20);
            data1.setNumNulls(10);
            data1.setNumDVs(10);
            col1Stats.setColName(COLUMN_1);
            col1Stats.setColType(HIVE_STR_TYPE);
            col1Stats.setDataType("stringStats");
            col1Stats.setDataValue(obj2Map(data1));
            colStatObjs.add(col1Stats);

            // Col2
            DecimalColumnStatsData data2 = new DecimalColumnStatsData();
            ColumnStatisticsObj col2Stats = new ColumnStatisticsObj();
            data2.setHighValue(new Decimal((short) 20, ByteBuffer.allocate(1).array()));
            data2.setLowValue(new Decimal((short) 10, ByteBuffer.allocate(1).array()));
            data2.setNumNulls(10);
            data2.setNumDVs(10);
            col2Stats.setColName(COLUMN_2);
            col2Stats.setColType(HIVE_DECIMAL_TYPE);
            col2Stats.setDataType("decimalStats");
            col2Stats.setDataValue(obj2Map(data2));
            colStatObjs.add(col2Stats);

            stats.setColumnStatisticsDesc(statsDesc);
            stats.setColumnStatisticsObjs(colStatObjs);

            // Save to DB
            tableService.updateTableColumnStatistics(PROJECT_ID, stats);
            List<String> columns = new ArrayList<String>() {{
                add(COLUMN_1);
                add(COLUMN_2);
            }};
            ColumnStatisticsObj[] statsObjs = tableService.getTableColumnStatistics(
                tableName, columns);

            assertEquals(2, statsObjs.length);
            StringColumnStatsData actualString = (StringColumnStatsData)(statsObjs[0].getDataValue());
            assertEquals(10, actualString.getAvgColLen());
            assertEquals(20, actualString.getMaxColLen());
            assertEquals(10, actualString.getNumDVs());
            assertEquals(10, actualString.getNumNulls());
            assertEquals(HIVE_DECIMAL_TYPE, statsObjs[1].getColType());
            DecimalColumnStatsData actualDecimal = (DecimalColumnStatsData) (statsObjs[1].getDataValue());
            assertEquals(10, actualDecimal.getLowValue().getScale());
            assertEquals(20, actualDecimal.getHighValue().getScale());
            assertEquals(10, actualDecimal.getNumDVs());
            assertEquals(10, actualDecimal.getNumNulls());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void should_delete_column_stats_success() {
        try {
            DatabaseName databaseName = new DatabaseName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME);
            TableInput tableInput = createTableInput(TABLE_NAME);
            tableService.createTable(databaseName, tableInput);

            TableName tableName = new TableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, TABLE_NAME);

            // Add ColumnStatistics for tbl to metastore DB via ObjectStore
            ColumnStatistics stats = new ColumnStatistics();
            ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc(true, CATALOG_NAME, DATABASE_NAME, TABLE_NAME);
            List<ColumnStatisticsObj> colStatObjs = new ArrayList<>();

            // Col1
            StringColumnStatsData data1 = new StringColumnStatsData();
            ColumnStatisticsObj col1Stats = new ColumnStatisticsObj();
            data1.setAvgColLen(10);
            data1.setMaxColLen(20);
            data1.setNumNulls(10);
            data1.setNumDVs(10);
            col1Stats.setColName(COLUMN_1);
            col1Stats.setColType(HIVE_STR_TYPE);
            col1Stats.setDataType("stringStats");
            col1Stats.setDataValue(obj2Map(data1));
            colStatObjs.add(col1Stats);

            // Col2
            DecimalColumnStatsData data2 = new DecimalColumnStatsData();
            ColumnStatisticsObj col2Stats = new ColumnStatisticsObj();
            data2.setHighValue(new Decimal((short) 20, ByteBuffer.allocate(1).array()));
            data2.setLowValue(new Decimal((short) 10, ByteBuffer.allocate(1).array()));
            data2.setNumNulls(10);
            data2.setNumDVs(10);
            col2Stats.setColName(COLUMN_2);
            col2Stats.setColType(HIVE_DECIMAL_TYPE);
            col2Stats.setDataType("decimalStats");
            col2Stats.setDataValue(obj2Map(data2));
            colStatObjs.add(col2Stats);

            stats.setColumnStatisticsDesc(statsDesc);
            stats.setColumnStatisticsObjs(colStatObjs);

            // Save to DB
            tableService.updateTableColumnStatistics(PROJECT_ID, stats);
            List<String> columns = new ArrayList<String>() {{
                add(COLUMN_1);
                add(COLUMN_2);
            }};

            tableService.deleteTableColumnStatistics(tableName, COLUMN_1);
            ColumnStatisticsObj[] statsObjs = tableService.getTableColumnStatistics(tableName, columns);
            assertEquals(1, statsObjs.length);
            tableService.deleteTableColumnStatistics(tableName, COLUMN_2);
            statsObjs = tableService.getTableColumnStatistics(tableName, columns);
            assertEquals(0, statsObjs.length);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void should_get_table_meta_success() {
        try {
            DatabaseName databaseName =new DatabaseName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME);
            TableInput tableInput = createTableInput(TABLE_NAME + "1");
            tableService.createTable(databaseName, tableInput);
            tableInput = createTableInput(TABLE_NAME + "2");
            tableService.createTable(databaseName, tableInput);
            tableInput = createTableInput(TABLE_NAME);
            tableService.createTable(databaseName, tableInput);

            List<String> tableTypes = new ArrayList<String>(){{
                add(TABLE_TYPE);}};
            CatalogName catalogName = new CatalogName(PROJECT_ID, CATALOG_NAME);
            TableBrief[] actual = databaseService.getTableFuzzy(catalogName, DATABASE_PATTERN, TABLE_PATTERN,
                tableTypes).getResult();
            assertEquals(3, actual.length);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @AfterEach
    private void cleanEnv() {
        deleteTestDataBaseAfterTest();
    }

    @BeforeEach
    private void makeEnv() {
        createTestDatabaseBeforeTest();
    }

}
