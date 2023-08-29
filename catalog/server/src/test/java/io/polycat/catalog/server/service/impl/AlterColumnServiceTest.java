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
package io.polycat.catalog.server.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.model.Catalog;
import io.polycat.catalog.common.model.CatalogCommit;
import io.polycat.catalog.common.model.CatalogName;
import io.polycat.catalog.common.model.Database;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.Table;
import io.polycat.catalog.common.model.TableCommit;
import io.polycat.catalog.common.model.TableOperationType;
import io.polycat.catalog.common.model.TraverseCursorResult;
import io.polycat.catalog.common.plugin.request.input.CatalogInput;
import io.polycat.catalog.common.plugin.request.input.ColumnChangeInput;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;
import io.polycat.catalog.common.plugin.request.input.TableInput;
import io.polycat.catalog.common.model.Column;
import io.polycat.catalog.common.types.DataTypes;
import io.polycat.catalog.common.utils.UuidUtil;
import io.polycat.catalog.store.common.StoreConvertor;
import io.polycat.catalog.common.model.TableIdent;

import io.polycat.catalog.common.model.TableName;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import static io.polycat.catalog.common.Operation.*;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
public class AlterColumnServiceTest extends TestUtil {

    static String myCatalogName = "alter_table_my_catalog";
    static String myDatabaseName = "alter_table_my_database";
    static String myTableName = "alter_table_my_table";

    static int defaultColumnsNum = 3;
    static int defaultPartitionColumnsNum = 1;
    static Catalog newCatalog;
    static Database newDatabase;
    static CatalogName catalogName;
    static DatabaseName databaseName;
    static TableName tableName;
    static TableIdent tableIdent;
    boolean isFirstCall = true;

    private void beforeAllSetUp() {
        if (isFirstCall) {
            TestUtil.clearFDB();
            // create catalog
            catalogName = new CatalogName(projectId, myCatalogName);
            CatalogInput catalogInput = getCatalogInput(catalogName.getCatalogName());
            newCatalog = catalogService.createCatalog(projectId, catalogInput);

            // create database
            databaseName = StoreConvertor.databaseName(projectId, myCatalogName, myDatabaseName);
            DatabaseInput databaseInput = getDatabaseDTO(projectId, myCatalogName, myDatabaseName);
            newDatabase = databaseService.createDatabase(catalogName, databaseInput);

            tableName = StoreConvertor.tableName(projectId, myCatalogName, myDatabaseName, myTableName);
            isFirstCall = false;
        }

    }

    @BeforeEach
    public void resetTable() throws MetaStoreException {
        beforeAllSetUp();
        // drop table if exists
        try {
            tableService.dropTable(tableName, true, false, false);
        } catch (Exception ignored) {

        }
        // create table
        TableInput tableInput = getTableDTOWithPartitionColumn(myTableName, defaultColumnsNum, defaultPartitionColumnsNum);
        tableService.createTable(databaseName, tableInput);
        TableName tableName = StoreConvertor.tableName(databaseName, tableInput.getTableName());
        tableIdent = TableObjectHelper.getTableIdentByName(tableName);
    }

    @Test
    public void should_success_add_new_column() {
        String newColName = UUID.randomUUID().toString();
        ColumnChangeInput colChangeIn = new ColumnChangeInput();
        colChangeIn.setChangeType(Operation.ADD_COLUMN);
        List<Column> addColumnList = new ArrayList<>();
        addColumnList.add(new Column(newColName, "STRING"));
        colChangeIn.setColumnList(addColumnList);

        tableService.alterColumn(tableName, colChangeIn);

        Table table = tableService.getTableByName(tableName);
        assertEquals(defaultColumnsNum + 1, table.getStorageDescriptor().getColumns().size());
        assertEquals(newColName, table.getStorageDescriptor().getColumns().get(defaultColumnsNum).getColumnName());

        TraverseCursorResult<List<TableCommit>> res = tableService.listTableCommits(tableName, 1, "");
        TableCommit tc = res.getResult().get(0);
        assertEquals(tableIdent.getTableId(), tc.getTableId());
        assertEquals(TableOperationType.DDL_ADD_COLUMN.name(), tc.getTableOperation().get(0).getOperType());

        TraverseCursorResult<List<CatalogCommit>> res1 = catalogService.getCatalogCommits(catalogName, 1, "");
        CatalogCommit cc = res1.getResult().get(0);
        assertEquals(ADD_COLUMN.getPrintName(), cc.getOperation());
    }

    @Test
    public void should_success_add_multi_columns_with_different_properties() {
        List<Column> addColList = new ArrayList<>();
        int expectedColumnNum = defaultColumnsNum;

        // build columns with different properties
        Column addColNotNull = new Column();
        addColNotNull.setColumnName("add_col_not_null");
        addColNotNull.setColType(DataTypes.STRING.getName());
        addColList.add(addColNotNull);
        expectedColumnNum++;

        Column addColWithComments = new Column();
        addColWithComments.setColumnName("add_col_with_comments");
        addColWithComments.setColType(DataTypes.INT.getName());
        String expectedComment = "this column is added with comments";
        addColWithComments.setComment(expectedComment);
        addColList.add(addColWithComments);
        expectedColumnNum++;

        Column addColWithDefaultValue = new Column();
        addColWithDefaultValue.setColumnName("add_col_with_default_value");
        addColWithDefaultValue.setColType(DataTypes.STRING.getName());
        String expectedDefaultVal = "default";
        addColList.add(addColWithDefaultValue);
        expectedColumnNum++;

        Column addColWithProperties = new Column();
        addColWithProperties.setColumnName("add_col_with_properties");
        addColWithProperties.setColType(DataTypes.STRING.getName());
        HashMap<String, String> expectedMap = new HashMap<>();
        expectedMap.put("usage", "Unit test");
        expectedMap.put("function name", "AlterTableTest");
        addColList.add(addColWithProperties);
        expectedColumnNum++;

        ColumnChangeInput colChangeIn = new ColumnChangeInput();
        colChangeIn.setChangeType(Operation.ADD_COLUMN);
        List<Column> addColumnList = new ArrayList<>();
        addColumnList.addAll(addColList);
        colChangeIn.setColumnList(addColumnList);
        tableService.alterColumn(tableName, colChangeIn);

        Table table = tableService.getTableByName(tableName);
        assertEquals(expectedColumnNum, table.getStorageDescriptor().getColumns().size());

        List<Column> columnList = table.getStorageDescriptor().getColumns();
        Column col = getColumnFromListByName(columnList, "add_col_not_null");
        assertNotNull(col);

        col = getColumnFromListByName(columnList, "add_col_with_comments");
        assertNotNull(col);
        assertEquals(expectedComment, col.getComment());

        col = getColumnFromListByName(columnList, "add_col_with_default_value");
        assertNotNull(col);

//        col = getColumnFromListByName(columnList, "add_col_with_properties");
//        assertNotNull(col);
//        assertEquals(expectedMap, col.getProperties());
    }

    private Column getColumnFromListByName(List<Column> columnList, String colName) {
        for (Column col : columnList) {
            if (col.getColumnName().equals(colName)) {
                return col;
            }
        }
        return null;
    }

    @Test
    public void should_fail_add_column_with_same_name() {
        Table table = tableService.getTableByName(tableName);
        Column column0 = table.getStorageDescriptor().getColumns().get(0);

        String sameColName = column0.getColumnName();
        Column addColumnInput = new Column();
        addColumnInput.setColumnName(sameColName);
        addColumnInput.setColType(DataTypes.STRING.getName());


        ColumnChangeInput colChangeIn = new ColumnChangeInput();
        colChangeIn.setChangeType(Operation.ADD_COLUMN);
        List<Column> addColumnList = new ArrayList<>();
        addColumnList.add(addColumnInput);
        colChangeIn.setColumnList(addColumnList);

        MetaStoreException e = assertThrows(MetaStoreException.class,
                () -> tableService.alterColumn(tableName, colChangeIn));
        assertEquals(ErrorCode.COLUMN_ALREADY_EXISTS, e.getErrorCode());
    }

    @Test
    public void should_fail_rename_non_exists_column() {
        String notExistName = UuidUtil.generateId();
        String newName = UuidUtil.generateId();
        HashMap<String, String> renameMap = new HashMap<>();
        renameMap.put(notExistName, newName);

        ColumnChangeInput colChangeIn1 = new ColumnChangeInput();
        colChangeIn1.setChangeType(Operation.RENAME_COLUMN);
        colChangeIn1.setRenameColumnMap(renameMap);

        MetaStoreException e = assertThrows(MetaStoreException.class,
            () -> tableService.alterColumn(tableName, colChangeIn1));
        assertEquals(ErrorCode.COLUMN_NOT_FOUND, e.getErrorCode());
    }

    @Test
    public void should_fail_new_name_already_exist() {
        Table table = tableService.getTableByName(tableName);
        String originName = table.getStorageDescriptor().getColumns().get(0).getColumnName();
        String newName =  table.getStorageDescriptor().getColumns().get(1).getColumnName();
        HashMap<String, String> renameMap = new HashMap<>();
        renameMap.put(originName, newName);

        ColumnChangeInput colChangeIn1 = new ColumnChangeInput();
        colChangeIn1.setChangeType(Operation.RENAME_COLUMN);
        colChangeIn1.setRenameColumnMap(renameMap);

        MetaStoreException e = assertThrows(MetaStoreException.class,
            () -> tableService.alterColumn(tableName, colChangeIn1));
        assertEquals(ErrorCode.COLUMN_ALREADY_EXISTS, e.getErrorCode());
    }

    @Test
    public void should_fail_new_name_duplicate() {
        Table table = tableService.getTableByName(tableName);
        String originName0 =  table.getStorageDescriptor().getColumns().get(0).getColumnName();
        String originName1 =  table.getStorageDescriptor().getColumns().get(1).getColumnName();
        String newName = "new_column_duplicate";
        HashMap<String, String> renameMap = new HashMap<>();
        renameMap.put(originName0, newName);
        renameMap.put(originName1, newName);

        ColumnChangeInput colChangeIn1 = new ColumnChangeInput();
        colChangeIn1.setChangeType(Operation.RENAME_COLUMN);
        colChangeIn1.setRenameColumnMap(renameMap);

        MetaStoreException e = assertThrows(MetaStoreException.class,
            () -> tableService.alterColumn(tableName, colChangeIn1));
        assertEquals(ErrorCode.COLUMN_ALREADY_EXISTS, e.getErrorCode());
    }

    @Test
    public void should_success_rename_multi_columns() {
        Table table = tableService.getTableByName(tableName);
        String originName0 = table.getStorageDescriptor().getColumns().get(0).getColumnName();
        String originName1 = table.getStorageDescriptor().getColumns().get(1).getColumnName();
        HashMap<String, String> renameMap = new HashMap<>();
        renameMap.put(originName0, "new_col_0");
        renameMap.put(originName1, "new_col_1");

        ColumnChangeInput colChangeIn1 = new ColumnChangeInput();
        colChangeIn1.setChangeType(Operation.RENAME_COLUMN);
        colChangeIn1.setRenameColumnMap(renameMap);

        tableService.alterColumn(tableName, colChangeIn1);

        table = tableService.getTableByName(tableName);
        assertEquals(defaultColumnsNum, table.getStorageDescriptor().getColumns().size());
        List<Column> columnList = table.getStorageDescriptor().getColumns();
        Column col = getColumnFromListByName(columnList, "new_col_0");
        assertNotNull(col);
        col = getColumnFromListByName(columnList, "new_col_1");
        assertNotNull(col);

        TraverseCursorResult<List<TableCommit>> res = tableService.listTableCommits(tableName, 1, "");
        TableCommit tc = res.getResult().get(0);
        assertEquals(tableIdent.getTableId(), tc.getTableId());
        assertEquals(TableOperationType.DDL_RENAME_COLUMN.name(), tc.getTableOperation().get(0).getOperType());

        TraverseCursorResult<List<CatalogCommit>> res1 = catalogService.getCatalogCommits(catalogName, 1, "");
        CatalogCommit cc = res1.getResult().get(0);
        assertEquals(RENAME_COLUMN.getPrintName(), cc.getOperation());
    }

    @Test
    public void should_success_rename_with_empty_map() {
        HashMap<String, String> renameMap = new HashMap<>();
        ColumnChangeInput colChangeIn1 = new ColumnChangeInput();
        colChangeIn1.setChangeType(Operation.RENAME_COLUMN);
        colChangeIn1.setRenameColumnMap(renameMap);

        tableService.alterColumn(tableName, colChangeIn1);

        Table table = tableService.getTableByName(tableName);
        assertEquals(defaultColumnsNum, table.getStorageDescriptor().getColumns().size());

        TraverseCursorResult<List<TableCommit>> res = tableService.listTableCommits(tableName, 1, "");
        TableCommit tc = res.getResult().get(0);
        assertEquals(tableIdent.getTableId(), tc.getTableId());
        assertEquals(TableOperationType.DDL_RENAME_COLUMN.name(), tc.getTableOperation().get(0).getOperType());

        TraverseCursorResult<List<CatalogCommit>> res1 = catalogService
            .getCatalogCommits(catalogName, 1, "");
        CatalogCommit cc = res1.getResult().get(0);
        assertEquals(RENAME_COLUMN.getPrintName(), cc.getOperation());
    }

    @Test
    public void rename_column_keep_ordinal_order() {
        // rename first column
        Table table = tableService.getTableByName(tableName);
        String originName0 = table.getStorageDescriptor().getColumns().get(0).getColumnName();
        HashMap<String, String> renameHeadColMap = new HashMap<>();
        renameHeadColMap.put(originName0, "new_col_head");
        ColumnChangeInput colChangeIn1 = new ColumnChangeInput();
        colChangeIn1.setChangeType(Operation.RENAME_COLUMN);
        colChangeIn1.setRenameColumnMap(renameHeadColMap);

        tableService.alterColumn(tableName, colChangeIn1);

        table = tableService.getTableByName(tableName);
        assertEquals(defaultColumnsNum, table.getStorageDescriptor().getColumns().size());
        assertEquals("new_col_head", table.getStorageDescriptor().getColumns().get(0).getColumnName());

        // rename middle column
        String originName1 = table.getStorageDescriptor().getColumns().get(1).getColumnName();
        HashMap<String, String> renameMidColMap = new HashMap<>();
        renameMidColMap.put(originName1, "new_col_mid");
        colChangeIn1 = new ColumnChangeInput();
        colChangeIn1.setChangeType(Operation.RENAME_COLUMN);
        colChangeIn1.setRenameColumnMap(renameMidColMap);

        tableService.alterColumn(tableName, colChangeIn1);

        table = tableService.getTableByName(tableName);
        assertEquals(defaultColumnsNum, table.getStorageDescriptor().getColumns().size());
        assertEquals("new_col_mid", table.getStorageDescriptor().getColumns().get(1).getColumnName());

        // rename last column
        String originName2 = table.getStorageDescriptor().getColumns().get(2).getColumnName();
        HashMap<String, String> renameTailColMap = new HashMap<>();
        renameTailColMap.put(originName2, "new_col_tail");
        colChangeIn1 = new ColumnChangeInput();
        colChangeIn1.setChangeType(Operation.RENAME_COLUMN);
        colChangeIn1.setRenameColumnMap(renameTailColMap);

        tableService.alterColumn(tableName, colChangeIn1);

        table = tableService.getTableByName(tableName);
        assertEquals(defaultColumnsNum, table.getStorageDescriptor().getColumns().size());
        assertEquals("new_col_tail", table.getStorageDescriptor().getColumns().get(2).getColumnName());
    }

    @Test
    public void add_column_keep_ordinal_order() {
        String newColName1 = "new_col_1";
        String newColName2 = "new_col_2";
        int expectedColumnNum = defaultColumnsNum + 2;

        Column addColumnInput1 = new Column();
        addColumnInput1.setColumnName(newColName1);
        addColumnInput1.setColType(DataTypes.STRING.getName());
        Column addColumnInput2 = new Column();
        addColumnInput2.setColumnName(newColName2);
        addColumnInput2.setColType(DataTypes.INT.getName());

        ColumnChangeInput colChangeIn = new ColumnChangeInput();
        colChangeIn.setChangeType(Operation.ADD_COLUMN);
        colChangeIn.setColumnList(Arrays.asList(addColumnInput1, addColumnInput2));

        tableService.alterColumn(tableName, colChangeIn);

        Table table = tableService.getTableByName(tableName);
        assertNotNull(table);
        assertEquals(expectedColumnNum, table.getStorageDescriptor().getColumns().size());
        assertEquals(newColName1, table.getStorageDescriptor().getColumns().get(defaultColumnsNum).getColumnName());
        assertEquals(newColName2, table.getStorageDescriptor().getColumns().get(defaultColumnsNum + 1).getColumnName());

    }

    @Test
    public void should_fail_drop_partition_column() {
        Table table = tableService.getTableByName(tableName);
        String partitionName = table.getPartitionKeys().get(0).getColumnName();
        ColumnChangeInput colChangeIn = new ColumnChangeInput();
        colChangeIn.setChangeType(Operation.DROP_COLUMN);
        colChangeIn.setDropColumnList(Arrays.asList(partitionName));

        MetaStoreException e = assertThrows(MetaStoreException.class,
                () -> tableService.alterColumn(tableName, colChangeIn));
        assertEquals(ErrorCode.COLUMN_CAN_NOT_BE_DROPPED, e.getErrorCode());
    }

    @Test
    public void should_success_drop_with_empty_name() {
        ColumnChangeInput colChangeIn = new ColumnChangeInput();
        colChangeIn.setChangeType(Operation.DROP_COLUMN);
        colChangeIn.setDropColumnList(Collections.emptyList());
        tableService.alterColumn(tableName, colChangeIn);

        Table table = tableService.getTableByName(tableName);
        assertEquals(defaultColumnsNum, table.getStorageDescriptor().getColumns().size());

        TraverseCursorResult<List<TableCommit>> res = tableService.listTableCommits(tableName, 1, "");
        TableCommit tc = res.getResult().get(0);
        assertEquals(tableIdent.getTableId(), tc.getTableId());
        assertEquals(TableOperationType.DDL_DELETE_COLUMN.name(), tc.getTableOperation().get(0).getOperType());

        TraverseCursorResult<List<CatalogCommit>> res1 = catalogService.getCatalogCommits(catalogName, 1, "");
        CatalogCommit cc = res1.getResult().get(0);
        assertEquals(DROP_COLUMN.getPrintName(), cc.getOperation());
    }

    @Test
    public void should_fail_drop_non_exists_column() {
        List<String> dropColumnList = new ArrayList<>();
        String notExistName = UuidUtil.generateId();
        dropColumnList.add(notExistName);

        ColumnChangeInput colChangeIn = new ColumnChangeInput();
        colChangeIn.setChangeType(Operation.DROP_COLUMN);
        colChangeIn.setDropColumnList(dropColumnList);

        MetaStoreException e = assertThrows(MetaStoreException.class,
                () ->    tableService.alterColumn(tableName, colChangeIn));
        assertEquals(ErrorCode.COLUMN_NOT_FOUND, e.getErrorCode());
    }

    @Test
    public void should_success_drop_exists_column() {
        Table table = tableService.getTableByName(tableName);
        String originName = table.getStorageDescriptor().getColumns().get(0).getColumnName();

        List<String> dropColumnList = new ArrayList<>();
        dropColumnList.add(originName);
        ColumnChangeInput colChangeIn = new ColumnChangeInput();
        colChangeIn.setChangeType(Operation.DROP_COLUMN);
        colChangeIn.setDropColumnList(dropColumnList);

        tableService.alterColumn(tableName, colChangeIn);

        table = tableService.getTableByName(tableName);
        assertEquals(defaultColumnsNum - 1, table.getStorageDescriptor().getColumns().size());
        List<Column> columnList = table.getStorageDescriptor().getColumns();
        Column col = getColumnFromListByName(columnList, originName);
        assertNull(col);

        TraverseCursorResult<List<TableCommit>> res = tableService.listTableCommits(tableName, 1, "");
        TableCommit tc = res.getResult().get(0);
        assertEquals(tableIdent.getTableId(), tc.getTableId());
        assertEquals(TableOperationType.DDL_DELETE_COLUMN.name(), tc.getTableOperation().get(0).getOperType());

        TraverseCursorResult<List<CatalogCommit>> res1 = catalogService.getCatalogCommits(catalogName, 1, "");
        CatalogCommit cc = res1.getResult().get(0);
        assertEquals(DROP_COLUMN.getPrintName(), cc.getOperation());
    }

    @Test
    public void should_success_drop_multi_columns() {
        Table table = tableService.getTableByName(tableName);
        String originName0 = table.getStorageDescriptor().getColumns().get(0).getColumnName();
        String originName1 = table.getStorageDescriptor().getColumns().get(1).getColumnName();
        List<String> dropColumnList = new ArrayList<>();
        dropColumnList.add(originName0);
        dropColumnList.add(originName1);

        ColumnChangeInput colChangeIn = new ColumnChangeInput();
        colChangeIn.setChangeType(Operation.DROP_COLUMN);
        colChangeIn.setDropColumnList(dropColumnList);

        tableService.alterColumn(tableName, colChangeIn);

        table = tableService.getTableByName(tableName);
        List<Column> columnList = table.getStorageDescriptor().getColumns();
        Column col = getColumnFromListByName(columnList, originName0);
        assertNull(col);
        col = getColumnFromListByName(columnList, originName1);
        assertNull(col);

        TraverseCursorResult<List<TableCommit>> res = tableService.listTableCommits(tableName, 1, "");
        TableCommit tc = res.getResult().get(0);
        assertEquals(tableIdent.getTableId(), tc.getTableId());
        assertEquals(TableOperationType.DDL_DELETE_COLUMN.name(), tc.getTableOperation().get(0).getOperType());

        TraverseCursorResult<List<CatalogCommit>> res1 = catalogService.getCatalogCommits(catalogName, 1, "");
        CatalogCommit cc = res1.getResult().get(0);
        assertEquals(DROP_COLUMN.getPrintName(), cc.getOperation());
    }
}
