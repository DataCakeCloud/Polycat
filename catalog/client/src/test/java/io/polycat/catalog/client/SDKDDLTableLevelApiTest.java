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
package io.polycat.catalog.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.model.Catalog;
import io.polycat.catalog.common.model.CatalogName;
import io.polycat.catalog.common.model.Database;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.StorageDescriptor;
import io.polycat.catalog.common.model.Table;
import io.polycat.catalog.common.plugin.request.AlterColumnRequest;
import io.polycat.catalog.common.plugin.request.CreateCatalogRequest;
import io.polycat.catalog.common.plugin.request.CreateDatabaseRequest;
import io.polycat.catalog.common.plugin.request.CreateTableRequest;
import io.polycat.catalog.common.plugin.request.GetTableRequest;
import io.polycat.catalog.common.plugin.request.input.CatalogInput;
import io.polycat.catalog.common.plugin.request.input.ColumnChangeInput;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;
import io.polycat.catalog.common.plugin.request.input.TableInput;
import io.polycat.catalog.common.model.Column;
import io.polycat.catalog.common.types.DataTypes;
import io.polycat.catalog.store.common.StoreConvertor;
import io.polycat.catalog.common.model.TableName;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SDKDDLTableLevelApiTest extends SDKTestUtil {
    private static final PolyCatClient client = SDKTestUtil.getClient();

    static final String myCatalogName = "sdk_ddl_my_catalog";
    static final String myDatabaseName = "sdk_ddl_my_database";
    static final String myLocationUri = "/location/uri";
    static final String myCol1 = "sdk_ddl_col_1";
    static final String myCol2 = "sdk_ddl_col_2";
    static final int defaultColumnNum = 2;
    static String myTableName;

    static CatalogName catalogName;
    static DatabaseName databaseName;
    static TableName tableName;

    @BeforeAll
    public static void setup() {
        catalogName = StoreConvertor.catalogName(PROJECT_ID, myCatalogName);
        databaseName = StoreConvertor.databaseName(PROJECT_ID, myCatalogName, myDatabaseName);
        createCatalog();
        createDatabase();
    }

    private static void createCatalog() {
        CatalogInput catalogInput = new CatalogInput();
        catalogInput.setOwner(OWNER);
        catalogInput.setCatalogName(myCatalogName);

        CreateCatalogRequest createCatalogRequest = new CreateCatalogRequest();
        createCatalogRequest.setProjectId(PROJECT_ID);
        createCatalogRequest.setInput(catalogInput);

        Catalog catalog = client.createCatalog(createCatalogRequest);
        assertNotNull(catalog);
        assertEquals(myCatalogName, catalog.getCatalogName());
    }

    private static void createDatabase() {
        DatabaseInput databaseInput = getDatabaseInput(myCatalogName, myDatabaseName, myLocationUri, OWNER);

        CreateDatabaseRequest createDatabaseRequest = new CreateDatabaseRequest();
        createDatabaseRequest.setProjectId(PROJECT_ID);
        createDatabaseRequest.setCatalogName(myCatalogName);
        createDatabaseRequest.setInput(databaseInput);

        Database database = client.createDatabase(createDatabaseRequest);
        assertNotNull(database);
        assertEquals(myDatabaseName, database.getDatabaseName());
    }

    @BeforeEach
    public void resetTable() {
        // create new table
        createTable();
    }

    private void createTable() {
        myTableName = "sdk_ddl_table_" + UUID.randomUUID();
        List<Column> columns = new ArrayList<>();
        columns.add(new Column(myCol1, DataTypes.STRING.getName()));
        columns.add(new Column(myCol2, DataTypes.INT.getName()));
        StorageDescriptor sin = new StorageDescriptor();
        sin.setLocation(myLocationUri);
        sin.setColumns(columns);

        TableInput tableInput = new TableInput();
        tableInput.setOwner(OWNER);
        tableInput.setTableName(myTableName);
        tableInput.setStorageDescriptor(sin);

        CreateTableRequest createTableRequest = new CreateTableRequest();
        createTableRequest.setProjectId(PROJECT_ID);
        createTableRequest.setCatalogName(myCatalogName);
        createTableRequest.setDatabaseName(myDatabaseName);
        createTableRequest.setInput(tableInput);

        Table table = client.createTable(createTableRequest);
        assertNotNull(table.getTableName());
        tableName = StoreConvertor.tableName(PROJECT_ID, myCatalogName, myDatabaseName, myTableName);
    }

    @Test
    public void mock() {
        assertTrue(true);
    }

    @Test
    public void should_success_add_column() {
        String newColName1 = "sdk_ddl_new_col_1";
        String newColName2 = "sdk_ddl_new_col_2";
        int expectedColNum = defaultColumnNum + 2;
        List<String> expectedColNameList = new ArrayList<>();
        expectedColNameList.add(myCol1);
        expectedColNameList.add(myCol2);
        expectedColNameList.add(newColName1);
        expectedColNameList.add(newColName2);
        Collections.sort(expectedColNameList);

        ColumnChangeInput colChangeIn = new ColumnChangeInput();
        colChangeIn.setChangeType(Operation.ADD_COLUMN);
        List<Column> addColumnList = new ArrayList<>();
        addColumnList.add(new Column(newColName1, "STRING"));
        addColumnList.add(new Column(newColName2, "INT"));
        colChangeIn.setColumnList(addColumnList);

        AlterColumnRequest request = new AlterColumnRequest();
        request.setProjectId(PROJECT_ID);
        request.setCatalogName(myCatalogName);
        request.setDatabaseName(myDatabaseName);
        request.setTableName(myTableName);
        request.setInput(colChangeIn);
        assertDoesNotThrow(() -> client.alterColumn(request));

        Table table = getTable();
        List<Column> fieldList = table.getStorageDescriptor().getColumns();
        assertEquals(expectedColNum, fieldList.size());
        List<String> actualColNameList = new ArrayList<>();
        fieldList.forEach(filed -> actualColNameList.add(filed.getColumnName()));
        Collections.sort(actualColNameList);
        assertEquals(expectedColNameList, actualColNameList);
    }

    private Table getTable() {
        GetTableRequest getTableRequest = new GetTableRequest(myTableName);
        getTableRequest.setProjectId(PROJECT_ID);
        getTableRequest.setCatalogName(myCatalogName);
        getTableRequest.setDatabaseName(myDatabaseName);

        Table table = client.getTable(getTableRequest);
        assertNotNull(table);
        assertNotNull(table.getStorageDescriptor().getColumns());
        return table;
    }

    @Test
    public void should_success_rename_multi_columns() {
        int expectedColNum = defaultColumnNum;

        // rename one column at first time
        String col1TmpName = "sdk_ddl_tmp_col";
        List<String> expectedColNameList = new ArrayList<>();
        expectedColNameList.add(col1TmpName);
        expectedColNameList.add(myCol2);
        Collections.sort(expectedColNameList);

        ColumnChangeInput colChangeIn1 = new ColumnChangeInput();
        colChangeIn1.setChangeType(Operation.RENAME_COLUMN);
        HashMap<String, String> renameMap = new HashMap<>();
        renameMap.put(myCol1, col1TmpName);
        colChangeIn1.setRenameColumnMap(renameMap);

        AlterColumnRequest request = new AlterColumnRequest();
        request.setProjectId(PROJECT_ID);
        request.setCatalogName(myCatalogName);
        request.setDatabaseName(myDatabaseName);
        request.setTableName(myTableName);
        request.setInput(colChangeIn1);
        assertDoesNotThrow(() -> client.alterColumn(request));

        Table table = getTable();
        List<Column> fieldList = table.getStorageDescriptor().getColumns();
        assertEquals(expectedColNum, fieldList.size());
        List<String> actualColNameList = new ArrayList<>();
        fieldList.forEach(filed -> actualColNameList.add(filed.getColumnName()));
        Collections.sort(actualColNameList);
        assertEquals(expectedColNameList, actualColNameList);

        // rename two columns at second time
        String col1FinalName = "sdk_ddl_new_col_1";
        String col2FinalName = "sdk_ddl_new_col_2";
        expectedColNameList.clear();
        expectedColNameList.add(col1FinalName);
        expectedColNameList.add(col2FinalName);
        Collections.sort(expectedColNameList);

        ColumnChangeInput colChangeIn2 = new ColumnChangeInput();
        colChangeIn2.setChangeType(Operation.RENAME_COLUMN);
        renameMap.clear();
        renameMap.put(col1TmpName, col1FinalName);
        renameMap.put(myCol2, col2FinalName);
        colChangeIn2.setRenameColumnMap(renameMap);

        AlterColumnRequest request2 = new AlterColumnRequest();
        request2.setProjectId(PROJECT_ID);
        request2.setCatalogName(myCatalogName);
        request2.setDatabaseName(myDatabaseName);
        request2.setTableName(myTableName);
        request2.setInput(colChangeIn2);
        assertDoesNotThrow(() -> client.alterColumn(request2));

        table = getTable();
        List<Column> fieldList2 = table.getStorageDescriptor().getColumns();
        assertEquals(expectedColNum, fieldList2.size());
        List<String> actualColNameList2 = new ArrayList<>();
        fieldList2.forEach(filed -> actualColNameList2.add(filed.getColumnName()));
        Collections.sort(actualColNameList2);
        assertEquals(expectedColNameList, actualColNameList2);
    }
}
