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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.model.Catalog;
import io.polycat.catalog.common.model.CatalogName;
import io.polycat.catalog.common.model.Database;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.StorageDescriptor;
import io.polycat.catalog.common.model.Table;
import io.polycat.catalog.common.model.TableCommit;
import io.polycat.catalog.common.model.Partition;
import io.polycat.catalog.common.plugin.request.input.CatalogInput;
import io.polycat.catalog.common.plugin.request.input.ColumnChangeInput;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;
import io.polycat.catalog.common.plugin.request.input.FilterInput;
import io.polycat.catalog.common.plugin.request.input.TableInput;
import io.polycat.catalog.common.model.Column;
import io.polycat.catalog.common.utils.UuidUtil;
import io.polycat.catalog.store.common.StoreConvertor;
import io.polycat.catalog.common.model.TableName;

import mockit.Mock;
import mockit.MockUp;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@SpringBootTest
public class TableServiceMultiThreadsTest extends TestUtil {

    static final String PROJECT_ID = "mttestproject_" + UUID.randomUUID().toString().substring(0, 8).toLowerCase();
    static final String CATALOG_NAME = "mttestcatalog_" + UUID.randomUUID().toString().substring(0, 8).toLowerCase();
    static final String DATABASE_NAME = "mttestdatabase_" + UUID.randomUUID().toString().substring(0, 8).toLowerCase();

    static Catalog newCatalog;
    static Database newDatabase;
    static DatabaseName databaseName;

    private static MockUp<UuidUtil> uuidUtilMockUp;

    boolean isFirstTest = true;

    public void beforeClass() throws Exception {
        if (isFirstTest) {
            clearFDB();

            // create catalog
            CatalogName catalogName = new CatalogName(PROJECT_ID, CATALOG_NAME);
            CatalogInput catalogInput = getCatalogInput(catalogName.getCatalogName());
            newCatalog = catalogService.createCatalog(PROJECT_ID, catalogInput);

            // create database
            databaseName = StoreConvertor.databaseName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME);
            DatabaseInput databaseInput = getDatabaseDTO(PROJECT_ID, CATALOG_NAME, DATABASE_NAME);
            newDatabase = databaseService.createDatabase(catalogName, databaseInput);
            isFirstTest = false;
        }
    }

    @BeforeEach
    public void beforeEach() throws Exception {
        beforeClass();
    }

    private TableInput createTableDTO(String tableName, int colNum) {
        TableInput tableInput = new TableInput();
        tableInput.setTableName(tableName);
        StorageDescriptor sd = new StorageDescriptor();
        sd.setColumns(getColumnDTO(colNum));
        tableInput.setStorageDescriptor(sd);
        tableInput.setOwner(userId);
        return tableInput;
    }


    @Test
    public void multiThreadCreateTable() throws InterruptedException {
        int threadNum = 2;
        Runnable runnable = () -> {
            String tableNameString = UUID.randomUUID().toString();
            TableInput tableInput = createTableDTO(tableNameString, 3);
            tableService.createTable(databaseName, tableInput);
            Table table = tableService.getTableByName(StoreConvertor.tableName(databaseName.getProjectId(),
                databaseName.getCatalogName(), databaseName.getDatabaseName(), tableNameString));
            assertTrue(table.getTableName().equals(tableNameString));
        };
        List<Throwable> uncaught = multiThreadExecute(runnable, "create", threadNum);

        assertEquals(uncaught.size(), 0);
    }

    @Test
    public void multiThreadCreateTableWithSameName() throws InterruptedException {
        String tableNameString = "tableTest_654321";

        int threadNum = 2;
        Runnable runnable = () -> {
            TableInput tableInput = createTableDTO(tableNameString, 3);
            tableService.createTable(databaseName, tableInput);
            Table table = tableService.getTableByName(StoreConvertor.tableName(databaseName.getProjectId(),
                databaseName.getCatalogName(), databaseName.getDatabaseName(), tableNameString));
            assertTrue(table.getTableName().equals(tableNameString));
        };
        List<Throwable> uncaught = multiThreadExecute(runnable, "drop", threadNum);

        assertEquals(uncaught.size(), 1);
        for (Throwable e : uncaught) {
            assertEquals(e.getMessage(), String.format("Table [tableTest_654321] already exists"));
        }
    }

    @Test
    public void  createTableWithCommitUnknownException() {
        String catalogCommitId = "mock_" + UuidUtil.generateUUID32();
        uuidUtilMockUp = new MockUp<UuidUtil>() {
            @Mock
            public String generateCatalogCommitId() {
                return catalogCommitId;
            }
        };

        String tableNameString1 = UuidUtil.generateUUID32();
        TableInput tableInput = createTableDTO(tableNameString1, 3);
        tableService.createTable(databaseName, tableInput);
        Table table = tableService.getTableByName(StoreConvertor.tableName(databaseName.getProjectId(),
            databaseName.getCatalogName(), databaseName.getDatabaseName(), tableNameString1));
        assertTrue(table.getTableName().equals(tableNameString1));

        String tableNameString2 = UuidUtil.generateUUID32();
        tableInput = createTableDTO(tableNameString2, 3);
        tableService.createTable(databaseName, tableInput);
        table = tableService.getTableByName(StoreConvertor.tableName(databaseName.getProjectId(),
            databaseName.getCatalogName(), databaseName.getDatabaseName(), tableNameString1));
        assertTrue(table.getTableName().equals(tableNameString1));

        CatalogServerException exception = assertThrows(CatalogServerException.class,
            () -> tableService.getTableByName(StoreConvertor.tableName(databaseName.getProjectId(),
                databaseName.getCatalogName(), databaseName.getDatabaseName(), tableNameString2)));
        assertEquals(exception.getMessage(),
            String.format("Table [%s] not found", tableNameString2));

        uuidUtilMockUp.tearDown();
    }

    @Test
    public void multiThreadDropTable() throws InterruptedException {
        String tableNameString = UUID.randomUUID().toString();
        TableInput tableInput = createTableDTO(tableNameString, 3);
        tableService.createTable(databaseName, tableInput);
        Table table = tableService.getTableByName(StoreConvertor.tableName(databaseName.getProjectId(),
            databaseName.getCatalogName(), databaseName.getDatabaseName(), tableNameString));
        assertTrue(table.getTableName().equals(tableNameString));

        TableName tableName = StoreConvertor.tableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, tableNameString);

        int threadNum = 2;
        Runnable runnable = () -> {
            tableService.dropTable(tableName, true, false, false);
            tableService.getTableByName(tableName);
        };
        List<Throwable> uncaught = multiThreadExecute(runnable, "drop", threadNum);

        assertEquals(uncaught.size(), threadNum);
        for (Throwable e : uncaught) {
            assertEquals(e.getMessage(), String.format("Table [%s] not found",  tableName.getTableName()));
        }
    }

    @Test
    public void multiThreadDropTablePurge() throws InterruptedException {
        String tableNameString = UUID.randomUUID().toString();
        TableInput tableInput = createTableDTO(tableNameString, 3);
        tableService.createTable(databaseName, tableInput);
        Table table = tableService.getTableByName(StoreConvertor.tableName(databaseName.getProjectId(),
            databaseName.getCatalogName(), databaseName.getDatabaseName(), tableNameString));
        assertTrue(table.getTableName().equals(tableNameString));

        TableName tableName = StoreConvertor.tableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, tableNameString);

        int threadNum = 2;
        Runnable runnable = () -> {
            tableService.dropTable(tableName, false, false, true);
            tableService.getTableByName(tableName);
        };
        List<Throwable> uncaught = multiThreadExecute(runnable, "drop-purge", threadNum);


        assertEquals(uncaught.size(), threadNum);
        for (Throwable e : uncaught) {
            assertEquals(e.getMessage(), String.format("Table [%s] not found",  tableName.getTableName()));
        }
    }

    @Test
    public void multiThreadRenameTable() throws InterruptedException {
        String tableNameString = UUID.randomUUID().toString();
        TableInput tableInput = createTableDTO(tableNameString, 3);
        tableService.createTable(databaseName, tableInput);
        Table table = tableService.getTableByName(StoreConvertor.tableName(databaseName.getProjectId(),
            databaseName.getCatalogName(), databaseName.getDatabaseName(), tableNameString));
        assertTrue(table.getTableName().equals(tableNameString));

        TableName tableName = StoreConvertor.tableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, tableNameString);
        String tableNameNew = UUID.randomUUID().toString();
        TableInput tableInputNew = createTableDTO(tableNameNew, 3);

        int threadNum = 2;
        Runnable runnable = () -> {
            tableService.alterTable(tableName, tableInputNew, null);
        };
        List<Throwable> uncaught = multiThreadExecute(runnable, "alter", threadNum);


        table = tableService.getTableByName(StoreConvertor.tableName(databaseName.getProjectId(),
            databaseName.getCatalogName(), databaseName.getDatabaseName(), tableNameNew));
        assertTrue(table.getTableName().equals(tableNameNew));

        assertEquals(uncaught.size(), 1);
        for (Throwable e : uncaught) {
            assertEquals(e.getMessage(), String.format("Table [%s] not found",  tableName.getTableName()));
        }
    }

    @Test
    public void multiThreadUndropTableName() throws InterruptedException {
        String tableNameString = UUID.randomUUID().toString();
        TableInput tableInput = createTableDTO(tableNameString, 3);
        tableService.createTable(databaseName, tableInput);
        Table table = tableService.getTableByName(StoreConvertor.tableName(databaseName.getProjectId(),
            databaseName.getCatalogName(), databaseName.getDatabaseName(), tableNameString));
        assertTrue(table.getTableName().equals(tableNameString));

        TableName tableName = StoreConvertor.tableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, tableNameString);
        tableService.dropTable(tableName, true, false, false);

        int threadNum = 2;
        Runnable runnable = () -> {
            tableService.undropTable(tableName, table.getTableId(), "");
        };
        List<Throwable> uncaught = multiThreadExecute(runnable, "undrop", threadNum);

        Table tableRecord = tableService.getTableByName(tableName);
        assertNotNull(tableRecord);
        assertTrue(tableRecord.getTableName().equals(tableNameString));

        assertEquals(uncaught.size(), 1);
        for (Throwable e : uncaught) {
            assertEquals(e.getMessage(), String.format("Table [%s] not found",  tableName.getTableName()));
        }
    }

    @Test
    public void multiThreadRestoreTable() throws InterruptedException {
        String tableNameString = UUID.randomUUID().toString();
        TableInput tableInput = createTableDTO(tableNameString, 3);
        tableService.createTable(databaseName, tableInput);
        Table table = tableService.getTableByName(StoreConvertor.tableName(databaseName.getProjectId(),
            databaseName.getCatalogName(), databaseName.getDatabaseName(), tableNameString));
        assertTrue(table.getTableName().equals(tableNameString));

        TableName tableName = StoreConvertor
            .tableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, tableNameString);
        TableCommit tableCommit1 = tableService.getLatestTableCommit(tableName);
        assertNotNull(tableCommit1);
        String version1 = tableCommit1.getCommitVersion();

        String tableNameNew = UUID.randomUUID().toString();
        TableInput tableInputNew = createTableDTO(tableNameNew, 3);
        tableService.alterTable(tableName, tableInputNew, null);

        TableName tablePathNameNew = StoreConvertor
            .tableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, tableNameNew);

        int threadNum = 2;
        Runnable runnable = () -> {
            tableService.restoreTable(tablePathNameNew, version1);
        };
        List<Throwable> uncaught = multiThreadExecute(runnable, "restore", threadNum);

        Table tableRecord = tableService.getTableByName(tableName);
        assertNotNull(tableRecord);
        assertTrue(tableRecord.getTableName().equals(tableNameString));

        assertEquals(uncaught.size(), 1);
        for (Throwable e : uncaught) {
            assertEquals(e.getMessage(), String.format("Table [%s] not found",  tableNameNew));
        }
    }


    @Test
    public void multiThreadInsertPartition() throws InterruptedException {
        String tableNameString = UUID.randomUUID().toString();
        TableInput tableInput = createTableDTO(tableNameString, 3);
        tableInput.setLmsMvcc(true);
        tableInput.setPartitionKeys(Collections.singletonList(new Column("partition", "String")));
        tableService.createTable(databaseName, tableInput);

        TableName tableName = StoreConvertor
            .tableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, tableNameString);

        Table checkTbl = tableService.getTableByName(tableName);
        assertNotNull(checkTbl);
        assertEquals(tableNameString, checkTbl.getTableName());

        int threadNum = 2;
        String[] partitionPath = {"partition=1", "partition=2"};
        Thread[] threads = new Thread[threadNum];
        for (int i = 0; i < threadNum; i ++) {
            String partitionPathParams = partitionPath[i];
            threads[i] = new Thread(() -> {
                partitionService.addPartition(tableName,
                    buildPartition(checkTbl, partitionPathParams, "path"));
            }, "insert-partition" + i);
        }

        List<Throwable> uncaught = multiThreadExecute(threads);
        assertEquals(uncaught.size(), 0);

        Partition[] partitions = partitionService.listPartitions(tableName, (FilterInput)null);
        assertEquals(partitions.length, 2);


    }

    @Test
    public void multiThreadAddColumn() throws InterruptedException {
        String tableNameString = UUID.randomUUID().toString();
        int defaultColNum = 3;
        TableInput tableInput = createTableDTO(tableNameString, defaultColNum);
        tableService.createTable(databaseName, tableInput);
        Table table = tableService.getTableByName(StoreConvertor.tableName(databaseName.getProjectId(),
            databaseName.getCatalogName(), databaseName.getDatabaseName(), tableNameString));
        assertTrue(table.getTableName().equals(tableNameString));

        TableName tableName = StoreConvertor
            .tableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, tableNameString);

        int threadNum = 3;
        Thread[] threads = new Thread[threadNum];
        for (int i = 0; i < threadNum; i++) {
            String newColName = "new_col_" + i%2;
            ColumnChangeInput colChangeIn = new ColumnChangeInput();
            colChangeIn.setChangeType(Operation.ADD_COLUMN);
            List<Column> addColumnList = new ArrayList<>();
            addColumnList.add(new Column(newColName, "STRING"));
            colChangeIn.setColumnList(addColumnList);

            threads[i] = new Thread(() -> {
                tableService.alterColumn(tableName, colChangeIn);
            }, "add-column" + i);
        }

        List<Throwable> uncaught = multiThreadExecute(threads);

        table = tableService.getTableByName(StoreConvertor.tableName(databaseName.getProjectId(),
            databaseName.getCatalogName(), databaseName.getDatabaseName(), tableNameString));
        assertEquals(defaultColNum + 2, table.getStorageDescriptor().getColumns().size());

        assertEquals(uncaught.size(), 1);
        for (Throwable e : uncaught) {
            assertEquals(String.format("Column [%s] is duplicated",  "new_col_0"), e.getMessage());
        }
    }

    @Test
    public void multiThreadRenameColumn() throws InterruptedException {
        String tableNameString = UUID.randomUUID().toString();
        int defaultColNum = 3;
        TableInput tableInput = createTableDTO(tableNameString, defaultColNum);
        tableService.createTable(databaseName, tableInput);
        Table table = tableService.getTableByName(StoreConvertor.tableName(databaseName.getProjectId(),
            databaseName.getCatalogName(), databaseName.getDatabaseName(), tableNameString));
        assertTrue(table.getTableName().equals(tableNameString));

        TableName tableName = StoreConvertor
            .tableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, tableNameString);

        table = tableService.getTableByName(tableName);
        String originName = table.getStorageDescriptor().getColumns().get(0).getColumnName();

        ColumnChangeInput colChangeIn1 = new ColumnChangeInput();
        colChangeIn1.setChangeType(Operation.RENAME_COLUMN);
        HashMap<String, String> renameMap = new HashMap<>();
        renameMap.put(originName, "new_col_654321");
        colChangeIn1.setRenameColumnMap(renameMap);

        int threadNum = 2;
        Runnable runnable = () -> {
            tableService.alterColumn(tableName, colChangeIn1);
        };
        List<Throwable> uncaught = multiThreadExecute(runnable, "rename-column", threadNum);

        table = tableService.getTableByName(tableName);
        assertTrue(table.getStorageDescriptor().getColumns().get(0).getColumnName().equals("new_col_654321"));

        assertEquals(uncaught.size(), 1);
        for (Throwable e : uncaught) {
            assertEquals(e.getMessage(), String.format("Column [%s] not found",  originName));
        }
    }
}
