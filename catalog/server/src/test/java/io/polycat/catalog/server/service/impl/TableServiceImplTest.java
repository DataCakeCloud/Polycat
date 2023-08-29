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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.model.Catalog;
import io.polycat.catalog.common.model.CatalogName;
import io.polycat.catalog.common.model.Column;
import io.polycat.catalog.common.model.Database;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.MetaObjectName;
import io.polycat.catalog.common.model.ScanRecordCursorResult;
import io.polycat.catalog.common.model.Table;
import io.polycat.catalog.common.model.TableBaseHistoryObject;
import io.polycat.catalog.common.model.TableCommit;
import io.polycat.catalog.common.model.TableCommitObject;
import io.polycat.catalog.common.model.TableIdent;
import io.polycat.catalog.common.model.TableName;
import io.polycat.catalog.common.model.TableSchemaHistoryObject;
import io.polycat.catalog.common.model.TableStorageHistoryObject;
import io.polycat.catalog.common.model.TraverseCursorResult;
import io.polycat.catalog.common.plugin.request.input.CatalogInput;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;
import io.polycat.catalog.common.plugin.request.input.TableInput;
import io.polycat.catalog.store.common.StoreConvertor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
public class TableServiceImplTest extends TestUtil {

    private static final Logger logger = Logger.getLogger(TableServiceImplTest.class);

    @BeforeEach
    public void beforeClass() throws NoSuchFieldException, IllegalAccessException {
        createCatalogBeforeClass();
        createDatabaseBeforeClass();
    }

    @Test
    public void createTableTest() {
        String tableNameString = UUID.randomUUID().toString().toLowerCase();

        TableInput tableInput = getTableDTO(tableNameString, 3);
        DatabaseName databaseName = StoreConvertor.databaseName(projectId, catalogNameString, databaseNameString);
        tableService.createTable(databaseName, tableInput);

        TableName tableName = StoreConvertor
            .tableName(projectId, catalogNameString, databaseNameString, tableNameString);
        TableIdent tableIdent = TableObjectHelper.getTableIdentByName(tableName);
        assertNotNull(tableIdent);

        Table table1 = tableService.getTableByName(tableName);
        assertEquals(catalogNameString, table1.getCatalogName());
        assertEquals(databaseNameString, table1.getDatabaseName());
        assertNotEquals(0, table1.getCreateTime());
        assertNotNull(table1.getStorageDescriptor().getLocation());
    }

    @Test
    public void createExistTableTest() {
        String tableName = UUID.randomUUID().toString().toLowerCase();

        TableInput tableInput = getTableDTO(tableName, 3);
        DatabaseName databaseName = StoreConvertor.databaseName(projectId, catalogNameString, databaseNameString);
        tableService.createTable(databaseName, tableInput);

        try {
            tableService.createTable(databaseName, tableInput);
        } catch (CatalogServerException e) {
            assertEquals(e.getErrorCode(), ErrorCode.TABLE_ALREADY_EXIST);
        }
    }

    @Test
    public void createMultiTableTest() {
        for (int idx = 0; idx < 3; ++idx) {
            String tableName = UUID.randomUUID().toString().toLowerCase();

            TableInput tableInput = getTableDTO(tableName, 3);
            DatabaseName databaseName = StoreConvertor.databaseName(projectId, catalogNameString, databaseNameString);
            tableService.createTable(databaseName, tableInput);
        }
    }


    @Test
    public void createSameNameTableTest() {
        String tableName = UUID.randomUUID().toString().toLowerCase();

        for (int idx = 0; idx < 3; ++idx) {
            String dbName = UUID.randomUUID().toString().toLowerCase().toLowerCase();

            DatabaseInput databaseInput = getDatabaseDTO(projectId, catalogNameString, dbName);
            CatalogName catalogName = StoreConvertor.catalogName(projectId, catalogNameString);
            String dbId = databaseService.createDatabase(catalogName, databaseInput).getDatabaseId();
            assertNotNull(dbId);

            TableInput tableInput = getTableDTO(tableName, 5);
            DatabaseName databaseName = StoreConvertor.databaseName(projectId, catalogNameString, dbName);
            tableService.createTable(databaseName, tableInput);
        }
    }

    @Test
    public void getNotExistTableByNameTest() {
        try {
            String tableNameString = UUID.randomUUID().toString().toLowerCase();
            TableName tableName = StoreConvertor
                .tableName(projectId, catalogNameString, databaseNameString, tableNameString);

            Table table = tableService.getTableByName(tableName);
            assertNull(table);
        } catch (CatalogServerException e) {
            assertEquals(e.getErrorCode(), ErrorCode.TABLE_NOT_FOUND);
        }
    }

    @Test
    public void getLatestTableTest() {
        String tableNameString = UUID.randomUUID().toString().toLowerCase();

        TableInput tableInput = getTableDTO(tableNameString, 3);
        DatabaseName databaseName = StoreConvertor.databaseName(projectId, catalogNameString, databaseNameString);
        tableService.createTable(databaseName, tableInput);
        TableName tableName = StoreConvertor
            .tableName(projectId, catalogNameString, databaseNameString, tableNameString);
        TableCommit tableCommit = tableService.getLatestTableCommit(tableName);
        assertNotNull(tableCommit);
        assertTrue(!tableCommit.getCommitVersion().isEmpty());
    }

    @Test
    public void getLatestNotExistTableTest() {
        String tableNameString = UUID.randomUUID().toString().toLowerCase();
        TableName tableName = StoreConvertor
            .tableName(projectId, catalogNameString, databaseNameString, tableNameString);

        CatalogServerException exception = assertThrows(CatalogServerException.class,
            () -> tableService.getLatestTableCommit(tableName));
        assertEquals(exception.getMessage(),
            String.format("Table [%s] not found", tableNameString));
    }

    @Test
    public void getLatestTableWithGetTest() {
        String tableNameString = UUID.randomUUID().toString().toLowerCase();

        TableInput tableInput = getTableDTO(tableNameString, 3);
        DatabaseName databaseName = StoreConvertor.databaseName(projectId, catalogNameString, databaseNameString);
        tableService.createTable(databaseName, tableInput);

        TableName tableName = StoreConvertor
            .tableName(projectId, catalogNameString, databaseNameString, tableNameString);
        TableCommit tableHistory1 = tableService.getLatestTableCommit(tableName);
        assertNotNull(tableHistory1);

        Table table1 = tableService.getTableByName(tableName);
        assertNotNull(table1);
        TableCommit tableHistory2 = tableService.getLatestTableCommit(tableName);
        assertNotNull(tableHistory2);
        assertEquals(tableHistory1, tableHistory2);
    }

    @Test
    public void dropTableExist() {
        String tableNameString = UUID.randomUUID().toString().toLowerCase();

        TableInput tableInput = getTableDTO(tableNameString, 3);
        DatabaseName databaseName = StoreConvertor.databaseName(projectId, catalogNameString, databaseNameString);
        tableService.createTable(databaseName, tableInput);

        TableName tableName = StoreConvertor
            .tableName(projectId, catalogNameString, databaseNameString, tableNameString);
        TableIdent tableIdent = TableObjectHelper.getTableIdentByName(tableName);
        assertNotNull(tableIdent);

        Table table1 = tableService.getTableByName(tableName);
        assertNotNull(table1);

        tableService.dropTable(tableName, false, false, false);

        CatalogServerException exception = assertThrows(CatalogServerException.class,
            () -> tableService.getTableByName(tableName));
        assertEquals(exception.getMessage(),
            String.format("Table [%s] not found", tableName.getTableName()));

    }

    private int getTableCommitRecordCnt(TableIdent tableIdent) {
        int cnt;
        ScanRecordCursorResult<List<TableCommitObject>> tableCommit = TableCommitHelper
            .listTableCommit(tableIdent);
        cnt = tableCommit.getResult().size();
        return cnt;
    }


    private int getTableStorageHistoryCnt(TableIdent tableIdent) {
        int cnt;
        ScanRecordCursorResult<List<TableStorageHistoryObject>> tableStorageHistory = TableStorageHelper
            .listTableStorageHistory(tableIdent);
        cnt = tableStorageHistory.getResult().size();
        return cnt;
    }


    private int getTablePropertiesHistoryCnt(TableIdent tableIdent) {
        int cnt;
        ScanRecordCursorResult<List<TableBaseHistoryObject>> tablePropertiesHistory = TableBaseHelper
            .listTableBaseHistory(tableIdent);
        cnt = tablePropertiesHistory.getResult().size();
        return cnt;
    }

    private int getTableSchemaHistoryCnt(TableIdent tableIdent) {
        int cnt;
        ScanRecordCursorResult<List<TableSchemaHistoryObject>> tableSchemaHistory = TableSchemaHelper
            .listTableSchemaHistory(tableIdent);
        cnt = tableSchemaHistory.getResult().size();
        return cnt;
    }

    @Test
    public void dropTablePurgeExist() {
        String tableNameString = UUID.randomUUID().toString().toLowerCase();

        // 1. build table
        TableInput tableInput = getTableDTO(tableNameString, 3);
        DatabaseName databaseName = StoreConvertor.databaseName(projectId, catalogNameString, databaseNameString);
        tableService.createTable(databaseName, tableInput);

        // 2. check table Reference
        TableName tableName = StoreConvertor
            .tableName(projectId, catalogNameString, databaseNameString, tableNameString);
        TableIdent tableIdent = TableObjectHelper.getTableIdentByName(tableName);
        assertNotNull(tableIdent);

        // 3. check history not null
        assertEquals(1, getTableCommitRecordCnt(tableIdent));
        assertEquals(1, getTableSchemaHistoryCnt(tableIdent));
        assertEquals(1, getTablePropertiesHistoryCnt(tableIdent));
        assertEquals(1, getTableStorageHistoryCnt(tableIdent));

        // 4. dropTablePurge
        tableService.dropTable(tableName, false, false, true);

        // 5. check Table not exist
        CatalogServerException exception = assertThrows(CatalogServerException.class,
            () -> tableService.getTableByName(tableName));
        assertEquals(String.format("Table [%s] not found", tableName.getTableName()),
            exception.getMessage());

        // 6. check Table can't recovery
        assertThrows(CatalogServerException.class,
            () -> tableService.undropTable(tableName, tableIdent.getTableId(), tableNameString));

        // 7. check history are null
        assertEquals(0, getTableCommitRecordCnt(tableIdent));
        assertEquals(0, getTableSchemaHistoryCnt(tableIdent));
        assertEquals(0, getTablePropertiesHistoryCnt(tableIdent));
        assertEquals(0, getTableStorageHistoryCnt(tableIdent));
    }

    @Test
    public void dropTablePurgeNotExist() {
        String tableNameString = UUID.randomUUID().toString().toLowerCase();

        // 1. build table
        TableInput tableInput = getTableDTO(tableNameString, 3);
        DatabaseName databaseName = StoreConvertor.databaseName(projectId, catalogNameString, databaseNameString);
        String tableId = "000000000000";
        TableName tableName = StoreConvertor
            .tableName(projectId, catalogNameString, databaseNameString, tableNameString);

        // 4. dropTablePurge
        assertThrows(CatalogServerException.class, () -> tableService.dropTable(tableName, false, false, true));
    }

    @Test
    public void dropTablePurgeOnSubBranchTest() {
        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String mainBranchDatabaseName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        int mainBranchTableNum = 2;

        //create branch catalog, database, tables
        List<Table> mainBranchTableList = prepareOneLevelBranchTest(mainBranchCatalogName, mainBranchDatabaseName,
            subBranchCatalogName, mainBranchTableNum);

        //get catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        DatabaseName mainDatabasePathName = StoreConvertor.databaseName(projectId, mainBranchCatalogName,
            mainBranchDatabaseName);
        Database mainBranchDatabaseRecord = databaseService.getDatabaseByName(mainDatabasePathName);
        assertNotNull(mainBranchDatabaseRecord);

        String dropTableName = mainBranchTableList.get(0).getTableName();

        TableName subBranchTablePathName = StoreConvertor.tableName(projectId, subBranchCatalogRecord.getCatalogName(),
            mainBranchDatabaseRecord.getDatabaseName(), dropTableName);

        //get table from sub-branch
        Table subBranchTable = tableService.getTableByName(subBranchTablePathName);
        assertNotNull(subBranchTable);
        assertEquals(subBranchTable.getTableName(), mainBranchTableList.get(0).getTableName());
        assertEquals(subBranchTable.getDatabaseName(), mainBranchDatabaseRecord.getDatabaseName());
        assertEquals(subBranchTable.getCatalogName(), subBranchCatalogRecord.getCatalogName());

        //drop sub-branch table
        tableService.dropTable(subBranchTablePathName, false, false, true);
//        tableService.dropTablePurge(subBranchTablePathName, false);

        //get table from sub-branch
        CatalogServerException exception = assertThrows(CatalogServerException.class,
            () -> tableService.getTableByName(subBranchTablePathName));
        assertEquals(exception.getMessage(),
            String.format("Table [%s] not found", subBranchTablePathName.getTableName()));

        //get table from parent
        TableName mainBranchTablePathName = StoreConvertor.tableName(projectId,
            mainBranchCatalogRecord.getCatalogName(),
            mainBranchDatabaseRecord.getDatabaseName(), dropTableName);
        Table mainBranchTable = tableService.getTableByName(mainBranchTablePathName);
        assertNotNull(mainBranchTable);
        assertEquals(mainBranchTable.getCatalogName(), mainBranchCatalogRecord.getCatalogName());
    }

    @Test
    public void dropTablePurgeOnMainBranchTest() {
        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String mainBranchDatabaseName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        int mainBranchTableNum = 2;

        //create branch catalog, database, tables
        List<Table> mainBranchTableList = prepareOneLevelBranchTest(mainBranchCatalogName, mainBranchDatabaseName,
            subBranchCatalogName, mainBranchTableNum);

        //get catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        DatabaseName mainDatabasePathName = StoreConvertor.databaseName(projectId, mainBranchCatalogName,
            mainBranchDatabaseName);
        Database mainBranchDatabaseRecord = databaseService.getDatabaseByName(mainDatabasePathName);
        assertNotNull(mainBranchDatabaseRecord);

        String dropTableName = mainBranchTableList.get(0).getTableName();

        TableName mainBranchTablePathName = StoreConvertor.tableName(projectId,
            mainBranchCatalogRecord.getCatalogName(),
            mainBranchDatabaseRecord.getDatabaseName(), dropTableName);

        //get table from sub-branch
        Table mainBranchTable = tableService.getTableByName(mainBranchTablePathName);
        assertNotNull(mainBranchTable);
        assertEquals(mainBranchTable.getTableId(), mainBranchTableList.get(0).getTableId());
        assertEquals(mainBranchTable.getDatabaseName(), mainBranchDatabaseRecord.getDatabaseName());
        assertEquals(mainBranchTable.getCatalogName(), mainBranchDatabaseRecord.getCatalogName());

        //drop main-branch table
        assertThrows(CatalogServerException.class,
            () -> tableService.dropTable(mainBranchTablePathName, false, false, true));
        //get table from main-branch
        mainBranchTable = tableService.getTableByName(mainBranchTablePathName);
        assertNotNull(mainBranchTable);

        //get table from parent
        TableName subBranchTablePathName = StoreConvertor.tableName(projectId, subBranchCatalogRecord.getCatalogName(),
            mainBranchDatabaseRecord.getDatabaseName(), dropTableName);
        Table subBranchTable = tableService.getTableByName(subBranchTablePathName);
        assertNotNull(subBranchTable);
        assertEquals(subBranchTable.getCatalogName(), subBranchCatalogRecord.getCatalogName());
    }

    @Test
    public void dropTablePurgeMutiVersion() {
        String tableNameString = UUID.randomUUID().toString().toLowerCase();

        // 1. build table
        TableInput tableInput = getTableDTO(tableNameString, 3);
        DatabaseName databaseName = StoreConvertor.databaseName(projectId, catalogNameString, databaseNameString);
        tableService.createTable(databaseName, tableInput);

        // 2. check table Reference
        TableName tableName = StoreConvertor
            .tableName(projectId, catalogNameString, databaseNameString, tableNameString);
        TableIdent tableIdent = TableObjectHelper.getTableIdentByName(tableName);
        assertNotNull(tableIdent);

        // 3. alter table 1000 times
        TableName latestTableName;
        latestTableName = new TableName(tableName);
        for (int i = 0; i < 200; i++) {
            String tableNameNew = UUID.randomUUID().toString().toLowerCase();
            tableInput = getTableDTO(tableNameNew, i % 10 + 1);
            tableService.alterTable(latestTableName, tableInput, null);
            latestTableName = new TableName(tableName);
            latestTableName.setTableName(tableNameNew);
        }

        // 4. check history not null
        assertEquals(201, getTableCommitRecordCnt(tableIdent));
        assertEquals(1, getTableSchemaHistoryCnt(tableIdent));
        assertEquals(1, getTablePropertiesHistoryCnt(tableIdent));
        assertEquals(1, getTableStorageHistoryCnt(tableIdent));

        // 5. dropTablePurge
        tableService.dropTable(latestTableName, false, false, true);
//        tableService.dropTablePurge(latestTableName, false);

        // 6. check Table not exist
        CatalogServerException exception = assertThrows(CatalogServerException.class,
            () -> tableService.getTableByName(tableName));
        assertEquals(String.format("Table [%s] not found", tableName.getTableName()), exception.getMessage());

        // 7. check Table can't recovery
        assertThrows(CatalogServerException.class,
            () -> tableService.undropTable(tableName, tableIdent.getTableId(), tableNameString));

        // 8. check history are null
        assertEquals(0, getTableCommitRecordCnt(tableIdent));
        assertEquals(0, getTableSchemaHistoryCnt(tableIdent));
        assertEquals(0, getTablePropertiesHistoryCnt(tableIdent));
        assertEquals(0, getTableStorageHistoryCnt(tableIdent));
    }


    @Test
    public void purgeTableByNameSuccess() {
        String tableNameString = UUID.randomUUID().toString().toLowerCase();

        // 1. build table
        TableInput tableInput = getTableDTO(tableNameString, 3);
        DatabaseName databaseName = StoreConvertor.databaseName(projectId, catalogNameString, databaseNameString);
        tableService.createTable(databaseName, tableInput);

        // 2. check table Reference
        TableName tableName = StoreConvertor
            .tableName(projectId, catalogNameString, databaseNameString, tableNameString);
        TableIdent tableIdent = TableObjectHelper.getTableIdentByName(tableName);
        assertNotNull(tableIdent);

        // 3. check history not null
        assertEquals(1, getTableCommitRecordCnt(tableIdent));
        assertEquals(1, getTableSchemaHistoryCnt(tableIdent));
        assertEquals(1, getTablePropertiesHistoryCnt(tableIdent));
        assertEquals(1, getTableStorageHistoryCnt(tableIdent));

        // 4. dropTable and purge table
        tableService.dropTable(tableName, false, false, false);
        tableService.purgeTable(tableName, null);

        // 5. check Table not exist
        CatalogServerException exception = assertThrows(CatalogServerException.class,
            () -> tableService.getTableByName(tableName));
        assertEquals(String.format("Table [%s] not found", tableName.getTableName()),
            exception.getMessage());

        // 6. check Table can't recovery
        assertThrows(CatalogServerException.class,
            () -> tableService.undropTable(tableName, tableIdent.getTableId(), tableNameString));

        // 7. check history are null
        assertEquals(0, getTableCommitRecordCnt(tableIdent));
        assertEquals(0, getTableSchemaHistoryCnt(tableIdent));
        assertEquals(0, getTablePropertiesHistoryCnt(tableIdent));
        assertEquals(0, getTableStorageHistoryCnt(tableIdent));
    }

    @Test
    public void purgeTableByNameNotDropFailed() {
        String tableNameString = UUID.randomUUID().toString().toLowerCase();

        // 1. build table
        TableInput tableInput = getTableDTO(tableNameString, 3);
        DatabaseName databaseName = StoreConvertor.databaseName(projectId, catalogNameString, databaseNameString);
        tableService.createTable(databaseName, tableInput);

        // 2. check table Reference
        TableName tableName = StoreConvertor
            .tableName(projectId, catalogNameString, databaseNameString, tableNameString);
        TableIdent tableIdent = TableObjectHelper.getTableIdentByName(tableName);
        assertNotNull(tableIdent);

        // 3. check history not null
        assertEquals(1, getTableCommitRecordCnt(tableIdent));
        assertEquals(1, getTableSchemaHistoryCnt(tableIdent));
        assertEquals(1, getTablePropertiesHistoryCnt(tableIdent));
        assertEquals(1, getTableStorageHistoryCnt(tableIdent));

        // 4. purge table not drop
        assertThrows(CatalogServerException.class, () -> tableService.purgeTable(tableName, tableIdent.getTableId()));
    }

    @Test
    public void purgeTableByNameSameNameFailed() {
        String tableNameString = UUID.randomUUID().toString().toLowerCase();

        // 1. build table and drop
        TableInput tableInput = getTableDTO(tableNameString, 3);
        DatabaseName databaseName = StoreConvertor.databaseName(projectId, catalogNameString, databaseNameString);
        tableService.createTable(databaseName, tableInput);
        TableName tableName = StoreConvertor
            .tableName(projectId, catalogNameString, databaseNameString, tableNameString);
        tableService.dropTable(tableName, false, false, false);
        // 2. build table with same name and drop
        tableService.createTable(databaseName, tableInput);
        tableService.dropTable(tableName, false, false, false);

        // 3. drop table failed
        CatalogServerException exception = assertThrows(CatalogServerException.class,
            () -> tableService.purgeTable(tableName, null));
        assertEquals("Table multiple exists", exception.getMessage());
    }

    @Test
    public void purgeTableByTableIdSameNameSuccess() {
        String tableNameString = UUID.randomUUID().toString().toLowerCase();

        // 1. build table and drop
        TableInput tableInput = getTableDTO(tableNameString, 3);
        DatabaseName databaseName = StoreConvertor.databaseName(projectId, catalogNameString, databaseNameString);
        tableService.createTable(databaseName, tableInput);
        TableName tableName = StoreConvertor
            .tableName(projectId, catalogNameString, databaseNameString, tableNameString);
        TableIdent tableIdent = TableObjectHelper.getTableIdentByName(tableName);
        tableService.dropTable(tableName, false, false, false);
        // 2. build table with same name and drop
        tableService.createTable(databaseName, tableInput);
        tableIdent = TableObjectHelper.getTableIdentByName(tableName);
        tableService.dropTable(tableName, false, false, false);

        assertEquals(2, getTableCommitRecordCnt(tableIdent));
        assertEquals(1, getTableSchemaHistoryCnt(tableIdent));
        assertEquals(1, getTablePropertiesHistoryCnt(tableIdent));
        assertEquals(1, getTableStorageHistoryCnt(tableIdent));

        // 3. purge table not drop
        tableService.purgeTable(tableName, tableIdent.getTableId());
        // 4. check history are null
        assertEquals(0, getTableCommitRecordCnt(tableIdent));
        assertEquals(0, getTableSchemaHistoryCnt(tableIdent));
        assertEquals(0, getTablePropertiesHistoryCnt(tableIdent));
        assertEquals(0, getTableStorageHistoryCnt(tableIdent));
    }

    @Test
    public void purgeTableOnMainBranchTest() {
        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String mainBranchDatabaseName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        int mainBranchTableNum = 2;

        //create branch catalog, database, tables
        List<Table> mainBranchTableList = prepareOneLevelBranchTest(mainBranchCatalogName, mainBranchDatabaseName,
            subBranchCatalogName, mainBranchTableNum);

        //get catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        DatabaseName mainDatabasePathName = StoreConvertor.databaseName(projectId, mainBranchCatalogName,
            mainBranchDatabaseName);
        Database mainBranchDatabaseRecord = databaseService.getDatabaseByName(mainDatabasePathName);
        assertNotNull(mainBranchDatabaseRecord);

        String dropTableName = mainBranchTableList.get(0).getTableName();

        TableName mainBranchTablePathName = StoreConvertor.tableName(projectId,
            mainBranchCatalogRecord.getCatalogName(),
            mainBranchDatabaseRecord.getDatabaseName(), dropTableName);

        //get table from sub-branch
        Table mainBranchTable = tableService.getTableByName(mainBranchTablePathName);
        assertNotNull(mainBranchTable);
        assertEquals(mainBranchTable.getTableName(), mainBranchTableList.get(0).getTableName());
        assertEquals(mainBranchTable.getDatabaseName(), mainBranchDatabaseRecord.getDatabaseName());
        assertEquals(mainBranchTable.getCatalogName(), mainBranchDatabaseRecord.getCatalogName());

        //drop main-branch table
        tableService.dropTable(mainBranchTablePathName, false, false, false);
        assertThrows(CatalogServerException.class, () -> tableService.purgeTable(mainBranchTablePathName, null));
        //get table from main-branch
        tableService.undropTable(mainBranchTablePathName, null, dropTableName);
        mainBranchTable = tableService.getTableByName(mainBranchTablePathName);
        assertNotNull(mainBranchTable);

        //get table from parent
        TableName subBranchTablePathName = StoreConvertor.tableName(projectId, subBranchCatalogRecord.getCatalogName(),
            mainBranchDatabaseRecord.getDatabaseName(), dropTableName);
        Table subBranchTable = tableService.getTableByName(subBranchTablePathName);
        assertNotNull(subBranchTable);
        assertEquals(subBranchTable.getCatalogName(), subBranchCatalogRecord.getCatalogName());
    }

    @Test
    public void purgeTableOnSubBranchTest() {
        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String mainBranchDatabaseName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        int mainBranchTableNum = 2;

        //create branch catalog, database, tables
        List<Table> mainBranchTableList = prepareOneLevelBranchTest(mainBranchCatalogName, mainBranchDatabaseName,
            subBranchCatalogName, mainBranchTableNum);

        //get catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        DatabaseName mainDatabasePathName = StoreConvertor.databaseName(projectId, mainBranchCatalogName,
            mainBranchDatabaseName);
        Database mainBranchDatabaseRecord = databaseService.getDatabaseByName(mainDatabasePathName);
        assertNotNull(mainBranchDatabaseRecord);

        String dropTableName = mainBranchTableList.get(0).getTableName();

        TableName subBranchTablePathName = StoreConvertor.tableName(projectId, subBranchCatalogRecord.getCatalogName(),
            mainBranchDatabaseRecord.getDatabaseName(), dropTableName);

        //get table from sub-branch
        Table subBranchTable = tableService.getTableByName(subBranchTablePathName);
        assertNotNull(subBranchTable);
        assertEquals(subBranchTable.getTableName(), mainBranchTableList.get(0).getTableName());
        assertEquals(subBranchTable.getDatabaseName(), mainBranchDatabaseRecord.getDatabaseName());
        assertEquals(subBranchTable.getCatalogName(), subBranchCatalogRecord.getCatalogName());

        //drop sub-branch table
        tableService.dropTable(subBranchTablePathName, false, false, false);
        tableService.purgeTable(subBranchTablePathName, null);

        //get table from sub-branch
        CatalogServerException exception = assertThrows(CatalogServerException.class,
            () -> tableService.undropTable(subBranchTablePathName, null, dropTableName));
        assertEquals(exception.getMessage(),
            String.format("Table [%s] not found", subBranchTablePathName.getTableName()));

        //get table from parent
        TableName mainBranchTablePathName = StoreConvertor.tableName(projectId,
            mainBranchCatalogRecord.getCatalogName(),
            mainBranchDatabaseRecord.getDatabaseName(), dropTableName);
        Table mainBranchTable = tableService.getTableByName(mainBranchTablePathName);
        assertNotNull(mainBranchTable);
        assertEquals(mainBranchTable.getCatalogName(), mainBranchCatalogRecord.getCatalogName());
    }

    @Test
    public void undropTableTest() {
        String tableNameString = UUID.randomUUID().toString().toLowerCase();

        TableInput tableInput = getTableDTO(tableNameString, 3);
        DatabaseName databaseName = StoreConvertor.databaseName(projectId, catalogNameString, databaseNameString);
        tableService.createTable(databaseName, tableInput);

        TableName tableName = StoreConvertor.tableName(databaseName, tableNameString);
        Table table1 = tableService.getTableByName(tableName);
        assertNotNull(table1);

        tableService.dropTable(tableName, false, false, false);

        Throwable exception = assertThrows(CatalogServerException.class, () -> tableService.getTableByName(tableName));
        assertNotEquals(-1, exception.getMessage().indexOf("not found"));

        tableService.undropTable(tableName, table1.getTableId(), "");
        Table table3 = tableService.getTableByName(tableName);
        assertNotNull(table3);
        assertEquals(table1, table3);
    }

    @Test
    public void undropTableWithNewNameTest() {
        String tableNameString = UUID.randomUUID().toString().toLowerCase();

        TableInput tableInput = getTableDTO(tableNameString, 3);
        DatabaseName databaseName = StoreConvertor.databaseName(projectId, catalogNameString, databaseNameString);
        tableService.createTable(databaseName, tableInput);

        TableName tableName = StoreConvertor
            .tableName(projectId, catalogNameString, databaseNameString, tableNameString);
        Table table1 = tableService.getTableByName(tableName);
        assertNotNull(table1);
        tableService.dropTable(tableName, false, false, false);

        Throwable exception = assertThrows(CatalogServerException.class, () -> tableService.getTableByName(tableName));
        assertNotEquals(-1, exception.getMessage().indexOf("not found"));
//        Table table2 = tableService.getTableByName(tableName);
//        assertNull(table2);

        String tableNameString1 = UUID.randomUUID().toString().toLowerCase();
        tableService.undropTable(tableName, table1.getTableId(), tableNameString1);
        TableName undropTableName = new TableName(tableName);
        undropTableName.setTableName(tableNameString1);
        Table table3 = tableService.getTableByName(undropTableName);
        assertNotNull(table3);
        assertEquals(table1.getCatalogName(), table3.getCatalogName());
        assertEquals(table1.getDatabaseName(), table3.getDatabaseName());
        assertEquals(tableNameString1, table3.getTableName());

        List<Column> schema1 = table1.getFields();
        List<Column> schema3 = table3.getFields();

        for (int i = 0; i < schema1.size(); i++) {
            assertEquals(schema1.get(i).getComment(), schema3.get(i).getComment());
            assertEquals(schema1.get(i).getColumnName(), schema3.get(i).getColumnName());
            assertEquals(schema1.get(i).getColType(), schema3.get(i).getColType());
        }
    }

    @Test
    public void restoreTableTest1() {
        String tableNameString = UUID.randomUUID().toString().toLowerCase();

        logger.info("tableNameString: " + tableNameString);

        TableInput tableInput = getTableDTO(tableNameString, 3);
        DatabaseName databaseName = StoreConvertor.databaseName(projectId, catalogNameString, databaseNameString);
        tableService.createTable(databaseName, tableInput);

        TableName tableName = StoreConvertor
            .tableName(projectId, catalogNameString, databaseNameString, tableNameString);
        TableCommit tableHistory1 = tableService.getLatestTableCommit(tableName);
        assertNotNull(tableHistory1);
        String version1 = tableHistory1.getCommitVersion();

        String encodeText = tableHistory1.getCommitVersion();
        logger.info("encodeText:" + encodeText);
        tableService.restoreTable(tableName, encodeText);
        TableCommit tableHistory2 = tableService.getLatestTableCommit(tableName);
        assertNotNull(tableHistory2);
        String version2 = tableHistory2.getCommitVersion();
        assertEquals(tableHistory1.getCatalogId(), tableHistory2.getCatalogId());
        assertEquals(tableHistory1.getDatabaseId(), tableHistory2.getDatabaseId());
        assertEquals(tableHistory1.getTableId(), tableHistory2.getTableId());
        assertTrue(version1.compareTo(version2) < 0);
    }

    @Test
    public void restoreTableTest2() {
        String tableNameString = UUID.randomUUID().toString().toLowerCase();

        logger.info("tableNameString: " + tableNameString);

        TableInput tableInput = getTableDTO(tableNameString, 3);
        DatabaseName databaseName = StoreConvertor.databaseName(projectId, catalogNameString, databaseNameString);
        tableService.createTable(databaseName, tableInput);

        TableName tableName = StoreConvertor
            .tableName(projectId, catalogNameString, databaseNameString, tableNameString);
        TableCommit tableHistory1 = tableService.getLatestTableCommit(tableName);
        assertNotNull(tableHistory1);
        String version1 = tableHistory1.getCommitVersion();

        String tableNameNewString = UUID.randomUUID().toString().toLowerCase();

        logger.info("tableNameNewString: " + tableNameNewString);

        tableInput = getTableDTO(tableNameNewString, 3);
        tableService.alterTable(tableName, tableInput, null);

        TableName newTableName = StoreConvertor
            .tableName(projectId, catalogNameString, databaseNameString, tableNameNewString);
        TableCommit tableHistory2 = tableService.getLatestTableCommit(newTableName);
        assertNotNull(tableHistory2);
        String version2 = tableHistory2.getCommitVersion();
        assertTrue(version1.compareTo(version2) <= 0);

        String encodeText = tableHistory1.getCommitVersion();
        logger.info("encodeText:" + encodeText);
        tableService.restoreTable(newTableName, encodeText);
        TableCommit tableHistory3 = tableService.getLatestTableCommit(tableName);
        assertNotNull(tableHistory3);
        String version3 = tableHistory3.getCommitVersion();
        assertEquals(tableHistory1.getCatalogId(), tableHistory3.getCatalogId());
        assertEquals(tableHistory1.getDatabaseId(), tableHistory3.getDatabaseId());
        assertEquals(tableHistory1.getTableId(), tableHistory3.getTableId());
        assertTrue(version2.compareTo(version3) < 0);
    }

    @Test
    public void alterTableExist() {
        String tableNameString = UUID.randomUUID().toString().toLowerCase();

        TableInput tableInput = getTableDTO(tableNameString, 3);
        DatabaseName databaseName = StoreConvertor.databaseName(projectId, catalogNameString, databaseNameString);
        tableService.createTable(databaseName, tableInput);

        TableName tableName = StoreConvertor
            .tableName(projectId, catalogNameString, databaseNameString, tableNameString);
        TableIdent tableIdent = TableObjectHelper.getTableIdentByName(tableName);
        assertNotNull(tableIdent);
        assertEquals(tableIdent.getTableId(), tableIdent.getTableId());

        Table table1 = tableService.getTableByName(tableName);
        assertNotNull(table1);

        String tableNameNewString = UUID.randomUUID().toString().toLowerCase();
        assertNotSame(tableNameString, tableNameNewString);

        tableInput = getTableDTO(tableNameNewString, 3);
        TableName newTableName = StoreConvertor
            .tableName(projectId, catalogNameString, databaseNameString, tableNameNewString);
        tableService.alterTable(tableName, tableInput, null);

        CatalogServerException exception = assertThrows(CatalogServerException.class,
            () -> tableService.getTableByName(tableName));
        assertEquals(exception.getMessage(),
            String.format("Table [%s] not found", tableName.getTableName()));

        table1 = tableService.getTableByName(newTableName);
        assertNotNull(table1);
        assertEquals(table1.getTableName(), tableNameNewString);

    }

    @Test
    public void listTables() {
        String tableNameString1 = UUID.randomUUID().toString().toLowerCase();
        String tableNameString2 = UUID.randomUUID().toString().toLowerCase();
        String databaseNameForList = UUID.randomUUID().toString().toLowerCase();

        DatabaseInput databaseInput = getDatabaseDTO(projectId, catalogNameString, databaseNameForList);

        // todo modify createDatabase
        CatalogName catalogName = StoreConvertor.catalogName(projectId, catalogNameString);
        Database database = databaseService.createDatabase(catalogName, databaseInput);
        assertNotNull(database);

        TableInput tableInput = getTableDTO(tableNameString1, 3);
        DatabaseName databaseName = StoreConvertor.databaseName(projectId, catalogNameString, databaseNameForList);
        tableService.createTable(databaseName, tableInput);

        tableInput = getTableDTO(tableNameString2, 3);

        tableService.createTable(databaseName, tableInput);

        TableName tableName = StoreConvertor
            .tableName(projectId, catalogNameString, databaseNameForList, tableNameString1);
        TableIdent tableIdent = TableObjectHelper.getTableIdentByName(tableName);
        assertNotNull(tableIdent);

        tableName = StoreConvertor.tableName(projectId, catalogNameString, databaseNameForList, tableNameString2);
        tableIdent = TableObjectHelper.getTableIdentByName(tableName);
        assertNotNull(tableIdent);

        Integer maxResultes = 100;
        String pageToken = null;
        String filter = null;

        TraverseCursorResult<List<Table>> tableList = null;
        DatabaseName databaseName2 = StoreConvertor.databaseName(projectId, catalogNameString, databaseNameForList);

        tableList = tableService.listTable(databaseName2, true, maxResultes, pageToken, filter);

        long findNum = 0L;
        long existNum = 2L;
        for (Table table : tableList.getResult()) {
            assertEquals(table.getDatabaseName(), databaseNameForList);
            assertNotEquals("", table.getCatalogName());
            if ((table.getTableName().equals(tableNameString1))
                && (table.getCatalogName().equals(catalogNameString))
                && (table.getDatabaseName().equals(databaseNameForList))) {
                findNum++;
            } else if ((table.getTableName().equals(tableNameString2))
                && (table.getCatalogName().equals(catalogNameString))
                && (table.getDatabaseName().equals(databaseNameForList))) {
                findNum++;
            }
        }

        assertEquals(findNum, existNum);
        assertEquals(tableList.getResult().size(), existNum);

    }

    private List<Table> prepareOneLevelBranchTestWithDropped(String mainBranchCatalogName,
        String mainBranchDatabaseName,
        String subBranchCatalogName, int mainBranchTableNum) {
        //create main branch
        Catalog mainCatalog = createCatalogPrepareTest(projectId, mainBranchCatalogName);

        //create main branch database
        CatalogName mainCatalogName = StoreConvertor.catalogName(projectId, mainCatalog.getCatalogName());
        Database mainDatabase = createDatabasePrepareTest(mainCatalogName, mainBranchDatabaseName);

        //create table
        DatabaseName mainDatabaseName = StoreConvertor.databaseName(mainCatalogName, mainDatabase.getDatabaseName());
        List<Table> tableList = createTablesPrepareTest(mainDatabaseName, mainBranchTableNum);

        //drop table
        for (Table table : tableList) {
            TableName tableName = StoreConvertor.tableName(projectId, mainCatalogName.getCatalogName(),
                mainDatabaseName.getDatabaseName(), table.getTableName());

            logger.info("prepare drop tableName " + tableName);
            tableService.dropTable(tableName, false, false, false);
        }

        //create table with same name
        List<Table> tableListNew = new ArrayList<>();
        for (int i = 0; i < mainBranchTableNum; i++) {
            String tableNameString = tableList.get(i).getTableName();

            TableInput tableInput = getTableDTO(tableNameString, 3);

            tableService.createTable(mainDatabaseName, tableInput);

            TableName tableName = StoreConvertor.tableName(mainDatabaseName.getProjectId(),
                mainDatabaseName.getCatalogName(),
                mainDatabaseName.getDatabaseName(), tableNameString);
            Table table = tableService.getTableByName(tableName);
            assertNotNull(table);
            tableListNew.add(table);
        }

        //create branch
        String subCatalogName = subBranchCatalogName;
        CatalogName parentCatalogName = mainCatalogName;

        String version = VersionManagerHelper.getLatestVersionByName(projectId, mainCatalogName.getCatalogName());
        CatalogInput catalogInput = getCatalogInput(subCatalogName, parentCatalogName.getCatalogName(), version);

        Catalog subBranchCatalog = catalogService.createCatalog(projectId, catalogInput);

        Catalog subBranchCatalogRecord = catalogService.getCatalog(
            StoreConvertor.catalogName(projectId, subCatalogName));
        assertEquals(subBranchCatalogRecord.getCatalogName(), subCatalogName);
        assertEquals(subBranchCatalogRecord.getParentName(), parentCatalogName.getCatalogName());
        String subBranchVersion = subBranchCatalogRecord.getParentVersion();
        String parentBranchVersion = version;
        assertEquals(subBranchVersion, parentBranchVersion);

        logger.info("main catalog " + mainCatalog);
        logger.info("main database " + mainDatabase);
        logger.info("sub catalog " + subBranchCatalog);

        for (Table table : tableListNew) {
            logger.info("main table  " + table);
        }

        return tableListNew;
    }


    private List<Table> prepareOneLevelBranchTestWithUndropped(String mainBranchCatalogName,
        String mainBranchDatabaseName,
        String subBranchCatalogName, int mainBranchTableNum) {
        //create main branch
        Catalog mainCatalog = createCatalogPrepareTest(projectId, mainBranchCatalogName);

        //create main branch database
        CatalogName mainCatalogName = StoreConvertor.catalogName(projectId, mainCatalog.getCatalogName());
        Database mainDatabase = createDatabasePrepareTest(mainCatalogName, mainBranchDatabaseName);

        //create table
        DatabaseName mainDatabaseName = StoreConvertor.databaseName(mainCatalogName, mainDatabase.getDatabaseName());
        List<Table> tableList = createTablesPrepareTest(mainDatabaseName, mainBranchTableNum);

        //drop table
        for (Table table : tableList) {
            TableName tableName = StoreConvertor.tableName(projectId, mainCatalogName.getCatalogName(),
                mainDatabaseName.getDatabaseName(), table.getTableName());

            tableService.dropTable(tableName, false, false, false);
        }

        //create new table
        List<Table> tableListNew = new ArrayList<>();
        for (int i = 0; i < mainBranchTableNum; i++) {

            String tableNameString = UUID.randomUUID().toString().toLowerCase();

            TableInput tableInput = getTableDTO(tableNameString, 3);

            tableService.createTable(mainDatabaseName, tableInput);

            TableName tableName = StoreConvertor.tableName(mainDatabaseName.getProjectId(),
                mainDatabaseName.getCatalogName(),
                mainDatabaseName.getDatabaseName(), tableNameString);
            Table table = tableService.getTableByName(tableName);
            assertNotNull(table);
            tableListNew.add(table);
        }

        //undrop table
        for (Table table : tableList) {
            TableName tableName = StoreConvertor.tableName(projectId, mainCatalogName.getCatalogName(),
                mainDatabaseName.getDatabaseName(), table.getTableName());

            logger.info("prepare undrop tableName " + tableName);
            tableService.undropTable(tableName, null, "");
        }

        //create branch
        String subCatalogName = subBranchCatalogName;
        CatalogName parentCatalogName = mainCatalogName;

        String version = VersionManagerHelper.getLatestVersionByName(projectId, parentCatalogName.getCatalogName());

        CatalogInput catalogInput = getCatalogInput(subCatalogName, parentCatalogName.getCatalogName(), version);

        Catalog subBranchCatalog = catalogService.createCatalog(projectId, catalogInput);

        Catalog subBranchCatalogRecord = catalogService.getCatalog(
            StoreConvertor.catalogName(projectId, subCatalogName));
        assertEquals(subBranchCatalogRecord.getCatalogName(), subCatalogName);
        assertEquals(subBranchCatalogRecord.getParentName(), parentCatalogName.getCatalogName());
        String subBranchVersion = subBranchCatalogRecord.getParentVersion();
        String parentBranchVersion = version;
        assertEquals(subBranchVersion, parentBranchVersion);

        logger.info("main catalog " + mainCatalog);
        logger.info("main database " + mainDatabase);
        logger.info("sub catalog " + subBranchCatalog);

        tableListNew.addAll(tableList);
        for (Table table : tableListNew) {
            logger.info("main table  " + table);
        }

        return tableListNew;
    }

    @Test
    public void listTableSubBranchTest() {
        String mainBranchCatalogName = "main_catalog" + UUID.randomUUID();
        String mainBranchDatabaseName = "main_database" + UUID.randomUUID();
        String subBranchCatalogName = "sub_catalog" + UUID.randomUUID();

        int mainBranchTableNum = 2;

        List<Table> mainBranchTableList = prepareOneLevelBranchTest(mainBranchCatalogName, mainBranchDatabaseName,
            subBranchCatalogName, mainBranchTableNum);

        //get catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        DatabaseName mainDatabasePathName = StoreConvertor.databaseName(projectId, mainBranchCatalogName,
            mainBranchDatabaseName);
        Database mainBranchDatabaseRecord = databaseService.getDatabaseByName(mainDatabasePathName);
        assertNotNull(mainBranchDatabaseRecord);

        logger.info(" main catalog " + " name " + mainBranchCatalogName);

        logger.info(" main database " + " name " + mainBranchDatabaseName);

        logger.info(" sub catalog " + " name " + subBranchCatalogName);

        DatabaseName subBranchDatabasePathName = StoreConvertor.databaseName(projectId, subBranchCatalogName,
            mainBranchDatabaseName);

        TraverseCursorResult<List<Table>> subTableList = tableService.listTable(subBranchDatabasePathName, false,
            1000, null, null);

        assertEquals(subTableList.getResult().size(), mainBranchTableList.size());

        for (int i = 0; i < subTableList.getResult().size(); i++) {

            logger.info("sub tableName :" + subTableList.getResult().get(i).getTableName());

            assertEquals(subTableList.getResult().get(i).getDatabaseName(), mainBranchDatabaseRecord.getDatabaseName());
            assertEquals(subTableList.getResult().get(i).getCatalogName(), subBranchCatalogRecord.getCatalogName());

            boolean findTable = false;
            for (int j = 0; j < mainBranchTableList.size(); j++) {
                if (subTableList.getResult().get(i).getTableName().equals(mainBranchTableList.get(j).getTableName())) {
                    findTable = true;
                }
            }

            assertEquals(findTable, true);
        }
    }


    @Test
    public void listTableSubBranchAfterUndropTest() {

        String mainBranchCatalogName = "main_catalog" + UUID.randomUUID().toString().toLowerCase();
        String mainBranchDatabaseName = "main_database" + UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = "sub_catalog" + UUID.randomUUID().toString().toLowerCase();

        int mainBranchTableNum = 2;

        List<Table> mainBranchTableList = prepareOneLevelBranchTestWithUndropped(mainBranchCatalogName,
            mainBranchDatabaseName,
            subBranchCatalogName, mainBranchTableNum);

        //get catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        DatabaseName mainDatabasePathName = StoreConvertor.databaseName(projectId, mainBranchCatalogName,
            mainBranchDatabaseName);
        Database mainBranchDatabaseRecord = databaseService.getDatabaseByName(mainDatabasePathName);
        assertNotNull(mainBranchDatabaseRecord);

        logger.info(" main catalog " + " name " + mainBranchCatalogName);

        logger.info(" main database " + " name " + mainBranchDatabaseName);

        logger.info(" sub catalog " + " name " + subBranchCatalogName);

        DatabaseName subBranchDatabasePathName = StoreConvertor.databaseName(projectId, subBranchCatalogName,
            mainBranchDatabaseName);

        TraverseCursorResult<List<Table>> subTableList = tableService.listTable(subBranchDatabasePathName, false,
            1000, null, null);

        assertEquals(subTableList.getResult().size(), mainBranchTableList.size());

        for (int i = 0; i < subTableList.getResult().size(); i++) {

            logger.info("list sub table :" + subTableList.getResult().get(i));

            assertEquals(subTableList.getResult().get(i).getDatabaseName(), mainBranchDatabaseRecord.getDatabaseName());
            assertEquals(subTableList.getResult().get(i).getCatalogName(), subBranchCatalogRecord.getCatalogName());

            boolean findTable = false;
            for (int j = 0; j < mainBranchTableList.size(); j++) {
                if (subTableList.getResult().get(i).getTableName().equals(mainBranchTableList.get(j).getTableName())) {
                    findTable = true;
                }
            }

            assertEquals(findTable, true);
        }
    }

    @Test
    public void listTableSubBranchAfterMainCreateTest() {
        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String mainBranchDatabaseName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        int mainBranchTableNum = 2;

        List<Table> mainBranchTableList = prepareOneLevelBranchTest(mainBranchCatalogName, mainBranchDatabaseName,
            subBranchCatalogName, mainBranchTableNum);

        DatabaseName subBranchDatabasePathName = StoreConvertor.databaseName(projectId, subBranchCatalogName,
            mainBranchDatabaseName);

        //get catalog, database
        Catalog subBranchCatalogRecord = getCatalogRecord(StoreConvertor.catalogName(projectId, subBranchCatalogName));
        Catalog mainBranchCatalogRecord = getCatalogRecord(
            StoreConvertor.catalogName(projectId, mainBranchCatalogName));
        DatabaseName mainDatabasePathName = StoreConvertor.databaseName(projectId, mainBranchCatalogName,
            mainBranchDatabaseName);
        Database mainBranchDatabaseRecord = getDatabaseRecord(mainDatabasePathName);

        //after creating a branch, create tables on the main-branch.
        int mainBranchTableNumNew = 2;
        List<Table> mainBranchCreateTableListNew = createTablesPrepareTest(
            mainDatabasePathName, mainBranchTableNumNew);

        //list sub-branch table
        TraverseCursorResult<List<Table>> subTableList = tableService.listTable(subBranchDatabasePathName, false,
            1000, null, null);

        assertEquals(subTableList.getResult().size(), mainBranchTableList.size());
        checkTableInfo(subTableList.getResult(), subBranchCatalogRecord.getCatalogName(),
            mainBranchDatabaseRecord.getDatabaseName());

        //list main-branch table
        TraverseCursorResult<List<Table>> mainBranchListTableNew = tableService.listTable(mainDatabasePathName, false,
            1000, null, null);

        assertEquals(mainBranchListTableNew.getResult().size(), mainBranchTableNumNew + mainBranchTableNum);

        mainBranchCreateTableListNew.addAll(mainBranchTableList);
        checkTableInfo(mainBranchCreateTableListNew, mainBranchCatalogRecord.getCatalogName(),
            mainBranchDatabaseRecord.getDatabaseName());
    }

    @Test
    public void dropTableOnSubBranchTest() {

        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String mainBranchDatabaseName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        int mainBranchTableNum = 2;

        //create branch catalog, database, tables
        List<Table> mainBranchTableList = prepareOneLevelBranchTest(mainBranchCatalogName, mainBranchDatabaseName,
            subBranchCatalogName, mainBranchTableNum);

        //get catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        DatabaseName mainDatabasePathName = StoreConvertor.databaseName(projectId, mainBranchCatalogName,
            mainBranchDatabaseName);
        Database mainBranchDatabaseRecord = databaseService.getDatabaseByName(mainDatabasePathName);
        assertNotNull(mainBranchDatabaseRecord);

        String dropTableName = mainBranchTableList.get(0).getTableName();

        TableName subBranchTablePathName = StoreConvertor.tableName(projectId, subBranchCatalogRecord.getCatalogName(),
            mainBranchDatabaseRecord.getDatabaseName(), dropTableName);

        //get table from sub-branch
        Table subBranchTable = tableService.getTableByName(subBranchTablePathName);
        assertNotNull(subBranchTable);
        assertEquals(subBranchTable.getTableName(), mainBranchTableList.get(0).getTableName());
        assertEquals(subBranchTable.getDatabaseName(), mainBranchDatabaseRecord.getDatabaseName());
        assertEquals(subBranchTable.getCatalogName(), subBranchCatalogRecord.getCatalogName());

        //drop sub-branch table
        tableService.dropTable(subBranchTablePathName, false, false, false);

        //get table from sub-branch
        TableName tableNameCheck = subBranchTablePathName;
        CatalogServerException exception = assertThrows(CatalogServerException.class,
            () -> tableService.getTableByName(tableNameCheck));
        assertEquals(exception.getMessage(),
            String.format("Table [%s] not found", tableNameCheck.getTableName()));

        //get table from parent
        TableName mainBranchTablePathName = StoreConvertor.tableName(projectId,
            mainBranchCatalogRecord.getCatalogName(),
            mainBranchDatabaseRecord.getDatabaseName(), dropTableName);
        Table mainBranchTable = tableService.getTableByName(mainBranchTablePathName);
        assertNotNull(mainBranchTable);
        assertEquals(mainBranchTable.getCatalogName(), mainBranchCatalogRecord.getCatalogName());

    }

    @Test
    public void dropTableMainBranchTest() {

        String mainBranchCatalogName = "maincatalog" + UUID.randomUUID().toString().toLowerCase();
        String mainBranchDatabaseName = "maindatabase" + UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = "subcatalog" + UUID.randomUUID().toString().toLowerCase();
        int mainBranchTableNum = 2;

        //create branch catalog, database, tables
        List<Table> mainBranchTableList = prepareOneLevelBranchTest(mainBranchCatalogName, mainBranchDatabaseName,
            subBranchCatalogName, mainBranchTableNum);

        //get catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        DatabaseName mainDatabasePathName = StoreConvertor.databaseName(projectId, mainBranchCatalogName,
            mainBranchDatabaseName);
        Database mainBranchDatabaseRecord = databaseService.getDatabaseByName(mainDatabasePathName);
        assertNotNull(mainBranchDatabaseRecord);

        String dropTableName = mainBranchTableList.get(0).getTableName();

        //get table from main-branch
        TableName mainBranchTablePathName = StoreConvertor.tableName(projectId,
            mainBranchCatalogRecord.getCatalogName(),
            mainBranchDatabaseRecord.getDatabaseName(), dropTableName);

        Table mainBranchTable = tableService.getTableByName(mainBranchTablePathName);
        assertNotNull(mainBranchTable);
        assertEquals(mainBranchTable.getTableName(), mainBranchTableList.get(0).getTableName());
        assertEquals(mainBranchTable.getDatabaseName(), mainBranchDatabaseRecord.getDatabaseName());
        assertEquals(mainBranchTable.getCatalogName(), mainBranchCatalogRecord.getCatalogName());

        //get table from sub-branch
        TableName subBranchTablePathName = StoreConvertor.tableName(projectId, subBranchCatalogRecord.getCatalogName(),
            mainBranchDatabaseRecord.getDatabaseName(), dropTableName);
        Table subBranchTable = tableService.getTableByName(subBranchTablePathName);
        assertNotNull(subBranchTable);
        assertEquals(subBranchTable.getCatalogName(), subBranchCatalogRecord.getCatalogName());
        assertEquals(mainBranchTable.getTableName(), mainBranchTableList.get(0).getTableName());
        assertEquals(mainBranchTable.getDatabaseName(), mainBranchDatabaseRecord.getDatabaseName());

        //drop main-branch table

        logger.info("dropped table " + mainBranchTablePathName);
        logger.info("dropped table " + " catalog " + " name " + mainBranchTable.getCatalogName());
        logger.info("dropped table " + " database " + " name " + mainBranchTable.getDatabaseName());
        logger.info("dropped table " + " table " + " name " + mainBranchTable.getTableName());
        tableService.dropTable(mainBranchTablePathName, false, false, false);

        //get table from main-branch
        TableName tableNameCheck = mainBranchTablePathName;
        CatalogServerException exception = assertThrows(CatalogServerException.class,
            () -> tableService.getTableByName(tableNameCheck));
        assertEquals(exception.getMessage(),
            String.format("Table [%s] not found", tableNameCheck.getTableName()));

        //get table from sub-branch
        subBranchTable = tableService.getTableByName(subBranchTablePathName);
        assertNotNull(subBranchTable);
    }

    @Test
    public void getTableByNameSubBranchTest() {
        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String mainBranchDatabaseName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        int mainBranchTableNum = 2;

        //create branch catalog, database, tables
        List<Table> mainBranchTableList = prepareOneLevelBranchTest(mainBranchCatalogName, mainBranchDatabaseName,
            subBranchCatalogName, mainBranchTableNum);

        //get catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        DatabaseName mainDatabasePathName = StoreConvertor.databaseName(projectId, mainBranchCatalogName,
            mainBranchDatabaseName);
        Database mainBranchDatabaseRecord = databaseService.getDatabaseByName(mainDatabasePathName);
        assertNotNull(mainBranchDatabaseRecord);

        //get table from sub-branch
        TableName subBranchTablePathName = StoreConvertor.tableName(projectId, subBranchCatalogRecord.getCatalogName(),
            mainBranchDatabaseRecord.getDatabaseName(), mainBranchTableList.get(0).getTableName());

        Table subBranchNewTable = tableService.getTableByName(subBranchTablePathName);
        assertNotNull(subBranchNewTable);
        assertEquals(subBranchNewTable.getCatalogName(), subBranchCatalogRecord.getCatalogName());

        //get table from main-branch
        TableName mainBranchTablePathName = StoreConvertor.tableName(projectId,
            mainBranchCatalogRecord.getCatalogName(),
            mainBranchDatabaseRecord.getDatabaseName(), mainBranchTableList.get(0).getTableName());

        Table mainBranchTable = tableService.getTableByName(mainBranchTablePathName);
        assertNotNull(mainBranchTable);
        assertEquals(mainBranchTable.getCatalogName(), mainBranchCatalogRecord.getCatalogName());
    }

    @Test
    public void alterTableSubBranchTest() {
        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String mainBranchDatabaseName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        int mainBranchTableNum = 2;

        //create branch catalog, database, tables
        List<Table> mainBranchTableList = prepareOneLevelBranchTest(mainBranchCatalogName, mainBranchDatabaseName,
            subBranchCatalogName, mainBranchTableNum);

        //get catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        DatabaseName mainDatabasePathName = StoreConvertor.databaseName(projectId, mainBranchCatalogName,
            mainBranchDatabaseName);
        Database mainBranchDatabaseRecord = databaseService.getDatabaseByName(mainDatabasePathName);
        assertNotNull(mainBranchDatabaseRecord);

        //alter table name at sub-branch
        String tableNameNewString = UUID.randomUUID().toString().toLowerCase();

        TableInput tableInput = getTableDTO(tableNameNewString, 3);

        TableName subBranchTablePathName = StoreConvertor.tableName(projectId, subBranchCatalogRecord.getCatalogName(),
            mainBranchDatabaseRecord.getDatabaseName(), mainBranchTableList.get(0).getTableName());

        tableService.alterTable(subBranchTablePathName, tableInput, null);

        TableName tableNameCheck = subBranchTablePathName;
        CatalogServerException exception = assertThrows(CatalogServerException.class,
            () -> tableService.getTableByName(tableNameCheck));
        assertEquals(exception.getMessage(),
            String.format("Table [%s] not found", tableNameCheck.getTableName()));

        //get table from sub-branch with new name
        subBranchTablePathName = StoreConvertor.tableName(projectId, subBranchCatalogRecord.getCatalogName(),
            mainBranchDatabaseRecord.getDatabaseName(), tableNameNewString);
        Table subBranchNewTable = tableService.getTableByName(subBranchTablePathName);
        assertNotNull(subBranchNewTable);
        assertEquals(subBranchNewTable.getCatalogName(), subBranchCatalogRecord.getCatalogName());
        assertEquals(subBranchNewTable.getDatabaseName(), subBranchTablePathName.getDatabaseName());
        assertEquals(subBranchNewTable.getTableName(), subBranchTablePathName.getTableName());
        assertEquals(subBranchNewTable.getTableName(), tableNameNewString);

        //get table from main-branch
        TableName mainBranchTablePathName = StoreConvertor.tableName(projectId,
            mainBranchCatalogRecord.getCatalogName(),
            mainBranchDatabaseRecord.getDatabaseName(), mainBranchTableList.get(0).getTableName());

        Table mainBranchTable = tableService.getTableByName(mainBranchTablePathName);
        assertNotNull(mainBranchTable);
        assertEquals(mainBranchTable.getCatalogName(), mainBranchTableList.get(0).getCatalogName());
        assertEquals(mainBranchTable.getDatabaseName(), mainBranchTableList.get(0).getDatabaseName());
        assertEquals(mainBranchTable.getTableName(), mainBranchTableList.get(0).getTableName());
    }

    @Test
    public void alterTableParentBranchTest() {
        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String mainBranchDatabaseName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        int mainBranchTableNum = 2;

        //create branch catalog, database, tables
        List<Table> mainBranchTableList = prepareOneLevelBranchTest(mainBranchCatalogName, mainBranchDatabaseName,
            subBranchCatalogName, mainBranchTableNum);

        //get catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalog = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalog);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalog = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalog);

        DatabaseName mainDatabasePathName = StoreConvertor.databaseName(projectId, mainBranchCatalogName,
            mainBranchDatabaseName);
        Database mainBranchDatabase = databaseService.getDatabaseByName(mainDatabasePathName);
        assertNotNull(mainBranchDatabase);

        //alter table name at main-branch
        String tableNameNewString = UUID.randomUUID().toString().toLowerCase();

        //get table from sub-branch
        TableName subBranchTableName = StoreConvertor.tableName(projectId, subBranchCatalog.getCatalogName(),
            mainBranchDatabase.getDatabaseName(), mainBranchTableList.get(0).getTableName());

        TableInput tableInput = getTableDTO(tableNameNewString, 3);
        TableName mainBranchTableName = StoreConvertor.tableName(projectId, mainBranchCatalog.getCatalogName(),
            mainBranchDatabase.getDatabaseName(), mainBranchTableList.get(0).getTableName());

        logger.info("alter table " + mainBranchTableName + " new name " + tableInput.getTableName());
        tableService.alterTable(mainBranchTableName, tableInput, null);

        //get table from sub-branch
        logger.info("get tableName " + subBranchTableName);
        Table subBranchNewTable = tableService.getTableByName(subBranchTableName);
        assertNotNull(subBranchNewTable);
        assertEquals(subBranchNewTable.getCatalogName(), subBranchCatalog.getCatalogName());
        assertEquals(subBranchNewTable.getDatabaseName(), mainBranchTableList.get(0).getDatabaseName());
        assertEquals(subBranchNewTable.getTableName(), mainBranchTableList.get(0).getTableName());

        //get table from main-branch with old name
        TableName tableNameCheck = mainBranchTableName;
        CatalogServerException exception = assertThrows(CatalogServerException.class,
            () -> tableService.getTableByName(tableNameCheck));
        assertEquals(exception.getMessage(),
            String.format("Table [%s] not found", tableNameCheck.getTableName()));

        TableName mainBranchTablePathNameNew = StoreConvertor.tableName(projectId,
            mainBranchCatalog.getCatalogName(),
            mainBranchDatabase.getDatabaseName(), tableNameNewString);

        Table mainBranchTable = tableService.getTableByName(mainBranchTablePathNameNew);
        assertNotNull(mainBranchTable);
        assertEquals(mainBranchTable.getCatalogName(), mainBranchTableList.get(0).getCatalogName(),
            "catalog Name not same");
        assertEquals(mainBranchTable.getDatabaseName(), mainBranchTableList.get(0).getDatabaseName(),
            "database Name not same");
        assertEquals(mainBranchTable.getTableName(), tableNameNewString, "table Name not same");
    }


    @Test
    public void createTableSubBranchTest() {

        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String mainBranchDatabaseName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        int mainBranchTableNum = 2;

        //create branch catalog, database, tables
        List<Table> mainBranchTableList = prepareOneLevelBranchTest(mainBranchCatalogName, mainBranchDatabaseName,
            subBranchCatalogName, mainBranchTableNum);

        //get catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        DatabaseName mainDatabasePathName = StoreConvertor.databaseName(projectId, mainBranchCatalogName,
            mainBranchDatabaseName);
        Database mainBranchDatabaseRecord = databaseService.getDatabaseByName(mainDatabasePathName);
        assertNotNull(mainBranchDatabaseRecord);

        //create table at sub-branch
        DatabaseName subBranchDatabaseName = StoreConvertor.databaseName(subCatalogPathName, mainBranchDatabaseName);
        List<Table> subBranchTableListNew = createTablesPrepareTest(subBranchDatabaseName, 2);
    }

    @Test
    public void createTableOnSubBranchExistInMainTest() {
        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String mainBranchDatabaseName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        int mainBranchTableNum = 2;

        //create branch catalog, database, tables
        List<Table> mainBranchTableList = prepareOneLevelBranchTest(mainBranchCatalogName, mainBranchDatabaseName,
            subBranchCatalogName, mainBranchTableNum);

        //get catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        DatabaseName mainDatabasePathName = StoreConvertor.databaseName(projectId, mainBranchCatalogName,
            mainBranchDatabaseName);
        Database mainBranchDatabaseRecord = databaseService.getDatabaseByName(mainDatabasePathName);
        assertNotNull(mainBranchDatabaseRecord);

        //create table at sub-branch
        DatabaseName subBranchDatabaseName = StoreConvertor.databaseName(subCatalogPathName, mainBranchDatabaseName);

        //create table exist in main
        TableInput tableInput = getTableDTO(mainBranchTableList.get(0).getTableName(), 3);

        logger.info("create table " + subBranchDatabaseName + " table name " + tableInput.getTableName());

        assertThrows(CatalogServerException.class, () -> tableService.createTable(subBranchDatabaseName, tableInput));
    }

    @Test
    public void createTableMainBranchExistInSubTest() {
        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String mainBranchDatabaseName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        int mainBranchTableNum = 2;

        //create branch catalog, database, tables
        List<Table> mainBranchTableList = prepareOneLevelBranchTest(mainBranchCatalogName, mainBranchDatabaseName,
            subBranchCatalogName, mainBranchTableNum);

        assertTrue(!mainBranchTableList.isEmpty());

        //get catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        DatabaseName mainDatabasePathName = StoreConvertor.databaseName(projectId, mainBranchCatalogName,
            mainBranchDatabaseName);
        Database mainBranchDatabaseRecord = databaseService.getDatabaseByName(mainDatabasePathName);
        assertNotNull(mainBranchDatabaseRecord);

        //create table at sub-branch
        DatabaseName subBranchDatabaseName = StoreConvertor.databaseName(subCatalogPathName, mainBranchDatabaseName);
        List<Table> subBranchTableListNew = createTablesPrepareTest(subBranchDatabaseName, 2);

        for (Table table : subBranchTableListNew) {
            logger.info("sub tableName :" + table.getTableName());
            TableName tableName = StoreConvertor.tableName(projectId, table.getCatalogName(),
                table.getDatabaseName(), table.getTableName());

            Table subTable = tableService.getTableByName(tableName);
            assertNotNull(subTable);
        }

        //create main table exist in sub-branch
        TableInput tableInput = getTableDTO(subBranchTableListNew.get(0).getTableName(), 3);

        tableService.createTable(mainDatabasePathName, tableInput);

        TableName tableName = StoreConvertor.tableName(mainDatabasePathName.getProjectId(),
            mainDatabasePathName.getCatalogName(),
            mainDatabasePathName.getDatabaseName(), subBranchTableListNew.get(0).getTableName());
        Table mainTable = tableService.getTableByName(tableName);
        assertNotNull(mainTable);
        assertEquals(mainTable.getCatalogName(), mainBranchDatabaseRecord.getCatalogName());
        assertEquals(mainTable.getDatabaseName(), mainBranchDatabaseRecord.getDatabaseName());
        assertEquals(mainTable.getTableName(), tableInput.getTableName());
    }

    @Test
    public void createTableSubBranchExistInMain2Test() {
        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String mainBranchDatabaseName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        int mainBranchTableNum = 2;

        //create branch catalog, database, tables
        List<Table> mainBranchTableList = prepareOneLevelBranchTest(mainBranchCatalogName, mainBranchDatabaseName,
            subBranchCatalogName, mainBranchTableNum);

        //get catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        DatabaseName mainDatabasePathName = StoreConvertor.databaseName(projectId, mainBranchCatalogName,
            mainBranchDatabaseName);
        Database mainBranchDatabaseRecord = databaseService.getDatabaseByName(mainDatabasePathName);
        assertNotNull(mainBranchDatabaseRecord);

        //create table at main-branch after create branch

        List<Table> mainBranchTableListNew = createTablesPrepareTest(mainDatabasePathName, 2);
        assertTrue(!mainBranchTableListNew.isEmpty());

        //create table at sub-branch
        DatabaseName subBranchDatabaseName = StoreConvertor.databaseName(subCatalogPathName, mainBranchDatabaseName);

        for (Table table : mainBranchTableListNew) {
            //create table exist in main
            TableInput tableInput = getTableDTO(table.getTableName(), 3);

            tableService.createTable(subBranchDatabaseName, tableInput);

            TableName tableName = StoreConvertor.tableName(subBranchDatabaseName, table.getTableName());
            Table subTable = tableService.getTableByName(tableName);
            assertNotNull(subTable);
            assertEquals(subTable.getCatalogName(), subBranchDatabaseName.getCatalogName());
            assertEquals(subTable.getDatabaseName(), subBranchDatabaseName.getDatabaseName());
            assertEquals(subTable.getTableName(), table.getTableName());
        }
    }

    @Test
    public void restoreTableSubBranch1Test() {
        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String mainBranchDatabaseName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        int mainBranchTableNum = 2;

        //create branch catalog, database, tables
        List<Table> mainBranchTableList = prepareOneLevelBranchTest(mainBranchCatalogName, mainBranchDatabaseName,
            subBranchCatalogName, mainBranchTableNum);

        //get catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        DatabaseName mainDatabasePathName = StoreConvertor.databaseName(projectId, mainBranchCatalogName,
            mainBranchDatabaseName);
        Database mainBranchDatabaseRecord = databaseService.getDatabaseByName(mainDatabasePathName);
        assertNotNull(mainBranchDatabaseRecord);

        //get sub branch table restore version
        DatabaseName subBranchDatabaseName = StoreConvertor.databaseName(subCatalogPathName, mainBranchDatabaseName);
        String subBranchTableName = mainBranchTableList.get(0).getTableName();
        TableName subBranchTablePathName = StoreConvertor.tableName(subBranchDatabaseName.getProjectId(),
            subBranchDatabaseName.getCatalogName(),
            subBranchDatabaseName.getDatabaseName(), subBranchTableName);
        TableCommit subBranchTableCommit1 = tableService.getLatestTableCommit(subBranchTablePathName);
        assertNotNull(subBranchTableCommit1);
        assertEquals(subBranchTablePathName.getCatalogName(), subBranchCatalogName);
        String version1 = subBranchTableCommit1.getCommitVersion();

        logger.info("get table history1 " + subBranchTablePathName);
        logger.info("sub table history1 " + subBranchTableCommit1);
        logger.info("sub table history version1 " + version1);

        //alter table name at sub-branch
        String tableNameNewString = UUID.randomUUID().toString().toLowerCase();

        TableInput tableInput = getTableDTO(tableNameNewString, 3);

        logger.info("alter table " + subBranchTablePathName + " new table name " + tableInput.getTableName());
        tableService.alterTable(subBranchTablePathName, tableInput, null);

        TableName subBranchTablePathNameNew = new TableName(subBranchTablePathName);
        subBranchTablePathNameNew.setTableName(tableNameNewString);

        Table subTableNew = tableService.getTableByName(subBranchTablePathNameNew);
        assertEquals(subTableNew.getTableName(), tableNameNewString);
        assertEquals(subTableNew.getCatalogName(), subBranchTablePathNameNew.getCatalogName());
        assertEquals(subTableNew.getDatabaseName(), subBranchTablePathNameNew.getDatabaseName());

        //restore at history version
        String encodeText = subBranchTableCommit1.getCommitVersion();
        logger.info("encodeText:" + encodeText);
        logger.info("restoreTable " + subBranchTablePathNameNew);
        tableService.restoreTable(subBranchTablePathNameNew, encodeText);

        //check result
        Table subTable = tableService.getTableByName(subBranchTablePathName);
        assertEquals(subTable.getTableName(), subBranchTableName);
        assertEquals(subTable.getTableName(), subBranchTableName);

        TableCommit subBranchTableHistory2 = tableService.getLatestTableCommit(subBranchTablePathName);
        assertNotNull(subBranchTableHistory2);
        String version2 = subBranchTableHistory2.getCommitVersion();

        logger.info("get table history2 " + subBranchTablePathName);
        logger.info("get table history2 " + subBranchTableHistory2);

        assertEquals(subBranchTableCommit1.getCatalogId(), subBranchTableHistory2.getCatalogId());
        assertEquals(subBranchTableCommit1.getDatabaseId(), subBranchTableHistory2.getDatabaseId());
        assertEquals(subBranchTableCommit1.getTableId(), subBranchTableHistory2.getTableId());

        assertTrue(version1.compareTo(version2) < 0);

    }

    @Test
    public void restoreTableSubBranch2Test() {
        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String mainBranchDatabaseName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        int mainBranchTableNum = 2;

        //create branch catalog, database, tables
        List<Table> mainBranchTableList = prepareOneLevelBranchTest(mainBranchCatalogName, mainBranchDatabaseName,
            subBranchCatalogName, mainBranchTableNum);

        //get catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        DatabaseName mainDatabasePathName = StoreConvertor.databaseName(projectId, mainBranchCatalogName,
            mainBranchDatabaseName);
        Database mainBranchDatabaseRecord = databaseService.getDatabaseByName(mainDatabasePathName);
        assertNotNull(mainBranchDatabaseRecord);

        //alter table name1 at sub-branch
        DatabaseName subBranchDatabaseName = StoreConvertor.databaseName(subCatalogPathName, mainBranchDatabaseName);
        String subBranchTableName = mainBranchTableList.get(0).getTableName();
        TableName subBranchTablePathName = StoreConvertor.tableName(subBranchDatabaseName.getProjectId(),
            subBranchDatabaseName.getCatalogName(),
            subBranchDatabaseName.getDatabaseName(), subBranchTableName);

        String tableNameNewString1 = UUID.randomUUID().toString().toLowerCase();

        TableInput tableInput = getTableDTO(
            tableNameNewString1, 3);

        logger.info("alter table " + subBranchTablePathName + " new table name " + tableInput.getTableName());
        tableService.alterTable(subBranchTablePathName, tableInput, null);

        TableName subBranchTablePathNameNew1 = new TableName(subBranchTablePathName);
        subBranchTablePathNameNew1.setTableName(tableNameNewString1);

        Table subTableNew = tableService.getTableByName(subBranchTablePathNameNew1);
        assertEquals(subTableNew.getTableName(), tableNameNewString1);
        assertEquals(subTableNew.getCatalogName(), subBranchTablePathName.getCatalogName());

        //get sub branch table restore version
        TableCommit subBranchTableHistory1 = tableService.getLatestTableCommit(subBranchTablePathNameNew1);
        assertNotNull(subBranchTableHistory1);
        String version1 = subBranchTableHistory1.getCommitVersion();

        logger.info("get table history1 " + subBranchTablePathNameNew1);
        logger.info("sub table history1 " + subBranchTableHistory1);
        logger.info("sub table history version1 " + version1);

        //alter table name2 at sub-branch
        String tableNameNewString2 = UUID.randomUUID().toString().toLowerCase();
        tableInput = getTableDTO(
            tableNameNewString2, 3);
        logger.info("alter table " + subBranchTablePathNameNew1 + " new table name " + tableInput.getTableName());
        tableService.alterTable(subBranchTablePathNameNew1, tableInput, null);

        //restore at history version
        TableName subBranchTablePathNameNew2 = new TableName(subBranchTablePathName);
        subBranchTablePathNameNew2.setTableName(tableNameNewString2);
        String encodeText = subBranchTableHistory1.getCommitVersion();
        tableService.restoreTable(subBranchTablePathNameNew2, encodeText);

        //check result
        logger.info("check restore table " + subBranchTableName);
        Table subTable = tableService.getTableByName(subBranchTablePathNameNew1);
        assertNotNull(subTable);
        assertEquals(subTable.getCatalogName(), subBranchTablePathNameNew1.getCatalogName());
        assertEquals(subTable.getDatabaseName(), subBranchTablePathNameNew1.getDatabaseName());
        assertEquals(subTable.getTableName(), tableNameNewString1);

        TableCommit subBranchTableHistory2 = tableService.getLatestTableCommit(subBranchTablePathNameNew1);
        assertNotNull(subBranchTableHistory2);
        String version2 = subBranchTableHistory2.getCommitVersion();

        logger.info("get table history2 " + subBranchTablePathName);
        logger.info("get table history2 " + subBranchTableHistory2);

        assertEquals(subBranchTableHistory1.getCatalogId(), subBranchTableHistory2.getCatalogId());
        assertEquals(subBranchTableHistory1.getDatabaseId(), subBranchTableHistory2.getDatabaseId());
        assertEquals(subBranchTableHistory1.getTableId(), subBranchTableHistory2.getTableId());

        assertTrue(version1.compareTo(version2) < 0);
    }


    @Test
    public void restoreTableParentBranch1Test() {
        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String mainBranchDatabaseName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        int mainBranchTableNum = 2;

        //create branch catalog, database, tables
        List<Table> mainBranchTableList = prepareOneLevelBranchTest(mainBranchCatalogName, mainBranchDatabaseName,
            subBranchCatalogName, mainBranchTableNum);

        //get catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        DatabaseName mainDatabasePathName = StoreConvertor.databaseName(projectId, mainBranchCatalogName,
            mainBranchDatabaseName);
        Database mainBranchDatabaseRecord = databaseService.getDatabaseByName(mainDatabasePathName);
        assertNotNull(mainBranchDatabaseRecord);

        //get main branch table restore version
        String mainBranchTableName = mainBranchTableList.get(0).getTableName();
        TableName mainBranchTablePathName = StoreConvertor.tableName(mainDatabasePathName.getProjectId(),
            mainDatabasePathName.getCatalogName(),
            mainDatabasePathName.getDatabaseName(), mainBranchTableName);
        TableCommit mainBranchTableHistory1 = tableService.getLatestTableCommit(mainBranchTablePathName);
        assertNotNull(mainBranchTableHistory1);
        String version1 = mainBranchTableHistory1.getCommitVersion();

        logger.info("get table history1 " + mainBranchTablePathName);
        logger.info("sub table history1 " + mainBranchTableHistory1);
        logger.info("sub table history version1 " + version1);

        //alter table name at main-branch
        String tableNameNewString = UUID.randomUUID().toString().toLowerCase();

        TableInput tableInput = getTableDTO(
            tableNameNewString, 3);

        logger.info("alter table " + mainBranchTablePathName + " new table name " + tableInput.getTableName());
        tableService.alterTable(mainBranchTablePathName, tableInput, null);

        TableName mainBranchTablePathNameNew = new TableName(mainBranchTablePathName);
        mainBranchTablePathNameNew.setTableName(tableNameNewString);

        Table subTableNew = tableService.getTableByName(mainBranchTablePathNameNew);
        assertEquals(subTableNew.getTableName(), tableNameNewString);
        assertEquals(subTableNew.getCatalogName(), mainBranchDatabaseRecord.getCatalogName());
        assertEquals(subTableNew.getDatabaseName(), mainBranchDatabaseRecord.getDatabaseName());

        //restore at history version
        String encodeText = mainBranchTableHistory1.getCommitVersion();
        tableService.restoreTable(mainBranchTablePathNameNew, encodeText);

        //check result
        Table subTable = tableService.getTableByName(mainBranchTablePathName);
        assertNotNull(subTable);
        assertEquals(subTable.getTableName(), mainBranchTableName);
        assertEquals(subTable.getCatalogName(), mainBranchDatabaseRecord.getCatalogName());
        assertEquals(subTable.getDatabaseName(), mainBranchDatabaseRecord.getDatabaseName());

        TableCommit mainBranchTableHistory2 = tableService.getLatestTableCommit(mainBranchTablePathName);
        assertNotNull(mainBranchTableHistory2);
        String version2 = mainBranchTableHistory2.getCommitVersion();

        logger.info("get table history2 " + mainBranchTablePathName);
        logger.info("get table history2 " + mainBranchTableHistory2);

        assertEquals(mainBranchTableHistory1.getCatalogId(), mainBranchTableHistory2.getCatalogId());
        assertEquals(mainBranchTableHistory1.getDatabaseId(), mainBranchTableHistory2.getDatabaseId());
        assertEquals(mainBranchTableHistory1.getTableId(), mainBranchTableHistory2.getTableId());
        assertTrue(version1.compareTo(version2) < 0);
    }


    @Test
    public void undropTableSubBranch1Test() {
        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String mainBranchDatabaseName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        int mainBranchTableNum = 2;

        //create branch catalog, database, tables
        List<Table> mainBranchTableList = prepareOneLevelBranchTest(mainBranchCatalogName, mainBranchDatabaseName,
            subBranchCatalogName, mainBranchTableNum);

        //get catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        DatabaseName mainDatabasePathName = StoreConvertor.databaseName(projectId, mainBranchCatalogName,
            mainBranchDatabaseName);
        Database mainBranchDatabaseRecord = databaseService.getDatabaseByName(mainDatabasePathName);
        assertNotNull(mainBranchDatabaseRecord);

        //drop table
        String dropTableName = mainBranchTableList.get(0).getTableName();

        TableName subBranchTablePathName = StoreConvertor.tableName(projectId, subBranchCatalogRecord.getCatalogName(),
            mainBranchDatabaseRecord.getDatabaseName(), dropTableName);

        //get table from sub-branch
        Table subBranchTable = tableService.getTableByName(subBranchTablePathName);
        assertNotNull(subBranchTable);
        assertEquals(subBranchTable.getTableName(), mainBranchTableList.get(0).getTableName());
        assertEquals(subBranchTable.getDatabaseName(), mainBranchDatabaseRecord.getDatabaseName());
        assertEquals(subBranchTable.getCatalogName(), subBranchCatalogRecord.getCatalogName());

        //drop sub-branch table
        tableService.dropTable(subBranchTablePathName, false, false, false);

        //get table from sub-branch
        TableName tableNameCheck = subBranchTablePathName;
        CatalogServerException exception = assertThrows(CatalogServerException.class,
            () -> tableService.getTableByName(tableNameCheck));
        assertEquals(exception.getMessage(),
            String.format("Table [%s] not found", tableNameCheck.getTableName()));

        //get table from parent
        TableName mainBranchTablePathName = StoreConvertor.tableName(projectId,
            mainBranchCatalogRecord.getCatalogName(),
            mainBranchDatabaseRecord.getDatabaseName(), dropTableName);
        Table mainBranchTable = tableService.getTableByName(mainBranchTablePathName);
        assertNotNull(mainBranchTable);

        //undrop table
        tableService.undropTable(subBranchTablePathName, subBranchTable.getTableId(), "");

        TableName subBranchTableName = StoreConvertor.tableName(projectId,
            subBranchTable.getCatalogName(), subBranchTable.getDatabaseName(), subBranchTable.getTableName());
        Table subBranchTable1 = tableService.getTableByName(subBranchTableName);
        assertNotNull(subBranchTable1);
        assertEquals(subBranchTable.getCatalogName(), subBranchTable1.getCatalogName());
        assertEquals(subBranchTable.getDatabaseName(), subBranchTable1.getDatabaseName());
        assertEquals(subBranchTable.getTableName(), subBranchTable1.getTableName());
    }

    private Catalog getCatalogRecord(CatalogName catalogPathName) {
        Catalog catalogRecord = catalogService.getCatalog(catalogPathName);
        assertNotNull(catalogRecord);
        return catalogRecord;
    }

    private Database getDatabaseRecord(DatabaseName databasePathName) {
        Database databaseRecord = databaseService.getDatabaseByName(databasePathName);
        assertNotNull(databaseRecord);
        return databaseRecord;
    }

    @Test
    public void undropTableSubBranchWithNewNameTest() {
        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String mainBranchDatabaseName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        int mainBranchTableNum = 2;

        //create branch catalog, database, tables
        List<Table> mainBranchTableList = prepareOneLevelBranchTest(mainBranchCatalogName, mainBranchDatabaseName,
            subBranchCatalogName, mainBranchTableNum);

        //get catalog, database
        Catalog subBranchCatalogRecord = getCatalogRecord(StoreConvertor.catalogName(projectId,
            subBranchCatalogName));

        Catalog mainBranchCatalogRecord = getCatalogRecord(StoreConvertor.catalogName(projectId,
            mainBranchCatalogName));

        Database mainBranchDatabaseRecord = getDatabaseRecord(StoreConvertor.databaseName(projectId,
            mainBranchCatalogName, mainBranchDatabaseName));

        //drop table
        String dropTableName = mainBranchTableList.get(0).getTableName();
        TableName subBranchTablePathName = StoreConvertor.tableName(projectId, subBranchCatalogRecord.getCatalogName(),
            mainBranchDatabaseRecord.getDatabaseName(), dropTableName);

        //get table from sub-branch
        Table subBranchTable = tableService.getTableByName(subBranchTablePathName);
        assertNotNull(subBranchTable);
        assertEquals(subBranchTable.getTableName(), mainBranchTableList.get(0).getTableName());
        assertEquals(subBranchTable.getDatabaseName(), mainBranchDatabaseRecord.getDatabaseName());
        assertEquals(subBranchTable.getCatalogName(), subBranchCatalogRecord.getCatalogName());

        //drop sub-branch table
        tableService.dropTable(subBranchTablePathName, false, false, false);

        //get table from sub-branch
        TableName tableNameCheck = subBranchTablePathName;
        CatalogServerException exception = assertThrows(CatalogServerException.class,
            () -> tableService.getTableByName(tableNameCheck));
        assertEquals(exception.getMessage(),
            String.format("Table [%s] not found", tableNameCheck.getTableName()));

        //get table from parent
        TableName mainBranchTablePathName = StoreConvertor.tableName(projectId,
            mainBranchCatalogRecord.getCatalogName(),
            mainBranchDatabaseRecord.getDatabaseName(), dropTableName);
        Table mainBranchTable = tableService.getTableByName(mainBranchTablePathName);
        assertNotNull(mainBranchTable);
        assertEquals(mainBranchTable.getCatalogName(), mainBranchCatalogRecord.getCatalogName());

        //undropp table with new name
        TableName subBranchTableName = StoreConvertor.tableName(projectId,
            subBranchTable.getCatalogName(), subBranchTable.getDatabaseName(), subBranchTable.getTableName());
        String tableNameNewString = UUID.randomUUID().toString().toLowerCase();
        tableService.undropTable(subBranchTablePathName, subBranchTable.getTableId(), tableNameNewString);
        TableName undropTableName = new TableName(subBranchTableName);
        undropTableName.setTableName(tableNameNewString);
        Table subBranchTable1 = tableService.getTableByName(undropTableName);
        assertNotNull(subBranchTable1);
        assertEquals(subBranchTable.getCatalogName(), subBranchTable1.getCatalogName());
        assertEquals(subBranchTable.getDatabaseName(), subBranchTable1.getDatabaseName());
        assertEquals(subBranchTable1.getTableName(), tableNameNewString);
    }

    @Test
    public void undropTableSubBranchWithTableNameTest() {
        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String mainBranchDatabaseName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        int mainBranchTableNum = 2;

        //create branch catalog, database, tables
        List<Table> mainBranchTableList = prepareOneLevelBranchTest(mainBranchCatalogName, mainBranchDatabaseName,
            subBranchCatalogName, mainBranchTableNum);

        //get catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        DatabaseName mainDatabasePathName = StoreConvertor.databaseName(projectId, mainBranchCatalogName,
            mainBranchDatabaseName);
        Database mainBranchDatabaseRecord = databaseService.getDatabaseByName(mainDatabasePathName);
        assertNotNull(mainBranchDatabaseRecord);

        //drop table
        String dropTableName = mainBranchTableList.get(0).getTableName();

        TableName subBranchTablePathName = StoreConvertor.tableName(projectId, subBranchCatalogRecord.getCatalogName(),
            mainBranchDatabaseRecord.getDatabaseName(), dropTableName);

        //get table from sub-branch
        Table subBranchTable = tableService.getTableByName(subBranchTablePathName);
        assertNotNull(subBranchTable);
        assertEquals(subBranchTable.getTableName(), mainBranchTableList.get(0).getTableName());
        assertEquals(subBranchTable.getDatabaseName(), mainBranchDatabaseRecord.getDatabaseName());
        assertEquals(subBranchTable.getCatalogName(), subBranchCatalogRecord.getCatalogName());

        //drop sub-branch table
        tableService.dropTable(subBranchTablePathName, false, false, false);

        //get table from sub-branch
        TableName tableNameCheck = subBranchTablePathName;
        CatalogServerException exception = assertThrows(CatalogServerException.class,
            () -> tableService.getTableByName(tableNameCheck));
        assertEquals(exception.getMessage(),
            String.format("Table [%s] not found", tableNameCheck.getTableName()));

        //get table from parent
        TableName mainBranchTablePathName = StoreConvertor.tableName(projectId,
            mainBranchCatalogRecord.getCatalogName(),
            mainBranchDatabaseRecord.getDatabaseName(), dropTableName);
        Table mainBranchTable = tableService.getTableByName(mainBranchTablePathName);
        assertNotNull(mainBranchTable);
        assertEquals(mainBranchTable.getCatalogName(), mainBranchCatalogRecord.getCatalogName());

        //undrop table with table name
        TableName subBranchTableName = StoreConvertor.tableName(projectId,
            subBranchTable.getCatalogName(),
            subBranchTable.getDatabaseName(), subBranchTable.getTableId());
        tableService.undropTable(subBranchTablePathName, null, "");

        Table subBranchTable1 = tableService.getTableByName(subBranchTablePathName);
        assertNotNull(subBranchTable1);
        assertEquals(subBranchTable.getCatalogName(), subBranchTable1.getCatalogName());
        assertEquals(subBranchTable.getDatabaseName(), subBranchTable1.getDatabaseName());
        assertEquals(subBranchTable.getTableName(), subBranchTable1.getTableName());
    }

    @Disabled
    public void undropTableSubBranchWithManyTableNameTest() {
        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String mainBranchDatabaseName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        int mainBranchTableNum = 2;

        //create branch catalog, database, tables
        List<Table> mainBranchTableList = prepareOneLevelBranchTestWithDropped(mainBranchCatalogName,
            mainBranchDatabaseName,
            subBranchCatalogName, mainBranchTableNum);

        //get catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        DatabaseName mainDatabasePathName = StoreConvertor.databaseName(projectId, mainBranchCatalogName,
            mainBranchDatabaseName);
        Database mainBranchDatabaseRecord = databaseService.getDatabaseByName(mainDatabasePathName);
        assertNotNull(mainBranchDatabaseRecord);

        //drop table
        String dropTableName = mainBranchTableList.get(0).getTableName();

        TableName subBranchTablePathName = StoreConvertor.tableName(projectId, subBranchCatalogRecord.getCatalogName(),
            mainBranchDatabaseRecord.getDatabaseName(), dropTableName);

        //get table from sub-branch
        Table subBranchTable = tableService.getTableByName(subBranchTablePathName);
        assertNotNull(subBranchTable);
        assertEquals(subBranchTable.getTableName(), mainBranchTableList.get(0).getTableName());
        assertEquals(subBranchTable.getDatabaseName(), mainBranchDatabaseRecord.getDatabaseName());
        assertEquals(subBranchTable.getCatalogName(), subBranchCatalogRecord.getCatalogName());

        //drop sub-branch table
        tableService.dropTable(subBranchTablePathName, false, false, false);

        //get table from sub-branch
        Table table = tableService.getTableByName(subBranchTablePathName);
        assertNull(table);

        //get table from parent
        TableName mainBranchTablePathName = StoreConvertor.tableName(projectId,
            mainBranchCatalogRecord.getCatalogName(),
            mainBranchDatabaseRecord.getDatabaseName(), dropTableName);
        Table mainBranchTable = tableService.getTableByName(mainBranchTablePathName);
        assertNotNull(mainBranchTable);

        //undrop table with table name

        tableService.undropTable(subBranchTablePathName, null, "");

        Table subBranchTable1 = tableService.getTableByName(subBranchTablePathName);
        assertNull(subBranchTable1);
    }

    @Test
    public void listTableCommitsSubBranch1Test() {
        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String mainBranchDatabaseName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        int mainBranchTableNum = 2;

        //create branch catalog, database, tables
        List<Table> mainBranchTableList = prepareOneLevelBranchTest(mainBranchCatalogName, mainBranchDatabaseName,
            subBranchCatalogName, mainBranchTableNum);

        //get catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = getCatalogRecord(subCatalogPathName);
        DatabaseName mainDatabasePathName = StoreConvertor.databaseName(projectId, mainBranchCatalogName,
            mainBranchDatabaseName);

        //create table on sub-branch
        DatabaseName subBranchDatabaseName = StoreConvertor.databaseName(subCatalogPathName, mainBranchDatabaseName);
        List<Table> subBranchTableListNew = createTablesPrepareTest(subBranchDatabaseName, 2);
        checkTableInfo(subBranchTableListNew);

        String token = null;
        int maxResults = Integer.MAX_VALUE;

        Table mainBranchTable = mainBranchTableList.get(0);
        TableName mainBranchTableName = StoreConvertor.tableName(mainDatabasePathName.getProjectId(),
            mainDatabasePathName.getCatalogName(), mainDatabasePathName.getDatabaseName(),
            mainBranchTable.getTableName());
        TraverseCursorResult<List<TableCommit>> tableCommitList = tableService.listTableCommits(mainBranchTableName,
            maxResults, token);

        assertEquals(tableCommitList.getResult().size(), 1);
        assertEquals(tableCommitList.getResult().get(0).getTableId(), mainBranchTableList.get(0).getTableId());

        TableName subBranchTableName = StoreConvertor.tableName(subBranchDatabaseName.getProjectId(),
            subBranchDatabaseName.getCatalogName(), subBranchDatabaseName.getDatabaseName(),
            mainBranchTable.getTableName());
        TraverseCursorResult<List<TableCommit>> subBranchTableCommitList = tableService
            .listTableCommits(subBranchTableName, maxResults, token);

        assertEquals(subBranchTableCommitList.getResult().size(), 1);
        assertEquals(subBranchTableCommitList.getResult().get(0).getTableId(), mainBranchTableList.get(0).getTableId());

        TableName subBranchTableNameNew = StoreConvertor.tableName(subBranchDatabaseName.getProjectId(),
            subBranchDatabaseName.getCatalogName(), subBranchDatabaseName.getDatabaseName(),
            subBranchTableListNew.get(0).getTableName());
        TraverseCursorResult<List<TableCommit>> subBranchTableCommitListNew =
            tableService.listTableCommits(subBranchTableNameNew, maxResults, token);

        assertTrue(subBranchTableCommitListNew.getResult().size() == 1);
        assertEquals(subBranchTableCommitListNew.getResult().get(0).getTableId(),
            subBranchTableListNew.get(0).getTableId());
    }

    @Test
    public void listTableCommitsSubBranch2Test() {
        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String mainBranchDatabaseName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        int mainBranchTableNum = 2;

        //create branch catalog, database, tables
        List<Table> mainBranchTableList = prepareOneLevelBranchTest(mainBranchCatalogName, mainBranchDatabaseName,
            subBranchCatalogName, mainBranchTableNum);

        //get catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        DatabaseName mainDatabasePathName = StoreConvertor.databaseName(projectId, mainBranchCatalogName,
            mainBranchDatabaseName);
        Database mainBranchDatabaseRecord = databaseService.getDatabaseByName(mainDatabasePathName);
        assertNotNull(mainBranchDatabaseRecord);

        //alter table name at sub-branch
        String tableNameNewString = UUID.randomUUID().toString().toLowerCase();

        TableInput tableInput = getTableDTO(
            tableNameNewString, 3);

        TableName subBranchTablePathName = StoreConvertor.tableName(projectId, subBranchCatalogRecord.getCatalogName(),
            mainBranchDatabaseRecord.getDatabaseName(), mainBranchTableList.get(0).getTableName());

        tableService.alterTable(subBranchTablePathName, tableInput, null);

        String token = null;
        int maxResults = Integer.MAX_VALUE;

        subBranchTablePathName = StoreConvertor.tableName(projectId, subBranchCatalogRecord.getCatalogName(),
            mainBranchDatabaseRecord.getDatabaseName(), tableNameNewString);
        TraverseCursorResult<List<TableCommit>> tableCommitList = tableService
            .listTableCommits(subBranchTablePathName, maxResults, token);

        assertTrue(tableCommitList.getResult().size() == 2);

        TableIdent tableIdent = TableObjectHelper.getTableIdentByName(subBranchTablePathName);
        for (TableCommit tableCommit : tableCommitList.getResult()) {
            assertEquals(tableCommit.getCatalogId(), tableIdent.getCatalogId());
            assertEquals(tableCommit.getTableId(), tableIdent.getTableId());
        }

        String version0 = tableCommitList.getResult().get(0).getCommitVersion();
        String version1 = tableCommitList.getResult().get(1).getCommitVersion();

        assertTrue(version0.compareTo(version1) > 0);
    }

    private void checkTableInfo(List<Table> subBranchTableListNew) {
        for (Table table : subBranchTableListNew) {
            TableName tableName = StoreConvertor.tableName(projectId, table.getCatalogName(),
                table.getDatabaseName(), table.getTableName());

            Table subTable = tableService.getTableByName(tableName);
            assertNotNull(subTable);

            logger.info("subtable table " + subTable.getTableName());
        }
    }

    private void checkTableInfo(List<Table> subTableList, String catalogName, String databaseName) {
        for (int i = 0; i < subTableList.size(); i++) {
            logger.info("sub tableName :" + subTableList.get(i).getTableName());
            assertEquals(subTableList.get(i).getDatabaseName(), databaseName);
            assertEquals(subTableList.get(i).getCatalogName(), catalogName);
        }
    }

    @Test
    public void listTableSubBranch2Test() {
        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String mainBranchDatabaseName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        int mainBranchTableNum = 2;

        //create branch catalog, database, tables
        List<Table> mainBranchTableList = prepareOneLevelBranchTest(mainBranchCatalogName, mainBranchDatabaseName,
            subBranchCatalogName, mainBranchTableNum);

        //get catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        DatabaseName mainDatabasePathName = StoreConvertor.databaseName(projectId, mainBranchCatalogName,
            mainBranchDatabaseName);
        Database mainBranchDatabaseRecord = databaseService.getDatabaseByName(mainDatabasePathName);
        assertNotNull(mainBranchDatabaseRecord);

        //create table at sub-branch lev1
        CatalogName subBranchCatalogNameObj = StoreConvertor.catalogName(projectId,
            subBranchCatalogRecord.getCatalogName());
        DatabaseName subBranchDatabaseName = StoreConvertor.databaseName(subCatalogPathName, mainBranchDatabaseName);
        List<Table> subBranchTableListNew = createTablesPrepareTest(subBranchDatabaseName, 2);

        checkTableInfo(subBranchTableListNew);

        //create level2 sub-branch
        String subCatalogNameLev2 = UUID.randomUUID().toString().toLowerCase();

        String version = VersionManagerHelper.getLatestVersionByName(projectId,
            subBranchCatalogNameObj.getCatalogName());

        CatalogInput catalogInputLev2 = getCatalogInput(subCatalogNameLev2, subBranchCatalogNameObj.getCatalogName(),
            version);

        Catalog subBranchCatalogLev2 = catalogService.createCatalog(projectId, catalogInputLev2);
        assertNotNull(subBranchCatalogLev2);

        DatabaseName subBranchDatabasePathNameLev2 = StoreConvertor.databaseName(projectId, subCatalogNameLev2,
            mainBranchDatabaseName);

        TraverseCursorResult<List<Table>> subTableList = tableService.listTable(subBranchDatabasePathNameLev2, false,
            1000, null, null);

        assertEquals(subTableList.getResult().size(), mainBranchTableList.size() + subBranchTableListNew.size());

        checkTableInfo(subTableList.getResult(), subBranchCatalogLev2.getCatalogName(),
            mainBranchDatabaseRecord.getDatabaseName());
    }

    private void listTableAndCheckResult(int maxBatchNum, DatabaseName databaseName, int checkNum,
        Boolean includeDropped) {
        for (int i = 1; i <= maxBatchNum; i++) {
            List<Table> subTableList = new ArrayList<>();
            String nextToken = "";

            while (true) {
                TraverseCursorResult<List<Table>> subTableListBatch = tableService
                    .listTable(databaseName, includeDropped,
                        i, nextToken, null);
                subTableList.addAll(subTableListBatch.getResult());

                if (subTableListBatch.getResult().size() > 0) {
                    nextToken = subTableListBatch.getContinuation().map(catalogToken -> catalogToken.toString())
                        .orElse(null);
                    if (nextToken == null) {
                        break;
                    }
                }
                if (subTableListBatch.getResult().size() == 0) {
                    break;
                }
            }

            assertEquals(subTableList.size(), checkNum);
        }
    }

    private void listTableAndCheckResult(int maxBatchNum, DatabaseName databaseName, int checkNum,
        Boolean dropped, Map<String, String> droppedMap) {
        for (int i = 1; i <= maxBatchNum; i++) {
            List<Table> subTableList = new ArrayList<>();
            String nextToken = "";

            while (true) {
                TraverseCursorResult<List<Table>> subTableListBatch = tableService
                    .listTable(databaseName, dropped,
                        i, nextToken, null);
                subTableList.addAll(subTableListBatch.getResult());

                if (subTableListBatch.getResult().size() > 0) {
                    nextToken = subTableListBatch.getContinuation().map(catalogToken -> {
                        return catalogToken.toString();
                    }).orElse(null);
                    if (nextToken == null) {
                        break;
                    }
                }
                if (subTableListBatch.getResult().size() == 0) {
                    break;
                }
            }
            logger.info("num " + i + " subTableList " + subTableList.size());
            logger.info("num " + i + " mainBranchTableList " + checkNum);
            assertEquals(subTableList.size(), checkNum);
            subTableList.stream().filter(table -> (table.getDroppedTime() > 0))
                .forEach(table -> assertTrue(droppedMap.containsKey(table.getTableName())));
        }
    }

    @Test
    public void listTableWithTokenTest() {
        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String mainBranchDatabaseName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        int mainBranchTableNum = 7;

        //create branch catalog, database, tables
        List<Table> mainBranchTableList = prepareOneLevelBranchTest(mainBranchCatalogName, mainBranchDatabaseName,
            subBranchCatalogName, mainBranchTableNum);

        //get catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        DatabaseName mainDatabasePathName = StoreConvertor.databaseName(projectId, mainBranchCatalogName,
            mainBranchDatabaseName);
        Database mainBranchDatabaseRecord = databaseService.getDatabaseByName(mainDatabasePathName);
        assertNotNull(mainBranchDatabaseRecord);

        listTableAndCheckResult(9, mainDatabasePathName, mainBranchTableList.size(), false);

        //create table at sub-branch lev1
        CatalogName subBranchCatalogNameObj = StoreConvertor
            .catalogName(projectId, subBranchCatalogRecord.getCatalogName());
        DatabaseName subBranchDatabaseName = StoreConvertor.databaseName(subCatalogPathName, mainBranchDatabaseName);
        List<Table> subBranchTableListNew = createTablesPrepareTest(subBranchDatabaseName, 2);

        checkTableInfo(subBranchTableListNew);

        //create level2 sub-branch
        String subCatalogNameLev2 = UUID.randomUUID().toString().toLowerCase();
        String version = VersionManagerHelper.getLatestVersionByName(projectId,
            subBranchCatalogNameObj.getCatalogName());

        CatalogInput catalogInputLev2 = getCatalogInput(subCatalogNameLev2, subBranchCatalogNameObj.getCatalogName(),
            version);

        Catalog subBranchCatalogLev2 = catalogService.createCatalog(projectId, catalogInputLev2);
        assertNotNull(subBranchCatalogLev2);

        DatabaseName subBranchDatabasePathNameLev2 = StoreConvertor.databaseName(projectId, subCatalogNameLev2,
            mainBranchDatabaseName);

        listTableAndCheckResult(9, subBranchDatabasePathNameLev2,
            mainBranchTableList.size() + subBranchTableListNew.size(), false);
    }

    @Test
    public void listTableWithTokenTest2() {
        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String mainBranchDatabaseName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        int mainBranchTableNum = 5;

        //create branch catalog, database, tables
        List<Table> mainBranchTableList = prepareOneLevelBranchTest(mainBranchCatalogName, mainBranchDatabaseName,
            subBranchCatalogName, mainBranchTableNum);

        //get catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        DatabaseName mainDatabasePathName = StoreConvertor.databaseName(projectId, mainBranchCatalogName,
            mainBranchDatabaseName);
        Database mainBranchDatabaseRecord = databaseService.getDatabaseByName(mainDatabasePathName);
        assertNotNull(mainBranchDatabaseRecord);

        //create table at sub-branch lev1
        DatabaseName subBranchDatabaseName = StoreConvertor.databaseName(subCatalogPathName, mainBranchDatabaseName);
        List<Table> subBranchTableListNew = createTablesPrepareTest(subBranchDatabaseName, 2);

        checkTableInfo(subBranchTableListNew);

        //create level2 sub-branch
        String subCatalogNameLev2 = UUID.randomUUID().toString().toLowerCase();
        String version = VersionManagerHelper.getLatestVersionByName(projectId,
            subBranchCatalogRecord.getCatalogName());

        CatalogInput catalogInputLev2 = getCatalogInput(subCatalogNameLev2, subBranchCatalogRecord.getCatalogName(),
            version);

        Catalog subBranchCatalogLev2 = catalogService.createCatalog(projectId, catalogInputLev2);
        assertNotNull(subBranchCatalogLev2);

        DatabaseName subBranchDatabasePathNameLev2 = StoreConvertor.databaseName(projectId, subCatalogNameLev2,
            mainBranchDatabaseName);

        //alter table in lev2 branch
        String tableNameNewString = UUID.randomUUID().toString().toLowerCase();

        TableInput tableInput = getTableDTO(
            tableNameNewString, 3);

        TableName subBranchTablePathName = StoreConvertor
            .tableName(projectId, subBranchDatabasePathNameLev2.getCatalogName(),
                mainBranchDatabaseRecord.getDatabaseName(), mainBranchTableList.get(0).getTableName());

        tableService.alterTable(subBranchTablePathName, tableInput, null);

        listTableAndCheckResult(7, subBranchDatabasePathNameLev2,
            mainBranchTableList.size() + subBranchTableListNew.size(), false);
    }

    @Test
    public void listTableWithTokenTest3() {
        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String mainBranchDatabaseName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        int mainBranchTableNum = 5;
        Map<String, String> droppedTableMapInLev2 = new HashMap<>();
        //create branch catalog, database, tables
        List<Table> mainBranchTableList = prepareOneLevelBranchTest(mainBranchCatalogName, mainBranchDatabaseName,
            subBranchCatalogName, mainBranchTableNum);

        //get catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = getCatalogRecord(subCatalogPathName);
        Catalog mainBranchCatalogRecord = getCatalogRecord(
            StoreConvertor.catalogName(projectId, mainBranchCatalogName));
        Database mainBranchDatabaseRecord = getDatabaseRecord(
            StoreConvertor.databaseName(projectId, mainBranchCatalogName,
                mainBranchDatabaseName));

        //create table at sub-branch lev1
        DatabaseName subBranchDatabaseName = StoreConvertor.databaseName(subCatalogPathName, mainBranchDatabaseName);
        List<Table> subBranchTableListNew = createTablesPrepareTest(subBranchDatabaseName, 2);
        checkTableInfo(subBranchTableListNew);

        //drop table level1
        TableName subBranchDropTablePathNameLev1 = StoreConvertor
            .tableName(projectId, subCatalogPathName.getCatalogName(),
                mainBranchDatabaseRecord.getDatabaseName(), subBranchTableListNew.get(0).getTableName());
        tableService.dropTable(subBranchDropTablePathNameLev1, false, false, false);
        droppedTableMapInLev2.put(subBranchTableListNew.get(0).getTableName(),
            subBranchTableListNew.get(0).getTableName());

        //create level2 sub-branch
        String subCatalogNameLev2 = UUID.randomUUID().toString().toLowerCase();
        String version = VersionManagerHelper.getLatestVersionByName(projectId,
            subBranchCatalogRecord.getCatalogName());
        CatalogInput catalogInputLev2 = getCatalogInput(subCatalogNameLev2, subBranchCatalogRecord.getCatalogName(),
            version);
        Catalog subBranchCatalogLev2 = catalogService.createCatalog(projectId, catalogInputLev2);
        assertNotNull(subBranchCatalogLev2);
        DatabaseName subBranchDatabasePathNameLev2 = StoreConvertor.databaseName(projectId, subCatalogNameLev2,
            mainBranchDatabaseName);

        //alter table in lev2 branch
        String tableNameNewString = UUID.randomUUID().toString().toLowerCase();
        TableInput tableInput = getTableDTO(
            tableNameNewString, 3);
        TableName subBranchTablePathName = StoreConvertor
            .tableName(projectId, subBranchDatabasePathNameLev2.getCatalogName(),
                mainBranchDatabaseRecord.getDatabaseName(), mainBranchTableList.get(0).getTableName());
        tableService.alterTable(subBranchTablePathName, tableInput, null);

        //drop table level2
        TableName subBranchDropTablePathNameLev2 = StoreConvertor
            .tableName(projectId, subBranchDatabasePathNameLev2.getCatalogName(),
                mainBranchDatabaseRecord.getDatabaseName(), mainBranchTableList.get(1).getTableName());
        tableService.dropTable(subBranchDropTablePathNameLev2, false, false, false);
        droppedTableMapInLev2.put(mainBranchTableList.get(1).getTableName(), mainBranchTableList.get(1).getTableName());
        listTableAndCheckResult(13, subBranchDatabasePathNameLev2,
            mainBranchTableList.size() + subBranchTableListNew.size(), true, droppedTableMapInLev2);
        listTableAndCheckResult(14, subBranchDatabasePathNameLev2,
            mainBranchTableList.size() + subBranchTableListNew.size() - 2, false);
    }

    @Test
    public void listTableWithTokenTest4() {
        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String mainBranchDatabaseName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        int mainBranchTableNum = 5;

        //create branch catalog, database, tables
        List<Table> mainBranchTableList = prepareBranchTestWithCreateTable(mainBranchCatalogName,
            mainBranchDatabaseName,
            mainBranchTableNum);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        DatabaseName mainDatabasePathName = StoreConvertor.databaseName(projectId, mainBranchCatalogName,
            mainBranchDatabaseName);

        //drop table level2
        TableName mainDropTablePathName = StoreConvertor
            .tableName(projectId, mainDatabasePathName.getCatalogName(),
                mainDatabasePathName.getDatabaseName(), mainBranchTableList.get(0).getTableName());

        tableService.dropTable(mainDropTablePathName, false, false, false);

        //create branch
        String subCatalogName = subBranchCatalogName;
        CatalogName parentCatalogName = StoreConvertor
            .catalogName(projectId, mainBranchCatalogRecord.getCatalogName());
        String version = VersionManagerHelper.getLatestVersionByName(projectId, parentCatalogName.getCatalogName());
        CatalogInput catalogInput = getCatalogInput(subCatalogName, parentCatalogName.getCatalogName(), version);
        Catalog subBranchCatalog = catalogService.createCatalog(projectId, catalogInput);
        assertNotNull(subBranchCatalog);

        //get catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        DatabaseName subBranchDatabasePathName = StoreConvertor
            .databaseName(subCatalogPathName, mainBranchDatabaseName);
        listTableAndCheckResult(1, subBranchDatabasePathName,
            mainBranchTableList.size() - 1, false);
    }

    private TableName createAlterTableNameHistory(int num, TableName tableName) {
        TableName latestTableName = tableName;
        for (int i = 0; i < num; i++) {
            //alter table name at sub-branch
            String tableNameNewString = UUID.randomUUID().toString().toLowerCase();

            TableInput tableInput = getTableDTO(
                tableNameNewString, 3);

            tableService.alterTable(latestTableName, tableInput, null);
            latestTableName = StoreConvertor.tableName(tableName.getProjectId(), tableName.getCatalogName(),
                tableName.getDatabaseName(), tableNameNewString);
        }

        return latestTableName;
    }

    private void listTableCommitsAndCheckResult(int maxBatchNum, TableName tableName, int resultNum,
        String catalogId, String taleId) {

        //list table commit in main branch
        for (int i = 1; i <= maxBatchNum; i++) {

            List<TableCommit> tableCommitList = new ArrayList<>();
            String nextToken = null;

            while (true) {
                TraverseCursorResult<List<TableCommit>> subTableListBatch =
                    tableService.listTableCommits(tableName, i, nextToken);
                tableCommitList.addAll(subTableListBatch.getResult());

                if (subTableListBatch.getResult().size() > 0) {
                    nextToken = subTableListBatch.getContinuation().map(catalogToken -> {
                        return catalogToken.toString();
                    }).orElse(null);
                    if (nextToken == null) {
                        break;
                    }
                }

                if (subTableListBatch.getResult().size() == 0) {
                    break;
                }
            }

            assertEquals(tableCommitList.size(), resultNum);
            tableCommitList.stream().forEach(tableCommit -> assertEquals(tableCommit.getCatalogId(), catalogId));
            tableCommitList.stream().forEach(tableCommit -> assertEquals(tableCommit.getTableId(), taleId));
        }
    }

    @Test
    public void listTableCommitsSubBranchTokenTest1() {
        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String mainBranchDatabaseName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        int mainBranchTableNum = 3;

        //create branch catalog, database, tables
        List<Table> mainBranchTableList = prepareOneLevelBranchTest(mainBranchCatalogName, mainBranchDatabaseName,
            subBranchCatalogName, mainBranchTableNum);

        //get catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = getCatalogRecord(subCatalogPathName);
        Database mainBranchDatabaseRecord = getDatabaseRecord(
            StoreConvertor.databaseName(projectId, mainBranchCatalogName,
                mainBranchDatabaseName));

        Table mainBranchTable = mainBranchTableList.get(0);
        TableName mainBranchTableName = StoreConvertor
            .tableName(projectId, mainBranchTable.getCatalogName(),
                mainBranchTable.getDatabaseName(), mainBranchTable.getTableName());

        //create table history in main branch
        TableName latestMainbranchTableName = createAlterTableNameHistory(10, mainBranchTableName);

        //create table on sub-branch
        DatabaseName subBranchDatabaseName = StoreConvertor.databaseName(subCatalogPathName, mainBranchDatabaseName);
        List<Table> subBranchTableListNew = createTablesPrepareTest(subBranchDatabaseName, 2);
        checkTableInfo(subBranchTableListNew);

        Table subBranchTable = subBranchTableListNew.get(0);
        TableName subBranchTableName = StoreConvertor
            .tableName(projectId, subBranchTable.getCatalogName(),
                subBranchTable.getDatabaseName(), subBranchTable.getTableName());

        //create table history in sub branch
        TableName latestSubbranchTableName = createAlterTableNameHistory(10, subBranchTableName);

        TableIdent tableIdent = TableObjectHelper.getTableIdentByName(latestMainbranchTableName);
        listTableCommitsAndCheckResult(12, latestMainbranchTableName, 11, tableIdent.getCatalogId(),
            tableIdent.getTableId());

        tableIdent = TableObjectHelper.getTableIdentByName(latestSubbranchTableName);
        listTableCommitsAndCheckResult(12, latestSubbranchTableName, 11, tableIdent.getCatalogId(),
            tableIdent.getTableId());

        //create table history in subbranch, the table inherit parent
        Table subBranchTable2 = mainBranchTableList.get(0);
        TableName subBranchTableName2 = StoreConvertor
            .tableName(projectId, subBranchDatabaseName.getCatalogName(),
                subBranchDatabaseName.getDatabaseName(), subBranchTable2.getTableName());
        //create table history in sub branch
        TableName latestSubbranchTableName2 = createAlterTableNameHistory(10,
            subBranchTableName2);

        tableIdent = TableObjectHelper.getTableIdentByName(latestSubbranchTableName2);
        listTableCommitsAndCheckResult(12, latestSubbranchTableName2, 11, tableIdent.getCatalogId(),
            tableIdent.getTableId());
    }


    @Test
    public void tableMapWithCreateTest() {
        String tableNameString = UUID.randomUUID().toString().toLowerCase();

        TableInput tableInput = getTableDTO(tableNameString, 3);
        buildLmsNameProperties(tableInput, String.format("%s", catalogNameString));
        DatabaseName databaseName = StoreConvertor.databaseName(projectId, catalogNameString, databaseNameString);
        tableService.createTable(databaseName, tableInput);

        Optional<MetaObjectName> tableMap = objectNameMapService.getObjectFromNameMap(projectId,
            ObjectType.TABLE.name(),
            databaseName.getDatabaseName(), tableNameString);
        assertEquals(tableMap.get().getObjectName(), tableNameString);
        assertEquals(tableMap.get().getDatabaseName(), databaseName.getDatabaseName());
        assertEquals(tableMap.get().getCatalogName(), databaseName.getCatalogName());
        assertEquals(tableMap.get().getProjectId(), projectId);
    }

    @Test
    public void tableMapWithUnDropTest() {
        String tableNameString = UUID.randomUUID().toString().toLowerCase();

        TableInput tableInput = getTableDTO(tableNameString, 3);
        buildLmsNameProperties(tableInput, String.format("%s", catalogNameString));
        DatabaseName databaseName = StoreConvertor.databaseName(projectId, catalogNameString, databaseNameString);
        tableService.createTable(databaseName, tableInput);

        //drop table
        TableName tableName = StoreConvertor.tableName(projectId, catalogNameString, databaseNameString,
            tableNameString);
        Table table = tableService.getTableByName(tableName);
        tableService.dropTable(tableName, false, false, false);
        Optional<MetaObjectName> metaObjectName = objectNameMapService
            .getObjectFromNameMap(projectId, ObjectType.TABLE.name(), databaseNameString, tableNameString);
        assertFalse(metaObjectName.isPresent());

        //undrop table
        tableService.undropTable(tableName, table.getTableId(), "");
        Optional<MetaObjectName> tableMap = objectNameMapService.getObjectFromNameMap(projectId,
            ObjectType.TABLE.name(),
            databaseName.getDatabaseName(), tableNameString);
        assertEquals(tableMap.get().getObjectName(), tableNameString);
        assertEquals(tableMap.get().getDatabaseName(), databaseName.getDatabaseName());
        assertEquals(tableMap.get().getCatalogName(), databaseName.getCatalogName());
        assertEquals(tableMap.get().getProjectId(), projectId);

        tableService.dropTable(tableName, false, false, false);
        metaObjectName = objectNameMapService
            .getObjectFromNameMap(projectId, ObjectType.TABLE.name(), databaseNameString, tableNameString);
        assertFalse(metaObjectName.isPresent());

        String tableNameStringNew = UUID.randomUUID().toString().toLowerCase();
        tableService.undropTable(tableName, table.getTableId(), tableNameStringNew);
        tableMap = objectNameMapService.getObjectFromNameMap(projectId, ObjectType.TABLE.name(),
            databaseName.getDatabaseName(), tableNameStringNew);
        assertEquals(tableMap.get().getObjectName(), tableNameStringNew);
        assertEquals(tableMap.get().getDatabaseName(), databaseName.getDatabaseName());
        assertEquals(tableMap.get().getCatalogName(), databaseName.getCatalogName());
        assertEquals(tableMap.get().getProjectId(), projectId);
    }

    @Test
    public void tableMapWithRestoreTest() {
        String tableNameString = UUID.randomUUID().toString().toLowerCase();

        TableInput tableInput = getTableDTO(tableNameString, 3);
        buildLmsNameProperties(tableInput, String.format("%s", catalogNameString));
        DatabaseName databaseName = StoreConvertor.databaseName(projectId, catalogNameString, databaseNameString);
        tableService.createTable(databaseName, tableInput);
        TableName tableName = StoreConvertor
            .tableName(projectId, catalogNameString, databaseNameString, tableNameString);

        //restore table prepare
        TableCommit tableHistory1 = tableService.getLatestTableCommit(tableName);
        assertNotNull(tableHistory1);
        String encodeText = tableHistory1.getCommitVersion();

        //alter table name
        String tableNameStringNew = UUID.randomUUID().toString().toLowerCase();
        tableInput = getTableDTO(tableNameStringNew, 3);
        tableService.alterTable(tableName, tableInput, null);

        Optional<MetaObjectName> tableMap = objectNameMapService
            .getObjectFromNameMap(projectId, ObjectType.TABLE.name(), databaseName.getDatabaseName(),
                tableNameStringNew);
        assertEquals(tableMap.get().getObjectName(), tableNameStringNew);
        assertEquals(tableMap.get().getDatabaseName(), databaseName.getDatabaseName());
        assertEquals(tableMap.get().getCatalogName(), databaseName.getCatalogName());
        assertEquals(tableMap.get().getProjectId(), projectId);

        //restore table
        TableName tableNameNew = StoreConvertor
            .tableName(projectId, catalogNameString, databaseNameString, tableNameStringNew);
        tableService.restoreTable(tableNameNew, encodeText);

        //get table map
        tableMap = objectNameMapService
            .getObjectFromNameMap(projectId, ObjectType.TABLE.name(), databaseName.getDatabaseName(), tableNameString);
        assertEquals(tableMap.get().getObjectName(), tableNameString);
        assertEquals(tableMap.get().getDatabaseName(), databaseName.getDatabaseName());
        assertEquals(tableMap.get().getCatalogName(), databaseName.getCatalogName());
        assertEquals(tableMap.get().getProjectId(), projectId);

        Optional<MetaObjectName> metaObjectName = objectNameMapService
            .getObjectFromNameMap(projectId, ObjectType.TABLE.name(), databaseNameString, tableNameStringNew);
        assertFalse(metaObjectName.isPresent());
    }

    @Test
    public void tableMapWithBranch() {
        String mainBranchCatalogName = "main_catalog" + UUID.randomUUID();
        String mainBranchDatabaseName = "main_database" + UUID.randomUUID();
        String subBranchCatalogName = "sub_catalog" + UUID.randomUUID();
        int mainBranchTableNum = 1;

        List<Table> mainBranchTableList = prepareOneLevelBranchTest(mainBranchCatalogName, mainBranchDatabaseName,
            subBranchCatalogName, mainBranchTableNum);

        //get catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        DatabaseName mainDatabasePathName = StoreConvertor.databaseName(projectId, mainBranchCatalogName,
            mainBranchDatabaseName);
        Database mainBranchDatabaseRecord = databaseService.getDatabaseByName(mainDatabasePathName);
        assertNotNull(mainBranchDatabaseRecord);

        String tableNameString = mainBranchTableList.get(0).getTableName();
        Optional<MetaObjectName> tableMap = objectNameMapService
            .getObjectFromNameMap(projectId, ObjectType.TABLE.name(), mainBranchDatabaseName, tableNameString);
        assertEquals(tableMap.get().getObjectName(), tableNameString);
        assertEquals(tableMap.get().getDatabaseName(), mainBranchDatabaseName);
        assertEquals(tableMap.get().getCatalogName(), mainBranchCatalogName);
        assertEquals(tableMap.get().getProjectId(), projectId);

        //alter table and set map
        TableInput tableInput = getTableDTO(
            tableNameString, 3);
        buildLmsNameProperties(tableInput, String.format("%s", subBranchCatalogName));
        TableName tableName = StoreConvertor
            .tableName(projectId, mainBranchCatalogName, mainBranchDatabaseName, tableNameString);
        tableService.alterTable(tableName, tableInput, null);

        tableMap = objectNameMapService
            .getObjectFromNameMap(projectId, ObjectType.TABLE.name(), mainBranchDatabaseName, tableNameString);
        assertEquals(tableMap.get().getObjectName(), tableNameString);
        assertEquals(tableMap.get().getDatabaseName(), mainBranchDatabaseName);
        assertEquals(tableMap.get().getCatalogName(), subBranchCatalogName);
        assertEquals(tableMap.get().getProjectId(), projectId);
    }

    @Test
    public void tableMapWithDatabaseAlter() {
        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String mainBranchDatabaseName = UUID.randomUUID().toString().toLowerCase();
        int mainBranchTableNum = 5;

        //create branch catalog, database, tables
        List<Table> mainBranchTableList = prepareBranchTestWithCreateTable(mainBranchCatalogName,
            mainBranchDatabaseName, mainBranchTableNum);

        for (Table table : mainBranchTableList) {
            Optional<MetaObjectName> tableMap = objectNameMapService
                .getObjectFromNameMap(projectId, ObjectType.TABLE.name(), mainBranchDatabaseName, table.getTableName());
            assertEquals(tableMap.get().getObjectName(), table.getTableName());
            assertEquals(tableMap.get().getDatabaseName(), mainBranchDatabaseName);
            assertEquals(tableMap.get().getCatalogName(), mainBranchCatalogName);
            assertEquals(tableMap.get().getProjectId(), projectId);
        }

        String dbNameNew = UUID.randomUUID().toString().toLowerCase();
        DatabaseInput dbDTO2 = getDatabaseDTO(projectId, mainBranchCatalogName, dbNameNew);
        DatabaseName currentDBFullName = StoreConvertor
            .databaseName(projectId, mainBranchCatalogName, mainBranchDatabaseName);
        databaseService.alterDatabase(currentDBFullName, dbDTO2);
        currentDBFullName = StoreConvertor
            .databaseName(projectId, mainBranchCatalogName, dbNameNew);
        Database renameDB = databaseService.getDatabaseByName(currentDBFullName);
        assertNotNull(renameDB);

        for (Table table : mainBranchTableList) {
            Optional<MetaObjectName> tableMap = objectNameMapService
                .getObjectFromNameMap(projectId, ObjectType.TABLE.name(), dbNameNew, table.getTableName());
            assertEquals(tableMap.get().getObjectName(), table.getTableName());
            assertEquals(tableMap.get().getDatabaseName(), dbNameNew);
            assertEquals(tableMap.get().getCatalogName(), mainBranchCatalogName);
            assertEquals(tableMap.get().getProjectId(), projectId);
        }
    }

    @Test
    public void tableMapWithDatabaseAlterBranch() {
        String mainBranchCatalogName = "main_catalog" + UUID.randomUUID();
        String mainBranchDatabaseName = "main_database" + UUID.randomUUID();
        String subBranchCatalogName = "sub_catalog" + UUID.randomUUID();
        int mainBranchTableNum = 1;

        List<Table> mainBranchTableList = prepareOneLevelBranchTest(mainBranchCatalogName, mainBranchDatabaseName,
            subBranchCatalogName, mainBranchTableNum);

        //get catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        DatabaseName mainDatabasePathName = StoreConvertor.databaseName(projectId, mainBranchCatalogName,
            mainBranchDatabaseName);
        Database mainBranchDatabaseRecord = databaseService.getDatabaseByName(mainDatabasePathName);
        assertNotNull(mainBranchDatabaseRecord);

        String tableNameString = mainBranchTableList.get(0).getTableName();
        Optional<MetaObjectName> tableMap = objectNameMapService
            .getObjectFromNameMap(projectId, ObjectType.TABLE.name(), mainBranchDatabaseName, tableNameString);
        assertEquals(tableMap.get().getObjectName(), tableNameString);
        assertEquals(tableMap.get().getDatabaseName(), mainBranchDatabaseName);
        assertEquals(tableMap.get().getCatalogName(), mainBranchCatalogName);
        assertEquals(tableMap.get().getProjectId(), projectId);

        //alter table and set map
        TableInput tableInput = getTableDTO(
            tableNameString, 3);
        buildLmsNameProperties(tableInput, String.format("%s", subBranchCatalogName));
        TableName tableName = StoreConvertor.tableName(projectId, mainBranchCatalogName, mainBranchDatabaseName,
            tableNameString);
        tableService.alterTable(tableName, tableInput, null);

        String dbNameNew = UUID.randomUUID().toString().toLowerCase();
        DatabaseInput dbDTO2 = getDatabaseDTO(projectId, mainBranchCatalogName, dbNameNew);
        DatabaseName currentDBFullName = StoreConvertor
            .databaseName(projectId, mainBranchCatalogName, mainBranchDatabaseName);
        databaseService.alterDatabase(currentDBFullName, dbDTO2);
        currentDBFullName = StoreConvertor
            .databaseName(projectId, mainBranchCatalogName, dbNameNew);
        Database renameDB = databaseService.getDatabaseByName(currentDBFullName);
        assertNotNull(renameDB);

        tableMap = objectNameMapService
            .getObjectFromNameMap(projectId, ObjectType.TABLE.name(), mainBranchDatabaseName, tableNameString);
        assertEquals(tableMap.get().getObjectName(), tableNameString);
        assertEquals(tableMap.get().getDatabaseName(), mainBranchDatabaseName);
        assertEquals(tableMap.get().getCatalogName(), subBranchCatalogName);
        assertEquals(tableMap.get().getProjectId(), projectId);
    }

    @Test
    public void desc_and_show_tables_should_return_same_struct() {
        String tableNameString = UUID.randomUUID().toString().toLowerCase();
        TableInput tableInput = getTableDTO(tableNameString, 3);
        DatabaseName databaseName = StoreConvertor.databaseName(projectId, catalogNameString, databaseNameString);
        tableService.createTable(databaseName, tableInput);

        // get table by name
        TableName tableName = StoreConvertor
            .tableName(projectId, catalogNameString, databaseNameString, tableNameString);
        Table table1 = tableService.getTableByName(tableName);
        assertNotNull(table1);
        assertEquals(tableNameString, table1.getTableName());

        // list tables
        TraverseCursorResult<List<Table>> CursorTable2 = tableService.listTable(databaseName, true, 100, null, null);
        assertEquals(1, CursorTable2.getResult().size());
        Table table2 = CursorTable2.getResult().get(0);
        assertEquals(tableNameString, table2.getTableName());

        // compare results, should be equal
        assertEquals(table1, table2);
    }
}
