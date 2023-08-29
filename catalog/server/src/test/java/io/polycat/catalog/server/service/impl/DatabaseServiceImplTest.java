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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.model.Catalog;
import io.polycat.catalog.common.model.CatalogIdent;
import io.polycat.catalog.common.model.CatalogName;
import io.polycat.catalog.common.model.Database;

import io.polycat.catalog.common.model.DatabaseHistory;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.DatabaseObject;

import io.polycat.catalog.common.model.MetaObjectName;
import io.polycat.catalog.common.model.TraverseCursorResult;
import io.polycat.catalog.common.plugin.request.input.CatalogInput;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;
import io.polycat.catalog.common.utils.CatalogToken;
import io.polycat.catalog.server.CatalogApplication;
import io.polycat.catalog.server.util.TransactionRunner;
import io.polycat.catalog.store.common.StoreConvertor;

import com.apple.foundationdb.tuple.Tuple;
import org.junit.Ignore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@SpringBootTest(classes = CatalogApplication.class)
public class DatabaseServiceImplTest extends TestUtil {

    private static final Logger log = Logger.getLogger(DatabaseServiceImplTest.class);
    static boolean isFirstTest = true;

    private void beforeClass() {
        if (isFirstTest) {
            createCatalogBeforeClass();
            isFirstTest = false;
        }
    }

    @BeforeEach
    public void beforeEach() {
        beforeClass();
    }

    private void assertSameDatabase(Database expect, Database actual) {
        assertNotNull(expect, "actual database is null");
        assertNotNull(actual, "expect database is null");

        assertEquals(expect.getCatalogName(), actual.getCatalogName());
        assertEquals(expect.getDatabaseName(), actual.getDatabaseName());
    }

    private void assertSameBranch(String catalogName, TraverseCursorResult<List<Database>> expect,
        List<Database> actual) {
        Set<String> parentBranchDatabases = actual.stream().map(Database::getDatabaseName)
            .collect(Collectors.toSet());

        for (Database database : expect.getResult()) {
            assertEquals(database.getCatalogName(), catalogName);
            assertTrue(parentBranchDatabases.contains(database.getDatabaseName()));
        }
    }

    @Test
    public void createNewDatabaseTest() {
        // Prepare: need to creat a new Catalog. Create a database, and then get database.
        Tuple catalogTuple = createNewCatalog();
        String catalogNameString = catalogTuple.getString(1);
        String dbName = UUID.randomUUID().toString().toLowerCase().toLowerCase();

        DatabaseInput databaseInput = getDatabaseDTO(projectId, catalogNameString, dbName);
        CatalogName catalogName = StoreConvertor.catalogName(projectId, catalogNameString);
        Database createDB = databaseService.createDatabase(catalogName, databaseInput);
        assertNotNull(createDB);
        assertEquals(createDB.getCatalogName(), catalogNameString);
        assertEquals(createDB.getDatabaseName(), dbName);

        DatabaseName databaseName = StoreConvertor.databaseName(projectId, catalogNameString, dbName);
        Database database1 = databaseService.getDatabaseByName(databaseName);
        assertNotNull(database1);
        assertSameDatabase(createDB, database1);
    }

    @Test
    public void createExistDatabaseTest() {
        // Prepare: need to creat a new Catalog.
        // Create a exist database, get DATABASE_ALREADY_EXIST exception.
        Tuple catalogTuple = createNewCatalog();

        String dbName = UUID.randomUUID().toString().toLowerCase().toLowerCase();
        String catalogNameString = catalogTuple.getString(1);
        DatabaseInput databaseInput = getDatabaseDTO(projectId, catalogNameString, dbName);
        CatalogName catalogName = StoreConvertor.catalogName(projectId, catalogNameString);
        databaseService.createDatabase(catalogName, databaseInput);

        CatalogServerException ex = assertThrows(CatalogServerException.class,
            () -> databaseService.createDatabase(catalogName, databaseInput));
        assertEquals(ex.getErrorCode(), ErrorCode.DATABASE_ALREADY_EXIST);
    }

    @Test
    public void createDBSameNameAsCatalogTest() {
        // Prepare: need to creat a new Catalog and database with the same name.
        Tuple catalogTuple = createNewCatalog();
        String sameName = catalogTuple.getString(1);

        DatabaseInput databaseInput = getDatabaseDTO(projectId, sameName, sameName);
        CatalogName catalogName = StoreConvertor.catalogName(projectId, sameName);
        Database createDB = databaseService.createDatabase(catalogName, databaseInput);
        assertNotNull(createDB);

        DatabaseName databaseName = StoreConvertor.databaseName(projectId, sameName, sameName);
        Database getDB = databaseService.getDatabaseByName(databaseName);
        assertNotNull(getDB);
        assertSameDatabase(createDB, getDB);
    }

    @Test
    public void createDatabaseInMultiCatalogTest() {
        String dbName = UUID.randomUUID().toString().toLowerCase().toLowerCase();

        for (int idx = 0; idx < 3; ++idx) {
            String catalogName = UUID.randomUUID().toString().toLowerCase();
            CatalogInput catalogInput = getCatalogInput(catalogName);
            Catalog catalog = catalogService.createCatalog(projectId, catalogInput);
            assertNotNull(catalog);

            DatabaseInput databaseInput = getDatabaseDTO(projectId, catalogName, dbName);
            CatalogName catalogNameObj = StoreConvertor.catalogName(projectId, catalogName);
            Database db = databaseService.createDatabase(catalogNameObj, databaseInput);
            assertNotNull(db);
        }
    }

    @Test
    public void getNotExistDatabaseTest() {
        // Prepare: need to creat a new Catalog with no databases. Get nonexistent databases.
        Tuple catalogTuple = createNewCatalog();

        String dbName = UUID.randomUUID().toString().toLowerCase();

        DatabaseName databaseName = StoreConvertor.databaseName(projectId, catalogTuple.getString(1), dbName);
        Database database1 = databaseService.getDatabaseByName(databaseName);
        assertNull(database1);
    }

    @Test
    public void listDatabasesTest() {
        // Prepare: need to create a new Catalog with 2 databases
        Tuple catalogTuple = createNewCatalog();
        String catalogNameString = catalogTuple.getString(1);
        int listDbNum = 2;
        List<Database> inputDataBases = new ArrayList<>();
        for (int i = 0; i < listDbNum; ++i) {
            String dbNameString = UUID.randomUUID().toString().toLowerCase();
            DatabaseInput dbDTO = getDatabaseDTO(projectId, catalogNameString, dbNameString);
            CatalogName catalogNameObj = StoreConvertor.catalogName(projectId, catalogNameString);
            Database db = databaseService.createDatabase(catalogNameObj, dbDTO);
            assertNotNull(db);
            inputDataBases.add(db);
        }
        inputDataBases.sort(Comparator.comparing(Database::getDatabaseName));

        // Case 1: list undropped databases from FDB that input
        CatalogName catalogName = StoreConvertor.catalogName(projectId, catalogNameString);
        TraverseCursorResult<List<Database>> dbs = databaseService.listDatabases(
            catalogName,
            false,
            1000,
            "",
            "");

        dbs.getResult().sort(Comparator.comparing(Database::getDatabaseName));

        for (int i = 0; i < listDbNum; ++i) {
            assertEquals(inputDataBases.get(i).getCatalogName(), dbs.getResult().get(i).getCatalogName());
            assertEquals(inputDataBases.get(i).getDatabaseName(), dbs.getResult().get(i).getDatabaseName());
        }

        // Case 2: 异常：project无效
        String invalidProjectId = UUID.randomUUID().toString().toLowerCase();
        CatalogName invalidCatalogName = StoreConvertor.catalogName(invalidProjectId, catalogNameString);

        try {
            databaseService.listDatabases(
                invalidCatalogName,
                false,
                1000,
                "",
                "");
        } catch (MetaStoreException e) {
            log.error("Exception Caught in listDatabasesTest(): Invalid ProjectID.");
        }

        // Case 3: 异常：catalog无效
        String invalidCatalogNameString = UUID.randomUUID().toString().toLowerCase();
        invalidCatalogName = StoreConvertor.catalogName(projectId, invalidCatalogNameString);

        try {
            databaseService.listDatabases(
                invalidCatalogName,
                false,
                1000,
                "",
                "");
        } catch (MetaStoreException e) {
            log.error("Exception Caught in listDatabasesTest(): Invalid CatalogID.");
        }

        // Case 4: 异常：limit非有效整数
        // scanNum should be integer, AND Range from 0 to Max (1000 for now)
        List<String> scanNumList = Arrays.asList("10.1", "-1", "1001");
        for (String scanNum : scanNumList) {
            try {
                databaseService.listDatabases(
                    catalogName,
                    false,
                    Integer.parseInt(scanNum),
                    "",
                    "");
            } catch (NumberFormatException e) {
                log.error("Exception Caught in listDatabasesTest(): Invalid MaximumToScan numeric '" + scanNum + "'.");
            } catch (CatalogServerException e) {
                log.error("Exception Caught in listDatabasesTest(): Invalid MaximumToScan range '" + scanNum + "'.");
            }
        }
    }

    @Test
    public void getDatabaseHistoryTest() {
        // Prepare: need to creat a new Catalog. Create a database, and then alter the database for 2 times
        Tuple catalogTuple = createNewCatalog();
        assertEquals(catalogTuple.getString(0), projectId);
        String catalogNameString = catalogTuple.getString(1);

        String dbName = UUID.randomUUID().toString().toLowerCase();
        DatabaseInput dbDTO = getDatabaseDTO(projectId, catalogNameString, dbName);
        CatalogName catalogNameObj = StoreConvertor.catalogName(projectId, catalogNameString);
        Database createDB = databaseService.createDatabase(catalogNameObj, dbDTO);
        assertNotNull(createDB);
        DatabaseName currentDBName = StoreConvertor.databaseName(projectId, catalogNameString, dbName);

        // rename database to newName1
        String newName1 = UUID.randomUUID().toString().toLowerCase();
        DatabaseInput newDTO1 = getDatabaseDTO(projectId, catalogNameString, newName1);
        databaseService.alterDatabase(currentDBName, newDTO1);
        currentDBName = StoreConvertor.databaseName(projectId, catalogNameString, newName1);
        Database renamedDB1 = databaseService.getDatabaseByName(currentDBName);
        assertNotNull(renamedDB1);
        assertEquals(renamedDB1.getCatalogName(), catalogNameString);
        assertEquals(renamedDB1.getDatabaseName(), newName1);


        // rename database to newName2
        String newName2 = UUID.randomUUID().toString().toLowerCase();
        DatabaseInput newDTO2 = getDatabaseDTO(projectId, catalogNameString, newName2);
        databaseService.alterDatabase(currentDBName, newDTO2);
        currentDBName = StoreConvertor.databaseName(projectId, catalogNameString, newName2);
        Database renamedDB2 =  databaseService.getDatabaseByName(currentDBName);
        assertNotNull(renamedDB2);
        assertEquals(renamedDB2.getDatabaseName(), newName2);

        // get database history
        List<DatabaseHistory> dbHistories = databaseService.getDatabaseHistory(currentDBName);
        assertNotNull(dbHistories);

        assertEquals(dbHistories.size(), 3);
        assertEquals(dbHistories.get(dbHistories.size() - 1).getName(), createDB.getDatabaseName());

        // get database by version
        DatabaseHistory dbhist = databaseService.getDatabaseByVersion(currentDBName, dbHistories.get(0).getVersion());
        assertNotNull(dbhist);
    }

    @Test
    public void dropDatabaseTest() {
        // Prepare: need to creat a new Catalog with two databases. And then drop one whose name is dbName1.
        Tuple catalogTuple = createNewCatalog();
        String catalogNameString = catalogTuple.getString(1);
        String dbName1 = UUID.randomUUID().toString().toLowerCase();
        String dbName2 = UUID.randomUUID().toString().toLowerCase();
        DatabaseInput dbDTO1 = getDatabaseDTO(projectId, catalogNameString, dbName1);
        CatalogName catalogNameObj = StoreConvertor.catalogName(projectId, catalogNameString);
        Database db1 = databaseService.createDatabase(catalogNameObj, dbDTO1);
        assertNotNull(db1);
        DatabaseInput dbDTO2 = getDatabaseDTO(projectId, catalogNameString, dbName2);
        Database db2 = databaseService.createDatabase(catalogNameObj, dbDTO2);
        assertNotNull(db2);

        DatabaseName dropDBName = StoreConvertor.databaseName(projectId, catalogNameString, dbName1);
        String droppedDBId = databaseService.dropDatabase(dropDBName, "false");

        // Case 1: list undropped databases
        CatalogName catalogName = StoreConvertor.catalogName(projectId, catalogNameString);
        TraverseCursorResult<List<Database>> undroppedDBs = databaseService.listDatabases(
            catalogName,
            false,
            1000,
            "",
            "");
        assertEquals(1, undroppedDBs.getResult().size());
        assertEquals(catalogNameString, undroppedDBs.getResult().get(0).getCatalogName());
        assertEquals(dbName2, undroppedDBs.getResult().get(0).getDatabaseName());
        // Case 2: list all databases including the dropped one
        TraverseCursorResult<List<Database>> allDBs = databaseService.listDatabases(
            catalogName,
            true,
            1000,
            "",
            "");
        assertEquals(2, allDBs.getResult().size());
        // Use Dropped Flag to sort in ascending order
        allDBs.getResult().sort(Comparator.comparing(Database::getDroppedTime));
        assertEquals(db2.getCatalogName(), allDBs.getResult().get(0).getCatalogName());
        assertEquals(dbName2, allDBs.getResult().get(0).getDatabaseName());
        assertEquals(0, allDBs.getResult().get(0).getDroppedTime(), "database2 is dropped");

        assertEquals(dbName1, allDBs.getResult().get(1).getDatabaseName());
        assertNotEquals(0, allDBs.getResult().get(1).getDescription(), "database1 is not dropped");
    }


    @Test
    public void undropDatabaseTest() {
        Tuple catalogTuple = createNewCatalog();
        String catalogNameString = catalogTuple.getString(1);
        CatalogName catalogName = StoreConvertor.catalogName(projectId, catalogNameString);

        // step1: drop DB1, drop DB2
        String dbName1 = UUID.randomUUID().toString().toLowerCase();
        String droppedDBId1 = prepareDropDatabaseTest(catalogNameString, dbName1);
        DatabaseName dropDBName1 = StoreConvertor.databaseName(projectId, catalogNameString, dbName1);

        String dbName2 = UUID.randomUUID().toString().toLowerCase();
        String droppedDBId2 = prepareDropDatabaseTest(catalogNameString, dbName2);

        // step2: undrop DB1 without name;
        DatabaseName droppedDatabaseName = StoreConvertor.databaseName(projectId, catalogNameString, dbName1);
        DatabaseObject dbRecordBefore = DatabaseObjectHelper.getDatabaseObject(droppedDatabaseName);
        assertNull(dbRecordBefore);
        databaseService.undropDatabase(dropDBName1, null, "");
        DatabaseObject dbRecordAfter = DatabaseObjectHelper.getDatabaseObject(droppedDatabaseName);
        assertNotNull(dbRecordAfter);
        assertEquals(dbRecordAfter.getDatabaseId(), droppedDBId1);
        assertEquals(dbRecordAfter.getName(), dbName1);

        // step3: list undropped databases, expect get DB1;
        TraverseCursorResult<List<Database>> undroppedDBs = databaseService.listDatabases(catalogName, false,
            1000, "", "");
        assertEquals(1, undroppedDBs.getResult().size());
        assertEquals(catalogNameString, undroppedDBs.getResult().get(0).getCatalogName());
        assertEquals(dbName1, undroppedDBs.getResult().get(0).getDatabaseName());

        // step4: list all databases including the dropped one
        TraverseCursorResult<List<Database>> allDBs = databaseService.listDatabases(catalogName, true,
            1000, "", "");
        assertEquals(2, allDBs.getResult().size());
        // Use Dropped Flag to sort in ascending order, expect DB1(normal)--DB2(dropped)
        allDBs.getResult().sort(Comparator.comparing(Database::getDroppedTime));
        assertEquals(catalogNameString, allDBs.getResult().get(0).getCatalogName());
        assertEquals(dbName1, allDBs.getResult().get(0).getDatabaseName());
        assertEquals(0, allDBs.getResult().get(0).getDroppedTime(), "database1 is dropped");

        assertEquals(catalogNameString, allDBs.getResult().get(1).getCatalogName());
        assertEquals(dbName2, allDBs.getResult().get(1).getDatabaseName());
        assertNotEquals(0, allDBs.getResult().get(1).getDescription(), "database2 is not dropped");
    }

    @Test
    public void undropDatabaseWithNewNameTest() {
        Tuple catalogTuple = createNewCatalog();
        String catalogNameString = catalogTuple.getString(1);
        CatalogName catalogName = StoreConvertor.catalogName(projectId, catalogNameString);
        // step1: drop DB1, drop DB2
        String dbName1 = UUID.randomUUID().toString().toLowerCase();
        String droppedDBId1 = prepareDropDatabaseTest(catalogNameString, dbName1);
        DatabaseName dropDBName1 = StoreConvertor.databaseName(projectId, catalogNameString, dbName1);

        String dbName2 = UUID.randomUUID().toString().toLowerCase();
        String droppedDBId2 = prepareDropDatabaseTest(catalogNameString, dbName2);

        // step2: undrop DB1 with newName;
        DatabaseObject dbRecordBefore = DatabaseObjectHelper.getDatabaseObject(dropDBName1);
        assertNull(dbRecordBefore);
        String dbNameNew = UUID.randomUUID().toString().toLowerCase();
        databaseService.undropDatabase(dropDBName1, droppedDBId1, dbNameNew);
        dropDBName1.setDatabaseName(dbNameNew);
        DatabaseObject dbRecordAfter = DatabaseObjectHelper.getDatabaseObject(dropDBName1);
        assertNotNull(dbRecordAfter);
        assertEquals(dbRecordAfter.getDatabaseId(), droppedDBId1);
        assertEquals(dbRecordAfter.getName(), dbNameNew);

        // step3: list undropped databases, expect get DB1;
        TraverseCursorResult<List<Database>> undroppedDBs = databaseService.listDatabases(catalogName, false,
            1000, "", "");
        assertEquals(1, undroppedDBs.getResult().size());
        assertEquals(catalogNameString, undroppedDBs.getResult().get(0).getCatalogName());
        assertEquals(dbNameNew, undroppedDBs.getResult().get(0).getDatabaseName());

        // step4: list all databases including the dropped one
        TraverseCursorResult<List<Database>> allDBs = databaseService.listDatabases(catalogName, true,
            1000, "", "");
        assertEquals(2, allDBs.getResult().size());
        // Use Dropped Flag to sort in ascending order, expect DB1(normal)--DB2(dropped)
        allDBs.getResult().sort(Comparator.comparing(Database::getDroppedTime));
        assertEquals(catalogNameString, allDBs.getResult().get(0).getCatalogName());
        assertEquals(dbNameNew, allDBs.getResult().get(0).getDatabaseName());
        assertEquals(0, allDBs.getResult().get(0).getDroppedTime(), "the new database is dropped");


        assertEquals(catalogNameString, allDBs.getResult().get(1).getCatalogName());
        assertEquals(dbName2, allDBs.getResult().get(1).getDatabaseName());
        assertNotEquals(0, allDBs.getResult().get(1).getDescription(), "database2 is not dropped");

    }

    @Test
    public void undropDatabaseOnBranchTest() {
        // create master catalog
        String catalogNameString = "main_catalog_name1";
        CatalogInput mainCatalogInput = getCatalogInput(catalogNameString);
        Catalog catalog = catalogService.createCatalog(projectId, mainCatalogInput);
        assertNotNull(catalog);

        // step1: drop DB1, drop DB2
        String dbName1 = "main_db_name1";
        String droppedDBId1 = prepareDropDatabaseTest(catalogNameString, dbName1);

        String dbName2 = "main_db_name2";

        // step2: create branch
        String subCatalogName = "sub_catalog_name1";
        CatalogName parentCatalogIdent = StoreConvertor.catalogName(projectId, catalogNameString);
        Catalog subBranchCatalog = createSubCatalogPrepareTest(subCatalogName, parentCatalogIdent);

        // step3: branch: undrop DB1(001) rename DB2(001);
        DatabaseName dropDBNameOnBranch = StoreConvertor.databaseName(projectId, subCatalogName, dbName1);
        DatabaseObject dbRecordBefore1 = DatabaseObjectHelper.getDatabaseObject(dropDBNameOnBranch);
        assertNull(dbRecordBefore1);
        databaseService.undropDatabase(dropDBNameOnBranch, "", dbName2);
        dropDBNameOnBranch.setDatabaseName(dbName2);
        DatabaseObject dbRecordAfter1 = DatabaseObjectHelper.getDatabaseObject(dropDBNameOnBranch);
        assertNotNull(dbRecordAfter1);
        assertEquals(dbRecordAfter1.getDatabaseId(), droppedDBId1);
        assertEquals(dbRecordAfter1.getName(), dbName2);
    }

    @Test
    public void undropDatabaseOnBranchTest1() {
        // create master catalog
        Tuple catalogTuple = createNewCatalog();
        String catalogNameString = catalogTuple.getString(1);
        CatalogName catalogNameObj = StoreConvertor.catalogName(projectId, catalogNameString);
        // step1: drop DB1, drop DB2
        String dbName1 = UUID.randomUUID().toString().toLowerCase();
        String droppedDBId1 = prepareDropDatabaseTest(catalogNameString, dbName1);
        DatabaseName dropDBName1 = StoreConvertor.databaseName(projectId, catalogNameString, dbName1);

        String dbName2 = UUID.randomUUID().toString().toLowerCase();
        String droppedDBId2 = prepareDropDatabaseTest(catalogNameString, dbName2);

        // step2: create branch
        String subCatalogName = UUID.randomUUID().toString().toLowerCase();
        CatalogName parentCatalogName = StoreConvertor.catalogName(catalogTuple.getString(0), catalogNameString);
        Catalog subBranchCatalog = createSubCatalogPrepareTest(subCatalogName, parentCatalogName);

        // step3: master: undrop DB1(001) rename DB2(001);
        DatabaseObject dbRecordBefore1 = DatabaseObjectHelper.getDatabaseObject(dropDBName1);
        assertNull(dbRecordBefore1);
        databaseService.undropDatabase(dropDBName1, null, dbName2);
        dropDBName1.setDatabaseName(dbName2);
        DatabaseObject dbRecordAfter1 = DatabaseObjectHelper.getDatabaseObject(dropDBName1);
        assertNotNull(dbRecordAfter1);
        assertEquals(dbRecordAfter1.getDatabaseId(), droppedDBId1);
        assertEquals(dbRecordAfter1.getName(), dbName2);

        // step4 master: create database DB1(003);
        DatabaseInput dbDTO3 = getDatabaseDTO(projectId, catalogNameString, dbName1);
        Database db3 = databaseService.createDatabase(catalogNameObj, dbDTO3);
        assertNotNull(db3);

        // step5: branch: undrop DB2(002) rename DB3(002);
        DatabaseName dropDBNameOnBranch = StoreConvertor.databaseName(projectId, subCatalogName, dbName2);
        DatabaseObject dbRecordBefore2 = DatabaseObjectHelper.getDatabaseObject(dropDBNameOnBranch);
        assertNull(dbRecordBefore2);
        String dbName3 = UUID.randomUUID().toString().toLowerCase();
        databaseService.undropDatabase(dropDBNameOnBranch, null, dbName3);
        dropDBNameOnBranch.setDatabaseName(dbName3);
        DatabaseObject dbRecordAfter2 = DatabaseObjectHelper.getDatabaseObject(dropDBNameOnBranch);
        assertNotNull(dbRecordAfter2);
        assertEquals(dbRecordAfter2.getDatabaseId(), droppedDBId2);
        assertEquals(dbRecordAfter2.getName(), dbName3);
    }

    @Test
    public void renameDatabaseTest() {
        // Prepare: need to creat a new Catalog.
        // Create one database with dbName1, and then rename it with dbName2.
        Tuple catalogTuple = createNewCatalog();
        String catalogNameString = catalogTuple.getString(1);
        String dbName1 = UUID.randomUUID().toString().toLowerCase();
        DatabaseInput dbDTO1 = getDatabaseDTO(projectId, catalogNameString, dbName1);
        CatalogName catalogNameObj = StoreConvertor.catalogName(projectId, catalogNameString);
        Database db = databaseService.createDatabase(catalogNameObj, dbDTO1);
        assertNotNull(db);

        String dbName2 = UUID.randomUUID().toString().toLowerCase();
        DatabaseName currentDBFullName = StoreConvertor.databaseName(projectId, catalogNameString, dbName1);
        databaseService.renameDatabase(currentDBFullName, dbName2);
        currentDBFullName = StoreConvertor.databaseName(projectId, catalogNameString, dbName2);
        Database renameDB = databaseService.getDatabaseByName(currentDBFullName);
        assertEquals(renameDB.getCatalogName(), catalogNameString);
        assertEquals(renameDB.getDatabaseName(), dbName2);
    }

    private Catalog createSubCatalogPrepareTest(String subCatalogName, CatalogName parentCatalogName) {
        String version = VersionManagerHelper.getLatestVersionByName(parentCatalogName.getProjectId(),
            parentCatalogName.getCatalogName());
        CatalogInput catalogInput = getCatalogInput(subCatalogName, parentCatalogName.getCatalogName(), version);
        Catalog subBranchCatalog = catalogService.createCatalog(projectId, catalogInput);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(
            StoreConvertor.catalogName(projectId, subCatalogName));
        assertEquals(subBranchCatalogRecord.getCatalogName(), subCatalogName);
        assertEquals(subBranchCatalogRecord.getParentName(), parentCatalogName.getCatalogName());
        String subBranchVersion = subBranchCatalogRecord.getParentVersion();
        String parentBranchVersion = version;
        assertEquals(subBranchVersion, parentBranchVersion);
        assertEquals(subBranchCatalogRecord.getCatalogName(), subBranchCatalog.getCatalogName());

        return subBranchCatalog;
    }

    private List<Database> createDatabasePrepareTest(CatalogName catalogName, int databaseNum) {

        List<Database> databaseList = new ArrayList<>(databaseNum);

        //create main branch table
        for (int i = 0; i < databaseNum; i++) {

            String databaseName = UUID.randomUUID().toString().toLowerCase();

            DatabaseInput databaseInput = getDatabaseDTO(catalogName.getProjectId(), catalogName.getCatalogName(),
                databaseName);
            Database createDB = databaseService.createDatabase(catalogName, databaseInput);
            assertNotNull(createDB);
            assertEquals(createDB.getDatabaseName(), databaseName);

            databaseList.add(createDB);
        }
        return databaseList;
    }

    private List<Database> createDatabasePrepareTestWithLmsName(CatalogName catalogName,
        int databaseNum) {

        List<Database> databaseList = new ArrayList<>(databaseNum);

        //create main branch table
        for (int i = 0; i < databaseNum; i++) {

            String databaseName = UUID.randomUUID().toString().toLowerCase();

            DatabaseInput databaseInput = getDatabaseDTO(catalogName.getProjectId(), catalogName.getCatalogName(),
                databaseName);
            buildLmsNameProperties(databaseInput, catalogName.getCatalogName());
            Database createDB = databaseService.createDatabase(catalogName, databaseInput);
            assertNotNull(createDB);
            assertEquals(createDB.getDatabaseName(), databaseName);

            databaseList.add(createDB);
        }
        return databaseList;
    }

    private List<Database> prepareOneLevelBranchTest(String mainBranchCatalogName, String subBranchCatalogName,
        int mainBranchDatabaseNum) {
        //create main branch
        Catalog mainCatalog = createCatalogPrepareTest(projectId, mainBranchCatalogName);

        //create main branch database
        CatalogName mainCatalogName = StoreConvertor
            .catalogName(projectId, mainCatalog.getCatalogName());
        List<Database> mainDatabaseList = createDatabasePrepareTest(mainCatalogName,
            mainBranchDatabaseNum);
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
        assertEquals(subBranchCatalogRecord.getCatalogName(), subBranchCatalog.getCatalogName());

        return mainDatabaseList;
    }

    private List<Database> prepareOneLevelBranchTestWithLmsName(String mainBranchCatalogName,
        String subBranchCatalogName,
        int mainBranchDatabaseNum) {
        //create main branch
        Catalog mainCatalog = createCatalogPrepareTest(projectId, mainBranchCatalogName);

        //create main branch database
        CatalogName mainCatalogName = StoreConvertor.catalogName(projectId, mainCatalog.getCatalogName());
        List<Database> mainDatabaseList = createDatabasePrepareTestWithLmsName(mainCatalogName,
            mainBranchDatabaseNum);

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
        assertEquals(subBranchCatalogRecord.getCatalogName(), subBranchCatalog.getCatalogName());

        return mainDatabaseList;
    }

    private String prepareDropDatabaseTest(String catalogName, String databseName) {
        DatabaseInput dbDTO = getDatabaseDTO(projectId, catalogName, databseName);
        CatalogName catalogNameObj = StoreConvertor.catalogName(projectId, catalogName);
        Database db = databaseService.createDatabase(catalogNameObj, dbDTO);
        assertNotNull(db);

        DatabaseName dropDBName = StoreConvertor.databaseName(projectId, catalogName, databseName);
        String droppedDBId = databaseService.dropDatabase(dropDBName, "false");

        return droppedDBId;
    }

    @Test
    public void listDatabaseSubBranchTest() {
        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        int mainBranchDatabaseNum = 2;

        //create sub-branch database
        List<Database> mainBranchDatabaseList = prepareOneLevelBranchTest(mainBranchCatalogName, subBranchCatalogName,
            mainBranchDatabaseNum);

        //get main catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        //list sub-branch
        int maxNum = 1000;
        TraverseCursorResult<List<Database>> subBranchDatabaseList = databaseService
            .listDatabases(subCatalogPathName, true, maxNum,
                "", "");
        assertEquals(subBranchDatabaseList.getResult().size(), mainBranchDatabaseNum);

        Set<String> mainBranchDatabases = mainBranchDatabaseList.stream().map(Database::getDatabaseName)
            .collect(Collectors.toSet());

        for (Database database : subBranchDatabaseList.getResult()) {
            assertEquals(database.getCatalogName(), subBranchCatalogRecord.getCatalogName());
            assertTrue(mainBranchDatabases.contains(database.getDatabaseName()));
        }
    }

    @Test
    public void listDatabaseAfterCreateDatabaseTest() {

        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();

        int mainBranchDatabaseNum = 2;

        //create sub-branch database
        List<Database> mainBranchDatabaseList = prepareOneLevelBranchTest(mainBranchCatalogName, subBranchCatalogName,
            mainBranchDatabaseNum);

        //get main catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        //create database at main-branch
        int mainBranchDatabaseNumNew = 2;
        List<Database> mainBranchDatabaseListNew = createDatabasePrepareTest(mainCatalogPathName,
            mainBranchDatabaseNumNew);

        //list sub-branch
        int maxNum = 1000;

        TraverseCursorResult<List<Database>> subBranchDatabaseList = databaseService
            .listDatabases(subCatalogPathName, true, maxNum,
                "", "");
        assertEquals(subBranchDatabaseList.getResult().size(), mainBranchDatabaseNum);

        Set<String> mainBranchDatabases = mainBranchDatabaseList.stream().map(Database::getDatabaseName)
            .collect(Collectors.toSet());

        for (Database database : subBranchDatabaseList.getResult()) {
            assertEquals(database.getCatalogName(), subBranchCatalogRecord.getCatalogName());
            assertTrue(mainBranchDatabases.contains(database.getDatabaseName()));
        }

        //list parent-branch
        TraverseCursorResult<List<Database>> parentBranchDatabaseList = databaseService
            .listDatabases(mainCatalogPathName, true, maxNum,
                "", "");
        assertEquals(parentBranchDatabaseList.getResult().size(), mainBranchDatabaseNum + mainBranchDatabaseNumNew);

        mainBranchDatabaseList.addAll(mainBranchDatabaseListNew);

        Set<String> parentBranchDatabases = mainBranchDatabaseList.stream().map(Database::getDatabaseName)
            .collect(Collectors.toSet());
        for (Database database : parentBranchDatabaseList.getResult()) {

            assertEquals(database.getCatalogName(), mainBranchCatalogRecord.getCatalogName());

            assertTrue(parentBranchDatabases.contains(database.getDatabaseName()));

        }

    }

    @Test
    public void createDatabaseSubBranchTest() {

        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();

        int mainBranchDatabaseNum = 2;

        //create sub-branch database
        List<Database> mainBranchDatabaseList = prepareOneLevelBranchTest(mainBranchCatalogName, subBranchCatalogName,
            mainBranchDatabaseNum);

        //get main catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        //create database at sub-branch
        int subBranchDatabaseNumNew = 2;
        List<Database> subBranchDatabaseListNew = createDatabasePrepareTest(subCatalogPathName,
            subBranchDatabaseNumNew);

        //list sub-branch
        int maxNum = 1000;

        TraverseCursorResult<List<Database>> subBranchDatabaseList = databaseService
            .listDatabases(subCatalogPathName, true, maxNum, "", "");
        assertEquals(subBranchDatabaseList.getResult().size(), mainBranchDatabaseNum + subBranchDatabaseNumNew);

        subBranchDatabaseListNew.addAll(mainBranchDatabaseList);

        assertSameBranch(subBranchCatalogRecord.getCatalogName(), subBranchDatabaseList, subBranchDatabaseListNew);
//        Map<String, String> subBranchDatabases = subBranchDatabaseListNew.stream()
//            .collect(Collectors.toMap(Database::getDatabaseId, Database::getCatalogId));
//
//        for (Database database : subBranchDatabaseList.getResult()) {
//
//            assertEquals(database.getCatalogId(), subBranchCatalogRecord.getCatalogId());
//            assertEquals(database.getCatalogName(), subBranchCatalogRecord.getCatalogName());
//
//            assertTrue(subBranchDatabases.containsKey(database.getDatabaseId()));
//
//        }

        //list parent-branch
        TraverseCursorResult<List<Database>> parentBranchDatabaseList = databaseService
            .listDatabases(mainCatalogPathName, true, maxNum,
                "", "");
        assertEquals(parentBranchDatabaseList.getResult().size(), mainBranchDatabaseNum);

        assertSameBranch(mainBranchCatalogRecord.getCatalogName(), parentBranchDatabaseList, mainBranchDatabaseList);

//        Map<String, String> parentBranchDatabases = mainBranchDatabaseList.stream()
//            .collect(Collectors.toMap(Database::getDatabaseId, Database::getCatalogId));
//
//        for (Database database : parentBranchDatabaseList.getResult()) {
//
//            assertEquals(database.getCatalogId(), mainBranchCatalogRecord.getCatalogId());
//            assertEquals(database.getCatalogName(), mainBranchCatalogRecord.getCatalogName());
//
//            assertTrue(parentBranchDatabases.containsKey(database.getDatabaseId()));
//
//        }
    }

    @Test
    public void createDatabaseSubBranchWithSameNameTest() {

        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();

        int mainBranchDatabaseNum = 2;

        //create sub-branch database
        List<Database> mainBranchDatabaseList = prepareOneLevelBranchTest(mainBranchCatalogName, subBranchCatalogName,
            mainBranchDatabaseNum);

        //get main catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        //create database at sub-branch
        String databaseNameNew = mainBranchDatabaseList.get(0).getDatabaseName();
        DatabaseInput databaseInput = getDatabaseDTO(projectId, subBranchCatalogName, databaseNameNew);

        CatalogServerException e = assertThrows(CatalogServerException.class,
            () -> databaseService.createDatabase(subCatalogPathName, databaseInput));
        assertEquals(e.getErrorCode(), ErrorCode.DATABASE_ALREADY_EXIST);
    }

    @Test
    public void getSubBranchDatabaseHistoryTest() {

        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();

        int mainBranchDatabaseNum = 2;

        //create sub-branch database
        List<Database> mainBranchDatabaseList = prepareOneLevelBranchTest(mainBranchCatalogName, subBranchCatalogName,
            mainBranchDatabaseNum);

        //get main catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        String subDatabaseName = mainBranchDatabaseList.get(0).getDatabaseName();
        DatabaseName databaseName = new DatabaseName(projectId, subBranchCatalogName, subDatabaseName);

        List<DatabaseHistory> dbHistories1 = databaseService.getDatabaseHistory(databaseName);
        assertNotNull(dbHistories1);

        // rename database to newName1
        String databaseNameNew1 = UUID.randomUUID().toString().toLowerCase();
        DatabaseName subBranchDatabasePathName = StoreConvertor.databaseName(subCatalogPathName.getProjectId(),
            subCatalogPathName.getCatalogName(), subDatabaseName);
        databaseService.renameDatabase(subBranchDatabasePathName, databaseNameNew1);
        DatabaseName currentDatabaseName = StoreConvertor.databaseName(subCatalogPathName.getProjectId(),
            subCatalogPathName.getCatalogName(), databaseNameNew1);
        Database renamedDB1 = databaseService.getDatabaseByName(currentDatabaseName);
        assertNotNull(renamedDB1);
        assertEquals(renamedDB1.getCatalogName(), subBranchDatabasePathName.getCatalogName());
        assertEquals(renamedDB1.getDatabaseName(), databaseNameNew1);

        // rename database to newName2
        String databaseNameNew2 = UUID.randomUUID().toString().toLowerCase();
        DatabaseName subBranchDatabasePathName1 = StoreConvertor.databaseName(subCatalogPathName.getProjectId(),
            subCatalogPathName.getCatalogName(), databaseNameNew1);
        databaseService.renameDatabase(subBranchDatabasePathName1, databaseNameNew2);
        currentDatabaseName = StoreConvertor.databaseName(subCatalogPathName.getProjectId(),
            subCatalogPathName.getCatalogName(), databaseNameNew2);
        Database renamedDB2 = databaseService.getDatabaseByName(currentDatabaseName);
        assertNotNull(renamedDB2);
        assertEquals(renamedDB2.getDatabaseName(), databaseNameNew2);

        // get database history
        List<DatabaseHistory> dbHistories = databaseService.getDatabaseHistory(currentDatabaseName);
        assertNotNull(dbHistories);
        assertEquals(dbHistories.size(), 3);

        assertEquals(dbHistories.get(0).getName(), databaseNameNew2);

        // get database by version
        String version = dbHistories.get(0).getVersion();
        DatabaseHistory dbhist = databaseService.getDatabaseByVersion(currentDatabaseName, version);
        assertNotNull(dbhist);
    }

    @Test
    public void dropDatabaseSubBranch1Test() {

        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();

        int mainBranchDatabaseNum = 2;

        //create sub-branch database
        List<Database> mainBranchDatabaseList = prepareOneLevelBranchTest(mainBranchCatalogName, subBranchCatalogName,
            mainBranchDatabaseNum);

        //get main catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        String subDatabaseName = mainBranchDatabaseList.get(0).getDatabaseName();

        //drop database at sub-branch
        DatabaseName dropDBName = StoreConvertor.databaseName(projectId, subBranchCatalogName, subDatabaseName);
        databaseService.dropDatabase(dropDBName, "false");

        //list sub-branch
        int maxNum = 1000;
        TraverseCursorResult<List<Database>> subBranchDatabaseList = databaseService
            .listDatabases(subCatalogPathName, true, maxNum,
                "", "");
        assertEquals(subBranchDatabaseList.getResult().size(), 2);

        subBranchDatabaseList = databaseService.listDatabases(subCatalogPathName, false, maxNum,
            "", "");
        assertEquals(subBranchDatabaseList.getResult().size(), 1);

        Set<String> subBranchDatabases = mainBranchDatabaseList.stream().map(Database::getDatabaseName)
            .collect(Collectors.toSet());

        for (Database database : subBranchDatabaseList.getResult()) {
            assertEquals(database.getCatalogName(), subBranchCatalogRecord.getCatalogName());
            assertTrue(subBranchDatabases.contains(database.getDatabaseName()));
        }

        //list parent-branch
        TraverseCursorResult<List<Database>> parentBranchDatabaseList = databaseService
            .listDatabases(mainCatalogPathName, true, maxNum,
                "", "");
        assertEquals(parentBranchDatabaseList.getResult().size(), mainBranchDatabaseNum);

        assertSameBranch(mainBranchCatalogRecord.getCatalogName(), parentBranchDatabaseList, mainBranchDatabaseList);
    }

    @Test
    public void dropDatabaseSubBranch2Test() {

        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();

        int mainBranchDatabaseNum = 2;

        //create sub-branch database
        List<Database> mainBranchDatabaseList = prepareOneLevelBranchTest(mainBranchCatalogName, subBranchCatalogName,
            mainBranchDatabaseNum);

        //get main catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        String subDatabaseName = mainBranchDatabaseList.get(0).getDatabaseName();

        //drop database at parent-branch
        DatabaseName dropDBName = StoreConvertor.databaseName(projectId, mainBranchCatalogName, subDatabaseName);
        String droppedDBId = databaseService.dropDatabase(dropDBName, "false");
//        assertEquals(subDatabaseId, droppedDBId);

        //list sub-branch
        int maxNum = 1000;

        TraverseCursorResult<List<Database>> subBranchDatabaseList = databaseService
            .listDatabases(subCatalogPathName, true, maxNum,
                "", "");
        assertEquals(subBranchDatabaseList.getResult().size(), mainBranchDatabaseNum);

        assertSameBranch(subBranchCatalogRecord.getCatalogName(), subBranchDatabaseList, mainBranchDatabaseList);

//        Map<String, String> subBranchDatabases = mainBranchDatabaseList.stream()
//            .collect(Collectors.toMap(Database::getDatabaseId, Database::getCatalogId));
//
//        for (Database database : subBranchDatabaseList.getResult()) {
//            assertEquals(database.getCatalogId(), subBranchCatalogRecord.getCatalogId());
//            assertEquals(database.getCatalogName(), subBranchCatalogRecord.getCatalogName());
//            assertTrue(subBranchDatabases.containsKey(database.getDatabaseId()));
//
//        }

        //list parent-branch
        TraverseCursorResult<List<Database>> parentBranchDatabaseList = databaseService
            .listDatabases(mainCatalogPathName, true, maxNum,
                "", "");
        assertEquals(parentBranchDatabaseList.getResult().size(), 2);

        parentBranchDatabaseList = databaseService.listDatabases(mainCatalogPathName, false, maxNum,
            "", "");
        assertEquals(parentBranchDatabaseList.getResult().size(), 1);

        assertSameBranch(mainBranchCatalogRecord.getCatalogName(), parentBranchDatabaseList, mainBranchDatabaseList);
//
//        Map<String, String> parentBranchDatabases = mainBranchDatabaseList.stream()
//            .collect(Collectors.toMap(Database::getDatabaseId, Database::getCatalogId));
//
//        for (Database database : parentBranchDatabaseList.getResult()) {
//            assertEquals(database.getCatalogId(), mainBranchCatalogRecord.getCatalogId());
//            assertEquals(database.getCatalogName(), mainBranchCatalogRecord.getCatalogName());
//            assertTrue(parentBranchDatabases.containsKey(database.getDatabaseId()));
//        }
    }

    @Test
    public void alterDatabaseSubBranch1Test() {
        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();

        int mainBranchDatabaseNum = 2;

        //create sub-branch database
        List<Database> mainBranchDatabaseList = prepareOneLevelBranchTest(mainBranchCatalogName, subBranchCatalogName,
            mainBranchDatabaseNum);

        //get main catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        //get database with old name at parent-branch
        DatabaseName mainDatabasePathName = StoreConvertor.databaseName(projectId,
            mainBranchCatalogRecord.getCatalogName(), mainBranchDatabaseList.get(0).getDatabaseName());
        Database mainDatabase0 = databaseService.getDatabaseByName(mainDatabasePathName);
        assertNotNull(mainDatabase0);

        String subDatabaseName = mainBranchDatabaseList.get(0).getDatabaseName();

        //alter database name at sub-branch
        String dbNameNew = UUID.randomUUID().toString().toLowerCase();
        DatabaseInput dbDTO2 = getDatabaseDTO(projectId, subBranchCatalogName, dbNameNew);
        DatabaseName currentDBFullName = StoreConvertor
            .databaseName(projectId, subBranchCatalogRecord.getCatalogName(), subDatabaseName);
        databaseService.alterDatabase(currentDBFullName, dbDTO2);
        DatabaseName currentDatabaseName = StoreConvertor.databaseName(projectId, subBranchCatalogRecord.getCatalogName(), dbNameNew);
        Database renameDB = databaseService.getDatabaseByName(currentDatabaseName);
        assertEquals(renameDB.getCatalogName(), subBranchCatalogRecord.getCatalogName());
        assertEquals(renameDB.getDatabaseName(), dbNameNew);

        //get database with old name at sub-branch
        DatabaseName subDatabasePathName = StoreConvertor.databaseName(projectId,
            subBranchCatalogRecord.getCatalogName(), subDatabaseName);
        Database subDatabase = databaseService.getDatabaseByName(subDatabasePathName);
        assertNull(subDatabase);

        //get database with old name at parent-branch
        Database mainDatabase = databaseService.getDatabaseByName(mainDatabasePathName);
        assertNotNull(mainDatabase);
        assertEquals(mainDatabase.getCatalogName(), mainBranchCatalogRecord.getCatalogName());
        assertEquals(mainDatabase.getDatabaseName(), mainBranchDatabaseList.get(0).getDatabaseName());

    }

    @Test
    public void alterDatabaseSubBranch2Test() {
        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();

        int mainBranchDatabaseNum = 2;

        //create sub-branch database
        List<Database> mainBranchDatabaseList = prepareOneLevelBranchTest(mainBranchCatalogName, subBranchCatalogName,
            mainBranchDatabaseNum);

        //get main catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        //alter database name at parent-branch
        String dbNameNew = UUID.randomUUID().toString().toLowerCase();
        DatabaseInput dbDTO2 = getDatabaseDTO(projectId, mainBranchCatalogRecord.getCatalogName(), dbNameNew);
        DatabaseName currentDBFullName = StoreConvertor.databaseName(projectId, mainBranchCatalogRecord.getCatalogName(),
            mainBranchDatabaseList.get(0).getDatabaseName());
        databaseService.alterDatabase(currentDBFullName, dbDTO2);
        currentDBFullName = StoreConvertor.databaseName(projectId, mainBranchCatalogRecord.getCatalogName(),
            dbNameNew);
        Database renameDB = databaseService.getDatabaseByName(currentDBFullName);
        assertEquals(renameDB.getCatalogName(), mainBranchCatalogRecord.getCatalogName());
        assertEquals(renameDB.getDatabaseName(), dbNameNew);

        //get database with old name at sub-branch
        String subDatabaseName = mainBranchDatabaseList.get(0).getDatabaseName();
        String subBranchDatabaseName = mainBranchDatabaseList.get(0).getDatabaseName();
        DatabaseName subDatabasePathName = StoreConvertor.databaseName(projectId,
            subBranchCatalogRecord.getCatalogName(), subDatabaseName);
        Database subDatabase = databaseService.getDatabaseByName(subDatabasePathName);
        assertNotNull(subDatabase);
        assertEquals(subDatabase.getCatalogName(), subBranchCatalogRecord.getCatalogName());
        assertEquals(subDatabase.getDatabaseName(), subBranchDatabaseName);

        //get database with old name at parent-branch
        DatabaseName mainDatabasePathName = StoreConvertor.databaseName(projectId,
            mainBranchCatalogRecord.getCatalogName(), mainBranchDatabaseList.get(0).getDatabaseName());
        Database mainDatabase = databaseService.getDatabaseByName(mainDatabasePathName);
        assertNull(mainDatabase);
    }

    private void listDatabaseAndCheckResult(int maxBatchNum, CatalogName catalogName, int checkNum,
        Boolean includeDropped) {
        for (int i = 1; i <= maxBatchNum; i++) {
            List<Database> databaseList = new ArrayList<>();
            String nextToken = "";

            while (true) {
                TraverseCursorResult<List<Database>> branchDatabaseList =
                    databaseService.listDatabases(catalogName, includeDropped, i, nextToken, "");
                databaseList.addAll(branchDatabaseList.getResult());

                if (branchDatabaseList.getResult().size() > 0) {
                    nextToken = branchDatabaseList.getContinuation().map(catalogToken -> {
                        return catalogToken.toString();
                    }).orElse(null);
                    if (nextToken == null) {
                        break;
                    }
                }
                if (branchDatabaseList.getResult().size() == 0) {
                    break;
                }
            }

            assertEquals(databaseList.size(), checkNum);
        }
    }

    private void listDatabaseAndCheckResult(int maxBatchNum, CatalogName catalogName, int checkNum,
        Boolean includeDropped, List<Database> branchValidDatabaseList) {
        Set<String> mainBranchDatabases = branchValidDatabaseList.stream().map(Database::getDatabaseName)
            .collect(Collectors.toSet());

        for (int i = 1; i <= maxBatchNum; i++) {
            List<Database> databaseList = new ArrayList<>();
            String nextToken = "";
            TraverseCursorResult<List<Database>> branchDatabaseList;
            while (true) {
                branchDatabaseList =
                    databaseService.listDatabases(catalogName, includeDropped, i, nextToken, "");
                databaseList.addAll(branchDatabaseList.getResult());

                if (branchDatabaseList.getResult().size() > 0) {
                    nextToken = branchDatabaseList.getContinuation().map(CatalogToken::toString).orElse(null);
                    if (nextToken == null) {
                        break;
                    }
                }
                if (branchDatabaseList.getResult().size() == 0) {
                    break;
                }
            }

            assertEquals(databaseList.size(), checkNum);

            for (Database database : branchDatabaseList.getResult()) {
                assertEquals(database.getCatalogName(), catalogName.getCatalogName());
                // dbName can alter, so don't check it
//                assertTrue(mainBranchDatabases.contains(database.getDatabaseName()));
            }
        }
    }

    @Test
    public void listDatabaseTokenTest1() {
        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        int mainBranchDatabaseNum = 7;

        //create sub-branch database
        List<Database> mainBranchDatabaseList = prepareOneLevelBranchTest(mainBranchCatalogName, subBranchCatalogName,
            mainBranchDatabaseNum);

        //get main catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        //list at sub branch
        listDatabaseAndCheckResult(10, subCatalogPathName, mainBranchDatabaseNum, true,
            mainBranchDatabaseList);

        //drop db at main branch
        String dropDatabaseName = mainBranchDatabaseList.get(0).getDatabaseName();
        DatabaseName dropDBName = StoreConvertor.databaseName(projectId, mainBranchCatalogName, dropDatabaseName);
        String droppedDBId = databaseService.dropDatabase(dropDBName, "false");

        //list at sub branch
        listDatabaseAndCheckResult(10, subCatalogPathName, mainBranchDatabaseNum, true,
            mainBranchDatabaseList);
        listDatabaseAndCheckResult(10, subCatalogPathName, mainBranchDatabaseNum, false,
            mainBranchDatabaseList);

        //drop db at sub branch
        String dropDatabaseNameSub = mainBranchDatabaseList.get(0).getDatabaseName();
        DatabaseName dropDBNameSub = StoreConvertor.databaseName(projectId, subBranchCatalogName, dropDatabaseNameSub);
        String droppedDBIdSub = databaseService.dropDatabase(dropDBNameSub, "false");

        //list at sub branch
        listDatabaseAndCheckResult(10, subCatalogPathName, mainBranchDatabaseNum, true,
            mainBranchDatabaseList);
        listDatabaseAndCheckResult(10, subCatalogPathName, mainBranchDatabaseNum - 1, false,
            mainBranchDatabaseList);
    }

    @Test
    public void listDatabaseTokenTest2() {
        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        int mainBranchDatabaseNum = 7;

        //create main branch
        Catalog mainCatalog = createCatalogPrepareTest(projectId, mainBranchCatalogName);

        //create main branch database
        CatalogName mainCatalogName = StoreConvertor
            .catalogName(projectId, mainCatalog.getCatalogName());
        List<Database> mainBranchDatabaseList = createDatabasePrepareTest(mainCatalogName,
            mainBranchDatabaseNum);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        //drop db at main branch
        String dropDatabaseName = mainBranchDatabaseList.get(0).getDatabaseName();
        DatabaseName dropDBName = StoreConvertor.databaseName(projectId, mainBranchCatalogName, dropDatabaseName);
        String droppedDBId = databaseService.dropDatabase(dropDBName, "false");

        //create sub branch
        String version = VersionManagerHelper.getLatestVersionByName(projectId, mainCatalogName.getCatalogName());
        CatalogInput catalogInput = getCatalogInput(subBranchCatalogName, mainCatalogName.getCatalogName(), version);
        Catalog subBranchCatalog = catalogService.createCatalog(projectId, catalogInput);
        assertNotNull(subBranchCatalog);

        //alter db at sub branch
        String dbNameNew = UUID.randomUUID().toString().toLowerCase();
        String subDBAlterName = mainBranchDatabaseList.get(1).getDatabaseName();
        DatabaseInput dbDTO2 = getDatabaseDTO(projectId, subBranchCatalogName, dbNameNew);
        DatabaseName subBranchDatabasePathName = StoreConvertor
            .databaseName(projectId, subBranchCatalogName, subDBAlterName);
        databaseService.alterDatabase(subBranchDatabasePathName, dbDTO2);

        DatabaseName currentDBFullName = StoreConvertor.databaseName(projectId, subBranchCatalogName, dbNameNew);
        Database renameDB = databaseService.getDatabaseByName(currentDBFullName);
        assertNotNull(renameDB);

        //get sub catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        //list at sub branch
        listDatabaseAndCheckResult(10, subCatalogPathName, mainBranchDatabaseNum, true,
            mainBranchDatabaseList);
        listDatabaseAndCheckResult(10, subCatalogPathName, mainBranchDatabaseNum - 1, false,
            mainBranchDatabaseList);

        //drop db at sub branch
        String dropDatabaseNameSub = mainBranchDatabaseList.get(2).getDatabaseName();
        DatabaseName dropDBNameSub = StoreConvertor.databaseName(projectId, subBranchCatalogName, dropDatabaseNameSub);
        String droppedDBIdSub = databaseService.dropDatabase(dropDBNameSub, "false");

        //list at sub branch
        listDatabaseAndCheckResult(10, subCatalogPathName, mainBranchDatabaseNum, true,
            mainBranchDatabaseList);
        listDatabaseAndCheckResult(10, subCatalogPathName, mainBranchDatabaseNum - 2, false,
            mainBranchDatabaseList);
    }

    private DatabaseName createAlterDatabaseNameHistory(int num, DatabaseName databaseName) {
        DatabaseName latestDatabase = databaseName;
        for (int i = 0; i < num; i++) {
            //alter database name
            String dbName2 = UUID.randomUUID().toString().toLowerCase();
            DatabaseInput dbDTO2 = getDatabaseDTO(databaseName.getProjectId(), databaseName.getCatalogName(), dbName2);
            databaseService.alterDatabase(latestDatabase, dbDTO2);
            DatabaseName currentDBFullName = StoreConvertor.databaseName(databaseName.getProjectId(),
            databaseName.getCatalogName(), dbName2);
            Database renameDB  = databaseService.getDatabaseByName(currentDBFullName);
            assertNotNull(renameDB);
            latestDatabase = StoreConvertor.databaseName(databaseName.getProjectId(),
                databaseName.getCatalogName(), renameDB.getDatabaseName());
        }

        return latestDatabase;
    }

    private void listDatabaseHistoryAndCheckResult(int maxBatchNum, DatabaseName databaseName, int resultNum) {
        //list table commit in main branch
        for (int i = 1; i < maxBatchNum; i++) {
            List<DatabaseHistory> databaseHistoryList = new ArrayList<>();
            String nextToken = null;

            while (true) {
                TraverseCursorResult<List<DatabaseHistory>> subTableListBatch =
                    databaseService.listDatabaseHistory(databaseName, i, nextToken);
                databaseHistoryList.addAll(subTableListBatch.getResult());

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

            assertEquals(databaseHistoryList.size(), resultNum);
        }
    }

    @Test
    public void listDatabaseHistoryTokenTest() {
        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        int mainBranchDatabaseNum = 1;
        int alterNameNum = 10;

        //create sub-branch database
        List<Database> mainBranchDatabaseList = prepareOneLevelBranchTest(mainBranchCatalogName, subBranchCatalogName,
            mainBranchDatabaseNum);

        //get main catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        //alter db name at main
        DatabaseName mainDatabaseName = StoreConvertor.databaseName(projectId,
            mainBranchDatabaseList.get(0).getCatalogName(), mainBranchDatabaseList.get(0).getDatabaseName());
        DatabaseName latestMainDatabaseName = createAlterDatabaseNameHistory(alterNameNum,
            mainDatabaseName);

        //list db history at main
        listDatabaseHistoryAndCheckResult(12, latestMainDatabaseName, alterNameNum + 1);

        //alter db name at sub-branch
        DatabaseName subDatabaseName = StoreConvertor.databaseName(projectId,
            subBranchCatalogRecord.getCatalogName(), mainBranchDatabaseList.get(0).getDatabaseName());
        DatabaseName latestSubDatabaseName = createAlterDatabaseNameHistory(alterNameNum,
            subDatabaseName);

        //list db history at main
        listDatabaseHistoryAndCheckResult(12, latestSubDatabaseName, alterNameNum + 1);

    }

    private void buildLmsNameProperties(DatabaseInput databaseInput, String lms_name) {
        if (databaseInput.getParameters() == null) {
            Map<String, String> map = new HashMap<>();
            map.put("lms_name", lms_name);
            databaseInput.setParameters(map);
        } else {
            databaseInput.getParameters().put("lms_name", lms_name);
        }
    }

    @Test
    public void databaseMapWithUnDropTest() {
        String dbName1 = UUID.randomUUID().toString().toLowerCase();
        DatabaseInput dbDTO1 = getDatabaseDTO(projectId, catalogNameString, dbName1);
        CatalogName catalogNameObj = StoreConvertor.catalogName(projectId, catalogNameString);
        buildLmsNameProperties(dbDTO1, catalogNameString);
        Database db1 = databaseService.createDatabase(catalogNameObj, dbDTO1);
        assertNotNull(db1);

        Optional<MetaObjectName> metaObjectName = objectNameMapService
            .getObjectFromNameMap(projectId, ObjectType.DATABASE.name(), dbName1);
        assertTrue(metaObjectName.isPresent());
        assertEquals(metaObjectName.get().getObjectName(), "");
        assertEquals(metaObjectName.get().getDatabaseName(), dbName1);
        assertEquals(metaObjectName.get().getCatalogName(), catalogNameString);
        assertEquals(metaObjectName.get().getProjectId(), projectId);

        //drop db
        DatabaseName dropDBName = StoreConvertor.databaseName(projectId, catalogNameString, dbName1);
        String droppedDBId = databaseService.dropDatabase(dropDBName, "false");

        metaObjectName = objectNameMapService.getObjectFromNameMap(projectId, ObjectType.DATABASE.name(), dbName1);
        assertFalse(metaObjectName.isPresent());

        //undrop db
        databaseService.undropDatabase(dropDBName, null, "");
        metaObjectName = objectNameMapService.getObjectFromNameMap(projectId, ObjectType.DATABASE.name(), dbName1);
        assertTrue(metaObjectName.isPresent());

    }

    @Test
    public void databaseMapWithAlterTest() {
        String dbName1 = UUID.randomUUID().toString().toLowerCase();
        DatabaseInput dbDTO1 = getDatabaseDTO(projectId, catalogNameString, dbName1);
        CatalogName catalogNameObj = StoreConvertor.catalogName(projectId, catalogNameString);
        buildLmsNameProperties(dbDTO1, catalogNameString);
        Database db1 = databaseService.createDatabase(catalogNameObj, dbDTO1);
        assertNotNull(db1);

        Optional<MetaObjectName> metaObjectName = objectNameMapService
            .getObjectFromNameMap(projectId, ObjectType.DATABASE.name(), dbName1);
        assertTrue(metaObjectName.isPresent());

        //alter db name
        String dbName2 = UUID.randomUUID().toString().toLowerCase();
        DatabaseInput dbDTO2 = getDatabaseDTO(projectId, catalogNameString, dbName2);
        DatabaseName currentDBFullName = StoreConvertor.databaseName(projectId, catalogNameString, dbName1);
        databaseService.alterDatabase(currentDBFullName, dbDTO2);
        currentDBFullName = StoreConvertor.databaseName(projectId, catalogNameString, dbName2);
        Database renameDB = databaseService.getDatabaseByName(currentDBFullName);

        Optional<MetaObjectName> databaseMap = objectNameMapService
            .getObjectFromNameMap(projectId, ObjectType.DATABASE.name(), dbName2);
        assertEquals(databaseMap.get().getDatabaseName(), dbName2);
        assertEquals(databaseMap.get().getCatalogName(), catalogNameString);
        assertEquals(databaseMap.get().getProjectId(), projectId);

        databaseMap = objectNameMapService.getObjectFromNameMap(projectId, ObjectType.DATABASE.name(), dbName1);
        assertFalse(databaseMap.isPresent());
    }

    @Test
    public void databaseMapWithAlterBranch() {
        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        int mainBranchDatabaseNum = 1;

        //create sub-branch database
        List<Database> mainBranchDatabaseList = prepareOneLevelBranchTestWithLmsName(mainBranchCatalogName,
            subBranchCatalogName, mainBranchDatabaseNum);

        //get main catalog, database
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        Catalog mainBranchCatalogRecord = catalogService.getCatalog(mainCatalogPathName);
        assertNotNull(mainBranchCatalogRecord);

        String mainBranchDatabaseName = mainBranchDatabaseList.get(0).getDatabaseName();

        Optional<MetaObjectName> databaseMap = objectNameMapService
            .getObjectFromNameMap(projectId, ObjectType.DATABASE.name(), mainBranchDatabaseName);
        assertEquals(databaseMap.get().getObjectName(), "");
        assertEquals(databaseMap.get().getDatabaseName(), mainBranchDatabaseName);
        assertEquals(databaseMap.get().getCatalogName(), mainBranchCatalogName);
        assertEquals(databaseMap.get().getProjectId(), projectId);

        //alter table and set map
        DatabaseInput dbDTO2 = getDatabaseDTO(projectId, subBranchCatalogName, mainBranchDatabaseName);
        buildLmsNameProperties(dbDTO2, subBranchCatalogName);
        DatabaseName currentDBFullName = StoreConvertor
            .databaseName(projectId, mainBranchCatalogName, mainBranchDatabaseName);
        databaseService.alterDatabase(currentDBFullName, dbDTO2);
        currentDBFullName = StoreConvertor
            .databaseName(projectId, mainBranchCatalogName, dbDTO2.getDatabaseName());
        Database renameDB = databaseService.getDatabaseByName(currentDBFullName);
        assertNotNull(renameDB);

        databaseMap = objectNameMapService
            .getObjectFromNameMap(projectId, ObjectType.DATABASE.name(), mainBranchDatabaseName);
        assertEquals(databaseMap.get().getObjectName(), "");
        assertEquals(databaseMap.get().getDatabaseName(), mainBranchDatabaseName);
        assertEquals(databaseMap.get().getCatalogName(), subBranchCatalogName);
        assertEquals(databaseMap.get().getProjectId(), projectId);
    }

}
