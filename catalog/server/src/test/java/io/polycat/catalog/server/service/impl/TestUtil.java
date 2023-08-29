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

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordVersion;
import com.apple.foundationdb.tuple.Tuple;
import io.polycat.catalog.common.model.Catalog;
import io.polycat.catalog.common.model.CatalogName;
import io.polycat.catalog.common.model.Database;
import io.polycat.catalog.common.model.DatabaseHistoryObject;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.StorageDescriptor;
import io.polycat.catalog.common.model.Table;
import io.polycat.catalog.common.model.TableName;
import io.polycat.catalog.common.model.base.PartitionInput;
import io.polycat.catalog.common.plugin.request.input.CatalogInput;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;
import io.polycat.catalog.common.plugin.request.input.FileInput;
import io.polycat.catalog.common.plugin.request.input.FileStatsInput;
import io.polycat.catalog.common.plugin.request.input.AddPartitionInput;
import io.polycat.catalog.common.plugin.request.input.TableInput;
import io.polycat.catalog.common.model.Column;
import io.polycat.catalog.common.types.DataTypes;
import io.polycat.catalog.common.utils.CodecUtil;
import io.polycat.catalog.common.utils.UuidUtil;
import io.polycat.catalog.common.utils.PartitionUtil;

import io.polycat.catalog.service.api.CatalogResourceService;
import io.polycat.catalog.service.api.CatalogService;
import io.polycat.catalog.service.api.DataLineageService;
import io.polycat.catalog.service.api.DatabaseService;
import io.polycat.catalog.service.api.DelegateService;
import io.polycat.catalog.service.api.FunctionService;
import io.polycat.catalog.service.api.GlobalShareService;
import io.polycat.catalog.service.api.NewRoleService;
import io.polycat.catalog.service.api.ObjectNameMapService;
import io.polycat.catalog.service.api.PartitionService;
import io.polycat.catalog.service.api.PolicyService;
import io.polycat.catalog.service.api.PrivilegeService;
import io.polycat.catalog.service.api.RoleService;
import io.polycat.catalog.service.api.ShareService;
import io.polycat.catalog.service.api.TableService;
import io.polycat.catalog.service.api.UsageProfileService;
import io.polycat.catalog.service.api.UserGroupService;
import io.polycat.catalog.service.api.ViewService;
import io.polycat.catalog.store.api.StoreBase;
import io.polycat.catalog.store.api.VersionManager;
import io.polycat.catalog.store.common.StoreConvertor;
import io.polycat.catalog.store.fdb.record.impl.StoreImpl;
import io.polycat.catalog.store.fdb.record.impl.TableDataStoreImpl;

import io.polycat.catalog.store.protos.common.DataFile;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

//@Component
public class TestUtil {
    public static final String projectId = UuidUtil.generateUUID32();

    public static CatalogResourceService catalogResourceService;
    @Autowired
    public CatalogService catalogService;
    @Autowired
    public DatabaseService databaseService;
    @Autowired
    public TableService tableService;
    @Autowired
    public ViewService viewService;
    @Autowired
    public ShareService shareService;
    @Autowired
    public RoleService roleService;
    @Autowired
    public PrivilegeService privilegeService;
    @Autowired
    public DelegateService delegateService;
    @Autowired
    public UsageProfileService usageProfileService;
    @Autowired
    public DataLineageService dataLineageService;
    @Autowired
    public ObjectNameMapService objectNameMapService;
    @Autowired
    public PartitionService partitionService;
    @Autowired
    public FunctionService functionService;

    public static final StoreBase storeBase = new StoreImpl();


    public static final String userId = "TestUser";

    public static Catalog catalog;
    public static String catalogNameString;

    public static String databaseId;
    public static String databaseNameString;

    public static String tableNameString;

    static boolean isFirstTest = true;

    @Autowired
    public void setCatalogResourceService(CatalogResourceService catalogResourceService) {
        this.catalogResourceService = catalogResourceService;
    }

    public void createResource(String projectId) {
        if (isFirstTest) {
            isFirstTest = false;
            catalogResourceService.createResource(projectId);
        }
    }

    @BeforeEach
    public void initServiceImpl() {
        createResource(projectId);
    }




    public static long getCommitVersion(byte[] versionStamp) {
        byte[] temp = new byte[8];
        System.arraycopy(versionStamp, 0, temp, 0, 8);

        return ByteBuffer.wrap(temp).getLong();
    }

    public CatalogInput getCatalogInput(String catalogName) {
        CatalogInput catalogInput = new CatalogInput();
        catalogInput.setCatalogName(catalogName);
        catalogInput.setOwner(userId);
        return catalogInput;
    }

    public CatalogInput getCatalogInput(String catalogName, String parentName, String parentVersion) {
        CatalogInput catalogInput = new CatalogInput();
        catalogInput.setCatalogName(catalogName);
        catalogInput.setParentName(parentName);
        catalogInput.setParentVersion(parentVersion);
        catalogInput.setOwner(userId);
        return catalogInput;
    }

    public DatabaseInput getDatabaseDTO(String projectId, String catalogName, String dbName) {
        DatabaseInput databaseInput = new DatabaseInput();
        databaseInput.setCatalogName(catalogName);
        databaseInput.setDatabaseName(dbName);
        databaseInput.setLocationUri("/database/test");
        databaseInput.setOwner(userId);
        return databaseInput;
    }

    public List<Column> getColumnDTO(int columnNum) {
        List<Column> columnInputs = new ArrayList<>(columnNum);

        for (int idx = 0; idx < columnNum; ++idx) {
            Column columnInput = new Column();
            columnInput.setColumnName(UuidUtil.generateId());
            columnInput.setColType(DataTypes.STRING.getName());
            columnInputs.add(columnInput);
        }

        return columnInputs;
    }

    public TableInput getTableDTO(String tableName, int colNum) {
        TableInput tableInput = new TableInput();
        StorageDescriptor storageInput = new StorageDescriptor();
        storageInput.setColumns(getColumnDTO(colNum));
        tableInput.setStorageDescriptor(storageInput);
        tableInput.setTableName(tableName);
        tableInput.setOwner(userId);
        return tableInput;
    }

    public TableInput getTableDTOWithPartitionColumn(String tableName, int colNum, int partitionColNum) {
        TableInput tableInput = new TableInput();
        tableInput.setTableName(tableName);
        StorageDescriptor sd = new StorageDescriptor();
        sd.setColumns(getColumnDTO(colNum));
        tableInput.setStorageDescriptor(sd);
        tableInput.setPartitionKeys(getColumnDTO(partitionColNum));
        tableInput.setOwner(userId);
        return tableInput;
    }

    public void createCatalogBeforeClass() {
        String catalogName = UUID.randomUUID().toString().toLowerCase();
        catalogNameString = catalogName;

        CatalogInput catalogInput = getCatalogInput(catalogName);

        catalog = catalogService.createCatalog(projectId, catalogInput);
        assertNotNull(catalog);
    }

    public Tuple createNewCatalog() {
        String catalogName = UUID.randomUUID().toString().toLowerCase();
        CatalogInput catalogInput = getCatalogInput(catalogName);
        Catalog catalog = catalogService.createCatalog(projectId, catalogInput);
        assertNotNull(catalog);
        return Tuple.from(projectId, catalogName);
    }

    public Catalog createBranch(CatalogName catalogName, String branchName, String projectId) {
        Catalog catalogRecord = catalogService.getCatalog(catalogName);
        CatalogInput catalogInput = getCatalogInput(branchName);
        catalogInput.setParentName(catalogRecord.getCatalogName());
        String lastVersion = VersionManagerHelper.getLatestVersionByName(projectId, catalogName.getCatalogName());
        catalogInput.setParentVersion(lastVersion);
        return catalogService.createCatalog(projectId, catalogInput);
    }

    public void createDatabaseBeforeClass() {
        String dbName = UUID.randomUUID().toString().toLowerCase();

        DatabaseInput databaseInput = getDatabaseDTO(projectId, catalog.getCatalogName(), dbName);

        databaseNameString = dbName;
        CatalogName catalogName = StoreConvertor.catalogName(projectId, catalogNameString);
        databaseId = databaseService.createDatabase(catalogName, databaseInput).getDatabaseId();
        assertNotNull(databaseId);
    }

    public void createTableBeforeClass() {
        String tableName = UUID.randomUUID().toString().toLowerCase();
        tableNameString = tableName;
        TableInput tableInput = getTableDTO(tableNameString, 3);
        DatabaseName databaseName = StoreConvertor.databaseName(projectId, catalogNameString, databaseNameString);
        tableService.createTable(databaseName, tableInput);
    }

    public static void clearFDB() {
        storeBase.clearDB();
    }

    public static AddPartitionInput buildPartition(Table table, String partitionName, String partitionPath) {
        AddPartitionInput partitionInput = new AddPartitionInput();

        DataFile file1 = DataFile.newBuilder().setFileName("file1.carbondata").setOffset(10).setLength(30)
            .setRowCount(2).build();

        FileInput[] fileInputs = new FileInput[1];
        fileInputs[0] = new FileInput();
        fileInputs[0].setFileName(file1.getFileName());

        FileStatsInput[] fileStatsInputs = new FileStatsInput[1];
        fileStatsInputs[0] = new FileStatsInput();
        String[] max = {"efg"};
        fileStatsInputs[0].setMaxValues(max);
        String[] min = {"abc"};
        fileStatsInputs[0].setMinValues(min);

        PartitionInput[] partitionBase = new PartitionInput[1];
        partitionBase[0] = new PartitionInput();
        List<Column> columnInputs = new ArrayList<>();
        for (Column field : table.getStorageDescriptor().getColumns()) {
            Column columnInput = new Column();
            columnInput.setColumnName(field.getColumnName());
            columnInput.setComment(field.getComment());
            columnInput.setColType(field.getColType());
            columnInputs.add(columnInput);
        }

        partitionBase[0].setFiles(fileInputs);
        partitionBase[0].setPartitionValues(PartitionUtil.convertNameToVals(partitionName));
        partitionBase[0].setIndex(fileStatsInputs);
        String baseLocation = table.getStorageDescriptor().getLocation();
        StorageDescriptor sd = new StorageDescriptor();
        sd.setColumns(columnInputs);
        sd.setLocation(baseLocation + File.separator + partitionPath);
        partitionBase[0].setStorageDescriptor(sd);

        partitionInput.setPartitions(partitionBase);
        return partitionInput;
    }

    public static AddPartitionInput buildPartitions(Table table, String partitionName, Integer num) {
        AddPartitionInput partitionInput = new AddPartitionInput();

        DataFile file1 = DataFile.newBuilder().setFileName("file1.carbondata").setOffset(10).setLength(30)
            .setRowCount(2).build();

        FileInput[] fileInputs = new FileInput[1];
        fileInputs[0] = new FileInput();
        fileInputs[0].setFileName(file1.getFileName());

        FileStatsInput[] fileStatsInputs = new FileStatsInput[1];
        fileStatsInputs[0] = new FileStatsInput();
        String[] max = {"efg"};
        fileStatsInputs[0].setMaxValues(max);
        String[] min = {"abc"};
        fileStatsInputs[0].setMinValues(min);

        List<Column> columnInputs = new ArrayList<>();
        for (Column field : table.getStorageDescriptor().getColumns()) {
            Column columnInput = new Column();
            columnInput.setColumnName(field.getColumnName());
            columnInput.setComment(field.getComment());
            columnInput.setColType(field.getColType());
            columnInputs.add(columnInput);
        }

        PartitionInput[] partitionBase = new PartitionInput[num];
        for (int i = 0; i < num; i++) {
            partitionBase[i] = new PartitionInput();
            partitionBase[i].setFiles(fileInputs);
            partitionBase[i].setPartitionValues(PartitionUtil.convertNameToVals(partitionName));
            partitionBase[i].setIndex(fileStatsInputs);
            String baseLocation = table.getStorageDescriptor().getLocation();
            if (partitionBase[i].getStorageDescriptor() == null) {
                partitionBase[i].setStorageDescriptor(new StorageDescriptor());
            }
            partitionBase[i].getStorageDescriptor().setColumns(columnInputs);
            partitionBase[i].getStorageDescriptor().setLocation(baseLocation + File.separator + "partitionPath");
        }

        partitionInput.setPartitions(partitionBase);
        return partitionInput;
    }

    public Catalog createCatalogPrepareTest(String projectId, String catalogName) {

        CatalogInput catalogInput = getCatalogInput(catalogName);
        Catalog catalog = catalogService.createCatalog(projectId, catalogInput);
        assertNotNull(catalog);
        assertEquals(catalog.getCatalogName(), catalogName);

        return catalog;

    }

    public Database createDatabasePrepareTest(CatalogName catalogName, String databaseName) {

        DatabaseInput databaseInput = getDatabaseDTO(catalogName.getProjectId(),catalogName.getCatalogName(), databaseName);
        Database createDB = databaseService.createDatabase(catalogName, databaseInput);
        assertNotNull(createDB);
        assertEquals(databaseName.toLowerCase(), createDB.getDatabaseName());

        return createDB;
    }

    public void buildLmsNameProperties(TableInput tableInput, String lms_name) {
        Map<String, String> map = new HashMap<>();
        if (tableInput.getParameters() != null) {
            map.putAll(tableInput.getParameters());
        }
        map.put("lms_name", lms_name);
        tableInput.setParameters(map);
    }

    public List<Table> createTablesPrepareTest(DatabaseName databaseName, int tableNum) {

        List<Table> tableList = new ArrayList<>(tableNum);

        //create main branch table
        for (int i = 0; i < tableNum; i++) {

            String tableNameString = UUID.randomUUID().toString().toLowerCase();

            TableInput tableInput = getTableDTO(tableNameString, 3);
            tableInput.setPartitionKeys(Collections.singletonList(new Column("partition", "String")));
            buildLmsNameProperties(tableInput, String.format("%s", databaseName.getCatalogName()));
            tableService.createTable(databaseName, tableInput);

            TableName tableName = StoreConvertor.tableName(databaseName.getProjectId(),
                databaseName.getCatalogName(),
                databaseName.getDatabaseName(), tableNameString);
            Table table = tableService.getTableByName(tableName);
            assertNotNull(table);
            tableList.add(table);

        }

        return tableList;
    }

    public List<Table> prepareBranchTestWithCreateTable(String mainBranchCatalogName,String mainBranchDatabaseName,
        int mainBranchTableNum) {
        //create main branch
        Catalog mainCatalog = createCatalogPrepareTest(projectId, mainBranchCatalogName);

        //create main branch database
        CatalogName mainCatalogName = StoreConvertor
            .catalogName(projectId, mainCatalog.getCatalogName());
        Database mainDatabase = createDatabasePrepareTest(mainCatalogName, mainBranchDatabaseName);

        //create table
        DatabaseName mainDatabaseName = StoreConvertor.databaseName(mainCatalogName, mainDatabase.getDatabaseName());

        return createTablesPrepareTest(mainDatabaseName, mainBranchTableNum);
    }

    public List<Table> prepareOneLevelBranchTest(String mainBranchCatalogName,String mainBranchDatabaseName,
        String subBranchCatalogName, int mainBranchTableNum) {
        //create main branch
        Catalog mainCatalog = createCatalogPrepareTest(projectId, mainBranchCatalogName);

        //create main branch database
        CatalogName mainCatalogName = StoreConvertor.catalogName(projectId, mainCatalog.getCatalogName());
        Database mainDatabase = createDatabasePrepareTest(mainCatalogName, mainBranchDatabaseName);

        //create table
        DatabaseName mainDatabaseName = StoreConvertor.databaseName(mainCatalogName, mainDatabase.getDatabaseName());
        List<Table> tableList = createTablesPrepareTest(mainDatabaseName, mainBranchTableNum);

        //create branch
        String version = VersionManagerHelper.getLatestVersionByName(projectId, mainBranchCatalogName);
        CatalogInput catalogInput = getCatalogInput(subBranchCatalogName, mainCatalogName.getCatalogName(), version);
        Catalog subBranchCatalog = catalogService.createCatalog(projectId, catalogInput);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(
            StoreConvertor.catalogName(projectId, subBranchCatalogName));
        assertEquals(subBranchCatalogRecord.getCatalogName(), subBranchCatalogName);
        assertEquals(subBranchCatalogRecord.getParentName(), mainCatalogName.getCatalogName());
        String subBranchVersion =subBranchCatalogRecord.getParentVersion();
        String parentBranchVersion = version;
        assertEquals(subBranchVersion, parentBranchVersion);
        assertEquals(subBranchCatalogRecord.getCatalogName(), subBranchCatalog.getCatalogName());

        return tableList;
    }

    public static void modifyTableStoreImplPrivateByReflexes(int indexNumber, int dataMaxSize, int dealMaxNumRecordPerTrans, int delMaxNumIndexPartitionSetPerTrans)
        throws NoSuchFieldException, IllegalAccessException {
        Field indexStoredMaxNumber = TableDataStoreImpl.class.getDeclaredField("indexStoredMaxNumber");
        indexStoredMaxNumber.setAccessible(true);
        Field modifiers1 = Field.class.getDeclaredField("modifiers");
        modifiers1.setAccessible(true);
        modifiers1.setInt(indexStoredMaxNumber, indexStoredMaxNumber.getModifiers() & ~Modifier.FINAL);
        indexStoredMaxNumber.set(indexStoredMaxNumber, indexNumber);

        Field dataStoredMaxSize = TableDataStoreImpl.class.getDeclaredField("dataStoredMaxSize");
        dataStoredMaxSize.setAccessible(true);
        Field modifiers2 = Field.class.getDeclaredField("modifiers");
        modifiers2.setAccessible(true);
        modifiers2.setInt(dataStoredMaxSize, dataStoredMaxSize.getModifiers() & ~Modifier.FINAL);
        dataStoredMaxSize.set(dataStoredMaxSize, dataMaxSize);

        Field dealMaxNumRecord = TableDataStoreImpl.class.getDeclaredField("dealMaxNumRecordPerTrans");
        dealMaxNumRecord.setAccessible(true);
        Field modifiers3 = Field.class.getDeclaredField("modifiers");
        modifiers3.setAccessible(true);
        modifiers3.setInt(dealMaxNumRecord, dealMaxNumRecord.getModifiers() & ~Modifier.FINAL);
        dealMaxNumRecord.set(dealMaxNumRecord, dealMaxNumRecordPerTrans);

        Field delMaxNumIndexPartitionSet = TableServiceImpl.class.getDeclaredField("delMaxNumIndexPartitionSetPerTrans");
        delMaxNumIndexPartitionSet.setAccessible(true);
        Field modifiers4 = Field.class.getDeclaredField("modifiers");
        modifiers4.setAccessible(true);
        modifiers4.setInt(delMaxNumIndexPartitionSet, delMaxNumIndexPartitionSet.getModifiers() & ~Modifier.FINAL);
        delMaxNumIndexPartitionSet.set(delMaxNumIndexPartitionSet, delMaxNumIndexPartitionSetPerTrans);
    }

    public List<Throwable> multiThreadExecute(Runnable runnable, String threadName, int threadNum)
        throws InterruptedException {
        Thread[] threads = new Thread[threadNum];
        for (int i = 0; i < threadNum; i ++) {
            threads[i] = new Thread(runnable , threadName + i);
        }

        return multiThreadExecute(threads);
    }

    public List<Throwable> multiThreadExecute(Thread[] threads) throws InterruptedException {
        List<Throwable> uncaught = Collections.synchronizedList(new ArrayList<>());
        for (Thread thread : threads) {
            thread.setUncaughtExceptionHandler((t, e) -> uncaught.add(e));
        }

        for (Thread t : threads) {
            t.start();
        }

        for (Thread t : threads) {
            t.join(10*1000);
        }

        return uncaught;
    }

    @AfterAll
    public static void afterClass() {
        clearFDB();
        //catalogResourceService.dropResource(projectId);
    }

    private static boolean isDropped(DatabaseHistoryObject databaseHistoryObject) {
        return databaseHistoryObject.getDroppedTime() > 0;
    }

    public static void printHistory(DatabaseHistoryObject history) {
        // git log format
        System.out.println("global version: " + CodecUtil.bytes2Hex(
            FDBRecordVersion.fromBytes(CodecUtil.hex2Bytes(history.getVersion())).getGlobalVersion()));
        System.out.println("Author: project id/name");
        System.out.println("Date: (e.g.) Mon  MM-DD-Time-YYYY  TIME-ZONE");

        System.out.println("Basic Info: DDL operation on  DatabaseName(" + history.getName()
                + "), isDropped:" + isDropped(history));

        System.out.println("Comments: add comments here");
        System.out.println();
    }

}
