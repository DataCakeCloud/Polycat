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

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.model.*;
import io.polycat.catalog.common.model.base.PartitionInput;
import io.polycat.catalog.common.plugin.request.input.*;
import io.polycat.catalog.common.utils.PartitionUtil;
import io.polycat.catalog.service.api.CatalogService;
import io.polycat.catalog.service.api.DatabaseService;
import io.polycat.catalog.service.api.PartitionService;
import io.polycat.catalog.service.api.TableService;
import io.polycat.catalog.store.common.StoreConvertor;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.File;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Slf4j
public class PGTableServiceImplTestV2 extends PGBaseServiceImplTest{

    private static final String DATABASE_NAME = "database_test";
    private static final String TABLE_NAME = "table_test";
    @Autowired
    TableService tableService;
    @Autowired
    CatalogService catalogService;
    @Autowired
    DatabaseService databaseService;

    @Autowired
    PartitionService partitionService;

    boolean isFirstTest = true;

    public void beforeClass() throws Exception {
        if (isFirstTest) {

            // create catalog
            CatalogName catalogName = new CatalogName(PROJECT_ID, CATALOG_NAME);
            CatalogInput catalogInput = getCatalogInput(catalogName.getCatalogName());
            catalogService.createCatalog(PROJECT_ID, catalogInput);

            // create database
            DatabaseName databaseName = StoreConvertor.databaseName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME);
            DatabaseInput databaseInput = getDatabaseDTO(PROJECT_ID, CATALOG_NAME, DATABASE_NAME);
            databaseService.createDatabase(catalogName, databaseInput);
            isFirstTest = false;
        }
    }

    @BeforeEach
    public void beforeEach() throws Exception {
        beforeClass();
    }


    @Test
    public void alter_table_should_success(){
        DatabaseName database = StoreConvertor.databaseName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME);
        TableInput tableInput = createTableDTO(TABLE_NAME, 20);
        tableService.createTable(database, tableInput);
        TableName tn = new TableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, TABLE_NAME);
        Table table = tableService.getTableByName(tn);
        valueAssertNotNull(table);
        valueAssertEquals(20, table.getFields().size());
        String newTableName = TABLE_NAME + "_new";
        tableInput.setTableName(newTableName);

        tableService.alterTable(tn, tableInput, null);
        TableName tableName = StoreConvertor.tableName(database, newTableName);
        Table tableByName = tableService.getTableByName(tableName);
        valueAssertNotNull(tableByName);
        log.info("Table: {}", tableByName);
        valueAssertEquals(newTableName, tableByName.getTableName());


        // once again
        String newTable2 = TABLE_NAME + "new2";
        tableInput.setTableName(newTable2);
        tableService.alterTable(tableName, tableInput, null);
        TableName tableName1 = StoreConvertor.tableName(database, newTable2);
        Table tableByName1 = tableService.getTableByName(tableName1);
        valueAssertNotNull(tableByName1);
        log.info("Table: {}", tableByName1);
        valueAssertEquals(newTable2, tableByName1.getTableName());

        // alter location test
        String location = "obs://xxxxx/" + DATABASE_NAME + "/" + newTable2;
        tableInput.getStorageDescriptor().setLocation(location);

        tableService.alterTable(tableName1, tableInput, null);
        Table tableByNameLocation = tableService.getTableByName(tableName1);
        valueAssertNotNull(tableByNameLocation);
        log.info("Table: {}", tableByNameLocation);
        valueAssertEquals(location, tableByNameLocation.getStorageDescriptor().getLocation());

        // alter owner test
        String newOwner = "aaaaaa";
        tableInput.setOwner(newOwner);
        tableService.alterTable(tableName1, tableInput, null);
        Table newOwnerTable = tableService.getTableByName(tableName1);
        valueAssertNotNull(newOwnerTable);
        log.info("Table: {}", newOwnerTable);
        valueAssertEquals(newOwner, newOwnerTable.getOwner());

        // alter sd test
        tableInput.getStorageDescriptor().getSerdeInfo().getParameters().put("key1", "value1");
        tableInput.getStorageDescriptor().setParameters(Collections.singletonMap("key2", "value2"));
        tableService.alterTable(tableName1, tableInput, null);
        Table newSdTable = tableService.getTableByName(tableName1);
        valueAssertNotNull(newSdTable);
        log.info("Table: {}", newSdTable);
        valueAssertEquals(tableInput.getStorageDescriptor().getSerdeInfo().getParameters(), newSdTable.getStorageDescriptor().getSerdeInfo().getParameters());
        valueAssertEquals(tableInput.getStorageDescriptor().getParameters(), newSdTable.getStorageDescriptor().getParameters());
    }

    @Test
    public void alter_table_view_should_success(){
        DatabaseName database = StoreConvertor.databaseName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME);
        TableInput tableInput = createTableViewDTO(TABLE_NAME, 20);
        tableService.createTable(database, tableInput);
        TableName tn = new TableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, TABLE_NAME);
        Table table = tableService.getTableByName(tn);
        valueAssertNotNull(table);
        valueAssertEquals(20, table.getFields().size());
        String newTableName = TABLE_NAME + "_new";
        tableInput.setTableName(newTableName);

        tableService.alterTable(tn, tableInput, null);
        TableName tableName = StoreConvertor.tableName(database, newTableName);
        Table tableByName = tableService.getTableByName(tableName);
        valueAssertNotNull(tableByName);
        log.info("Table: {}", tableByName);
        valueAssertEquals(newTableName, tableByName.getTableName());


        // once again
        String newTable2 = TABLE_NAME + "_new2";
        tableInput.setTableName(newTable2);
        tableService.alterTable(tableName, tableInput, null);
        TableName tableName1 = StoreConvertor.tableName(database, newTable2);
        Table tableByName1 = tableService.getTableByName(tableName1);
        valueAssertNotNull(tableByName1);
        log.info("Table: {}", tableByName1);
        valueAssertEquals(newTable2, tableByName1.getTableName());

        // alter location test
        String location = "obs://xxxxx/" + DATABASE_NAME + "/" + newTable2;
        tableInput.getStorageDescriptor().setLocation(location);

        tableService.alterTable(tableName1, tableInput, null);
        Table tableByNameLocation = tableService.getTableByName(tableName1);
        valueAssertNotNull(tableByNameLocation);
        log.info("Table: {}", tableByNameLocation);
        valueAssertEquals(location, tableByNameLocation.getStorageDescriptor().getLocation());

        // alter owner test
        String newOwner = "aaaaaa";
        tableInput.setOwner(newOwner);
        tableService.alterTable(tableName1, tableInput, null);
        Table newOwnerTable = tableService.getTableByName(tableName1);
        valueAssertNotNull(newOwnerTable);
        log.info("Table: {}", newOwnerTable);
        valueAssertEquals(newOwner, newOwnerTable.getOwner());

        // alter view sql test
        String sql = tableInput.getViewExpandedText().trim() + " LIMIT 11";
        tableInput.setViewExpandedText(sql);
        tableInput.setViewOriginalText(sql);
        tableService.alterTable(tableName1, tableInput, null);
        Table viewTable = tableService.getTableByName(tableName1);
        valueAssertNotNull(viewTable);
        log.info("Table: {}, sql: {}", viewTable, sql);
        valueAssertEquals(sql, viewTable.getViewExpandedText());
        valueAssertEquals(sql, viewTable.getViewOriginalText());

        // alter sd test
        tableInput.getStorageDescriptor().getSerdeInfo().getParameters().put("key1", "value1");
        tableInput.getStorageDescriptor().setParameters(Collections.singletonMap("key2", "value2"));
        tableService.alterTable(tableName1, tableInput, null);
        Table newSdTable = tableService.getTableByName(tableName1);
        valueAssertNotNull(newSdTable);
        log.info("Table: {}", newSdTable);
        valueAssertEquals(tableInput.getStorageDescriptor().getSerdeInfo().getParameters(), newSdTable.getStorageDescriptor().getSerdeInfo().getParameters());
        valueAssertEquals(tableInput.getStorageDescriptor().getParameters(), newSdTable.getStorageDescriptor().getParameters());
    }


    @Test
    public void multiThreadAddColumn() throws InterruptedException {
        DatabaseName databaseName = StoreConvertor.databaseName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME);
        int defaultColNum = 3;
        TableInput tableInput = createTableDTO(TABLE_NAME, defaultColNum);
        tableService.createTable(databaseName, tableInput);
        Table table = tableService.getTableByName(StoreConvertor.tableName(databaseName.getProjectId(),
                databaseName.getCatalogName(), databaseName.getDatabaseName(), TABLE_NAME));
        assertTrue(table.getTableName().equals(TABLE_NAME));

        TableName tableName = StoreConvertor
                .tableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, TABLE_NAME);

        int threadNum = 3;
        //Thread[] threads = new Thread[threadNum];
        for (int i = 0; i < threadNum; i++) {
            String newColName = "new_col_" + i%2;
            ColumnChangeInput colChangeIn = new ColumnChangeInput();
            colChangeIn.setChangeType(Operation.ADD_COLUMN);
            List<Column> addColumnList = new ArrayList<>();
            addColumnList.add(new Column(newColName, "STRING"));
            colChangeIn.setColumnList(addColumnList);
            tableService.alterColumn(tableName, colChangeIn);
           /* threads[i] = new Thread(() -> {
                tableService.alterColumn(tableName, colChangeIn);
            }, "add-column" + i);*/
        }

        //List<Throwable> uncaught = multiThreadExecute(threads);

        table = tableService.getTableByName(StoreConvertor.tableName(databaseName.getProjectId(),
                databaseName.getCatalogName(), databaseName.getDatabaseName(), TABLE_NAME));
        assertEquals(defaultColNum + 2, table.getStorageDescriptor().getColumns().size());

        /*assertEquals(uncaught.size(), 1);
        for (Throwable e : uncaught) {
            assertEquals(String.format("Column [%s] is duplicated",  "new_col_0"), e.getMessage());
        }*/
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

    @Test
    public void create_table_should_success(){
        DatabaseName database = StoreConvertor.databaseName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME);
        TableInput tableInput = createTableDTO(TABLE_NAME, 20);
        tableInput.setTableName(TABLE_NAME);
        tableService.createTable(database, tableInput);
        TableName tableName = StoreConvertor.tableName(database, TABLE_NAME);
        Table tableByName = tableService.getTableByName(tableName);
        valueAssertNotNull(tableByName);
        log.info("Table: {}", tableByName);
        valueAssertEquals(TABLE_NAME, tableByName.getTableName());
    }


    @Test
    public void create_complex_type_should_success(){
        DatabaseName database = StoreConvertor.databaseName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME);
        TableInput tableInput = createTableDTO(TABLE_NAME, 20);
        String complexColName = "complexType";
        String complexColType = "STRUCT<SELECT:STRING,DMP:STRING,PROFILE:STRING,OTHER:STRING,TYPE:INT>";
        tableInput.getStorageDescriptor().getColumns().add(new Column(complexColName, complexColType, "comment complexType"));
        tableInput.setTableName(TABLE_NAME);
        tableService.createTable(database, tableInput);
        TableName tableName = StoreConvertor.tableName(database, TABLE_NAME);
        Table tableByName = tableService.getTableByName(tableName);
        valueAssertNotNull(tableByName);
        log.info("Table: {}", tableByName);
        List<Column> columns = tableByName.getStorageDescriptor().getColumns();
        valueAssertEquals(complexColType, columns.get(columns.size()-1).getColType());
        valueAssertEquals(complexColName, columns.get(columns.size()-1).getColumnName());
    }
    @Test
    public void create_partitiontable_should_success(){
        DatabaseName database = StoreConvertor.databaseName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME);
        TableInput tableInput = createTableDTO(TABLE_NAME, 20);
        tableInput.setPartitionKeys(Collections.singletonList(new Column("dt", "string", "partition:dt")));
        tableInput.setTableName(TABLE_NAME);
        tableService.createTable(database, tableInput);
        TableName tableName = StoreConvertor.tableName(database, TABLE_NAME);
        Table tableByName = tableService.getTableByName(tableName);
        valueAssertNotNull(tableByName);
        log.info("Table: {}", tableByName);
        valueAssertEquals(TABLE_NAME, tableByName.getTableName());
    }

    private TableInput createTableViewDTO(String tableName, int colNum) {
        TableInput tableInput = createTableDTO(tableName, colNum);
        tableInput.setTableType(TableTypeInput.VIRTUAL_VIEW.name());
        String sql = "select * from test_db.test_table";
        tableInput.setViewExpandedText(sql);
        tableInput.setViewOriginalText(sql);
        return tableInput;
    }

    private TableInput createTableDTO(String tableName, int colNum) {
        TableInput tableInput = new TableInput();
        tableInput.setTableType(TableTypeInput.EXTERNAL_TABLE.name());
        tableInput.setTableName(tableName);
        StorageDescriptor sd = new StorageDescriptor();
        sd.setColumns(getColumnDTO(colNum));
        sd.setLocation("/data/" + tableName);
        sd.setFileFormat("parquet");
        sd.setSourceShortName("textfile");
        sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
        sd.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");

        SerDeInfo serDeInfo = new SerDeInfo();
        serDeInfo.setSerializationLibrary("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
        sd.setSerdeInfo(serDeInfo);
        tableInput.setStorageDescriptor(sd);
        tableInput.setOwner(userId);
        Map<String, String> params = new HashMap<String, String>();
        params.put(LMS_NAME_KEY, CATALOG_NAME);
        tableInput.setParameters(params);
        return tableInput;
    }

    @Test
    public void partition_add_should_success() {
        String parNamePrefix = "datepart=20201014/hour=18/request_app=app1/event=download";
        DatabaseName database = StoreConvertor.databaseName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME);
        TableInput tableInput = createTableDTO(TABLE_NAME, 20);
        tableInput.setPartitionKeys(Arrays.asList(new Column("datepart", "string", "partition:datepart"),
                new Column("hour", "string", "partition:hour"),
                new Column("request_app", "string", "partition:request_app"),
                new Column("event", "string", "partition:event")));
        tableInput.setTableName(TABLE_NAME);
        tableService.createTable(database, tableInput);

        TableName tableName = StoreConvertor.tableName(database, TABLE_NAME);
        Table tableByName = tableService.getTableByName(tableName);
        int loop = 1;
        int perLoopCount = 100;
        AddPartitionInput addPartitionInput;
        for (int i = 0; i < loop; i++) {
            addPartitionInput = buildPartitions(tableByName, perLoopCount, parNamePrefix + "_" + i);
            partitionService.addPartitions(StoreConvertor.tableName(database, TABLE_NAME), addPartitionInput);
        }
        valueAssertEquals(loop * perLoopCount, partitionService.listPartitionNames(tableName, -1).length);
    }

        @Test
    public void partition_crud_should_success() {
        DatabaseName database = StoreConvertor.databaseName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME);
        TableInput tableInput = createTableDTO(TABLE_NAME, 20);
        tableInput.setPartitionKeys(Collections.singletonList(new Column("dt", "string", "partition:dt")));
        tableInput.setTableName(TABLE_NAME);
        tableService.createTable(database, tableInput);


        TableName tableName = StoreConvertor.tableName(database, TABLE_NAME);
        Table tableByName = tableService.getTableByName(tableName);
        final AddPartitionInput addPartitionInput = buildPartition(tableByName, "dt=0", "dt=0");
        partitionService.addPartitions(StoreConvertor.tableName(database, TABLE_NAME), addPartitionInput);

        final Partition partition = partitionService.getPartitionByName(tableName, "dt=0");
        valueAssertEquals(TABLE_NAME, partition.getTableName());
        final List<String> values = Collections.singletonList("0");
        valueAssertEquals(partition.getPartitionValues(), values);


        final AlterPartitionInput alterPartitionInput = new AlterPartitionInput();
        PartitionAlterContext context = new PartitionAlterContext();
        context.setOldValues(values);
        final List<String> newValues = Collections.singletonList("1");
        context.setNewValues(newValues);
        context.setParameters(partition.getParameters());
        context.setInputFormat(partition.getStorageDescriptor().getInputFormat());
        context.setOutputFormat(partition.getStorageDescriptor().getOutputFormat());
        context.setLocation("/data/" + TABLE_NAME + "/dt=1");
        alterPartitionInput.setPartitionContexts(new PartitionAlterContext[]{context});
        partitionService.alterPartition(tableName, alterPartitionInput);


        final Partition newPartition = partitionService.getPartitionByName(tableName, "dt=1");
        valueAssertEquals(newValues, newPartition.getPartitionValues());
        valueAssertEquals("/data/" + TABLE_NAME + "/dt=1", newPartition.getStorageDescriptor().getLocation());

        final DropPartitionInput dropPartitionInput = new DropPartitionInput(Collections.singletonList("dt=1"), false, false, false,
            false);
        partitionService.dropPartition(tableName, dropPartitionInput);
        Assertions.assertThrows(
            CatalogServerException.class, () -> partitionService.getPartitionByName(tableName, "dt=1"));

    }

    @Test
    public void get_partition_by_filter_should_success() {
        DatabaseName database = StoreConvertor.databaseName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME);
        TableInput tableInput = createTableDTO(TABLE_NAME, 20);
        final ArrayList<Column> columns = new ArrayList<>();
        columns.add(new Column("dt", "string", "partition:dt"));
        columns.add(new Column("id", "int", "partition:id"));
        tableInput.setPartitionKeys(columns);
        tableInput.setTableName(TABLE_NAME);
        tableService.createTable(database, tableInput);

        TableName tableName = StoreConvertor.tableName(database, TABLE_NAME);
        Table tableByName = tableService.getTableByName(tableName);
        final AddPartitionInput addPartitionInput = buildPartition(tableByName, "dt=20221212/id=0", "dt=20221212/id=0");
        partitionService.addPartitions(StoreConvertor.tableName(database, TABLE_NAME), addPartitionInput);

        final PartitionFilterInput partitionFilterInput = new PartitionFilterInput();
        partitionFilterInput.setFilter("dt=\"20221212\" and id=0");
        partitionFilterInput.setMaxParts(10);
        final Partition[] partitionsByFilter = partitionService.getPartitionsByFilter(tableName, partitionFilterInput);
        valueAssertEquals(partitionsByFilter.length, 1);
        System.out.println(partitionsByFilter[0]);

        partitionFilterInput.setFilter("");
        final Partition[] partitionsByFilter1 = partitionService.getPartitionsByFilter(tableName, partitionFilterInput);
        valueAssertEquals(partitionsByFilter1.length, 1);
        System.out.println(partitionsByFilter1[0]);
    }

    public static AddPartitionInput buildPartition(Table table, String partitionName, String partitionPath) {
        AddPartitionInput partitionInput = new AddPartitionInput();

        io.polycat.catalog.store.protos.common.DataFile file1 = io.polycat.catalog.store.protos.common.DataFile.newBuilder().setFileName("file1.carbondata").setOffset(10).setLength(30)
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



    public static AddPartitionInput buildPartitions(Table table, int partionNums, String parNamePrefix) {
        long l = System.currentTimeMillis();
        AddPartitionInput partitionInput = new AddPartitionInput();
        PartitionInput[] partitionBases = getPartitionBases(table, partionNums, parNamePrefix);
        partitionInput.setPartitions(partitionBases);
        log.info("buildPartitions size: {}, spendTime: {}", partionNums, System.currentTimeMillis() - l);
        return partitionInput;
    }

    private static PartitionInput[] getPartitionBases(Table table, int partionNums, String parNamePrefix) {
        PartitionInput[] partitionBases = new PartitionInput[partionNums];
        PartitionInput base;
        for (int i = 0; i < partionNums; i++) {
            base = buildPartitionBase(table, parNamePrefix + i );
            partitionBases[i] = base;
        }
        return partitionBases;
    }

    private static PartitionInput buildPartitionBase(Table table, String partitionName) {
        PartitionInput partitionBase = new PartitionInput();
        FileInput[] fileInputs = new FileInput[1];
        io.polycat.catalog.store.protos.common.DataFile file1 = io.polycat.catalog.store.protos.common.DataFile.newBuilder().setFileName("file1.carbondata").setOffset(10).setLength(30)
                .setRowCount(2).build();
        fileInputs[0] = new FileInput();
        fileInputs[0].setFileName(file1.getFileName());

        FileStatsInput[] fileStatsInputs = new FileStatsInput[1];
        fileStatsInputs[0] = new FileStatsInput();
        String[] max = {"efg"};
        fileStatsInputs[0].setMaxValues(max);
        String[] min = {"abc"};
        fileStatsInputs[0].setMinValues(min);

        partitionBase = new PartitionInput();
        List<Column> columnInputs = new ArrayList<>();
        for (Column field : table.getStorageDescriptor().getColumns()) {
            Column columnInput = new Column();
            columnInput.setColumnName(field.getColumnName());
            columnInput.setComment(field.getComment());
            columnInput.setColType(field.getColType());
            columnInputs.add(columnInput);
        }

        partitionBase.setFiles(fileInputs);
        partitionBase.setPartitionValues(PartitionUtil.convertNameToVals(partitionName));
        partitionBase.setIndex(fileStatsInputs);
        String baseLocation = table.getStorageDescriptor().getLocation();
        StorageDescriptor sd = new StorageDescriptor();
        sd.setColumns(columnInputs);
        sd.setLocation(baseLocation + File.separator + "path_" + partitionName);
        partitionBase.setStorageDescriptor(sd);
        return partitionBase;
    }
}
