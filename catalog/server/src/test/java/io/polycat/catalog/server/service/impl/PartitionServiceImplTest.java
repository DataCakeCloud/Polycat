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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.model.Catalog;
import io.polycat.catalog.common.types.DataTypes;
import io.polycat.catalog.common.model.CatalogName;
import io.polycat.catalog.common.model.Column;
import io.polycat.catalog.common.model.Database;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.Partition;
import io.polycat.catalog.common.model.Table;
import io.polycat.catalog.common.model.TableName;
import io.polycat.catalog.common.model.TraverseCursorResult;
import io.polycat.catalog.common.model.record.IntWritable;
import io.polycat.catalog.common.model.record.StringWritable;
import io.polycat.catalog.common.plugin.request.input.AddPartitionInput;
import io.polycat.catalog.common.plugin.request.input.ColumnChangeInput;
import io.polycat.catalog.common.plugin.request.input.DropPartitionInput;
import io.polycat.catalog.common.plugin.request.input.FilterInput;
import io.polycat.catalog.common.utils.GsonUtil;
import io.polycat.catalog.common.utils.UuidUtil;
import io.polycat.catalog.store.common.StoreConvertor;
import io.polycat.common.expression.Expression;
import io.polycat.common.expression.FieldExpression;
import io.polycat.common.expression.LiteralExpression;
import io.polycat.common.expression.bool.AndExpression;
import io.polycat.common.expression.comparison.EqualExpression;
import io.polycat.common.expression.comparison.GreaterThanExpression;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
public class PartitionServiceImplTest extends TestUtil {
    private static final Logger logger = Logger.getLogger(PartitionServiceImplTest.class);

    @BeforeEach
    public void beforeClass() throws NoSuchFieldException, IllegalAccessException {
        createCatalogBeforeClass();
        createDatabaseBeforeClass();
        modifyTableStoreImplPrivateByReflexes(10, 512, 100, 1);
    }

    private void showTablePartitionsAndCheckResult(int maxBatchNum, TableName tableName, int resultNum) {
        //list table commit in main branch
        for (int i = 1; i <= maxBatchNum; i++) {

            List<Partition> tablePartitionList = new ArrayList<>();
            String nextToken = null;

            while (true) {
                TraverseCursorResult<List<Partition>> tablePartitionListBatch =
                    partitionService.showTablePartition(tableName, i, nextToken, null);
                tablePartitionList.addAll(tablePartitionListBatch.getResult());

                if (tablePartitionListBatch.getResult().size() > 0) {
                    nextToken = tablePartitionListBatch.getContinuation().map(catalogToken -> {
                        return catalogToken.toString();
                    }).orElse(null);
                    if (nextToken == null) {
                        break;
                    }
                }

                if (tablePartitionListBatch.getResult().size() == 0) {
                    break;
                }
            }

            assertEquals(resultNum, tablePartitionList.size());
        }
    }


    @Test
    public void showPartitionsTest() {
        String mainBranchCatalogName = UuidUtil.generateUUID32();
        String mainBranchDatabaseName = UuidUtil.generateUUID32();
        int mainBranchTableNum = 1;

        //create branch catalog, database, tables
        List<Table> mainBranchTableList = prepareBranchTestWithCreateTable(mainBranchCatalogName,
            mainBranchDatabaseName, mainBranchTableNum);

        Table mainBranchTable = mainBranchTableList.get(0);
        TableName mainBranchTableName = StoreConvertor
            .tableName(projectId, mainBranchTable.getCatalogName(),
                mainBranchTable.getDatabaseName(), mainBranchTable.getTableName());

        int partitionNum = 1;
        for (int i = 0; i < partitionNum; i++) {
            partitionService.addPartition(mainBranchTableName,
                buildPartition(mainBranchTable, "partition=" + i, "partition_path"));
        }

        showTablePartitionsAndCheckResult(3, mainBranchTableName, partitionNum);
    }

    @Test
    public void addPartitionsTest() {
        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String mainBranchDatabaseName = UUID.randomUUID().toString().toLowerCase();
        int mainBranchTableNum = 1;

        //create branch catalog, database, tables
        List<Table> mainBranchTableList = prepareBranchTestWithCreateTable(mainBranchCatalogName,
            mainBranchDatabaseName, mainBranchTableNum);

        Table mainBranchTable = mainBranchTableList.get(0);
        TableName mainBranchTableName = StoreConvertor
            .tableName(projectId, mainBranchTable.getCatalogName(),
                mainBranchTable.getDatabaseName(), mainBranchTable.getTableName());

        Integer partitionNum = 25;
        AddPartitionInput partition = buildPartitions(mainBranchTable, "partition=0", partitionNum);
        int batchNum = 5;
        for (int i = 0; i < batchNum; i++) {
            partitionService.addPartitions(mainBranchTableName, partition);
        }

        showTablePartitionsAndCheckResult(100, mainBranchTableName, partitionNum*batchNum);
    }


    @Test
    public void dropPartitionsTest() throws NoSuchFieldException, IllegalAccessException {
        String catalogName = UUID.randomUUID().toString().toLowerCase();
        String databaseName = UUID.randomUUID().toString().toLowerCase();

        modifyTableStoreImplPrivateByReflexes(10, 512, 100, 5);
        Catalog mainCatalog = createCatalogPrepareTest(projectId, catalogName);

        CatalogName mainCatalogName = StoreConvertor.catalogName(projectId, mainCatalog.getCatalogName());
        DatabaseName mainDatabaseName = StoreConvertor.databaseName(projectId,
            mainCatalogName.getCatalogName(), databaseName);
        Database mainDatabase = createDatabasePrepareTest(mainCatalogName, databaseName);

        List<Table> tables = createTablesPrepareTest(mainDatabaseName, 1);
        Table table = tables.get(0);
        String tableName = table.getTableName();
        TableName mainTableName = StoreConvertor.tableName(projectId, mainDatabaseName.getCatalogName(),
            mainDatabaseName.getDatabaseName(), tableName);

        for (int i = 0; i < 60; i++) {
            partitionService.addPartition(mainTableName, buildPartition(table, "partition=" + i,
                "partition_path"));
        }

        List<String> ptNames = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            ptNames.add("partition=" + i);
        }
        DropPartitionInput dropPartitionInput = new DropPartitionInput();
        dropPartitionInput.setPartitionNames(ptNames);
        partitionService.dropPartition(mainTableName, dropPartitionInput);
        Partition[] partitions = partitionService.listPartitions(mainTableName, (FilterInput)null);
        assertEquals(30, partitions.length);
        HashSet<String> names = new HashSet<>();
        for(Partition partition : partitions) {
            names.add(partition.getPartitionValues().get(0));
        }

        assertEquals(true, names.contains("30"));
        assertEquals(true, names.contains("45"));
        assertEquals(true, names.contains("59"));
    }

    @Test
    public void listFileSubBranchTest() {
        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String mainBranchDatabaseName = UUID.randomUUID().toString().toLowerCase();
        String subBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
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

        TableName mainTablePathName = StoreConvertor.tableName(projectId, mainDatabasePathName.getCatalogName(),
            mainDatabasePathName.getDatabaseName(), mainBranchTableList.get(0).getTableName());
        partitionService.addPartition(mainTablePathName, buildPartition(mainBranchTableList.get(0),
            "partition=1", "path1"));

        //get table from sub-branch
        TableName subBranchTablePathName = StoreConvertor.tableName(projectId, subBranchCatalogRecord.getCatalogName(),
            mainBranchDatabaseRecord.getDatabaseName(), mainBranchTableList.get(0).getTableName());

        logger.info("test list files " + subBranchTablePathName);
        Partition[] tablePartitions = partitionService.listPartitions(subBranchTablePathName, (FilterInput)null);
        assertEquals(0, tablePartitions.length);


        Table subBranchTable = tableService.getTableByName(subBranchTablePathName);
        partitionService.addPartition(subBranchTablePathName, buildPartition(subBranchTable,
            "partition=2", "path2"));
        Partition[] tablePartitionNews = partitionService.listPartitions(subBranchTablePathName, (FilterInput)null);
        assertEquals(1, tablePartitionNews.length);
        assertEquals(1, tablePartitionNews[0].getDataFiles().size());

    }

    private Expression buildFilter(String stringFilter, BigDecimal intFilter) {
        if (stringFilter == null && intFilter == null) {
            return null;
        }
        Expression equal = null;
        if (stringFilter != null) {
            FieldExpression stringField = new FieldExpression(0, DataTypes.STRING);
            equal = new EqualExpression(stringField, new LiteralExpression(
                new StringWritable(stringFilter), DataTypes.STRING));
        }
        Expression gt = null;
        if (intFilter != null) {
            FieldExpression intField = new FieldExpression(1, DataTypes.INTEGER);
            gt = new GreaterThanExpression(intField, new LiteralExpression(
                new IntWritable(intFilter.intValue()), DataTypes.INTEGER));

        }
        if (equal != null && gt != null) {
            List<Expression> operands = new ArrayList<>(2);
            operands.add(equal);
            operands.add(gt);
            return new AndExpression(operands, DataTypes.BOOLEAN);
        }
        return equal != null ? equal : gt;
    }

    @Disabled
    public void should_list_files_with_filter_success() throws CatalogServerException {
        String mainBranchCatalogName = UUID.randomUUID().toString().toLowerCase();
        String mainBranchDatabaseName = UUID.randomUUID().toString().toLowerCase();
        int mainBranchTableNum = 1;

        //create branch catalog, database, tables
        List<Table> mainBranchTableList = prepareBranchTestWithCreateTable(mainBranchCatalogName,
            mainBranchDatabaseName, mainBranchTableNum);

        Table mainBranchTable = mainBranchTableList.get(0);
        TableName mainBranchTableName = StoreConvertor
            .tableName(projectId, mainBranchTable.getCatalogName(),
                mainBranchTable.getDatabaseName(), mainBranchTable.getTableName());

        for (int i = 0; i < 2; i++) {
            partitionService.addPartition(mainBranchTableName, buildPartition(mainBranchTableList.get(0),
                "partition" + i,
                "partition_path"));
        }

        FilterInput filterInput = new FilterInput();
        Expression expression = buildFilter("bbc", null);
        filterInput.setExpressionGson(GsonUtil.toJson(expression));

        Partition[] tablePartitions = partitionService.listPartitions(mainBranchTableName, filterInput);
        assertEquals(1, tablePartitions.length);
        assertEquals(1, tablePartitions[0].getDataFiles().size());
    }

    @Test
    public void should_success_list_files_with_right_schema_after_alter_col() throws NoSuchFieldException, IllegalAccessException {
        String catalogName = UUID.randomUUID().toString().toLowerCase();
        String databaseName = UUID.randomUUID().toString().toLowerCase();

        modifyTableStoreImplPrivateByReflexes(10, 512, 100, 5);
        Catalog mainCatalog = createCatalogPrepareTest(projectId, catalogName);

        CatalogName mainCatalogName = StoreConvertor.catalogName(projectId, mainCatalog.getCatalogName());
        DatabaseName mainDatabaseName = StoreConvertor.databaseName(projectId,
            mainCatalogName.getCatalogName(), databaseName);
        Database mainDatabase = createDatabasePrepareTest(mainCatalogName, databaseName);

        List<Table> tables = createTablesPrepareTest(mainDatabaseName, 1);
        Table table = tables.get(0);
        String tableName = table.getTableName();
        TableName mainTableName = StoreConvertor.tableName(projectId, mainDatabaseName.getCatalogName(),
            mainDatabaseName.getDatabaseName(), tableName);

        partitionService.addPartition(mainTableName, buildPartition(table, "partition=1",
            "partition_path"));

        ColumnChangeInput colChangeIn = new ColumnChangeInput();
        colChangeIn.setChangeType(Operation.ADD_COLUMN);
        List<Column> addColumnList = new ArrayList<>();
        addColumnList.add(new Column("newColName1", "STRING"));
        colChangeIn.setColumnList(addColumnList);
        tableService.alterColumn(mainTableName, colChangeIn);
        partitionService.addPartition(mainTableName, buildPartition(table, "partition=2",
            "partition_path"));

        Partition[] tablePartitions = partitionService.listPartitions(mainTableName, (FilterInput)null);
        assertEquals(2, tablePartitions.length);
        assertEquals(3, tablePartitions[0].getStorageDescriptor().getColumns().size());
        assertEquals(4, tablePartitions[1].getStorageDescriptor().getColumns().size());
    }

}
