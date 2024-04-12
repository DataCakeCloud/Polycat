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

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.Catalog;
import io.polycat.catalog.common.model.CatalogName;
import io.polycat.catalog.common.model.Column;
import io.polycat.catalog.common.model.Database;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.Table;
import io.polycat.catalog.common.model.Partition;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.plugin.request.input.CatalogInput;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;
import io.polycat.catalog.common.plugin.request.input.FilterInput;
import io.polycat.catalog.common.plugin.request.input.MergeBranchInput;
import io.polycat.catalog.common.plugin.request.input.TableInput;
import io.polycat.catalog.common.utils.UuidUtil;
import io.polycat.catalog.store.common.StoreConvertor;

import io.polycat.catalog.common.model.TableName;

import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.BeforeAll;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
public class CatalogServiceMultiThreadsTest extends TestUtil {

    private static final Logger logger = Logger.getLogger(CatalogServiceMultiThreadsTest.class);

    static final String PROJECT_ID = "CatalogMTTestProject_" + UUID.randomUUID().toString().toLowerCase().substring(0, 8).toLowerCase();
    static Boolean flag = false;

    @BeforeAll
    public static void setUp() throws Exception {
        clearFDB();
    }

    @Test
    public void multiThreadCreateCatalog() throws InterruptedException {
        int threadNum = 2;
        Runnable runnable = () -> {
            String catalogName = "multiThread_" + UUID.randomUUID().toString().toLowerCase();
            CatalogInput catalogInput = getCatalogInput(catalogName);
            catalogService.createCatalog(PROJECT_ID, catalogInput);
            CatalogName catalogPathName = StoreConvertor.catalogName(PROJECT_ID, catalogName);
            Catalog catalogRecordByName = catalogService.getCatalog(catalogPathName);
            assertNotNull(catalogRecordByName);
            assertTrue(catalogRecordByName.getCatalogName().equals(catalogName));
        };
        List<Throwable> uncaught = multiThreadExecute(runnable, "create", threadNum);

        assertEquals(uncaught.size(), 0);
    }

    @Test
    public void multiThreadCreateCatalogWithSameName() throws InterruptedException {
        String catalogName = "catalogTest_654321";

        int threadNum = 2;
        Runnable runnable = () -> {
            CatalogInput catalogInput = getCatalogInput(catalogName);
            catalogService.createCatalog(PROJECT_ID, catalogInput);
            CatalogName catalogPathName = StoreConvertor.catalogName(PROJECT_ID, catalogName);
            Catalog catalogRecordByName = catalogService.getCatalog(catalogPathName);
            assertNotNull(catalogRecordByName);
            assertTrue(catalogRecordByName.getCatalogName().equals(catalogName));
        };
        List<Throwable> uncaught = multiThreadExecute(runnable, "create", threadNum);

        assertEquals(uncaught.size(), 1);
        for (Throwable e : uncaught) {
            assertEquals(e.getMessage(), String.format("Catalog [catalogTest_654321] already exists"));
        }
    }

    @Test
    public void  createCatalogWithCommitUnknownException() {
        flag = true;
        MockUp<TransactionContext> transactionContextMockUp = new MockUp<TransactionContext>() {
            @Mock
            public void commit(Invocation invocation) {
                invocation.proceed();
                if (flag) {
                    flag = false;
                    throw new RuntimeException("catalog commitUnknow mock");
                }
            }
        };

        String catalogName = "CommitUnknown_"+ UUID.randomUUID().toString().toLowerCase();
        CatalogInput catalogInput = getCatalogInput(catalogName);
        catalogService.createCatalog(PROJECT_ID, catalogInput);

        transactionContextMockUp.tearDown();
    }

    @Test
    public void multiThreadDropCatalog() throws InterruptedException {
        String catalogName = UUID.randomUUID().toString().toLowerCase();
        CatalogInput catalogInput = getCatalogInput(catalogName);
        catalogService.createCatalog(PROJECT_ID, catalogInput);

        CatalogName catalogPathName = StoreConvertor.catalogName(PROJECT_ID, catalogName);
        Catalog catalogRecord = catalogService.getCatalog(catalogPathName);
        assertNotNull(catalogRecord);
        assertTrue(catalogRecord.getCatalogName().equals(catalogName));

        int threadNum = 2;
        Runnable runnable = () -> {
            catalogService.dropCatalog(catalogPathName);
        };
        List<Throwable> uncaught = multiThreadExecute(runnable, "drop", threadNum);

        MetaStoreException exception = assertThrows(MetaStoreException.class,
            () -> catalogService.getCatalog(catalogPathName));
        assertEquals(exception.getMessage(), String.format("Catalog [%s] not found", catalogPathName.getCatalogName()));

        assertEquals(uncaught.size(), threadNum - 1);
        for (Throwable e : uncaught) {
            assertEquals(e.getMessage(), String.format("Catalog [%s] not found", catalogPathName.getCatalogName()));
        }
    }

    @Test
    public void multiThreadAlterCatalogName() throws InterruptedException {
        String catalogName = UuidUtil.generateUUID32();
        CatalogInput catalogInput = getCatalogInput(catalogName);
        catalogService.createCatalog(PROJECT_ID, catalogInput);
        CatalogName catalogPathName = StoreConvertor.catalogName(PROJECT_ID, catalogName);
        Catalog catalogRecord = catalogService.getCatalog(catalogPathName);
        assertNotNull(catalogRecord);
        assertTrue(catalogRecord.getCatalogName().equals(catalogName));

        String catalogNameNew = UuidUtil.generateUUID32();
        CatalogInput catalogInputNew = getCatalogInput(catalogNameNew);
        int threadNum = 2;
        Runnable runnable = () -> {
            catalogService.alterCatalog(catalogPathName, catalogInputNew);
        };
        List<Throwable> uncaught = multiThreadExecute(runnable, "alter", threadNum);

        CatalogName catalogPathNameNew = StoreConvertor.catalogName(PROJECT_ID, catalogNameNew);
        catalogRecord = catalogService.getCatalog(catalogPathNameNew);
        assertNotNull(catalogRecord);
        assertTrue(catalogRecord.getCatalogName().equals(catalogNameNew));

        assertEquals(1, uncaught.size());
        for (Throwable e : uncaught) {
            assertEquals(e.getMessage(), String.format("Catalog [%s] not found", catalogName));
        }
    }

    private static MergeBranchInput createMergeBranchInput(String userId, String catalogName,
        String srcBranchName, String destBranchName) {
        MergeBranchInput mergeBranchInput = new MergeBranchInput();

        mergeBranchInput.setOwner(userId);
        mergeBranchInput.setSrcBranchName(srcBranchName);
        mergeBranchInput.setDestBranchName(destBranchName);

        return mergeBranchInput;
    }

    private void prepareMultiThreadMergeBranch(String catalogName, String databaseName, String tableName) {
        CatalogInput catalogInput = getCatalogInput(catalogName);
        Catalog catalog = catalogService.createCatalog(PROJECT_ID, catalogInput);
        assertNotNull(catalog);
        CatalogName catalogPathName = StoreConvertor.catalogName(PROJECT_ID, catalogName);

        DatabaseName databasePathName = StoreConvertor.databaseName(catalogPathName, databaseName);
        DatabaseInput databaseInput = getDatabaseDTO(PROJECT_ID, catalogNameString, databaseName);
        Database database = databaseService.createDatabase(catalogPathName, databaseInput);
        assertNotNull(database);

        TableInput tableInput = getTableDTO(tableName, 3);
        tableInput.setLmsMvcc(true);
        tableInput.setPartitionKeys(Collections.singletonList(new Column("partition", "String")));
        tableService.createTable(databasePathName, tableInput);
    }

    private void prepareBranchPartition(Catalog catalogRecord, Database database,
        Table table, String branchName) {
        CatalogName catalogName = StoreConvertor.catalogName(PROJECT_ID, catalogRecord.getCatalogName());
        Catalog branchRecord1 = createBranch(catalogName, branchName, PROJECT_ID);
        assertNotNull(branchRecord1);

        TableName branchTableName1 = StoreConvertor
            .tableName(PROJECT_ID, branchName, database.getDatabaseName(), table.getTableName());
        Table table1 = tableService.getTableByName(branchTableName1);
        partitionService.addPartition(branchTableName1,
            buildPartition(table1, "partition=" + branchName, "path"));
    }

    @Test
    public void multiThreadMergeBranch() throws InterruptedException {
        String catalogName = UUID.randomUUID().toString().toLowerCase();
        String databaseName = UUID.randomUUID().toString().toLowerCase();
        String tableName = UUID.randomUUID().toString().toLowerCase();

        System.out.println(databaseName);

        prepareMultiThreadMergeBranch(catalogName, databaseName, tableName);
        CatalogName catalogPathName = StoreConvertor.catalogName(PROJECT_ID, catalogName);
        Catalog catalogRecord = catalogService.getCatalog(catalogPathName);
        DatabaseName databasePathName = StoreConvertor.databaseName(PROJECT_ID, catalogName, databaseName);
        Database database = databaseService.getDatabaseByName(databasePathName);
        TableName tablePathName = StoreConvertor
            .tableName(PROJECT_ID, catalogName, databaseName, tableName);
        Table table = tableService.getTableByName(tablePathName);

        partitionService.addPartition(tablePathName,
            buildPartition(table, "partition=main", "path"));

        //create branch1
        String branchName1 = "branch1_" + UUID.randomUUID().toString().toLowerCase();
        prepareBranchPartition(catalogRecord, database, table, branchName1);
        //create branch2
        String branchName2 = "branch2_" + UUID.randomUUID().toString().toLowerCase();
        prepareBranchPartition(catalogRecord, database, table, branchName2);

        //merge branch1 and branch2
        MergeBranchInput mergeBranchInput1 = createMergeBranchInput(userId, catalogName,
            branchName1, catalogName);
        catalogService.mergeBranch(PROJECT_ID, mergeBranchInput1);
        MergeBranchInput mergeBranchInput2 = createMergeBranchInput(userId, catalogName,
            branchName2, catalogName);
        catalogService.mergeBranch(PROJECT_ID, mergeBranchInput2);

        int threadNum = 2;
        Thread[] threads = new Thread[threadNum];
        MergeBranchInput[] mergeBranchInputs = {mergeBranchInput1, mergeBranchInput2};
        for (int i = 0; i < threadNum; i ++) {
            MergeBranchInput mergeBranchInput = mergeBranchInputs[i];
            threads[i] = new Thread(() -> {
                catalogService.mergeBranch(PROJECT_ID, mergeBranchInput);
            }, "merge-branch" + i);
        }

        List<Throwable> uncaught = multiThreadExecute(threads);
        assertEquals(uncaught.size(), 0);

        Partition[] partitions = partitionService.listPartitions(tablePathName, (FilterInput)null);
        assertEquals(partitions.length, 3);
    }
}
