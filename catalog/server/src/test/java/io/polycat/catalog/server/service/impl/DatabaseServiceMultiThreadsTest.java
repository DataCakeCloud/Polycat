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

import java.util.List;
import java.util.UUID;

import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.model.Catalog;
import io.polycat.catalog.common.model.CatalogName;
import io.polycat.catalog.common.model.Database;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.plugin.request.input.CatalogInput;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;
import io.polycat.catalog.store.common.StoreConvertor;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
public class DatabaseServiceMultiThreadsTest extends TestUtil {

    private static final Logger logger = Logger.getLogger(DatabaseServiceMultiThreadsTest.class);

    static final String PROJECT_ID = "MTTestProject_" + UUID.randomUUID().toString().substring(0, 8).toLowerCase();
    static final String CATALOG_NAME = "MTTestCatalog_" + UUID.randomUUID().toString().substring(0, 8).toLowerCase();

    static Catalog newCatalog;
    boolean isFirstTest = true;
    static Boolean flag = true;

    private void beforeClass() throws Exception {

        if (isFirstTest) {
            TestUtil.clearFDB();
            // create catalog
            CatalogName catalogName = new CatalogName(PROJECT_ID, CATALOG_NAME);
            CatalogInput catalogInput = getCatalogInput(catalogName.getCatalogName());
            newCatalog = catalogService.createCatalog(PROJECT_ID, catalogInput);
            isFirstTest = false;
        }
    }

    @BeforeEach
    public void beforeEach() throws Exception {
        beforeClass();
    }

    @Test
    public void multiThreadCreateDatabase() throws InterruptedException {
        int threadNum = 2;
        Runnable runnable = () -> {
            String databaseNameString = UUID.randomUUID().toString();
            CatalogName catalogName = StoreConvertor.catalogName(PROJECT_ID, CATALOG_NAME);
            DatabaseInput databaseInput = getDatabaseDTO(PROJECT_ID, CATALOG_NAME, databaseNameString);
            Database databaseRecord = databaseService.createDatabase(catalogName, databaseInput);
            assertNotNull(databaseRecord);
            assertTrue(databaseRecord.getDatabaseName().equals(databaseNameString));
        };
        List<Throwable> uncaught = multiThreadExecute(runnable, "drop", threadNum);

        assertEquals(uncaught.size(), 0);
    }

    @Test
    public void multiThreadCreateDatabaseWithSameName() throws InterruptedException {
        String databaseNameString = UUID.randomUUID().toString();
        CatalogName catalogName = StoreConvertor.catalogName(PROJECT_ID, CATALOG_NAME);

        int threadNum = 2;
        Runnable runnable = () -> {
            DatabaseInput databaseInput = getDatabaseDTO(PROJECT_ID, CATALOG_NAME, databaseNameString);
            Database databaseRecord = databaseService.createDatabase(catalogName, databaseInput);
            assertNotNull(databaseRecord);
            assertTrue(databaseRecord.getDatabaseName().equals(databaseNameString));
        };
        List<Throwable> uncaught = multiThreadExecute(runnable, "create", threadNum);

        assertEquals(uncaught.size(), 1);
        for (Throwable e : uncaught) {
            assertEquals(e.getMessage(), String.format("Database [%s] already exists", databaseNameString));
        }
    }


    @Test
    public void createDatabaseWithCommitUnknownException() {
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


        String databaseNameString = UUID.randomUUID().toString();
        CatalogName catalogName = StoreConvertor.catalogName(PROJECT_ID, CATALOG_NAME);
        DatabaseInput databaseInput = getDatabaseDTO(PROJECT_ID, CATALOG_NAME, databaseNameString);
        Database databaseRecord = databaseService.createDatabase(catalogName, databaseInput);
        assertNotNull(databaseRecord);
        assertTrue(databaseRecord.getDatabaseName().equals(databaseNameString));

        transactionContextMockUp.tearDown();
    }

    @Test
    public void multiThreadDropDatabase() throws InterruptedException {
        String databaseNameString = UUID.randomUUID().toString();
        CatalogName catalogName = StoreConvertor.catalogName(PROJECT_ID, CATALOG_NAME);
        DatabaseName databaseName = StoreConvertor.databaseName(catalogName, databaseNameString);
        DatabaseInput databaseInput = getDatabaseDTO(PROJECT_ID, CATALOG_NAME, databaseNameString);
        Database databaseRecord = databaseService.createDatabase(catalogName, databaseInput);
        assertNotNull(databaseRecord);
        assertTrue(databaseRecord.getDatabaseName().equals(databaseNameString));

        int threadNum = 2;
        Runnable runnable = () -> {
            databaseService.dropDatabase(databaseName, "false");
            databaseService.getDatabaseByName(databaseName);
        };
        List<Throwable> uncaught = multiThreadExecute(runnable, "drop", threadNum);

        assertEquals(uncaught.size(), 1);
        for (Throwable e : uncaught) {
            assertEquals(e.getMessage(), String.format("Database [%s] not found", databaseName.getDatabaseName()));
        }
    }

    @Test
    public void multiThreadUndropDatabase() throws InterruptedException {
        String databaseNameString = UUID.randomUUID().toString();
        CatalogName catalogName = StoreConvertor.catalogName(PROJECT_ID, CATALOG_NAME);

        DatabaseName databaseName = StoreConvertor.databaseName(catalogName, databaseNameString);
        DatabaseInput databaseInput = getDatabaseDTO(PROJECT_ID, CATALOG_NAME, databaseNameString);
        Database databaseRecord = databaseService.createDatabase(catalogName, databaseInput);
        assertNotNull(databaseRecord);
        assertTrue(databaseRecord.getDatabaseName().equals(databaseNameString));

        databaseService.dropDatabase(databaseName, "false");

        int threadNum = 2;
        String databaseId = databaseRecord.getDatabaseId();
        Runnable runnable = () -> databaseService.undropDatabase(databaseName, databaseId, "");
        List<Throwable> uncaught = multiThreadExecute(runnable, "undrop", threadNum);

        databaseRecord = databaseService.getDatabaseByName(databaseName);
        assertNotNull(databaseRecord);
        assertTrue(databaseRecord.getDatabaseName().equals(databaseNameString));

        assertEquals(uncaught.size(), 1);
        for (Throwable e : uncaught) {
            assertEquals(e.getMessage(), String.format("Database [%s] not found", databaseName.getDatabaseName()));
        }
    }

    @Test
    public void multiThreadAlterDatabaseName() throws InterruptedException {
        String databaseNameString = UUID.randomUUID().toString();
        CatalogName catalogName = StoreConvertor.catalogName(PROJECT_ID, CATALOG_NAME);
        DatabaseName databaseName = StoreConvertor.databaseName(catalogName, databaseNameString);
        DatabaseInput databaseInput = getDatabaseDTO(PROJECT_ID, CATALOG_NAME, databaseNameString);
        Database databaseRecord = databaseService.createDatabase(catalogName, databaseInput);
        assertNotNull(databaseRecord);
        assertTrue(databaseRecord.getDatabaseName().equals(databaseNameString));

        String databaseNameStringNew = UUID.randomUUID().toString();
        DatabaseInput databaseInputNew = getDatabaseDTO(PROJECT_ID, CATALOG_NAME, databaseNameStringNew);
        int threadNum = 2;
        Runnable runnable = () -> {
            databaseService.alterDatabase(databaseName, databaseInputNew);
        };
        List<Throwable> uncaught = multiThreadExecute(runnable, "alter", threadNum);

        DatabaseName databaseNameNew = StoreConvertor.databaseName(catalogName, databaseNameStringNew);
        databaseRecord = databaseService.getDatabaseByName(databaseNameNew);
        assertNotNull(databaseRecord);
        assertTrue(databaseRecord.getDatabaseName().equals(databaseNameStringNew));

        assertEquals(uncaught.size(), 1);
        for (Throwable e : uncaught) {
            assertEquals(e.getMessage(), String.format("Database [%s] not found", databaseNameString));
        }
    }
}
