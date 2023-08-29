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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import io.polycat.catalog.common.model.CatalogName;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.Table;
import io.polycat.catalog.common.model.TableUsageProfile;
import io.polycat.catalog.common.plugin.request.input.TableInput;
import io.polycat.catalog.common.plugin.request.input.TableUsageProfileInput;
import io.polycat.catalog.common.plugin.request.input.TopTableUsageProfileInput;
import io.polycat.catalog.store.common.StoreConvertor;
import io.polycat.catalog.common.model.TableName;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
public class UsageProfileServiceImplTest extends TestUtil {

    boolean isFirstTest = true;
    private void setUp() {
        if (isFirstTest) {
            createCatalogBeforeClass();
            createDatabaseBeforeClass();
            createTableBeforeClass();
            isFirstTest = false;
        }
    }

    @BeforeEach
    public void clearUsageProfile() {
        setUp();
        usageProfileService.deleteUsageProfilesByTime(projectId, 0, Integer.MAX_VALUE);
    }

    @Test
    public void recordUsageProfileTest() {
        TableName tableName = StoreConvertor
            .tableName(projectId, catalogNameString, databaseNameString, tableNameString);
        List<TableUsageProfile> tableUsageProfileList = new ArrayList<>();
        TableUsageProfileInput tableUsageProfileInput = new TableUsageProfileInput(tableUsageProfileList);
        Table table = new Table();
        table.setCatalogName(tableName.getCatalogName());
        table.setDatabaseName(tableName.getDatabaseName());
        table.setTableName(tableName.getTableName());
        TableUsageProfile tableUsageProfile = new TableUsageProfile(projectId, table, "READ", 1000,
           new BigInteger("100"));
        tableUsageProfileInput.getTableUsageProfiles().add(tableUsageProfile);

        usageProfileService.recordTableUsageProfile(tableUsageProfileInput);
    }

    @Test
    void getTopHotTablesByCountTest() {
        List<TableUsageProfile> tableUsageProfileList = new ArrayList<>();
        TableUsageProfileInput tableUsageProfileInput = new TableUsageProfileInput(tableUsageProfileList);
        TableName tableName = StoreConvertor
            .tableName(projectId, catalogNameString, databaseNameString, tableNameString);
        Table table = new Table();
        table.setCatalogName(tableName.getCatalogName());
        table.setDatabaseName(tableName.getDatabaseName());
        table.setTableName(tableName.getTableName());

        TableUsageProfile tableUsageProfile = new TableUsageProfile(projectId, table, "WRITE", 1000,
            new BigInteger("100"));
        tableUsageProfileInput.getTableUsageProfiles().add(tableUsageProfile);

        //creat table
        String name = UUID.randomUUID().toString();
        TableInput tableInput = getTableDTO(name, 3);
        DatabaseName databaseName = StoreConvertor.databaseName(projectId, catalogNameString, databaseNameString);
        tableService.createTable(databaseName, tableInput);
        //record usageProfile table
        Table tableNew = new Table();
        tableNew.setCatalogName(catalogNameString);
        tableNew.setDatabaseName(databaseNameString);
        tableNew.setTableName(name);
        TableUsageProfile tableUsageProfileNew = new TableUsageProfile(projectId, tableNew, "WRITE", 1000,
            new BigInteger("200"));

        tableUsageProfileInput.getTableUsageProfiles().add(tableUsageProfileNew);
        usageProfileService.recordTableUsageProfile(tableUsageProfileInput);

        CatalogName catalogName = StoreConvertor.catalogName(projectId, catalogNameString);
        TopTableUsageProfileInput topTableUsageProfileInput = new TopTableUsageProfileInput(1000,
            1000, "WRITE", 3, 0);
        List<TableUsageProfile> tableUsageProfiles = usageProfileService.getTopTablesByCount(catalogName.getProjectId(),
            catalogName.getCatalogName(), topTableUsageProfileInput);
        assertEquals(2, tableUsageProfiles.size());
        assertEquals(new BigInteger("200"), tableUsageProfiles.get(0).getSumCount());

        TopTableUsageProfileInput topColdTableUsageProfileInput = new TopTableUsageProfileInput(1000,
            1000, "WRITE", 3, 1);
        List<TableUsageProfile> tableUsageProfilesByCold = usageProfileService
            .getTopTablesByCount(catalogName.getProjectId(), catalogName.getCatalogName(),
                topColdTableUsageProfileInput);
        assertEquals(2, tableUsageProfilesByCold.size());
        assertEquals(new BigInteger("100"), tableUsageProfilesByCold.get(0).getSumCount());
    }

    @Test
    void getUsageProfileByTableTest() {
        TableName tableName = StoreConvertor
            .tableName(projectId, catalogNameString, databaseNameString, tableNameString);
        List<TableUsageProfile> tableUsageProfileList = new ArrayList<>();
        TableUsageProfileInput tableUsageProfileInput = new TableUsageProfileInput(tableUsageProfileList);
        Table table = new Table();
        table.setCatalogName(tableName.getCatalogName());
        table.setDatabaseName(tableName.getDatabaseName());
        table.setTableName(tableName.getTableName());

        TableUsageProfile tableUsageProfile = new TableUsageProfile(projectId, table, "READ", 1000,
            new BigInteger("100"));
        tableUsageProfileInput.getTableUsageProfiles().add(tableUsageProfile);
        usageProfileService.recordTableUsageProfile(tableUsageProfileInput);

        List<TableUsageProfile> tableUsageProfiles = usageProfileService.getUsageProfileByTable(tableName,
            0, 0, null, null, null);
        assertEquals(tableUsageProfiles.size(), 1);
    }

    @Test
    public void deleteUsageProfilesByTimeTest() {
        List<TableUsageProfile> tableUsageProfileList = new ArrayList<>();
        TableUsageProfileInput tableUsageProfileInput = new TableUsageProfileInput(tableUsageProfileList);
        TableName tableName = StoreConvertor
            .tableName(projectId, catalogNameString, databaseNameString, tableNameString);
        Table table = new Table();
        table.setCatalogName(tableName.getCatalogName());
        table.setDatabaseName(tableName.getDatabaseName());
        table.setTableName(tableName.getTableName());

        long time = 10003;
        TableUsageProfile tableUsageProfile1 = new TableUsageProfile(projectId, table, "WRITE", time,
            new BigInteger("100"));
        tableUsageProfileInput.getTableUsageProfiles().add(tableUsageProfile1);
        time = 10004;
        TableUsageProfile tableUsageProfile2 = new TableUsageProfile(projectId, table, "READ", time,
            new BigInteger("100"));
        tableUsageProfileInput.getTableUsageProfiles().add(tableUsageProfile2);
        usageProfileService.recordTableUsageProfile(tableUsageProfileInput);

        List<TableUsageProfile> tableUsageProfileList1 = usageProfileService.getUsageProfileByTable(tableName,
            10003, 10004, null, null, null);
        assertEquals(tableUsageProfileList1.size(), 2);

        usageProfileService.deleteUsageProfilesByTime(projectId, 10003, 10003);
        List<TableUsageProfile> tableUsageProfileList2 = usageProfileService.getUsageProfileByTable(tableName,
            10003, 10004, null, null, null);
        assertEquals(tableUsageProfileList2.size(), 1);
    }
}
