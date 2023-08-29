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
import java.util.List;
import java.util.UUID;

import io.polycat.catalog.common.DataLineageType;
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.model.DataLineage;
import io.polycat.catalog.common.model.DataSource;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.Table;
import io.polycat.catalog.common.model.TableSource;
import io.polycat.catalog.common.plugin.request.input.DataLineageInput;
import io.polycat.catalog.common.plugin.request.input.TableInput;
import io.polycat.catalog.store.common.StoreConvertor;
import io.polycat.catalog.common.model.TableName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
public class DataLineageServiceImplTest extends TestUtil {

    @BeforeEach
    public void clearAndSetup() throws Exception {
        clearFDB();
        createCatalogBeforeClass();
        createDatabaseBeforeClass();
        createTableBeforeClass();
    }

    @Test
    public void getSourceTablesByTableTest() {
        TableName tableName = StoreConvertor
            .tableName(projectId, catalogNameString, databaseNameString, tableNameString);
        Table table = new Table();
        table.setCatalogName(tableName.getCatalogName());
        table.setTableName(tableName.getTableName());
        table.setDatabaseName(tableName.getDatabaseName());

        String name = UUID.randomUUID().toString();
        TableInput tableInput = getTableDTO(name, 3);
        DatabaseName databaseName = StoreConvertor.databaseName(projectId, catalogNameString, databaseNameString);
        tableService.createTable(databaseName, tableInput);
        TableSource sourceTable = new TableSource();
        sourceTable.setProjectId(databaseName.getProjectId());
        sourceTable.setCatalogName(databaseName.getCatalogName());
        sourceTable.setDatabaseName(databaseName.getDatabaseName());
        sourceTable.setTableName(name);
        List<DataSource> sourceTables = new ArrayList<>();
        sourceTables.add(new DataSource(sourceTable));

        DataLineage dataLineage = new DataLineage();
        dataLineage.setDataOutput(new DataSource(new TableSource(projectId, table)));
        dataLineage.setDataInput(sourceTables);
        dataLineage.setOperation(Operation.INSERT_TABLE);

        DataLineageInput dataLineageInput = new DataLineageInput(dataLineage);
        dataLineageService.recordDataLineage(dataLineageInput);

        List<DataLineage> dataLineageList = dataLineageService.getDataLineageByTable(projectId, table.getCatalogName(),
            table.getDatabaseName(), table.getTableName(), DataLineageType.UPSTREAM);
        assertEquals(1, dataLineageList.size());
    }

    @Test
    public void getTargetTablesByTableTest() {
        TableName tableName = StoreConvertor
            .tableName(projectId, catalogNameString, databaseNameString, tableNameString);
        Table table = new Table();
        table.setCatalogName(tableName.getCatalogName());
        table.setTableName(tableName.getTableName());
        table.setDatabaseName(tableName.getDatabaseName());

        String name = UUID.randomUUID().toString();
        TableInput tableInput = getTableDTO(name, 3);
        DatabaseName databaseName = StoreConvertor.databaseName(projectId, catalogNameString, databaseNameString);
        tableService.createTable(databaseName, tableInput);
        TableSource sourceTable = new TableSource();
        sourceTable.setProjectId(databaseName.getProjectId());
        sourceTable.setCatalogName(databaseName.getCatalogName());
        sourceTable.setDatabaseName(databaseName.getDatabaseName());
        sourceTable.setTableName(name);
        List<DataSource> sourceTables = new ArrayList<>();
        sourceTables.add(new DataSource(sourceTable));

        DataLineage dataLineage = new DataLineage();
        dataLineage.setDataOutput(new DataSource(new TableSource(projectId, table)));
        dataLineage.setDataInput(sourceTables);
        dataLineage.setOperation(Operation.INSERT_TABLE);

        DataLineageInput dataLineageInput = new DataLineageInput(dataLineage);
        dataLineageService.recordDataLineage(dataLineageInput);

        List<DataLineage> dataLineageList = dataLineageService.getDataLineageByTable(projectId, sourceTable.getCatalogName(),
            sourceTable.getDatabaseName(), sourceTable.getTableName(), DataLineageType.DOWNSTREAM);
        assertEquals(1, dataLineageList.size());
    }
}
