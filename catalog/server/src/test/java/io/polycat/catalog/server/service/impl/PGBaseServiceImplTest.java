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

import io.polycat.catalog.common.model.Column;
import io.polycat.catalog.common.model.StorageDescriptor;
import io.polycat.catalog.common.plugin.request.input.CatalogInput;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;
import io.polycat.catalog.common.plugin.request.input.TableInput;
import io.polycat.catalog.common.types.DataTypes;
import io.polycat.catalog.common.utils.UuidUtil;
import io.polycat.catalog.service.api.CatalogResourceService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.function.Executable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@SpringBootTest
public class PGBaseServiceImplTest {

    public static final String PROJECT_ID = "project_test";

    public static final String CATALOG_NAME = "catalog_test";

    public static final String userId = "userid_test";

    public static final String LMS_NAME_KEY = "lms_name";

    @Autowired
    private CatalogResourceService catalogResourceService;

    private static CatalogResourceService staticResourceService;


    @BeforeEach
    public void createResource() {
        catalogResourceService.createResource(PROJECT_ID);
        staticResourceService = catalogResourceService;
    }

    @AfterAll
    public static void afterClass() {
        staticResourceService.dropResource(PROJECT_ID);
        log.info("Test finished, clear resource success.");
    }

    public void valueAssertEquals(Object expected, Object actual) {
        assertEquals(expected, actual);
    }

    public void valueAssertNotNull(Object actual) {
        assertNotNull(actual);
    }

    public void functionAssertDoesNotThrow(Executable executable) {
        assertDoesNotThrow(executable);
    }

    public static String getNamePostfix() {
        return UUID.randomUUID().toString().substring(0, 8).toLowerCase();
    }

    public CatalogInput getCatalogInput(String catalogName) {
        CatalogInput catalogInput = new CatalogInput();
        catalogInput.setCatalogName(catalogName);
        catalogInput.setDescription("test");
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
        HashMap<String, String> map = new HashMap<>();
        map.put(LMS_NAME_KEY, CATALOG_NAME);
        databaseInput.setParameters(map);
        return databaseInput;
    }

    public List<Column> getColumnDTO(int columnNum) {
        List<Column> columnInputs = new ArrayList<>(columnNum);

        for (int idx = 0; idx < columnNum; ++idx) {
            Column columnInput = new Column();
            columnInput.setColumnName("column_test_" + idx);
            columnInput.setComment("Field column_test_" + idx);
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
        HashMap<String, String> map = new HashMap<>();
        map.put(LMS_NAME_KEY, CATALOG_NAME);
        tableInput.setParameters(map);
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
}
