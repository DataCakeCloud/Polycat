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
package io.polycat.catalog.client;

import java.util.ArrayList;
import java.util.List;

import io.polycat.catalog.common.model.Catalog;
import io.polycat.catalog.common.model.Database;
import io.polycat.catalog.common.model.StorageDescriptor;
import io.polycat.catalog.common.model.Table;
import io.polycat.catalog.common.plugin.request.CreateCatalogRequest;
import io.polycat.catalog.common.plugin.request.CreateDatabaseRequest;
import io.polycat.catalog.common.plugin.request.CreateTableRequest;
import io.polycat.catalog.common.plugin.request.GetCatalogRequest;
import io.polycat.catalog.common.plugin.request.GetTableRequest;
import io.polycat.catalog.common.plugin.request.input.CatalogInput;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;
import io.polycat.catalog.common.plugin.request.input.TableInput;
import io.polycat.catalog.common.model.Column;

import io.polycat.catalog.common.types.DataTypes;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class SDKDDLCatalogBasicTest extends SDKTestUtil {

    private final PolyCatClient client = SDKTestUtil.getClient();

    @Test
    public void should_success_create_get_delete_catalog() {
        // Parameters to make requests
        String catalogName = "catalog_name_xyz";
        String databaseName = "database_name_zhang3";
        String tableName = "table_name_t1";
        String locationUri = "/location/uri";

        // Parameters get from response when Create,Get,Delete requests succeeded
        String databaseId = "";
        String tableId = "";

        // Case: create catalog
        createCatalog(catalogName);

        // Case: get catalog by name
        GetCatalogRequest getCatalogRequest = new GetCatalogRequest(PROJECT_ID, catalogName);
        Catalog catalog = client.getCatalog(getCatalogRequest);
        assertEquals(catalogName, catalog.getCatalogName());
        assertNotEquals(0, catalog.getCreateTime());

        // Case: create database
        createDataBase(catalogName, databaseName, locationUri);

        // Case: create table
        String colName1 = "col_name_studentName";
        String colName2 = "col_name_studentAge";

        Table table = createTable(catalogName, databaseName, tableName, locationUri, colName1, colName2);
        String tableNameBak = table.getTableName();
        assertNotEquals("", tableNameBak);

        // Case: get table
        getTable(catalogName, databaseName, tableName, colName1, colName2);
    }

    private String createCatalog(String catalogName) {
        String catalogId;
        CatalogInput catalogInput = new CatalogInput();
        catalogInput.setCatalogName(catalogName);
        catalogInput.setOwner(OWNER);

        CreateCatalogRequest createCatalogRequest = new CreateCatalogRequest();
        createCatalogRequest.setProjectId(PROJECT_ID);
        createCatalogRequest.setInput(catalogInput);

        Catalog catalog = client.createCatalog(createCatalogRequest);
        assertEquals(catalogName, catalog.getCatalogName());
        return catalogName;
    }

    private void getTable(String catalogName, String databaseName, String tableName, String colName1, String colName2) {
        Table table;
        GetTableRequest getTableRequest = new GetTableRequest(tableName);
        getTableRequest.setProjectId(PROJECT_ID);
        getTableRequest.setCatalogName(catalogName);
        getTableRequest.setDatabaseName(databaseName);

        table = client.getTable(getTableRequest);
        List<Column> columns = table.getStorageDescriptor().getColumns();
        assertEquals(catalogName, table.getCatalogName());
        assertEquals(databaseName, table.getDatabaseName());
        assertEquals(tableName, table.getTableName());
        assertEquals(colName1.toLowerCase(), columns.get(0).getColumnName().toLowerCase());
        assertEquals(colName2.toLowerCase(), columns.get(1).getColumnName().toLowerCase());
        assertNotNull(table.getCreateTime());
        assertNotNull(table.getStorageDescriptor().getLocation());
    }

    private Table createTable(String catalogName, String databaseName, String tableName, String locationUri,
        String colName1, String colName2) {
        List<Column> columns = new ArrayList<>();
        columns.add(new Column(colName1, DataTypes.STRING.getName()));
        columns.add(new Column(colName2, DataTypes.INT.getName()));
        StorageDescriptor si = new StorageDescriptor();
        si.setLocation(locationUri);
        si.setColumns(columns);

        TableInput tableInput = new TableInput();
        tableInput.setTableName(tableName);
        tableInput.setStorageDescriptor(si);
        tableInput.setOwner(OWNER);

        CreateTableRequest createTableRequest = new CreateTableRequest();
        createTableRequest.setProjectId(PROJECT_ID);
        createTableRequest.setCatalogName(catalogName);
        createTableRequest.setDatabaseName(databaseName);
        createTableRequest.setInput(tableInput);

        return client.createTable(createTableRequest);
    }

    private void createDataBase(String catalogName, String databaseName, String locationUri) {
        String databaseId;
        DatabaseInput databaseInput = getDatabaseInput(catalogName, databaseName, locationUri, OWNER);

        CreateDatabaseRequest createDatabaseRequest = new CreateDatabaseRequest();
        createDatabaseRequest.setProjectId(PROJECT_ID);
        createDatabaseRequest.setCatalogName(catalogName);
        createDatabaseRequest.setInput(databaseInput);

        Database database = client.createDatabase(createDatabaseRequest);
        assertEquals(catalogName, database.getCatalogName());
        assertEquals(databaseName, database.getDatabaseName());
        assertEquals(0, database.getDroppedTime());
    }
}
