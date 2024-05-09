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
import io.polycat.catalog.common.model.Column;
import io.polycat.catalog.common.model.Database;
import io.polycat.catalog.common.model.LakeProfile;
import io.polycat.catalog.common.model.StorageDescriptor;
import io.polycat.catalog.common.model.UserProfileContext;
import io.polycat.catalog.common.plugin.request.CreateCatalogRequest;
import io.polycat.catalog.common.plugin.request.CreateDatabaseRequest;
import io.polycat.catalog.common.plugin.request.CreateTableRequest;
import io.polycat.catalog.common.plugin.request.GetLakeProfileRequest;
import io.polycat.catalog.common.plugin.request.input.CatalogInput;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;
import io.polycat.catalog.common.plugin.request.input.TableInput;
import io.polycat.catalog.common.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SDKLakeProfileTest extends SDKTestUtil{
    private final PolyCatClient client = SDKTestUtil.getClient();

    @Test
    public void collectDatabaseAndTableTest() {
        String catalogName = "catalog";
        String databaseName = "database";
        String tableName = "table";
        String locationUri = "/location/uri";
        int dbNum = 10;
        int tableNum = 110;
        String colName1 = "col_name_studentName";
        String colName2 = "col_name_studentAge";

        createCatalog(catalogName);

        for (int i = 0; i < dbNum; i++) {
            createDataBase(catalogName, databaseName+i, locationUri+i);
            for (int j = 0; j < tableNum; j++) {
                createTable(catalogName, databaseName+i, tableName+i+j, locationUri+i+"/"+tableName+i+j, colName1, colName2);
            }
        }

        GetLakeProfileRequest request = new GetLakeProfileRequest(ACCOUNT_ID, PROJECT_ID);
        LakeProfile lakeProfile = client.getLakeProfile(request);
        long tableCount = 0;
        long databaseCount = 0;
        for (UserProfileContext userProfileContext : lakeProfile.getUserProfileContexts()) {
            tableCount += userProfileContext.getTotalTable();
            databaseCount += userProfileContext.getTotalDatabase();
        }
        assertEquals(dbNum, databaseCount);
        assertEquals(tableNum*dbNum, tableCount);
    }

    private String createCatalog(String catalogName) {
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

    private void createDataBase(String catalogName, String databaseName, String locationUri) {
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

    private void createTable(String catalogName, String databaseName, String tableName, String locationUri,
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

        client.createTable(createTableRequest);
        return;
    }
}
