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

import io.polycat.catalog.common.model.CatalogName;
import io.polycat.catalog.common.model.Database;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.Role;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;
import io.polycat.catalog.common.plugin.request.input.RoleInput;
import io.polycat.catalog.service.api.DatabaseService;
import io.polycat.catalog.service.api.RoleService;
import io.polycat.catalog.store.common.StoreConvertor;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.HashMap;


@Slf4j
@SpringBootTest
public class PGDatabaseServiceImplTest extends PGCatalogServiceImplTest {

    public static final String DATABASE_NAME = "database_test";

    @Autowired
    private DatabaseService databaseService;

    @BeforeEach
    public void before() {
        create_catalog_should_success();
    }

    @Test
    public void create_database_should_success() {
        DatabaseInput databaseInput = getDatabaseDTO(PROJECT_ID, CATALOG_NAME, DATABASE_NAME);
        CatalogName catalogName = new CatalogName(PROJECT_ID, CATALOG_NAME);
        databaseService.createDatabase(catalogName, databaseInput);

        DatabaseName databaseName = StoreConvertor.databaseName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME);
        Database databaseByName = databaseService.getDatabaseByName(databaseName);
        valueAssertNotNull(databaseByName);
        valueAssertEquals(DATABASE_NAME, databaseByName.getDatabaseName());
    }

    @Test
    public void alter_database_should_success() {
        DatabaseInput databaseInput = getDatabaseDTO(PROJECT_ID, CATALOG_NAME, DATABASE_NAME);
        CatalogName catalogName = new CatalogName(PROJECT_ID, CATALOG_NAME);
        databaseService.createDatabase(catalogName, databaseInput);

        DatabaseName databaseName = StoreConvertor.databaseName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME);
        Database databaseByName = databaseService.getDatabaseByName(databaseName);
        valueAssertNotNull(databaseByName);
        valueAssertEquals(DATABASE_NAME, databaseByName.getDatabaseName());

        HashMap<String, String> map = new HashMap<>();
        map.put("key1", "value1");

        databaseInput.setParameters(map);
        String alterDatabaseName = "alter_database";
        databaseInput.setDatabaseName(alterDatabaseName);
        databaseService.alterDatabase(databaseName, databaseInput);
        databaseName.setDatabaseName(alterDatabaseName);
        Database alterDatabase = databaseService.getDatabaseByName(databaseName);
        valueAssertNotNull(alterDatabase);
        valueAssertEquals(alterDatabaseName, alterDatabase.getDatabaseName());
        valueAssertEquals("value1", alterDatabase.getParameters().get("key1"));
        valueAssertEquals(true, alterDatabase.getParameters().containsKey(LMS_NAME_KEY));
    }
}
