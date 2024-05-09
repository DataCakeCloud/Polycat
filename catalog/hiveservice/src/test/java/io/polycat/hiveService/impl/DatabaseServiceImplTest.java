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
package io.polycat.hiveService.impl;

import java.util.List;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.model.CatalogName;
import io.polycat.catalog.common.model.Database;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.TraverseCursorResult;
import io.polycat.catalog.common.plugin.request.input.CatalogInput;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;


import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

public class DatabaseServiceImplTest extends TestUtil {

    private static final String CATALOG_NAME = "testc7";
    private static final String TEST_DB1_NAME = "testdb1";
    private static final String PROJECT_ID = "shenzhen";
    public static final String DATABASE_NAME = "db1";


    @Test
    public void should_create_database_success() {
        try {
            CatalogInput catalogInput = getCatalogInput(CATALOG_NAME);
            catalogService.createCatalog(PROJECT_ID, catalogInput);
            CatalogName catalogName = new CatalogName(PROJECT_ID, CATALOG_NAME);

            DatabaseName databaseName = new DatabaseName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME);
            DatabaseInput databaseInput = getDatabaseInput(databaseName);
            databaseService.createDatabase(catalogName, databaseInput);

            Database actual = databaseService.getDatabaseByName(databaseName);
            assertEquals(databaseName.getDatabaseName(), actual.getDatabaseName());

            databaseService.dropDatabase(databaseName, "false");
            catalogService.dropCatalog(catalogName);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void  should_drop_database_success() {
        try {
            CatalogInput catalogInput = getCatalogInput(CATALOG_NAME);
            catalogService.createCatalog(PROJECT_ID, catalogInput);
            CatalogName catalogName = new CatalogName(PROJECT_ID, CATALOG_NAME);

            DatabaseName databaseName = new DatabaseName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME);
            DatabaseInput databaseInput = getDatabaseInput(databaseName);
            databaseService.createDatabase(catalogName, databaseInput);
            Database actual = databaseService.getDatabaseByName(databaseName);
            assertEquals(databaseName.getDatabaseName(), actual.getDatabaseName());

            databaseService.dropDatabase(databaseName, "false");
            Throwable exception = assertThrows(CatalogServerException.class, ()->databaseService.getDatabaseByName(databaseName));
            assertEquals(String.format("NoSuchObjectException(message:%s)", DATABASE_NAME), exception.getMessage());

            catalogService.dropCatalog(catalogName);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void should_alter_database_success() {
        try {
            CatalogInput catalogInput = getCatalogInput(CATALOG_NAME);
            catalogService.createCatalog(PROJECT_ID, catalogInput);
            CatalogName catalogName = new CatalogName(PROJECT_ID, CATALOG_NAME);

            DatabaseName databaseName = new DatabaseName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME);
            DatabaseInput databaseInput = getDatabaseInput(databaseName);
            databaseService.createDatabase(catalogName, databaseInput);
            Database actual = databaseService.getDatabaseByName(databaseName);
            assertEquals(databaseName.getDatabaseName(), actual.getDatabaseName());
            
            databaseInput.setDescription(NEW_DESCRIPTION);
            databaseService.alterDatabase(databaseName, databaseInput);
            actual = databaseService.getDatabaseByName(databaseName);
            assertEquals(NEW_DESCRIPTION, actual.getDescription());

            databaseService.dropDatabase(databaseName, "false");
            catalogService.dropCatalog(catalogName);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void should_get_databases_success() {
        try {
            CatalogInput catalogInput = getCatalogInput(CATALOG_NAME);
            catalogService.createCatalog(PROJECT_ID, catalogInput);
            CatalogName catalogName = new CatalogName(PROJECT_ID, CATALOG_NAME);

            DatabaseName databaseName1 = new DatabaseName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME + "1");
            DatabaseInput databaseInput1 = getDatabaseInput(databaseName1);
            databaseService.createDatabase(catalogName, databaseInput1);

            DatabaseName databaseName2 = new DatabaseName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME + "2");
            DatabaseInput databaseInput2 = getDatabaseInput(databaseName2);
            databaseService.createDatabase(catalogName, databaseInput2);

            TraverseCursorResult<List<String>> DBs = databaseService.getDatabaseNames(catalogName, false, Integer.MAX_VALUE,
                null, "db*");
            assertEquals(2, DBs.getResult().size());

            DBs = databaseService.getDatabaseNames(catalogName, false, Integer.MAX_VALUE,
                null, "*1");
            assertEquals(1, DBs.getResult().size());

            databaseService.dropDatabase(databaseName1, "false");
            databaseService.dropDatabase(databaseName2, "false");
            catalogService.dropCatalog(catalogName);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

}
