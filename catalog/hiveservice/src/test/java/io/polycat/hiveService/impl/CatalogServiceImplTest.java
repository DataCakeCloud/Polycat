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
import java.util.stream.Collectors;

import io.polycat.catalog.common.model.Catalog;
import io.polycat.catalog.common.model.CatalogName;
import io.polycat.catalog.common.model.TraverseCursorResult;
import io.polycat.catalog.common.plugin.request.input.CatalogInput;
import io.polycat.catalog.common.CatalogServerException;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Disabled("need set up Hive environment")
public class CatalogServiceImplTest extends TestUtil {
    private static final String CATALOG_NAME = "testc7";
    private static final String TEST_DB1_NAME = "testdb1";
    private static final String TEST_DB2_NAME = "testdb2";
    private static final String PROJECT_ID = "shenzhen";

    @Test
    public void should_create_catalog_success() {
        try {
            CatalogInput catalogInput = getCatalogInput(CATALOG_NAME);
            catalogService.createCatalog(PROJECT_ID, catalogInput);

            CatalogName expect = new CatalogName(PROJECT_ID, CATALOG_NAME);

            Catalog actual = catalogService.getCatalog(expect);
            assertEquals(expect.getCatalogName(), actual.getCatalogName());
            catalogService.dropCatalog(expect);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void should_alter_catalog_success() {
        try {
            CatalogInput catalogInput = getCatalogInput(CATALOG_NAME);
            catalogService.createCatalog(PROJECT_ID, catalogInput);

            String expect = "new description";
            catalogInput.setDescription(expect);

            CatalogName catalogName = new CatalogName(PROJECT_ID, CATALOG_NAME);
            catalogService.alterCatalog(catalogName, catalogInput);
            Catalog actual = catalogService.getCatalog(catalogName);
            assertEquals(expect, actual.getDescription());

            catalogService.dropCatalog(catalogName);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void should_drop_catalog_success() {
        try {
            CatalogInput catalogInput = getCatalogInput(CATALOG_NAME);
            catalogService.createCatalog(PROJECT_ID, catalogInput);
            CatalogName expect = new CatalogName(PROJECT_ID, CATALOG_NAME);

            Catalog actual = catalogService.getCatalog(expect);
            assertEquals(expect.getCatalogName(), actual.getCatalogName());

            CatalogName catalogName = new CatalogName(PROJECT_ID, CATALOG_NAME);
            catalogService.dropCatalog(catalogName);
            Throwable exception = assertThrows(CatalogServerException.class,
                () -> catalogService.getCatalog(catalogName));
            assertEquals(String.format("NoSuchObjectException(message:No catalog %s)", CATALOG_NAME),
                exception.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void should_list_catalogs_success() {
        try {
            String catalog1 = CATALOG_NAME + "_list1";
            String catalog2 = CATALOG_NAME + "_list2";
            TraverseCursorResult<List<Catalog>> catalogList = catalogService.getCatalogs(PROJECT_ID,
                Integer.MAX_VALUE, null, null);

            List<String> catNames = catalogList.getResult().stream().map(Catalog::getCatalogName)
                .collect(Collectors.toList());
            if (catNames.contains(catalog1)) {
                catalogService.dropCatalog(new CatalogName(PROJECT_ID, catalog1));
            }
            if (catNames.contains(catalog2)) {
                catalogService.dropCatalog(new CatalogName(PROJECT_ID, catalog2));
            }

            int expect = catalogService.getCatalogs(PROJECT_ID,
                Integer.MAX_VALUE, null, null).getResult().size() + 2;


            CatalogInput catalogInput = getCatalogInput(catalog1);
            catalogService.createCatalog(PROJECT_ID, catalogInput);
            catalogInput = getCatalogInput(catalog2);
            catalogService.createCatalog(PROJECT_ID, catalogInput);

            catalogList = catalogService.getCatalogs(PROJECT_ID,
                Integer.MAX_VALUE, null, null);

            // check new catalog can be listed
            catalogList = catalogService.getCatalogs(PROJECT_ID,
                Integer.MAX_VALUE, null, null);
            catNames = catalogList.getResult().stream().map(Catalog::getCatalogName)
                .collect(Collectors.toList());
            assertTrue(catNames.contains(catalog1));
            assertTrue(catNames.contains(catalog2));

            CatalogName catalogName =
                new CatalogName(PROJECT_ID, catalog1);
            catalogService.dropCatalog(catalogName);
            catalogName = new CatalogName(PROJECT_ID, catalog2);
            catalogService.dropCatalog(catalogName);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
