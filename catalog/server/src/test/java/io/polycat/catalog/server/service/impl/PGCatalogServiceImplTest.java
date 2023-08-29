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

import io.polycat.catalog.common.model.*;
import io.polycat.catalog.common.plugin.request.input.CatalogInput;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;
import io.polycat.catalog.common.plugin.request.input.TableInput;
import io.polycat.catalog.service.api.CatalogService;
import io.polycat.catalog.service.api.DatabaseService;
import io.polycat.catalog.service.api.TableService;
import io.polycat.catalog.store.common.StoreConvertor;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Collections;


@Slf4j
public class PGCatalogServiceImplTest extends PGBaseServiceImplTest{

    @Autowired
    CatalogService catalogService;

    @Test
    public void create_catalog_should_success(){
        CatalogName catalogName = new CatalogName(PROJECT_ID, CATALOG_NAME);
        CatalogInput catalogInput = getCatalogInput(catalogName.getCatalogName());
        catalogService.createCatalog(PROJECT_ID, catalogInput);

        Catalog catalog = catalogService.getCatalog(catalogName);
        valueAssertNotNull(catalog);
        log.info("catalog: {}", catalog);
        valueAssertEquals(CATALOG_NAME, catalog.getCatalogName());
    }

}
