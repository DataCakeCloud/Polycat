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

import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.ViewName;
import io.polycat.catalog.common.model.ViewRecordObject;
import io.polycat.catalog.common.plugin.request.input.ViewInput;
import io.polycat.catalog.common.model.ViewStatement;
import io.polycat.catalog.store.common.StoreConvertor;
import io.polycat.catalog.store.protos.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
public class ViewServiceImplTestUtil extends TestUtil{

    private static final String userId = "TestUser";
    boolean isFirstTest = true;
    private void beforeClass() {
        if (isFirstTest) {
            createCatalogBeforeClass();
            createDatabaseBeforeClass();
            createTableBeforeClass();
            isFirstTest = false;
        }
    }

    @BeforeEach
    public void beforeEach() {
        beforeClass();
    }

    @Test
    public void creatNewViewTest() {
        String viewNameString = "testView1";

        ViewInput viewInput = makeViewInput(viewNameString);
        DatabaseName databaseName = StoreConvertor.databaseName(projectId, catalogNameString, databaseNameString);
        viewService.createView(databaseName, viewInput);

        ViewName viewName = StoreConvertor.viewName(projectId, catalogNameString, databaseNameString, viewNameString);
        ViewRecordObject view = viewService.getViewByName(viewName);
        assertNotNull(view);
        assertEquals(view.getDatabaseId(), databaseId);
        assertEquals(view.getName(), viewNameString);

        viewService.dropView(viewName);
//        View view3 = viewService.getViewByName(viewName);
//        assertNull(view3);
    }

    ViewInput makeViewInput(String viewName) {
        ViewInput viewInput = new ViewInput();
        ViewStatement viewStatement[] = new ViewStatement[1];
        viewStatement[0] = new ViewStatement();
        viewStatement[0].setCatalogName(catalogNameString);
        viewStatement[0].setDatabaseName(databaseNameString);
        viewStatement[0].setObjectName(tableNameString);
        viewStatement[0].setRenameColumn(false);
        viewStatement[0].setFilterFlag(false);
        viewInput.setViewName(viewName);

        viewInput.setViewStatements(viewStatement);
        return viewInput;
    }
}