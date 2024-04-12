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

import java.util.List;

import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.model.CatalogCommit;
import io.polycat.catalog.common.model.CatalogName;
import io.polycat.catalog.common.model.TraverseCursorResult;
import io.polycat.catalog.common.plugin.request.input.FunctionInput;
import io.polycat.catalog.store.common.StoreConvertor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import static io.polycat.catalog.common.Operation.CREATE_FUNCTION;
import static io.polycat.catalog.common.Operation.DROP_FUNCTION;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@SpringBootTest
public class FunctionTest extends TestUtil {

    private static final Logger logger = Logger.getLogger(FunctionTest.class);

    String udfClassName = "polycat.udf.fake.class.name";

    @BeforeEach
    public void clearAndSetup() {
        createCatalogBeforeClass();
        createDatabaseBeforeClass();
    }

    @Test
    public void create_function_should_success() {
        // create function
        String funcNameStr = "new_function_1";
        createFunction(funcNameStr);

        // get function
        FunctionInput resFunc = functionService.getFunction(projectId, catalogNameString, databaseNameString,
            funcNameStr);
        assertEquals(catalogNameString, resFunc.getCatalogName());
        assertEquals(databaseNameString, resFunc.getDatabaseName());
        assertEquals(funcNameStr, resFunc.getFunctionName());
        assertEquals(udfClassName, resFunc.getClassName());

        // check catalog commit CREATE_FUNCTION
        CatalogName catalogName = StoreConvertor.catalogName(projectId, catalogNameString);
        TraverseCursorResult<List<CatalogCommit>> resCC = catalogService.getCatalogCommits(catalogName, 1, "");
        CatalogCommit cc = resCC.getResult().get(0);
        assertEquals(CREATE_FUNCTION.getPrintName(), cc.getOperation());
    }

    @Test
    public void drop_function_should_success() {
        // create function
        String funcNameStr = "drop_function_1";
        createFunction(funcNameStr);

        // drop function
        assertDoesNotThrow(() ->
            functionService.dropFunction(projectId, catalogNameString, databaseNameString, funcNameStr));

        // check function not exists
        FunctionInput resFunc = functionService.getFunction(projectId, catalogNameString, databaseNameString,
            funcNameStr);
        assertNull(resFunc);

        // check catalog commit DROP_FUNCTION
        CatalogName catalogName = StoreConvertor.catalogName(projectId, catalogNameString);
        TraverseCursorResult<List<CatalogCommit>> resCC = catalogService.getCatalogCommits(catalogName, 1, "");
        CatalogCommit cc = resCC.getResult().get(0);
        assertEquals(DROP_FUNCTION.getPrintName(), cc.getOperation());
    }

    @Test
    public void show_functions_should_success() {
        // create functions
        String funcNameStr1 = "show_function_1";
        String funcNameStr2 = "show_function_2";
        createFunction(funcNameStr1);
        createFunction(funcNameStr2);

        // show functions
        List<String> functions = functionService.listFunctions(projectId, catalogNameString, databaseNameString,
            "show");
        assertEquals(2, functions.size());
        assertEquals(funcNameStr1, functions.get(0));
        assertEquals(funcNameStr2, functions.get(1));
    }

    @Test
    public void get_all_functions_should_success() {
        // create functions
        String funcNameStr1 = "show_function_1";
        String funcNameStr2 = "show_function_2";
        createFunction(funcNameStr1);
        createFunction(funcNameStr2);

        // show functions
        final List<FunctionInput> functions = functionService.getAllFunctions(projectId, catalogNameString);
        assertEquals(2, functions.size());
        assertEquals(funcNameStr1, functions.get(0).getFunctionName());
        assertEquals(funcNameStr2, functions.get(1).getFunctionName());
    }

    private void createFunction(String funcNameStr) {
        FunctionInput funcInput = makeFunctionInput(funcNameStr);
        assertDoesNotThrow(() -> functionService.createFunction(projectId, catalogNameString, databaseNameString, funcInput));
    }

    private FunctionInput makeFunctionInput(String funcNameStr) {
        FunctionInput funcInput = new FunctionInput();
        funcInput.setFunctionName(funcNameStr);
        funcInput.setClassName(udfClassName);
        funcInput.setOwner("dash");
        funcInput.setOwnerType("USER");
        funcInput.setFuncType("JAVA");
        return funcInput;
    }
}
