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

import io.polycat.catalog.common.plugin.request.input.FunctionInput;
import io.polycat.catalog.common.plugin.request.input.FunctionResourceUri;
import io.polycat.catalog.service.api.FunctionService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;


@Slf4j
public class PGFunctionServiceImplTest extends PGDatabaseServiceImplTest {

    private static final String FUNCTION_NAME = "function_test";
    private static final String funcClassName = "com.xxx.xxxx.Class";

    @Autowired
    FunctionService functionService;

    boolean firstFlag = true;

    @BeforeEach
    public void beforeClass() throws Exception {

        if (firstFlag) {
            create_database_should_success();
        }
    }

    @Test
    public void create_function_should_success() {
        FunctionInput functionInput = makeFunctionInput();

        functionService.createFunction(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, functionInput);
        FunctionInput function = functionService.getFunction(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, FUNCTION_NAME);
        valueAssertNotNull(function);
        valueAssertEquals(funcClassName, function.getClassName());
        valueAssertEquals(FUNCTION_NAME, function.getFunctionName());
        valueAssertEquals(2, function.getResourceUris().size());

    }

    private FunctionInput makeFunctionInput() {
        FunctionInput functionInput = new FunctionInput();
        functionInput.setFunctionName(FUNCTION_NAME);
        functionInput.setCatalogName(CATALOG_NAME);
        functionInput.setClassName(funcClassName);
        functionInput.setOwner(userId);
        functionInput.setFuncType("JAR");
        ArrayList<FunctionResourceUri> list = new ArrayList<>();
        list.add(new FunctionResourceUri("JAR", "s3://test/test"));
        list.add(new FunctionResourceUri("JAR", "s3://test/test1"));
        functionInput.setResourceUris(list);
        return functionInput;
    }

}
