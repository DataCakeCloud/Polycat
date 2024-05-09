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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.plugin.request.input.FunctionInput;
import io.polycat.catalog.common.plugin.request.input.FunctionResourceUri;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

@Disabled("need set up Hive environment")
public class UDFServiceImplTest extends TestUtil {

    public enum FunctionType {
        JAVA(1);

        private final int value;

        FunctionType(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public static FunctionType findByValue(int value) {
            switch (value) {
                case 1:
                    return JAVA;
                default:
                    return null;
            }
        }
    }

    public enum PrincipalType {
        USER(1),
        ROLE(2),
        GROUP(3);

        private final int value;

        PrincipalType(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public static PrincipalType findByValue(int value) {
            switch (value) {
                case 1:
                    return USER;
                case 2:
                    return ROLE;
                case 3:
                    return GROUP;
                default:
                    return null;
            }
        }
    }

    public enum ResourceType {
        JAR(1),
        FILE(2),
        ARCHIVE(3);

        private final int value;

        ResourceType(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public static ResourceType findByValue(int value) {
            switch (value) {
                case 1:
                    return JAR;
                case 2:
                    return FILE;
                case 3:
                    return ARCHIVE;
                default:
                    return null;
            }
        }
    }

    private final ArrayList<FunctionResourceUri> RESOURCE_URI_LIST = new ArrayList<FunctionResourceUri>() {{
        add(new FunctionResourceUri(ResourceType.JAR.name(), "resource_uri_list"));
    }};
    protected static final int CREATE_TIME = (int) (System.currentTimeMillis() / 1000);
    protected static final String FUNCTION_NAME = "function_name";
    protected static final String NEW_FUNCTION_NAME = "new_function_name";
    protected static final String TEST_CLASSNAME = "test.classname";
    private static final String HIVE_CATALOG_NAME = "hive";

    @Test
    public void should_create_udf_success() {
        try {
            FunctionInput expect = new FunctionInput(FUNCTION_NAME, TEST_CLASSNAME, OWNER_NAME,
                PrincipalType.USER.name(), FunctionType.JAVA.name(), CREATE_TIME, DATABASE_NAME, CATALOG_NAME,
                RESOURCE_URI_LIST);
            functionService.createFunction(CATALOG_NAME, DATABASE_NAME, TABLE_NAME, expect);

            DatabaseName databaseName = new DatabaseName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME);
            FunctionInput actual = functionService.getFunction(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, FUNCTION_NAME);

            assertEquals(expect.getFunctionName(), actual.getFunctionName());
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void should_get_all_udf_success() {
        try {
            int begSize = functionService.getAllFunctions(PROJECT_ID, HIVE_CATALOG_NAME).size();
            String function_name1 = FUNCTION_NAME + "1";
            String function_name2 = FUNCTION_NAME + "2";
            String function_name3 = FUNCTION_NAME + "3";
            FunctionInput function = new FunctionInput(function_name1, TEST_CLASSNAME, OWNER_NAME,
                PrincipalType.USER.name(), FunctionType.JAVA.name(), CREATE_TIME, DATABASE_NAME, HIVE_CATALOG_NAME,
                RESOURCE_URI_LIST);
            functionService.createFunction(HIVE_CATALOG_NAME, DATABASE_NAME, TABLE_NAME, function);
            function = new FunctionInput(function_name2, TEST_CLASSNAME, OWNER_NAME,
                PrincipalType.USER.name(), FunctionType.JAVA.name(), CREATE_TIME, DATABASE_NAME, HIVE_CATALOG_NAME,
                RESOURCE_URI_LIST);
            functionService.createFunction(HIVE_CATALOG_NAME, DATABASE_NAME, TABLE_NAME, function);
            function = new FunctionInput(function_name3, TEST_CLASSNAME, OWNER_NAME,
                PrincipalType.USER.name(), FunctionType.JAVA.name(), CREATE_TIME, DATABASE_NAME, HIVE_CATALOG_NAME,
                RESOURCE_URI_LIST);
            functionService.createFunction(HIVE_CATALOG_NAME, DATABASE_NAME, TABLE_NAME, function);

            List<FunctionInput> udfs = functionService.getAllFunctions(PROJECT_ID, HIVE_CATALOG_NAME);
            assertEquals(begSize + 3, udfs.size());
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        } finally {
            databaseService.dropDatabase(databaseName, "true");
        }
    }

    @Test
    public void should_delete_udf_success() {
        try {
            FunctionInput expect = new FunctionInput(FUNCTION_NAME, TEST_CLASSNAME, OWNER_NAME,
                PrincipalType.USER.name(), FunctionType.JAVA.name(), CREATE_TIME, DATABASE_NAME, CATALOG_NAME,
                RESOURCE_URI_LIST);
            functionService.createFunction(CATALOG_NAME, DATABASE_NAME, TABLE_NAME, expect);
            DatabaseName databaseName = new DatabaseName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME);
            functionService.dropFunction(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, FUNCTION_NAME);

            Throwable exception = assertThrows(CatalogServerException.class,
                () -> functionService.getFunction(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, FUNCTION_NAME));
            assertEquals(String.format(Locale.ROOT, "NoSuchObjectException(message:Function @%s#%s.%s does not exist)",
                CATALOG_NAME, DATABASE_NAME, FUNCTION_NAME), exception.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void should_get_udfs_success() {
        try {
            String function_name1 = FUNCTION_NAME + "1";
            String function_name2 = FUNCTION_NAME + "2";
            String function_name3 = FUNCTION_NAME + "3";
            FunctionInput function = new FunctionInput(function_name1, TEST_CLASSNAME, OWNER_NAME,
                PrincipalType.USER.name(), FunctionType.JAVA.name(), CREATE_TIME, DATABASE_NAME, CATALOG_NAME,
                RESOURCE_URI_LIST);
            functionService.createFunction(CATALOG_NAME, DATABASE_NAME, TABLE_NAME, function);
            function = new FunctionInput(function_name2, TEST_CLASSNAME, OWNER_NAME,
                PrincipalType.USER.name(), FunctionType.JAVA.name(), CREATE_TIME, DATABASE_NAME, CATALOG_NAME,
                RESOURCE_URI_LIST);
            functionService.createFunction(CATALOG_NAME, DATABASE_NAME, TABLE_NAME, function);
            function = new FunctionInput(function_name3, TEST_CLASSNAME, OWNER_NAME,
                PrincipalType.USER.name(), FunctionType.JAVA.name(), CREATE_TIME, DATABASE_NAME, CATALOG_NAME,
                RESOURCE_URI_LIST);
            functionService.createFunction(CATALOG_NAME, DATABASE_NAME, TABLE_NAME, function);

            List<String> actual = functionService.listFunctions(PROJECT_ID, CATALOG_NAME, DATABASE_NAME,
                FUNCTION_NAME + "*");

            assertEquals(3, actual.size());
            assertEquals(function_name1, actual.get(0));
            assertEquals(function_name2, actual.get(1));
            assertEquals(function_name3, actual.get(2));
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void should_alter_udf_success() {
        try {
            FunctionInput expect = new FunctionInput(FUNCTION_NAME, TEST_CLASSNAME, OWNER_NAME,
                PrincipalType.USER.name(), FunctionType.JAVA.name(), CREATE_TIME, DATABASE_NAME, CATALOG_NAME,
                RESOURCE_URI_LIST);
            functionService.createFunction(CATALOG_NAME, DATABASE_NAME, TABLE_NAME, expect);

            expect.setFunctionName(NEW_FUNCTION_NAME);

            functionService.alterFunction(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, FUNCTION_NAME, expect);

            assertThrows(CatalogServerException.class,
                () -> functionService.getFunction(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, FUNCTION_NAME));
            FunctionInput actual = functionService.getFunction(PROJECT_ID, CATALOG_NAME, DATABASE_NAME,
                NEW_FUNCTION_NAME);

            assertEquals(expect.getFunctionName(), actual.getFunctionName());
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @AfterEach
    private void cleanEnv() {
        deleteTestDataBaseAfterTest();
    }

    @BeforeEach
    private void makeEnv() {
        createTestDatabaseBeforeTest();
    }
}
