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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.polycat.catalog.common.model.CatalogName;
import io.polycat.catalog.common.model.Column;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.SerDeInfo;
import io.polycat.catalog.common.model.StorageDescriptor;
import io.polycat.catalog.common.plugin.request.input.CatalogInput;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;
import io.polycat.catalog.common.plugin.request.input.TableInput;
import io.polycat.catalog.service.api.CatalogService;
import io.polycat.catalog.service.api.DatabaseService;
import io.polycat.catalog.service.api.FunctionService;
import io.polycat.catalog.service.api.PartitionService;
import io.polycat.catalog.service.api.TableService;
import io.polycat.catalog.service.api.TokenService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;

public class TestUtil {

    protected static HiveMetaStoreClient client;
    protected static Configuration conf;

    public CatalogService catalogService;
    public DatabaseService databaseService;
    public TableService tableService;
    public PartitionService partitionService;
    public FunctionService functionService;
    public TokenService tokenService;
    public static final String AUTH_SOURCE_TYPE = "fake_auth_source_type";
    public static final String ACCOUNT_ID = "fake_account_id";

    protected static final String USER_ID = "test";
    protected static final String LOCATION = "/location/uri";
    protected static final Map<String, String> PARAMS = new HashMap<>();
    protected static final String OWNER_TYPE = "USER";
    protected static final String NEW_DESCRIPTION = "new Description Test";
    protected static final String OWNER_NAME = "owner_name";
    protected static final String CATALOG_NAME = "testc2";
    protected static final String PROJECT_ID = "shenzhen";
    protected static final String DATABASE_NAME = "db2";
    protected static final String TABLE_NAME = "t4";
    public static final String MOCK_DESCRIPTION = "";
    public static final String OWNER = "owner";
    public static final List<Column> COLUMNS = new ArrayList<>();
    public static final List<Column> PARTITIONS = new ArrayList<>();
    public static final StorageDescriptor STORAGE_INPUT = new StorageDescriptor();
    public static final SerDeInfo SER_DE_INFO = new SerDeInfo();

    public static final String COLUMN_1 = "column1";
    public static final String COLUMN_2 = "column2";

    public static final String COLUMN_COMMENT = "";
    public static final String TABLE_TYPE = "MANAGED_TABLE";
    public static final String STORAGE_OUTPUT_FORMAT = "";
    public static final String STORAGE_INPUT_FORMAT = "";
    public static final String MOCK_LOCATION = "/mock/location";
    public static final String SERDE = "mock serde";
    public static final String SERDE_LIB = "serde.lib";

    public static final String HIVE_STR_TYPE = "string";
    public static final String HIVE_DECIMAL_TYPE = "decimal";

    protected DatabaseName databaseName = new DatabaseName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME);
    private String HIVE_CATALOG_NAME = "hive";

    public TestUtil() {
        initServices();
    }

    private void initServices() {
        catalogService = new HiveCatalogServiceImp();
        databaseService = new HiveDatabaseServiceImp();
        tableService = new HiveTableServiceImp();
        functionService = new HiveFuncitonServiceImp();
        partitionService = new HivePartitionServiceImp();
        tokenService = new HiveTokenServiceImp();
    }

    static {
        Column column = new Column();
        column.setColumnName(COLUMN_1);
        column.setColType(HIVE_STR_TYPE);
        column.setComment(COLUMN_COMMENT);
        Column column2 = new Column();
        column2.setColumnName(COLUMN_2);
        column2.setColType(HIVE_STR_TYPE);
        column2.setComment(COLUMN_COMMENT);
        COLUMNS.add(column);
        COLUMNS.add(column2);

        PARTITIONS.add(column);
        PARTITIONS.add(column2);

        fillInStorageInput();
    }

    private static void fillInStorageInput() {
        fillInSerDeInfo();
        STORAGE_INPUT.setInputFormat(STORAGE_INPUT_FORMAT);
        STORAGE_INPUT.setOutputFormat(STORAGE_OUTPUT_FORMAT);
        STORAGE_INPUT.setLocation(MOCK_LOCATION);
        STORAGE_INPUT.setCompressed(false);
        STORAGE_INPUT.setNumberOfBuckets(0);
        STORAGE_INPUT.setParameters(new HashMap<>());
        STORAGE_INPUT.setSerdeInfo(SER_DE_INFO);
        STORAGE_INPUT.setColumns(COLUMNS);
    }

    private static void fillInSerDeInfo() {
        SER_DE_INFO.setSerializationLibrary(SERDE_LIB);
        SER_DE_INFO.setName(SERDE);
        SER_DE_INFO.setParameters(Collections.emptyMap());
    }

//    @BeforeAll
//    public static void setup() {
//        try {
//            service = new HiveMetaStoreServiceImpl();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

    protected CatalogInput getCatalogInput(String catalog) {
        CatalogInput catalogInput = new CatalogInput();
        catalogInput.setCatalogName(catalog);
        catalogInput.setLocation(LOCATION);
        catalogInput.setDescription("");
        catalogInput.setOwner(USER_ID);
        return catalogInput;
    }

    protected DatabaseInput getDatabaseInput(DatabaseName databaseName) {
        DatabaseInput databaseInput = new DatabaseInput();
        databaseInput.setCatalogName(databaseName.getCatalogName());
        databaseInput.setDatabaseName(databaseName.getDatabaseName());
        databaseInput.setDescription("");
        databaseInput.setLocationUri(LOCATION);
        databaseInput.setParameters(PARAMS);
        databaseInput.setAuthSourceType(AUTH_SOURCE_TYPE);
        databaseInput.setAccountId(ACCOUNT_ID);
        databaseInput.setOwner(USER_ID);
        databaseInput.setOwnerType(OWNER_TYPE);
        return databaseInput;
    }

    protected void deleteTestDataBaseAfterTest() {
        CatalogName catalogName = new CatalogName(PROJECT_ID, CATALOG_NAME);

        DatabaseName databaseName = new DatabaseName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME);
        databaseService.dropDatabase(databaseName, true, true, "true");
        catalogService.dropCatalog(catalogName);

        databaseName = new DatabaseName(PROJECT_ID, HIVE_CATALOG_NAME, DATABASE_NAME);
        databaseService.dropDatabase(databaseName, "true");
    }

    protected void createTestDatabaseBeforeTest() {
        try {
            CatalogInput catalogInput = getCatalogInput(CATALOG_NAME);
            catalogService.createCatalog(PROJECT_ID, catalogInput);
            CatalogName catalogName = new CatalogName(PROJECT_ID, CATALOG_NAME);

            DatabaseName databaseName = new DatabaseName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME);
            DatabaseInput databaseInput = getDatabaseInput(databaseName);
            databaseService.createDatabase(catalogName, databaseInput);

            catalogName = new CatalogName(PROJECT_ID, HIVE_CATALOG_NAME);
            databaseName = new DatabaseName(PROJECT_ID, HIVE_CATALOG_NAME, DATABASE_NAME);
            databaseInput = getDatabaseInput(databaseName);
            databaseService.createDatabase(catalogName, databaseInput);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void createTestTableBeforeTest() {
        createTestDatabaseBeforeTest();
        tableService.createTable(databaseName, createTableInput(TABLE_NAME));
    }

    protected TableInput createTableInput(String tableName) {
        TableInput input = new TableInput();
        input.setTableName(tableName);
        input.setDescription(MOCK_DESCRIPTION);
        input.setOwner(OWNER);
        input.setOwnerType(OWNER_TYPE);
        input.setRetention(0);
        input.setPartitionKeys(PARTITIONS);
        input.setTableType(TABLE_TYPE);
        input.setStorageDescriptor(STORAGE_INPUT);
        input.setParameters(new HashMap<>());
        input.setLmsMvcc(false);
        return input;
    }

    // In the actual scenario, this field comes from JSON deserialization
    // and is stored in the form of map
    protected Map<String, Object> obj2Map(Object obj) throws Exception {
        Map<String, Object> map = new HashMap<String, Object>();
        Class<?> curObj = obj.getClass();
        while (!Object.class.equals(curObj)) {
            Field[] fields = curObj.getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                map.put(field.getName(), field.get(obj));
            }
            curObj = curObj.getSuperclass();
        }

        return map;
    }
}
