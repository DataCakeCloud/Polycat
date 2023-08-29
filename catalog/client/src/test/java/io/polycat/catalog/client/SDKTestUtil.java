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

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import io.polycat.catalog.common.plugin.request.input.DatabaseInput;
import io.polycat.catalog.server.CatalogApplication;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBTransactionContext;
import com.apple.foundationdb.subspace.Subspace;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

import static java.lang.Thread.sleep;

public class SDKTestUtil {

    public static final String OWNER_TYPE = "USER";
    public static final String AUTH_SOURCE_TYPE = "iam";
    protected static final Map<String, String> PARAMS = new HashMap<>();
    private static ConfigurableApplicationContext catalogApp;

    public static final String ACCOUNT_ID = "test";
    public static final String PROJECT_ID = "shenzhen";
    public static final String OWNER = "test";
    public static final String PASSWORD = "dash";

    @BeforeAll
    public static void runCatalogApp() throws IOException {
        // clear meta in catalog fdb
        clearFdb();

        // run catalog application
        catalogApp = SpringApplication.run(CatalogApplication.class);
        try {
            sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @AfterAll
    public static void stopAndClear() {
        // stop catalog application
        catalogApp.close();
        try {
            sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // clear all under the project
        clearFdb();

    }

    private static void clearFdb() {
        FDBDatabase fdb = FDBDatabaseFactory.instance().getDatabase();
        try (FDBRecordContext context = fdb.openContext()) {
            Field field = FDBTransactionContext.class.getDeclaredField("transaction");
            field.setAccessible(true);
            Transaction txn = (Transaction) Objects.requireNonNull(field.get(context));
            final byte[] st = new Subspace(new byte[]{(byte) 0x00}).getKey();
            final byte[] en = new Subspace(new byte[]{(byte) 0xFF}).getKey();
            txn.clear(st, en);
            context.commit();
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    protected static PolyCatClient getClient() {
        PolyCatClient client = new PolyCatClient();
        client.setContext(CatalogUserInformation.doAuth(OWNER, PASSWORD));
        client.getContext().setTenantName(ACCOUNT_ID);
        return client;
    }

    protected static DatabaseInput getDatabaseInput(String catalogName, String databaseName, String location,
        String owner) {
        DatabaseInput databaseInput = new DatabaseInput();
        databaseInput.setCatalogName(catalogName);
        databaseInput.setDatabaseName(databaseName);
        databaseInput.setDescription("");
        databaseInput.setLocationUri(location);
        databaseInput.setParameters(PARAMS);
        databaseInput.setAuthSourceType(AUTH_SOURCE_TYPE);
        databaseInput.setAccountId(ACCOUNT_ID);
        databaseInput.setOwner(owner);
        databaseInput.setOwnerType(OWNER_TYPE);
        return databaseInput;
    }
}
