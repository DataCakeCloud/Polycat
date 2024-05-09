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
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.subspace.Subspace;
import io.polycat.catalog.authentication.Authentication;
import io.polycat.catalog.authentication.model.AuthenticationResult;
import io.polycat.catalog.authentication.model.LocalIdentity;
import io.polycat.catalog.client.PolyCatClient;
import io.polycat.catalog.common.model.Database;
import io.polycat.catalog.common.plugin.request.CreateCatalogRequest;
import io.polycat.catalog.common.plugin.request.CreateDatabaseRequest;
import io.polycat.catalog.common.plugin.request.GetDatabaseRequest;
import io.polycat.catalog.common.plugin.request.input.CatalogInput;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;
import io.polycat.catalog.store.fdb.record.RecordStoreHelper;

public class HMSClientTestUtil {

    private static String userName = "test";
    public static PolyCatClient catalogClient = new PolyCatClient("127.0.0.1", 8082, userName, "dash");

    public static void cleanFDB() {
        FDBDatabase db = RecordStoreHelper.getFdbDatabase();
        try (FDBRecordContext context = db.openContext()) {
            Transaction tx = RecordStoreHelper.getTransaction(context);
            final byte[] st = new Subspace(new byte[]{(byte) 0x00}).getKey();
            final byte[] en = new Subspace(new byte[]{(byte) 0xFF}).getKey();
            tx.clear(st, en);
            context.commit();
        } finally {
            db.close();
        }
    }

    private static AuthenticationResult doAuthentication(String userName, String password) {
        LocalIdentity localIdentity = new LocalIdentity();
        localIdentity.setUserId(userName);
        localIdentity.setPasswd(password);
        localIdentity.setIdentityOwner("LocalAuthenticator");
        return Authentication.authAndCreateToken(localIdentity);
    }

    public static void createCatalog(String catalogName) {
        CreateCatalogRequest createCatalogRequest = new CreateCatalogRequest();
        CatalogInput catalogInput = new CatalogInput();
        catalogInput.setCatalogName(catalogName);
        createCatalogRequest.setInput(catalogInput);
        createCatalogRequest.setProjectId(catalogClient.getProjectId());
        createCatalogRequest.getInput().setUserId(userName);
        catalogClient.createCatalog(createCatalogRequest);
    }

    public static void createDatabase(String catalogName, String databaseName) {
        CreateDatabaseRequest createDatabaseRequest = new CreateDatabaseRequest();
        DatabaseInput databaseInput = new DatabaseInput();
        databaseInput.setDatabaseName(databaseName);
        databaseInput.setLocationUri("target/" + databaseName);
        createDatabaseRequest.setInput(databaseInput);
        createDatabaseRequest.setProjectId(catalogClient.getProjectId());
        createDatabaseRequest.getInput().setUserId(userName);
        createDatabaseRequest.setCatalogName(catalogName);
        catalogClient.createDatabase(createDatabaseRequest);
    }

    public static Database getDatabase(String catalogName, String databaseName) {
        GetDatabaseRequest getDatabaseRequest = new GetDatabaseRequest();
        getDatabaseRequest.setDatabaseName(databaseName);
        getDatabaseRequest.setCatalogName(catalogName);
        getDatabaseRequest.setProjectId(catalogClient.getProjectId());
        return catalogClient.getDatabase(getDatabaseRequest);
    }


}
