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
import io.polycat.catalog.common.plugin.request.GetDatabaseRequest;
import io.polycat.catalog.server.CatalogApplication;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClient;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.security.core.parameters.P;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CreateOperatorTest {

    private static ConfigurableApplicationContext catalogServer;
    private static IMetaStoreClient metaStoreClient;
    private static String catalogName = "default";

    @BeforeAll
    static void beforeAll() {
        HMSClientTestUtil.cleanFDB();
        catalogServer = SpringApplication.run(CatalogApplication.class, new String[0]);
        HMSClientTestUtil.createCatalog(catalogName);
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("polycat.projectId", HMSClientTestUtil.catalogClient.getProjectId());
        metaStoreClient = new SessionHiveMetaStoreClient(hiveConf, null, true);
    }

    @Test
    void createDatabasesTest() throws TException {
        HashMap<String, String> param = new HashMap<>();
        //param.put("param1", "1");
        //param.put("param2", "2");
        Database database = new Database("db3", "description test", "target/db3", param);
        metaStoreClient.createDatabase(database);
        io.polycat.catalog.common.model.Database db3 = HMSClientTestUtil.getDatabase(catalogName, "db3");
        assertEquals("db3", db3.getDatabaseName());
        assertEquals("description test", db3.getDescription());
        assertEquals(param, db3.getParameters());
    }

    @AfterAll
    static void afterAll() {
        try {
            if (catalogServer != null) {
                try {
                    catalogServer.close();
                } catch (Exception e) {

                }
            }
        } finally {
            HMSClientTestUtil.cleanFDB();
        }
    }
}
