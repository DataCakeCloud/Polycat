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
import io.polycat.catalog.client.PolyCatClient;
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

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GetOperatorTest {

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
        HMSClientTestUtil.createDatabase(catalogName, "db1");
        HMSClientTestUtil.createDatabase(catalogName, "db2");

    }

    @Test
    void getDatabasesTest() throws TException {
        List<String> databases = metaStoreClient.getDatabases(null);
        assertEquals(2, databases.size());
        assertTrue(databases.contains("db1"));
        assertTrue(databases.contains("db2"));
    }

    @Test
    void getDatabaseTest() throws TException {
        Database db1 = metaStoreClient.getDatabase("db1");
        assertEquals("db1", db1.getName());
        assertEquals("target/db1", db1.getLocationUri());
        assertEquals("test", db1.getOwnerName());
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
