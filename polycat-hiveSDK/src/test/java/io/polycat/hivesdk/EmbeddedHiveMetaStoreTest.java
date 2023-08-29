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
package io.polycat.hivesdk;

import io.polycat.hivesdk.conf.FDBTestTools;
import io.polycat.hivesdk.conf.TestTools;
import io.polycat.hivesdk.config.HiveCatalogContext;
import io.polycat.hivesdk.impl.PolyCatMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import static java.lang.Thread.sleep;

@Disabled("some Hive feature, Catalog not support yet.")
@TestInstance(Lifecycle.PER_CLASS)
public class EmbeddedHiveMetaStoreTest extends PolyCatMetaStoreClientTest {
    static TestTools tools = new FDBTestTools();

    private static final int DEFAULT_LIMIT_PARTITION_REQUEST = 100;
    public static void setUpConfig() throws Exception {
        conf = MetastoreConf.newMetastoreConf();

        // set some values to use for getting conf. vars
        MetastoreConf.setBoolVar(conf, ConfVars.METRICS_ENABLED, true);
        conf.set("hive.key1", "value1");
        conf.set("hive.key2", "http://www.example.com");
        conf.set("hive.key3", "");
        conf.set("hive.key4", "0");
        conf.set("datanucleus.autoCreateTables", "false");

        MetaStoreTestUtils.setConfForStandloneMode(conf);
        MetastoreConf.setLongVar(conf, ConfVars.BATCH_RETRIEVE_MAX, 2);
        MetastoreConf.setLongVar(conf, ConfVars.LIMIT_PARTITION_REQUEST, DEFAULT_LIMIT_PARTITION_REQUEST);
        MetastoreConf.setVar(conf, ConfVars.STORAGE_SCHEMA_READER_IMPL, "no.such.class");
    }
    @BeforeAll
    public static void openWarehouse() throws Exception {
        setUpConfig();
        runCatalogApp();
        warehouse = new Warehouse(conf);
        client = createClient();
        tools.clear();
    }

    @AfterAll
    public void tearDown() throws TException {
        client.close();
        stopAndClear();
    }

    protected static IMetaStoreClient createClient() throws Exception {
        try {
            if (context == null) {
                throw new Exception();
            }

            return new PolyCatMetaStoreClient();
        } catch (Throwable e) {
            System.err.println("Unable to open the metastore");
            System.err.println(StringUtils.stringifyException(e));
            throw new Exception(e);
        }
    }
}
