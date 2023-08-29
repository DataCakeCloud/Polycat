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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.polycat.catalog.authentication.Authentication;
import io.polycat.catalog.authentication.model.AuthenticationResult;
import io.polycat.catalog.authentication.model.LocalIdentity;
import io.polycat.catalog.common.plugin.CatalogContext;
import io.polycat.catalog.server.CatalogApplication;
import io.polycat.hivesdk.impl.PolyCatMetaStoreClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

import static java.lang.Thread.sleep;
import static javolution.testing.TestContext.fail;

public class PolyCatTestUtils {
    private static final Logger LOG = LoggerFactory.getLogger(PolyCatTestUtils.class);

    public static final String DEFAULT_CATALOG_NAME = "hive";

    public static final String ACCOUNT_ID = "test";
    public static final String PROJECT_ID = "shenzhen";
    public static final String USER_ID = "test";
    public static final String PASSWORD = "dash";
    private static final String TENANT = "tenantA";

    private static final int DEFAULT_LIMIT_PARTITION_REQUEST = 100;
    protected static Configuration conf;
    static ConfigurableApplicationContext catalogApp;
    public static String token;
    public static CatalogContext context;
    public static ConfigurableApplicationContext runCatalogApp(String testType) throws Exception {
            // set configuration
            if (testType.equals("polyCat")) {
                // run catalog application
                catalogApp = SpringApplication.run(CatalogApplication.class);
                try {
                    sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                AuthenticationResult response = doAuthentication(USER_ID, PASSWORD);
                token = response.getToken();
                context = new CatalogContext(PROJECT_ID, USER_ID, TENANT, token, DEFAULT_CATALOG_NAME);
                return catalogApp;
            }

        return null;
    }

    private static AuthenticationResult doAuthentication(String userName, String password) {
        String[] tenant = password.split(":");
        if (tenant.length == 2 && tenant[0].equals(tenant[1])) {
            LocalIdentity localIdentity = new LocalIdentity();
            localIdentity.setAccountId(tenant[0]);
            localIdentity.setIdentityOwner("IMCAuthenticator");
            return Authentication.authAndCreateToken(localIdentity);
        }
        LocalIdentity localIdentity = new LocalIdentity();
        localIdentity.setUserId(userName);
        localIdentity.setPasswd(password);
        localIdentity.setIdentityOwner("LocalAuthenticator");
        return Authentication.authAndCreateToken(localIdentity);
    }
    public static void setUpPolyCatServerConfig() throws Exception {
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
    public static void runHiveMetaStore(String testType) {
        if (testType.equals("polyCat")) {
            return;
        }
    }

    public static Warehouse createWarehouse(String testType) throws Exception {
        if (testType.equals("polyCat")) {
            setUpPolyCatServerConfig();
            new Warehouse(conf);;
        }
        return null;
    }

    public static IMetaStoreClient createHMSClient(String testType) throws Exception {
        if (testType.equals("polyCat")) {
            try {
                return new PolyCatMetaStoreClient();
            } catch (Throwable e) {
                System.err.println("Unable to open the metastore");
                System.err.println(StringUtils.stringifyException(e));
                throw new Exception(e);
            }
        }
        return null;
    }

    public static void clearTestEnv(String testType) {
        if (testType.equals("polyCat")) {
            return;
        }
    }

    public static Configuration createConf(String testType) throws Exception {
        if (testType.equals("polyCat")) {
            setUpPolyCatServerConfig();
            return conf;
        }
        return null;
    }

    public static void stopCatalogApp(String testType) throws TException {
        if (testType.equals("polyCat")) {
            // stop catalog application
            catalogApp.close();
            try {
                sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static List<Throwable> multiThreadExecute(Runnable runnable, String threadName, int threadNum)
        throws InterruptedException {
        Thread[] threads = new Thread[threadNum];
        for (int i = 0; i < threadNum; i ++) {
            threads[i] = new Thread(runnable , threadName + i);
        }

        return multiThreadExecute(threads);
    }

    public static List<Throwable> multiThreadExecute(Thread[] threads) throws InterruptedException {
        List<Throwable> uncaught = Collections.synchronizedList(new ArrayList<>());
        for (Thread thread : threads) {
            thread.setUncaughtExceptionHandler((t, e) -> uncaught.add(e));
        }

        for (Thread t : threads) {
            t.start();
        }

        for (Thread t : threads) {
            t.join(10*1000);
        }

        return uncaught;
    }
}
