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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;

public class HiveServiceTest {
    public static void main(String[] args) throws MetaException {
        Thread thread = null;
        try {
            Configuration conf = MetastoreConf.newMetastoreConf();
            HiveMetaStore metaStore = new HiveMetaStore();
            thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        metaStore.startMetaStore(9083, HadoopThriftAuthBridge.getBridge(), conf);
                    } catch (Throwable throwable) {
                    }
                }
            }, "hiveStore");
            thread.setDaemon(true);
            thread.start();
            Thread.sleep(5000);
            IMetaStoreClient client = new HiveMetaStoreClient(conf);
            client.getCatalog("no");

        } catch (Throwable e) {
            e.printStackTrace();
        }
        System.exit(0);
    }
}
