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
package io.polycat.hiveService.hive;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveMetaStoreClientUtil {

    private static final Logger log = LoggerFactory.getLogger(HiveMetaStoreClientUtil.class);
    protected static HiveConf conf = new HiveConf();
    private static final ThreadLocal<IMetaStoreClient> threadLocalMap = ThreadLocal.withInitial(() -> create());

    public static IMetaStoreClient getHMSClient() {
        return threadLocalMap.get();
    }

    public static void returnHMSClient() {
        threadLocalMap.remove();
    }

    private static IMetaStoreClient create() {
        try {
            IMetaStoreClient client = RetryingMetaStoreClient.getProxy(conf, true);
            log.info("get hive metastore client success");
            return client;
        } catch (MetaException e) {
            log.error("Failed to get client, due to {}, stack trace {}", e.getLocalizedMessage(), e.getStackTrace());
        }
        return null;
    }
}
