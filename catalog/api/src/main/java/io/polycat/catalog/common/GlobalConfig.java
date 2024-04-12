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
package io.polycat.catalog.common;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;

public class GlobalConfig {

    public static final Item<Integer> SERVER_TRY_RECONNECT_OFFLINE_AGENT_MAX_COUNT =
        Item.newIntItem(
            "server.try_reconnect_offline_agent_max_count",
            10,
            "server try reconnect offlien agent max count");

    public static final Item<Integer> PROCESSOR_PROGRESS_TIMEOUT =
        Item.newIntItem(
            "processor.execution.timeout",
            300,
            "timeout in seconds for downsteam processor to wait for upstream processor");

    public static final Item<Integer> SERVER_CHECK_AGENTS_TRUST_INTERVAL =
        Item.newIntItem(
            "server.check.agents.trust.interval",
            60,
            "time interval in seconds that server check and retry agent trust");

    public static final Item<Integer> WORKER_HEARTBEAT_INTERVAL =
        Item.newIntItem(
            "worker.heartbeat.interval",
            300,
            "time interval in seconds that worker sends heartbeat to server");

    public static final Item<Integer> AGENT_RESULT_FETCHER_DEAD_LINE =
        Item.newIntItem(
            "agent.resultfetcher.deadline",
            300,
            "maximum execution time inside for one stage in the agent");

    public static final Item<Integer> WORKER_EXECUTION_PROGRESS_TIMEOUT =
        Item.newIntItem(
            "worker.execution.timeout",
            300,
            "timeout in seconds for worker while executing job");

    public static final Item<Integer> AGENT_RESULT_FETCHER_THREAD_POOL_SIZE =
        Item.newIntItem(
            "agent.resultfetcher.threadpool.size",
            10,
            "thread pool size for result fetcher between workers");

    public static final Item<Integer> SERVER_HEARTBEAT_TIMEOUT_SECOND =
        Item.newIntItem(
            "server.heartbeat.timeout",
            1000,
            "heartbeat monitor timeout in seconds in server side");

    public static final Item<Integer> TRUST_PROCESS_TIMEOUT =
        Item.newIntItem(
            "server.agent.trust.timeout",
            1000,
            "agent trust process timeout in seconds waiting by server");

    public static final Item<Integer> RPC_TIMEOUT =
        Item.newIntItem(
            "rpc.timeout",
            300,
            "timeout in seconds for RPC call data flow, such as sending processor result");

    public static final Item<Integer> RPC_CONTROL_FLOW_TIMEOUT =
        Item.newIntItem(
            "rpc.control.flow.timeout",
            1000,
            "timeout in seconds for RPC call control flow, such as deploying agent, invoke agent...");

    // createWorker请求需要遍历计划并返回workerName，这个过程会比较久而且因为要返回workerName不好做异步，因此把这个超时时间延长
    public static final Item<Integer> CREATE_WORKER_TIMEOUT =
        Item.newIntItem(
            "create.worker.timeout",
            200,
            "timeout in seconds for create worker to deploying agent");

    public static final Item<Integer> SQL_QUERY_TIMEOUT =
        Item.newIntItem(
            "sql.query.timeout",
            600,
            "query timeout in seconds");

    public static final Item<String> JOB_STATUS_STORE_PATH =
        Item.newStringItem(
            "job.status.store.path",
            "./job_log",
            "job status log path");

    public static final Item<Integer> JOB_STATUS_REPORT_PERIOD =
        Item.newIntItem(
            "job.status.report.period",
            10,
            "job status report period time in seconds in agent side");

    public static final Item<String> SERVER_CERT =
        Item.newStringItem(
            "server.cert",
            "cert/server.cer",
            "server cert relative path in server resources");

    public static final Item<String> SERVER_KEY =
        Item.newStringItem(
            "server.key",
            "cert/server_private_key.der",
            "server key relative path in server resources");

    public static final Item<Integer> PUSH_QUEUE_SIZE =
        Item.newIntItem(
            "push.queue.size",
            10,
            "size of the queue to store output from upstream processor");

    public static final Item<Integer> SQL_RECORD_OUTPUTCOUNT =
        Item.newIntItem(
            "sql.job.output.count",
            100,
            "sampleRowCount of sql job output to server");

    public static final Item<Integer> RPC_MESSAGE_MAX_SIZE =
        Item.newIntItem(
            "rpc.message.size",
            400 * 1024 * 1024,
            "max message size for rpc message");

    public static final Item<Integer> JOB_STATUS_QUERY_INTERVAL =
        Item.newIntItem(
            "job.status.query.interval",
            5 * 1000,
            "interval in milliseconds for job status query");

    public static final Item<String> OBS_AK =
        Item.newStringItem(
            "fs.obs.access.key",
            "",
            "fs obs access key");

    public static final Item<String> OBS_SK =
        Item.newStringItem(
            "fs.obs.secret.key",
            "",
            "fs obs secret key");

    public static final Item<String> OBS_TOKEN =
        Item.newStringItem(
            "fs.obs.session.token",
            "",
            "fs.obs.session.token");

    public static final Item<String> OBS_ENDPOINT =
        Item.newStringItem(
            "fs.obs.endpoint",
            "",
            "fs obs endpoint");

    public static final Item<String> OBS_IMPL =
        Item.newStringItem(
            "fs.obs.impl",
            "org.apache.hadoop.fs.obs.OBSFileSystem",
            "fs obs impl");

    public static final Item<Integer> CONNECTOR_STREAM_BATCH_SIZE =
        Item.newIntItem(
            "connector.stream.batch.size",
            1000,
            "row count for each batch returning by connector query");

    public static final Item<Boolean> FUSION_EXECUTION_ENABLED =
        Item.newBoolItem(
            "fusion.execution.enabled",
            false,
            "perform map reduce fusion optimization");

    public static final Item<Boolean> REGISTER_ASYNC_ENABLED =
        Item.newBoolItem(
            "register.async.enabled",
            true,
            "register async enabled");

    public static final Item<Integer> JOIN_CONVERGENCE_THRESHOLD =
        Item.newIntItem(
            "join.convergence.threshold",
            1,
            "join convergence max count");

    // 并行流支持存储的最大记录数,默认500w,则批数量为500w/1000=100（流式处理，最多存储500w+1000条，不会溢出，当前单节点agg内存足够）
    public static final Item<Integer> PALSTREAM_MAX_RECORD_COUNT =
        Item.newIntItem(
            "paralle.stream.max.record.batchcount",
            500 * 10000,
            "size of the queue to store output from upstream processor");

    // 并行流插入发生阻塞时的最大阻塞时间，超过了说明存在大大表join.流无法正常进行传输。单位秒
    public static final Item<Integer> PALSTREAM_PUT_BLOCK_TIMEOUT =
        Item.newIntItem(
            "parallel.stream.put.block.timeout",
            120,
            "paralle stream block time out");

    public static final Item<Integer> REDUCER_MAX_PARALLELISM =
        Item.newIntItem(
            "reducer.max.parallelism",
            10,
            "maximum parallelism for the reduce stage");

    public static final Item<Boolean> SOURCE_READ_AS_ARROW =
        Item.newBoolItem(
            "source.arrow.reader",
            false,
            "for data source interface, read data as arrow vector");

    public static final Item<String> IDENTITY_FILE_PATH =
        Item.newStringItem(
            "identity.file.path",
            "",
            "identity configuration file path");

    public static final Item<String> WAREHOUSE =
        Item.newStringItem(
            "catalog.sql.warehouse.dir",
            "file:/tmp/hive/warehouse",
            "warehouse path");

    public static final Item<Integer> USAGE_PROFILE_COLLECT_CYCLE = Item
        .newIntItem("usage.profile.collect.cycle", 60 * 1000, "usage profile collect time gap");

    public static final Item<String> CONF_DIR =
        Item.newStringItem(
            "catalog.conf.dir",
            "../conf",
            "configuration path");

    public static final Item<String> AUTH_IMPL =
        Item.newStringItem(
            "auth.impl",
            "LocalAuthenticator",
            "authenticator impl");

    public static final Item<Boolean> MV_REWRITE_ENABLED =
        Item.newBoolItem(
            "mv.rewrite.enabled",
            false,
            "perform query rewrite");

    public static final Item<Boolean> AUTH_ENABLED =
        Item.newBoolItem(
            "auth.enable",
            false,
            "authorization enable");

    public static final Item<Boolean> AUTH2_ENABLED =
        Item.newBoolItem(
            "auth2.enable",
            false,
            "authorization version2  enable");

    private static final Map<String, Item<?>> CONF = new ConcurrentHashMap<>();

    static {
        loadConfig();
    }

    private static class Item<T> {

        private String name;

        private T value;

        private T defaultValue;

        private String description;

        private Item(String name, T defaultValue, String description) {
            this.name = name;
            this.defaultValue = defaultValue;
            this.description = description;
        }

        static Item<Boolean> newBoolItem(String name, boolean defaultValue, String description) {
            return new Item<>(name, defaultValue, description);
        }

        static Item<Integer> newIntItem(String name, int defaultValue, String description) {
            return new Item<>(name, defaultValue, description);
        }

        static Item<String> newStringItem(String name, String defaultValue, String description) {
            return new Item<>(name, defaultValue, description);
        }

        T getValue() {
            return value == null ? defaultValue : value;
        }

        String getDescription() {
            return description;
        }
    }

    public static void loadConfig() {
        Field[] fields = GlobalConfig.class.getDeclaredFields();
        try {
            for (Field field : fields) {
                field.setAccessible(true);
                if (field.getType().toString().contains("GlobalConfig$Item")
                    && Modifier.isStatic(field.getModifiers())) {
                    Item<?> item = (Item<?>) field.get(null);
                    CONF.put(item.name, item);
                }
            }
        } catch (IllegalArgumentException | IllegalAccessException e) {
            throw new RuntimeException("loadConfig failed", e);
        }
    }

    public static <T> void set(Item<T> item, T value) {
        item.value = value;
    }

    public static <T> void setDefault(Item<T> item, T value) {
        item.defaultValue = value;
    }

    public static <T> void resetToDefault(Item<T> item) {
        item.value = item.defaultValue;
    }

    public static String getName(Item<String> item) {
        if (CONF.containsKey(item.name)) {
            return item.name;
        } else {
            throw new RuntimeException("item not exist: " + item.name);
        }
    }

    public static boolean isTrue(Item<Boolean> item) {
        if (CONF.containsKey(item.name)) {
            return ((Item<Boolean>) CONF.get(item.name)).getValue();
        } else {
            throw new RuntimeException("item not exist: " + item.name);
        }
    }

    public static int getInt(Item<Integer> item) {
        if (CONF.containsKey(item.name)) {
            return ((Item<Integer>) CONF.get(item.name)).getValue();
        } else {
            throw new RuntimeException("item not exist: " + item.name);
        }
    }

    public static String getString(Item<String> item) {
        if (CONF.containsKey(item.name)) {
            return ((Item<String>) CONF.get(item.name)).getValue();
        } else {
            throw new RuntimeException("item not exist: " + item.name);
        }
    }

    public static <T> T getValue(String name) {
        if (CONF.containsKey(name)) {
            return ((Item<T>) CONF.get(name)).getValue();
        } else {
            throw new RuntimeException("item not exist: " + name);
        }
    }

    // 根据item名去setvalue
    public static <T> void setItem(String itemName, T value) {
        if (CONF.containsKey(itemName)) {
            Item<T> item = (Item<T>) CONF.get(itemName);
            set(item, value);
        }
    }

    public static List<String> getItemNameList() {
        return new ArrayList<>(CONF.keySet());
    }

    public static void configItem(Configuration config, Item<String> item) {
        config.set(item.name, getString(item));
    }
}
