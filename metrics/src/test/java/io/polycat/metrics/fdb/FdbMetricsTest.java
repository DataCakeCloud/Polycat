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
package io.polycat.metrics.fdb;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import com.alibaba.fastjson.JSONObject;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FdbMetricsTest {

    FDB fdb = FDB.selectAPIVersion(610); // 610 is the api version for current fdb client, whose version v6.3.18

    @Test
    public void directly_get_fdb_static_stat() {
        try (Database db = fdb.open()) {
            String sStatus = db.run(transaction -> {
                // the inner special key of fdb status is "\ff\ff/status/json"
                ByteBuffer statusKey = ByteBuffer.allocate("/status/json".length() + 2);
                statusKey.put((byte) 0xff);
                statusKey.put((byte) 0xff);
                statusKey.put("/status/json".getBytes(Charset.defaultCharset()));
                byte[] result = transaction.get(statusKey.array()).join();

                return new String(result, StandardCharsets.UTF_8);
            });

            System.out.println(sStatus);
            JSONObject jStatus = JSONObject.parseObject(sStatus);
            assertNotNull(jStatus);

            /****** client status ******/
            assertTrue(jStatus.getJSONObject("client").getJSONObject("database_status").getBoolean("healthy"));
            assertTrue(jStatus.getJSONObject("client").getJSONObject("database_status").getBoolean("available"));
            assertEquals("/etc/foundationdb/fdb.cluster",
                jStatus.getJSONObject("client").getJSONObject("cluster_file").getString("path"));

            /****** cluster status ******/
            assertNotNull(jStatus.getJSONObject("cluster").getJSONObject("configuration"));
            assertNotNull(jStatus.getJSONObject("cluster").getJSONObject("data").getInteger("total_disk_used_bytes"));

            // healthy status
            assertNotNull(
                jStatus.getJSONObject("cluster").getJSONObject("data").getJSONObject("state").getBoolean("healthy"));
            assertNotNull(jStatus.getJSONObject("cluster").getJSONObject("data").getJSONObject("state")
                .getBoolean("min_replicas_remaining"));

            // status of each process
            JSONObject processes = jStatus.getJSONObject("cluster").getJSONObject("processes");
            for (String key : processes.keySet()) {
                JSONObject process = processes.getJSONObject(key);
                assertNotNull(process.getString("address"));
                assertNotNull(process.getJSONObject("disk").getLong("free_bytes"));
            }

            // status of each machine
            JSONObject machines = jStatus.getJSONObject("cluster").getJSONObject("machines");
            for (String key : machines.keySet()) {
                // cpu, memory status
                JSONObject machine = machines.getJSONObject(key);
                assertNotNull(machine.getJSONObject("cpu").getBigDecimal("logical_core_utilization"));
                assertNotNull(machine.getJSONObject("memory").getLong("committed_bytes"));
                assertNotNull(machine.getJSONObject("memory").getLong("free_bytes"));

                // network status
                assertNotNull(machine.getJSONObject("network").getJSONObject("megabits_received").getBigDecimal("hz"));
                assertNotNull(machine.getJSONObject("network").getJSONObject("megabits_sent").getBigDecimal("hz"));
                assertNotNull(
                    machine.getJSONObject("network").getJSONObject("tcp_segments_retransmitted").getInteger("hz"));
            }

            // performance prediction
            JSONObject probe = jStatus.getJSONObject("cluster").getJSONObject("latency_probe");
            assertNotNull(probe.getBigDecimal("transaction_start_seconds"));
            assertNotNull(probe.getBigDecimal("commit_seconds"));
            assertNotNull(probe.getBigDecimal("read_seconds"));

            // IO per second
            JSONObject operations = jStatus.getJSONObject("cluster").getJSONObject("workload")
                .getJSONObject("operations");
            assertNotNull(operations.getJSONObject("reads").getBigDecimal("hz"));
            assertNotNull(operations.getJSONObject("writes").getInteger("hz"));

            // transactions per second
            JSONObject txn = jStatus.getJSONObject("cluster").getJSONObject("workload").getJSONObject("transactions");
            assertNotNull(txn.getJSONObject("started").getBigDecimal("hz"));
            assertNotNull(txn.getJSONObject("committed").getBigDecimal("hz"));
            assertNotNull(txn.getJSONObject("conflicted").getBigDecimal("hz"));
        }
    }
}