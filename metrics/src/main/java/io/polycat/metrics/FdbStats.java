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
package io.polycat.metrics;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import io.polycat.catalog.common.Logger;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;

@Data
public class FdbStats {

    private static final Logger logger = Logger.getLogger(FdbExporter.class);
    private  Boolean available;
    private Boolean healthy;
    private Integer replicaRemains;
    private Integer diskUsedBytes;
    private Long freeBytesTotal = 0L;
    private Map<String, ProcessStat> processStatMap = new HashMap<String, ProcessStat>();
    private Map<String, MachineStat> machineStatMap = new HashMap<String, MachineStat>();
    private BigDecimal ioReadsHz;
    private BigDecimal ioWritesHz;
    private BigDecimal txnStartHz;
    private BigDecimal txnCommitHz;
    private BigDecimal txnConflictHz;
    private BigDecimal probeStartSeconds;
    private BigDecimal probeCommitSeconds;
    private BigDecimal probeReadSeconds;
    private JSONObject fdbConfiguration;
    private BigDecimal qosTransactionsPerSecondLimit;
    private BigDecimal qosReleaseTransactionsPerSecond;
    private BigDecimal qosWorstQueueBytesStorageServer;
    private BigDecimal qosWorstDurabilityLagStorageServerSeconds;
    private BigDecimal qosWorstDurabilityLagStorageServerVersions;
    private BigDecimal qosWorstQueueBytesLogServer;

    public FdbStats(JSONObject object) {
        //available
        available = object.getJSONObject("cluster").getJSONObject("layers").getBoolean("_valid");
        try {
            //configuration
            fdbConfiguration = object.getJSONObject("cluster").getJSONObject("configuration");
            // healthy
            healthy = object.getJSONObject("cluster").getJSONObject("data").getJSONObject("state")
                .getBoolean("healthy");
            replicaRemains = object.getJSONObject("cluster").getJSONObject("data").getJSONObject("state")
                .getInteger("min_replicas_remaining");

            // used capacity
            diskUsedBytes = object.getJSONObject("cluster").getJSONObject("data").getInteger("total_disk_used_bytes");

            // status of each process
            JSONObject processes = object.getJSONObject("cluster").getJSONObject("processes");
            for (String key : processes.keySet()) {
                JSONObject process = processes.getJSONObject(key);
                ProcessStat processStat = new ProcessStat(process);
                processStatMap.put(key, processStat);
                freeBytesTotal += processStat.getFreeByte();
            }

            // status of each machine
            JSONObject machines = object.getJSONObject("cluster").getJSONObject("machines");
            for (String key : machines.keySet()) {
                // cpu, memory, network status
                JSONObject machine = machines.getJSONObject(key);
                MachineStat machineStat = new MachineStat(machine);
                machineStatMap.put(key, machineStat);
            }

            // IO per second
            JSONObject operations = object.getJSONObject("cluster").getJSONObject("workload")
                .getJSONObject("operations");
            ioReadsHz = operations.getJSONObject("reads").getBigDecimal("hz");
            ioWritesHz = operations.getJSONObject("writes").getBigDecimal("hz");

            // transactions per second
            JSONObject txn = object.getJSONObject("cluster").getJSONObject("workload").getJSONObject("transactions");
            txnStartHz = txn.getJSONObject("started").getBigDecimal("hz");
            txnCommitHz = txn.getJSONObject("committed").getBigDecimal("hz");
            txnConflictHz = txn.getJSONObject("conflicted").getBigDecimal("hz");

            // performance prediction
            JSONObject probe = object.getJSONObject("cluster").getJSONObject("latency_probe");
            probeStartSeconds = probe.getBigDecimal("transaction_start_seconds");
            probeCommitSeconds = probe.getBigDecimal("commit_seconds");
            probeReadSeconds = probe.getBigDecimal("read_seconds");

            JSONObject qos = object.getJSONObject("cluster").getJSONObject("qos");
            qosTransactionsPerSecondLimit = qos.getBigDecimal("transactions_per_second_limit");
            qosReleaseTransactionsPerSecond = qos.getBigDecimal("released_transactions_per_second");
            qosWorstQueueBytesStorageServer = qos.getBigDecimal("worst_queue_bytes_storage_server");
            qosWorstQueueBytesLogServer = qos.getBigDecimal("worst_queue_bytes_log_server");
            qosWorstDurabilityLagStorageServerVersions = qos.getJSONObject("worst_durability_lag_storage_server")
                .getBigDecimal("versions");
            qosWorstDurabilityLagStorageServerSeconds = qos.getJSONObject("worst_durability_lag_storage_server")
                .getBigDecimal("seconds");
        }catch (Exception e) {
            available = false;
        }
    }

    @Data
    public class ProcessStat {

        private String processId;
        private String machineId;
        private String zoneId;
        private String address;
        private String commandLine;
        private BigDecimal diskBusy; // bandwidth ratio of disk
        private BigDecimal readHz;
        private BigDecimal writeHz;
        private Long freeByte;
        private Long memUsedBytes;

        public ProcessStat(JSONObject obj) {
            processId = obj.getJSONObject("locality").getString("processid");
            machineId = obj.getJSONObject("locality").getString("machinedid");
            zoneId = obj.getJSONObject("locality").getString("zoneid");
            address = obj.getString("address");
            commandLine = obj.getString("command_line");
            diskBusy = obj.getJSONObject("disk").getBigDecimal("busy");
            readHz = obj.getJSONObject("disk").getJSONObject("reads").getBigDecimal("hz");
            writeHz = obj.getJSONObject("disk").getJSONObject("writes").getBigDecimal("hz");
            freeByte = obj.getJSONObject("disk").getLong("free_bytes");
            memUsedBytes = obj.getJSONObject("memory").getLong("used_bytes");
        }
    }


    @Data
    public class MachineStat {

        private String machineId;
        private String address;
        private double cpu;
        private Long memUsedBytes;
        private Long memFreeBytes;
        private BigDecimal networkMegabitSentHz;
        private BigDecimal networkMegabitReceivedHz;
        private Integer networkTcpRetransmit;

        public MachineStat(JSONObject obj) {
            machineId = obj.getString("machine_id");
            address = obj.getString("address");
            cpu = obj.getJSONObject("cpu").getDouble("logical_core_utilization");
            memUsedBytes = obj.getJSONObject("memory").getLong("committed_bytes");
            memFreeBytes = obj.getJSONObject("memory").getLong("free_bytes");
            networkMegabitSentHz = obj.getJSONObject("network").getJSONObject("megabits_sent").getBigDecimal("hz");
            networkMegabitReceivedHz = obj.getJSONObject("network").getJSONObject("megabits_received")
                .getBigDecimal("hz");
            networkTcpRetransmit = obj.getJSONObject("network").getJSONObject("tcp_segments_retransmitted")
                .getInteger("hz");
        }
    }
}
