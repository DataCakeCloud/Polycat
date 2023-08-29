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

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import io.polycat.catalog.common.PolyCatConf;
import io.polycat.catalog.common.Logger;

import com.alibaba.fastjson.JSONObject;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import io.prometheus.client.Collector;
import io.prometheus.client.CounterMetricFamily;
import io.prometheus.client.GaugeMetricFamily;
import io.prometheus.client.exporter.HTTPServer;

public class FdbExporter extends Collector {

    private static final Logger logger = Logger.getLogger(FdbExporter.class);
    private final FDB fdb;
    private FdbStats fdbStats;
    private JSONObject oldFdbConfiguration;

    public FdbExporter() {
        fdb = FDB.selectAPIVersion(610);
    }

    @Override
    public List<MetricFamilySamples> collect() {
        ArrayList mfs = new ArrayList();

        fdbStats = getStatsFromFdb(fdb);
        getFdbRunStatus(mfs,fdbStats);
        if (fdbStats.getAvailable() == false) {
            return mfs;
        }
        getConfiguration(mfs, fdbStats);
        getMetricsOfHealthy(mfs, fdbStats);
        getMetricsOfDisk(mfs, fdbStats);
        getMetricsOfCPU(mfs, fdbStats);
        getMachinesMetricsOfMemory(mfs, fdbStats);
        getFdbMetricsOfMemory(mfs, fdbStats);
        getMetricsOfNetwork(mfs, fdbStats);
        getMetricsOfPerformance(mfs, fdbStats);
        getMetricsOfQos(mfs, fdbStats);
        return mfs;
    }

    private FdbStats getStatsFromFdb(FDB fdb) {
        JSONObject jStatus;
        try (Database db = fdb.open()) {
            String sStatus = db.run(transaction -> {
                byte[] statusKey = getFdbStatusKey();
                byte[] result = transaction.get(statusKey).join();
                return new String(result, StandardCharsets.UTF_8);
            });
            jStatus = JSONObject.parseObject(sStatus);
        }

        return fdbStats = new FdbStats(jStatus);
    }

    private void getFdbRunStatus(ArrayList mfs, FdbStats fdbStats) {
        Integer status = (fdbStats.getAvailable() == false) ? 0 : 1;
        mfs.add(new GaugeMetricFamily("fdb_run_status_normal", "fdb run status.", status));
    }

    private void getConfiguration(ArrayList mfs, FdbStats fdbStats) {
        JSONObject fdbCfg = fdbStats.getFdbConfiguration();
        Integer equal = fdbCfg.equals(oldFdbConfiguration) ? 0 : 1;
        mfs.add(new GaugeMetricFamily("fdb_configuration_changed", "fdb configuration changed.", equal));
        oldFdbConfiguration = fdbCfg;

    }

    private void getMetricsOfHealthy(ArrayList mfs, FdbStats fdbStats) {
        Boolean healthy = fdbStats.getHealthy();
        double healthyD = healthy ? 1 : 0;
        mfs.add(new GaugeMetricFamily("fdb_cluster_healthy", "fdb_cluster healthy.", healthyD));
    }

    private byte[] getFdbStatusKey() {
        // the inner special key of fdb status is "\ff\ff/status/json"
        ByteBuffer statusKey = ByteBuffer.allocate("/status/json".length() + 2);
        statusKey.put((byte) 0xff);
        statusKey.put((byte) 0xff);
        statusKey.put("/status/json".getBytes(Charset.defaultCharset()));
        return statusKey.array();
    }

    private void getMetricsOfDisk(ArrayList mfs, FdbStats fdbStats) {
        int used = fdbStats.getDiskUsedBytes();
        //System.out.println("Start getMetricsOfDisk."+ fdbStats);
        long free = fdbStats.getFreeBytesTotal();
        double ratio = (double) used / (used + free);
        mfs.add(new GaugeMetricFamily("disk_used_bytes", "Disk used in bytes.", used));
        mfs.add(new GaugeMetricFamily("disk_usage_ratio", "Disk usage ratio.", ratio));
    }

    private void getMetricsOfCPU(ArrayList mfs, FdbStats fdbStats) {
        Map<String, FdbStats.MachineStat> machineStat = fdbStats.getMachineStatMap();
        double totalCpuUseRatio = 0;
        int keyCount = 0;
        for (String key : machineStat.keySet()) {
            FdbStats.MachineStat macheine = machineStat.get(key);
            double cpuUseRatio = macheine.getCpu();
            totalCpuUseRatio += cpuUseRatio;
            keyCount++;
        }
        double cpuUseRatio = 0;
        if (keyCount != 0) {
            cpuUseRatio = totalCpuUseRatio / keyCount;
        }
        mfs.add(new GaugeMetricFamily("avg_cpu_used_ratio", "Avg cpu used ratio", cpuUseRatio));
    }

    private void getMachinesMetricsOfMemory(ArrayList mfs, FdbStats fdbStats) {
        Map<String, FdbStats.MachineStat> machineStat = fdbStats.getMachineStatMap();
        Long totalUsed = 0L;
        Long totalFree = 0L;
        for (String key : machineStat.keySet()) {
            FdbStats.MachineStat macheine = machineStat.get(key);
            long used = macheine.getMemUsedBytes();
            long free = macheine.getMemFreeBytes();
            totalUsed += used;
            totalFree += free;
        }
        double ratio = 0;
        if (totalFree != 0) {
            ratio = (double) totalUsed / (totalUsed + totalFree);
        }
        mfs.add(new GaugeMetricFamily("machines_memory_used_bytes", "Machines memory used in bytes.", totalUsed));
        mfs.add(new GaugeMetricFamily("machines_memory_used_ratio", "Machines memory usage ratio.", ratio));
    }

    private void getFdbMetricsOfMemory(ArrayList mfs, FdbStats fdbStats) {
        Map<String, FdbStats.ProcessStat> processStat = fdbStats.getProcessStatMap();
        Long totalUsed = 0L;
        for (String key : processStat.keySet()) {
            FdbStats.ProcessStat process = processStat.get(key);
            long used = process.getMemUsedBytes();
            totalUsed += used;
        }
        mfs.add(new GaugeMetricFamily("fdb_process_memory_used_bytes", "Fdb process memory used in bytes.", totalUsed));
    }

    private void getMetricsOfNetwork(ArrayList mfs, FdbStats fdbStats) {
        Map<String, FdbStats.MachineStat> machineStat = fdbStats.getMachineStatMap();
        GaugeMetricFamily retransmitGuageFamily = new GaugeMetricFamily("network_tcp_segments_retransmit",
            "Network tcp segments retransmit.", Arrays.asList("node_ip"));
        GaugeMetricFamily sendGuageFamily = new GaugeMetricFamily("network_megabits_send", "Network megabits send.",
            Arrays.asList("node_ip"));
        GaugeMetricFamily receivedGuageFamily = new GaugeMetricFamily("network_megabits_received",
            "Network megabits received.", Arrays.asList("node_ip"));
        for (String key : machineStat.keySet()) {
            FdbStats.MachineStat macheine = machineStat.get(key);
            Integer TcpRetransmit = macheine.getNetworkTcpRetransmit();
            BigDecimal send = macheine.getNetworkMegabitSentHz();
            BigDecimal received = macheine.getNetworkMegabitReceivedHz();
            String address = macheine.getAddress();
            retransmitGuageFamily.addMetric(Arrays.asList(address), TcpRetransmit);
            sendGuageFamily.addMetric(Arrays.asList(address), send.doubleValue());
            receivedGuageFamily.addMetric(Arrays.asList(address), received.doubleValue());
        }
        mfs.add(retransmitGuageFamily);
        mfs.add(sendGuageFamily);
        mfs.add(receivedGuageFamily);
    }

    private void getMetricsOfPerformance(ArrayList mfs, FdbStats fdbStats) {
        BigDecimal ioReadsHz = fdbStats.getIoReadsHz();
        BigDecimal ioWritesHz = fdbStats.getIoWritesHz();
        BigDecimal txnStartHz = fdbStats.getTxnStartHz();
        BigDecimal txnCommitHz = fdbStats.getTxnCommitHz();
        BigDecimal txConflictHz = fdbStats.getTxnConflictHz();
        BigDecimal probeTxnStartHz = fdbStats.getProbeStartSeconds();
        BigDecimal probeTxnCommitHz = fdbStats.getProbeCommitSeconds();
        BigDecimal probeReadSeconds = fdbStats.getProbeReadSeconds();
        mfs.add(new GaugeMetricFamily("performance_io_reads_iops", "io reads iops.", ioReadsHz.doubleValue()));
        mfs.add(new GaugeMetricFamily("performance_io_writes_iops", "io writes iops.", ioWritesHz.doubleValue()));
        mfs.add(new GaugeMetricFamily("transaction_start_ops", "transaction start ops.", txnStartHz.doubleValue()));
        mfs.add(new GaugeMetricFamily("transaction_commit_ops", "transaction commit ops.", txnCommitHz.doubleValue()));
        mfs.add(
            new GaugeMetricFamily("transaction_conflict_ops", "transaction conflict ops.", txConflictHz.doubleValue()));
        mfs.add(new GaugeMetricFamily("transaction_probe_start", "probe transaction start.",
            probeTxnStartHz.doubleValue()));
        mfs.add(new GaugeMetricFamily("transaction_probe_commit", "probe transaction commit.",
            probeTxnCommitHz.doubleValue()));
        mfs.add(new GaugeMetricFamily("transaction_probe_read_seconds", "probe read seconds.",
            probeReadSeconds.doubleValue()));
    }

    private void getMetricsOfQos(ArrayList mfs, FdbStats fdbStats) {
        BigDecimal qosTransactionsPerSecondLimit = fdbStats.getQosTransactionsPerSecondLimit();
        BigDecimal qosReleaseTransactionsPerSecond = fdbStats.getQosReleaseTransactionsPerSecond();
        BigDecimal qosWorstQueueBytesStorageServer = fdbStats.getQosWorstQueueBytesStorageServer();
        BigDecimal qosWorstDurabilityLagStorageServerSeconds = fdbStats.getQosWorstDurabilityLagStorageServerSeconds();
        BigDecimal qosWorstDurabilityLagStorageServerVersions = fdbStats.getQosWorstDurabilityLagStorageServerVersions();
        BigDecimal qosWorstQueueBytesLogServer = fdbStats.getQosWorstQueueBytesLogServer();

        mfs.add(new GaugeMetricFamily("transactions_per_second_limit",
            "qos transactions per second limit.", qosTransactionsPerSecondLimit.doubleValue()));
        mfs.add(new GaugeMetricFamily("released_transactions_per_second",
            "qos released transactions per second.", qosReleaseTransactionsPerSecond.doubleValue()));
        mfs.add(new GaugeMetricFamily("worst_queue_bytes_storage_server",
            "qos worst queue bytes storage server.", qosWorstQueueBytesStorageServer.doubleValue()));
        mfs.add(new GaugeMetricFamily("worst_durability_lag_storage_server_seconds",
            "qos worst durability lag storage server seconds.", qosWorstDurabilityLagStorageServerSeconds.doubleValue()));
        mfs.add(new GaugeMetricFamily("worst_durability_lag_storage_server_versions",
            "qos worst durability lag storage server seconds versions.", qosWorstDurabilityLagStorageServerVersions.doubleValue()));
        mfs.add(new GaugeMetricFamily("worst_queue_bytes_log_server",
            "qos worst queue bytes log server.", qosWorstQueueBytesLogServer.doubleValue()));
    }

    public void start(int port) {
        try {
            new HTTPServer(port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static int getPort(String[] args) {
        String confPath;
        String className;
        if (args.length == 2) {
            // start server based on conf
            className = args[0];
            confPath = args[1];
        } else {
            // no conf file is provided, probably start from IDE
            String rootPath = System.getProperty("user.dir");
            confPath = rootPath + "/conf/fdb-metrics.conf";
        }

        logger.info("starting gateway server");
        PolyCatConf conf = new PolyCatConf(confPath);
        return conf.getMyperf4jPort();

    }

    public static void main(String[] args) {
        int port = getPort(args);
        logger.info("Starting fdb monitor, port: " + port);
        FdbExporter fdbExporter = new FdbExporter();
        fdbExporter.start(port);
        fdbExporter.register();
    }

    //static final FdbExporter requests = new FdbExporter().register();
}


