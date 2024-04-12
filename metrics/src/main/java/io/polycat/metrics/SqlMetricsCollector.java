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

import io.prometheus.client.Histogram;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SqlMetricsCollector extends MetricsCollector {
    // metrics for prometheus
    @Getter
    private Histogram sqlExecutionOverview;
    @Getter
    private Histogram sqlBreakdown;

    public SqlMetricsCollector() {
        initTimers();
    }

    @Override
    public List<MetricFamilySamples> collect() {
        // code to collect self defined metrics or statistics from job executor.
        // example below: gauge with name{metricName}, label{"metrics_prototype"=v0.1, "ddl"=730}, help{"help message add here"} and value{1}.
        List<MetricFamilySamples> mfs = new ArrayList<>();
        String metricName = "collect_metrics_example";

        MetricFamilySamples.Sample s1 = new MetricFamilySamples.Sample(metricName,
                Arrays.asList("metrics_prototype", "DeadLine"), Arrays.asList("v0.1", "730"), 1);
        MetricFamilySamples samples = new MetricFamilySamples(metricName, Type.GAUGE, "help message add here",
                Arrays.asList(s1));
        mfs.add(samples);
        return mfs;
    }

    private void initTimers() {
        // metrics: histogram
        sqlBreakdown = Histogram.build().name("sql_server_breakdown_seconds").help("runtime breakdown of each sql statement")
                .labelNames("jobId", "sqlStatement", "step", "phase").register(this.registry);
        sqlExecutionOverview = Histogram.build().name("sql_server_seconds").help("overview for sql")
                .labelNames("status").register(this.registry);
    }

    public enum Status {
        success,
        fail;
    }
}
