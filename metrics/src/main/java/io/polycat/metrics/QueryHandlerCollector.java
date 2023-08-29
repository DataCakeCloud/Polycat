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

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Histogram;
import lombok.Getter;

@Getter
public class QueryHandlerCollector extends Collector {

    private CollectorRegistry registry = CollectorRegistry.defaultRegistry;

    private Histogram queryJobBreakdown;

    private Stack<Histogram.Timer> timers = new Stack<>();

    public QueryHandlerCollector(String jobId) {
        initTimer(jobId);
    }

    private void initTimer(String jobId) {
        String name = "query_breakdown_job_id_" + jobId;
        queryJobBreakdown = new Histogram.Builder().name(name)
                .help("breakdown of each query job, with labels: \"phase\"").labelNames("phase")
                .register(this.registry);
    }

    public enum QueryPhase {
        build_DAG,
        allocate_cores,
        build_stages,
        deploy_stages;
    }

    public void startPhase(QueryPhase phase) {
        startPhase(phase.name());
    }

    public void startPhase(String phaseName) {
        timers.push(queryJobBreakdown.labels(phaseName).startTimer());
    }

    public void endPhase() {
        timers.pop().observeDuration();
    }

    @Override
    public List<MetricFamilySamples> collect() {
        return new ArrayList<>();
    }
}
