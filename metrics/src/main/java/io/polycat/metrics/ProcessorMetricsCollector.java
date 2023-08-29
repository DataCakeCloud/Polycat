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
import java.util.UUID;

import io.polycat.catalog.common.Logger;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Histogram;
import lombok.Getter;

@Getter
public class ProcessorMetricsCollector extends MetricsCollector {

    private static final Logger LOGGER = Logger.getLogger(ProcessorMetricsCollector.class.getName());

    private CollectorRegistry registry = CollectorRegistry.defaultRegistry;

    private Histogram processorCollector;

    private Histogram.Timer timer;

    public String name;

    public ProcessorMetricsCollector(String jobID, int stageNo, String clzName) {
        init(jobID, stageNo, clzName);
    }

    private void init(String jobID, int stageNo, String clzName) {
        String shortUUID = UUID.randomUUID().toString().split("-")[0];
        String name = "jobId_" + jobID + "_stageNo_" + stageNo + "_processor_" + clzName + "_" + shortUUID;
        this.name = name;
        processorCollector = Histogram.build().name(name).help("processing latency of each processor")
                .labelNames("phase").register(this.registry);
    }

    public Histogram.Timer start(String phase) {
        this.timer =  processorCollector.labels(phase).startTimer();
        //LOGGER.info(name + " "+ phase + " " + "observe_start: " + new SimpleDateFormat("HH:mm:ss.SSS").format(new Date()));
        return this.timer;
    }

    public void observeElapsed() {
        //LOGGER.info( name + " "+ "observe_end: " + new SimpleDateFormat("HH:mm:ss.SSS").format(new Date()));
        this.timer.observeDuration();
    }

    public Histogram.Child getChild(String phase) {
        return this.processorCollector.labels(phase);
    }

    @Override
    public List<MetricFamilySamples> collect() {
        return new ArrayList<>();
    }

    public enum Phase {
        HAS_NEXT,
        NEXT,
        COLLECT_INPUT,
        PROCESS_INPUT,
        SEND_DATA,
        META_DATA;
    }
}
