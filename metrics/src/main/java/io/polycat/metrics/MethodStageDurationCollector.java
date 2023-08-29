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

import java.io.Closeable;
import java.util.Map;

import io.prometheus.client.Histogram;
import io.prometheus.client.SimpleTimer;
import io.prometheus.client.exemplars.Exemplar;
import lombok.Getter;

public class MethodStageDurationCollector extends MetricsCollector {
    static private Histogram methodStage = null;

    static public Histogram getMethodStageCollector() {
        return methodStage;
    }

    static public void initMetricsCollectors() {
        if (methodStage == null) {
            methodStage = Histogram.build().name("method_call_stage_duration").help("duration of each method call stage")
                .labelNames("method_name", "stage").register();
        }
    }

    synchronized static public MethodStageDurationCollector.Timer startCollectTimer() {
        initMetricsCollectors();
        return new MethodStageDurationCollector.Timer(methodStage, System.nanoTime());
    }

    public static class Timer {
        private Histogram collector;
        private long start;
        private long prev;

        private Timer(Histogram collector, long start) {
            this.collector = collector;
            this.start = start;
            this.prev = start;
        }

        public double observePrevDuration(String... exemplarLabels) {
            long current = System.nanoTime();
            double elapsed = SimpleTimer.elapsedSecondsFromNanos(this.prev, current);
            this.collector.labels(exemplarLabels).observe(elapsed);
            this.prev = current;
            return elapsed;
        }

        public double observeTotalDuration(String... exemplarLabels) {
            double elapsed = SimpleTimer.elapsedSecondsFromNanos(this.start, System.nanoTime());
            this.collector.labels(exemplarLabels).observe(elapsed);
            return elapsed;
        }
    }
}
