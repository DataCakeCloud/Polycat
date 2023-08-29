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

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;

public class MetricsCollector extends Collector {
    protected final CollectorRegistry registry = CollectorRegistry.defaultRegistry;

    // simple timer
    private Long startNanoTime;
    private Long prevObserveNanoTime;

    public void startTimer() {
        startNanoTime = System.nanoTime();
        prevObserveNanoTime = startNanoTime;
    }

    public double observeFromStartTimer() {
        return elapsedSecondsFromNanos(startNanoTime, System.nanoTime());
    }

    public double observeFromPrevObserveTimer() {
        return elapsedSecondsFromNanos(prevObserveNanoTime, System.nanoTime());
    }

    private double elapsedSecondsFromNanos(Long startNanos, long endNanos) {
        prevObserveNanoTime = endNanos;
        return (endNanos - startNanos) / Collector.NANOSECONDS_PER_SECOND;
    }

    @Override
    public List<MetricFamilySamples> collect() {
        return new ArrayList<>();
    }
}
