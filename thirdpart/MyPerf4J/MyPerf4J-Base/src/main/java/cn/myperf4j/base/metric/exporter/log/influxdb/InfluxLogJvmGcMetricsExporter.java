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
package cn.myperf4j.base.metric.exporter.log.influxdb;

import cn.myperf4j.base.metric.JvmGcMetrics;
import cn.myperf4j.base.metric.formatter.JvmGcMetricsFormatter;
import cn.myperf4j.base.metric.formatter.influxdb.InfluxJvmGcMetricsFormatter;
import cn.myperf4j.base.metric.exporter.log.AbstractLogJvmGcMetricsExporter;

import java.util.Collections;

/**
 * Created by LinShunkang on 2018/8/25
 */
public class InfluxLogJvmGcMetricsExporter extends AbstractLogJvmGcMetricsExporter {

    private static final JvmGcMetricsFormatter METRICS_FORMATTER = new InfluxJvmGcMetricsFormatter();

    @Override
    public void process(JvmGcMetrics metrics, long processId, long startMillis, long stopMillis) {
        logger.log(METRICS_FORMATTER.format(Collections.singletonList(metrics), startMillis, stopMillis));
    }
}
