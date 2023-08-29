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
package cn.myperf4j.base.metric.exporter.log.standard;

import cn.myperf4j.base.metric.MethodMetrics;
import cn.myperf4j.base.metric.formatter.standard.StdMethodMetricsFormatter;
import cn.myperf4j.base.metric.formatter.MethodMetricsFormatter;
import cn.myperf4j.base.metric.exporter.log.AbstractLogMethodMetricsExporter;
import cn.myperf4j.base.util.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.HashMap;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.PushGateway;
import io.prometheus.client.GaugeMetricFamily;
import io.prometheus.client.Gauge;

public class StdLogMethodMetricsExporter extends AbstractLogMethodMetricsExporter {

    private static final MethodMetricsFormatter METRICS_FORMATTER = new StdMethodMetricsFormatter();

    private final ConcurrentHashMap<Long, List<MethodMetrics>> metricsMap = new ConcurrentHashMap<>(8);

    @Override
    public void beforeProcess(long processId, long startMillis, long stopMillis) {
        metricsMap.put(processId, new ArrayList<MethodMetrics>(64));
    }

    @Override
    public void process(MethodMetrics metrics, long processId, long startMillis, long stopMillis) {
        List<MethodMetrics> metricsList = metricsMap.get(processId);
        if (metricsList != null) {
            metricsList.add(metrics);
        } else {
            Logger.error("StdLogMethodMetricsExporter.process(" + processId + ", " + startMillis + ", "
                + stopMillis + "): metricsList is null!!!");
        }
    }

    protected String getPushGatewayEnv() {
        String env = System.getenv("PUSHGATEWAY_URL");
        if (env == null || env.length() == 0) {
            env = "localhost:9091";
        }
        return env;
    }

    protected final String adjustApiName(String apiName) {
        int firstPos = apiName.indexOf("(");
        if (firstPos != -1) {
            apiName = apiName.substring(0, firstPos);
        }
        apiName = apiName.replaceAll("\\.", "_");
        return apiName;
    }

    protected void flushDelay2PushGateway(final String readApiName, double delayTime,
        final String desc, final String job_desc) {
        try {
            String url = getPushGatewayEnv();

            Gauge delayTimeGuage = Gauge.build(readApiName, desc).create();
            CollectorRegistry delayRegistry = new CollectorRegistry();
            delayTimeGuage.set(delayTime);
            delayTimeGuage.register(delayRegistry);
            PushGateway avgPg = new PushGateway(url);
            avgPg.pushAdd(delayRegistry, job_desc);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void flushCound2PushGateway(final String readApiName, long count) {
        try {
            String url = getPushGatewayEnv();

            Gauge countGuage = Gauge.build(readApiName, "Api calc delay count").create();
            CollectorRegistry countRegistry = new CollectorRegistry();
            countGuage.set(count);
            countGuage.register(countRegistry);
            PushGateway countPg = new PushGateway(url);
            countPg.pushAdd(countRegistry, "polycat_api_count");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void afterProcess(long processId, long startMillis, long stopMillis) {
        List<MethodMetrics> metricsList = metricsMap.remove(processId);
        if (metricsList != null) {
            for (int i = 0; i < metricsList.size(); ++i) {
                MethodMetrics metrics = metricsList.get(i);
                if (metrics.getTotalCount() <= 0) {
                    continue;
                }
                final String realApiName = adjustApiName(metrics.getMethodTag().getSimpleDesc());
                flushDelay2PushGateway(realApiName, metrics.getAvgTime(),
                    "Api vag time delay", "polycat_api_avg_delay(ms)");
                flushDelay2PushGateway(realApiName, metrics.getMinTime(),
                    "Api min time delay", "polycat_api_min_delay(ms)");
                flushDelay2PushGateway(realApiName, metrics.getMaxTime(),
                    "Api max time delay", "polycat_api_max_delay(ms)");
                flushCound2PushGateway(realApiName, metrics.getTotalCount());
            }
            logger.logAndFlush(METRICS_FORMATTER.format(metricsList, startMillis, stopMillis));
        } else {
            Logger.error("StdLogMethodMetricsExporter.afterProcess(" + processId + ", " + startMillis + ", "
                + stopMillis + "): metricsList is null!!!");
        }
    }
}
