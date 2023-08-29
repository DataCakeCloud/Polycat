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
package cn.myperf4j.base.metric.formatter.influxdb;

import cn.myperf4j.base.config.ProfilingConfig;
import cn.myperf4j.base.metric.JvmThreadMetrics;
import cn.myperf4j.base.metric.formatter.JvmThreadMetricsFormatter;

import java.util.List;

import static cn.myperf4j.base.util.IpUtils.getLocalhostName;
import static cn.myperf4j.base.util.LineProtocolUtils.processTagOrField;

/**
 * Created by LinShunkang on 2020/5/17
 */
public class InfluxJvmThreadMetricsFormatter implements JvmThreadMetricsFormatter {

    private static final ThreadLocal<StringBuilder> SB_TL = new ThreadLocal<StringBuilder>() {
        @Override
        protected StringBuilder initialValue() {
            return new StringBuilder(256);
        }
    };

    @Override
    public String format(List<JvmThreadMetrics> metricsList, long startMillis, long stopMillis) {
        StringBuilder sb = SB_TL.get();
        try {
            long startNanos = startMillis * 1000 * 1000L;
            for (int i = 0; i < metricsList.size(); ++i) {
                JvmThreadMetrics metrics = metricsList.get(i);
                appendLineProtocol(metrics, startNanos, sb);
                sb.append('\n');
            }
            return sb.substring(0, sb.length() - 1);
        } finally {
            sb.setLength(0);
        }
    }

    private void appendLineProtocol(JvmThreadMetrics metrics, long startNanos, StringBuilder sb) {
        sb.append("jvm_thread_metrics")
                .append(",AppName=").append(ProfilingConfig.basicConfig().appName())
                .append(",host=").append(processTagOrField(getLocalhostName()))
                .append(" TotalStarted=").append(metrics.getTotalStarted()).append('i')
                .append(",Active=").append(metrics.getActive()).append('i')
                .append(",Peak=").append(metrics.getPeak()).append('i')
                .append(",Daemon=").append(metrics.getDaemon()).append('i')
                .append(",New=").append(metrics.getNews()).append('i')
                .append(",Runnable=").append(metrics.getRunnable()).append('i')
                .append(",Blocked=").append(metrics.getBlocked()).append('i')
                .append(",Waiting=").append(metrics.getWaiting()).append('i')
                .append(",TimedWaiting=").append(metrics.getTimedWaiting()).append('i')
                .append(",Terminated=").append(metrics.getTerminated()).append('i')
                .append(' ').append(startNanos);
    }
}
