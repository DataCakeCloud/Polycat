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

import cn.myperf4j.base.MethodTag;
import cn.myperf4j.base.config.ProfilingConfig;
import cn.myperf4j.base.metric.MethodMetrics;
import cn.myperf4j.base.metric.formatter.MethodMetricsFormatter;
import cn.myperf4j.base.util.ListUtils;

import java.util.List;

import static cn.myperf4j.base.util.IpUtils.getLocalhostName;
import static cn.myperf4j.base.util.LineProtocolUtils.processTagOrField;
import static cn.myperf4j.base.util.NumFormatUtils.doubleFormat;

/**
 * Created by LinShunkang on 2020/5/17
 */
public final class InfluxMethodMetricsFormatter implements MethodMetricsFormatter {

    private static final ThreadLocal<StringBuilder> SB_TL = new ThreadLocal<StringBuilder>() {
        @Override
        protected StringBuilder initialValue() {
            return new StringBuilder(32 * 1024);
        }
    };

    @Override
    public String format(List<MethodMetrics> metricsList, long startMillis, long stopMillis) {
        if (ListUtils.isEmpty(metricsList)) {
            return "";
        }

        StringBuilder sb = SB_TL.get();
        try {
            long startNanos = startMillis * 1000 * 1000L;
            for (int i = 0; i < metricsList.size(); ++i) {
                MethodMetrics metrics = metricsList.get(i);
                appendLineProtocol(metrics, startNanos, sb);
                sb.append('\n');
            }
            return sb.substring(0, sb.length() - 1);
        } finally {
            sb.setLength(0);
        }
    }

    private void appendLineProtocol(MethodMetrics metrics, long startNanos, StringBuilder sb) {
        MethodTag methodTag = metrics.getMethodTag();
        String methodDesc = processTagOrField(methodTag.getSimpleDesc());
        sb.append("method_metrics")
                .append(",AppName=").append(ProfilingConfig.basicConfig().appName())
                .append(",ClassName=").append(methodTag.getSimpleClassName())
                .append(",Method=").append(methodDesc)
                .append(",Type=").append(methodTag.getType())
                .append(",Level=").append(methodTag.getLevel())
                .append(",host=").append(processTagOrField(getLocalhostName()))
                .append(" TotalTimePercent=").append(metrics.getTotalTimePercent())
                .append(",RPS=").append(metrics.getRPS()).append('i')
                .append(",Avg=").append(doubleFormat(metrics.getAvgTime()))
                .append(",Min=").append(metrics.getMinTime()).append('i')
                .append(",Max=").append(metrics.getMaxTime()).append('i')
                .append(",StdDev=").append(doubleFormat(metrics.getStdDev()))
                .append(",Count=").append(metrics.getTotalCount()).append('i')
                .append(",TP50=").append(metrics.getTP50()).append('i')
                .append(",TP90=").append(metrics.getTP90()).append('i')
                .append(",TP95=").append(metrics.getTP95()).append('i')
                .append(",TP99=").append(metrics.getTP99()).append('i')
                .append(",TP999=").append(metrics.getTP999()).append('i')
                .append(",TP9999=").append(metrics.getTP9999()).append('i')
                .append(' ').append(startNanos);
    }
}
