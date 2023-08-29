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
package cn.myperf4j.base.metric.formatter.standard;

import cn.myperf4j.base.metric.JvmCompilationMetrics;
import cn.myperf4j.base.metric.formatter.JvmCompilationMetricsFormatter;
import cn.myperf4j.base.util.DateFormatUtils;

import java.util.List;

import static cn.myperf4j.base.util.SysProperties.LINE_SEPARATOR;

/**
 * Created by LinShunkang on 2018/8/21
 */
public final class StdJvmCompilationMetricsFormatter implements JvmCompilationMetricsFormatter {

    @Override
    public String format(List<JvmCompilationMetrics> metricsList, long startMillis, long stopMillis) {
        String dataTitleFormat = "%-16s%16s%n";
        StringBuilder sb = new StringBuilder((metricsList.size() + 2) * (16 + 24));
        sb.append("MyPerf4J JVM Compilation Metrics [").append(DateFormatUtils.format(startMillis)).append(", ")
                .append(DateFormatUtils.format(stopMillis)).append(']').append(LINE_SEPARATOR);
        sb.append(String.format(dataTitleFormat, "Time(ms)", "TotalTime(ms)"));
        if (metricsList.isEmpty()) {
            return sb.toString();
        }

        String dataFormat = "%-16d%16d%n";
        for (int i = 0; i < metricsList.size(); ++i) {
            JvmCompilationMetrics metrics = metricsList.get(i);
            sb.append(String.format(dataFormat, metrics.getTime(), metrics.getTotalTime()));
        }
        return sb.toString();
    }
}
