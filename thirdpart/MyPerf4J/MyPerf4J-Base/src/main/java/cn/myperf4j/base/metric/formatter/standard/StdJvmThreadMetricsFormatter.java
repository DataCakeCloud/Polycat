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

import cn.myperf4j.base.metric.JvmThreadMetrics;
import cn.myperf4j.base.metric.formatter.JvmThreadMetricsFormatter;
import cn.myperf4j.base.util.DateFormatUtils;

import java.util.List;

import static cn.myperf4j.base.util.SysProperties.LINE_SEPARATOR;

/**
 * Created by LinShunkang on 2018/8/21
 */
public class StdJvmThreadMetricsFormatter implements JvmThreadMetricsFormatter {

    @Override
    public String format(List<JvmThreadMetrics> metricsList, long startMillis, long stopMillis) {
        String dataTitleFormat = "%-14s%14s%14s%14s%14s%14s%14s%14s%14s%14s%n";
        StringBuilder sb = new StringBuilder((metricsList.size() + 2) * (14 * 10 + 64));
        sb.append("MyPerf4J JVM Thread Metrics [").append(DateFormatUtils.format(startMillis)).append(", ")
                .append(DateFormatUtils.format(stopMillis)).append(']').append(LINE_SEPARATOR);
        sb.append(String.format(dataTitleFormat, "TotalStarted", "Active", "Peak", "Daemon", "New", "Runnable",
                "Blocked", "Waiting", "TimedWaiting", "Terminated"));
        if (metricsList.isEmpty()) {
            return sb.toString();
        }

        String dataFormat = "%-14s%14d%14d%14d%14d%14d%14d%14d%14d%14d%n";
        for (int i = 0; i < metricsList.size(); ++i) {
            JvmThreadMetrics metrics = metricsList.get(i);
            sb.append(
                    String.format(dataFormat,
                            metrics.getTotalStarted(),
                            metrics.getActive(),
                            metrics.getPeak(),
                            metrics.getDaemon(),
                            metrics.getNews(),
                            metrics.getRunnable(),
                            metrics.getBlocked(),
                            metrics.getWaiting(),
                            metrics.getTimedWaiting(),
                            metrics.getTerminated()
                    )
            );
        }
        return sb.toString();
    }
}
