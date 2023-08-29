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
package cn.myperf4j.base.metric.collector;

import cn.myperf4j.base.metric.JvmCompilationMetrics;

import java.lang.management.CompilationMXBean;
import java.lang.management.ManagementFactory;

/**
 * Created by LinShunkang on 2019/11/03
 */
public final class JvmCompilationCollector {

    private static volatile long lastTime;

    private static final CompilationMXBean COMPILATION_MX_BEAN = ManagementFactory.getCompilationMXBean();

    private JvmCompilationCollector() {
        //empty
    }

    public static JvmCompilationMetrics collectCompilationMetrics() {
        CompilationMXBean mxBean = COMPILATION_MX_BEAN;
        if (mxBean != null && mxBean.isCompilationTimeMonitoringSupported()) {
            long totalTime = mxBean.getTotalCompilationTime();
            long timeInterval = totalTime - lastTime;
            lastTime = totalTime;
            return new JvmCompilationMetrics(timeInterval, totalTime);
        }
        return new JvmCompilationMetrics(0L, 0L);
    }
}
