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

import cn.myperf4j.base.metric.JvmFileDescriptorMetrics;
import com.sun.management.UnixOperatingSystemMXBean;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

/**
 * Created by LinShunkang on 2019/11/03
 */
public final class JvmFileDescCollector {

    private static final OperatingSystemMXBean SYSTEM_MX_BEAN = ManagementFactory.getOperatingSystemMXBean();

    private JvmFileDescCollector() {
        //empty
    }

    public static JvmFileDescriptorMetrics collectFileDescMetrics() {
        if (SYSTEM_MX_BEAN instanceof UnixOperatingSystemMXBean) {
            UnixOperatingSystemMXBean unixMXBean = (UnixOperatingSystemMXBean) SYSTEM_MX_BEAN;
            return new JvmFileDescriptorMetrics(unixMXBean.
                    getOpenFileDescriptorCount(),
                    unixMXBean.getMaxFileDescriptorCount());
        }
        return new JvmFileDescriptorMetrics(0L, 0L);
    }
}
