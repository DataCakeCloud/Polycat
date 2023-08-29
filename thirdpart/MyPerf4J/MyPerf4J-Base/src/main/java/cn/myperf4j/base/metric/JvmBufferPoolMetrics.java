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
package cn.myperf4j.base.metric;

import java.lang.management.BufferPoolMXBean;

/**
 * Created by LinShunkang on 2018/11/1
 */
public class JvmBufferPoolMetrics extends Metrics {

    private static final long serialVersionUID = 1308517280962399791L;

    private final String name;

    private final long count;

    private final long memoryUsed; //KB

    private final long memoryCapacity; //KB

    public JvmBufferPoolMetrics(BufferPoolMXBean mxBean) {
        this.name = mxBean.getName();
        this.count = mxBean.getCount();
        this.memoryUsed = mxBean.getMemoryUsed() >> 10;
        this.memoryCapacity = mxBean.getTotalCapacity() >> 10;
    }

    public String getName() {
        return name;
    }

    public long getCount() {
        return count;
    }

    public long getMemoryUsed() {
        return memoryUsed;
    }

    public long getMemoryCapacity() {
        return memoryCapacity;
    }

    @Override
    public String toString() {
        return "JvmBufferPoolMetrics{" +
                "name='" + name + '\'' +
                ", count=" + count +
                ", memoryUsed=" + memoryUsed +
                ", memoryCapacity=" + memoryCapacity +
                '}';
    }
}
