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

import cn.myperf4j.base.util.NumUtils;

/**
 * Created by LinShunkang on 2019/11/03
 */
public class JvmFileDescriptorMetrics extends Metrics {

    private static final long serialVersionUID = -4979694928407737915L;

    private final long openCount;

    private final long maxCount;

    public JvmFileDescriptorMetrics(long openCount, long maxCount) {
        this.openCount = openCount;
        this.maxCount = maxCount;
    }

    public long getOpenCount() {
        return openCount;
    }

    public long getMaxCount() {
        return maxCount;
    }

    public double getOpenPercent() {
        return NumUtils.getPercent(openCount, maxCount);
    }

    @Override
    public String toString() {
        return "JvmFileDescriptorMetrics{" +
                "curCount=" + openCount +
                ", maxCount=" + maxCount +
                "} " + super.toString();
    }
}
