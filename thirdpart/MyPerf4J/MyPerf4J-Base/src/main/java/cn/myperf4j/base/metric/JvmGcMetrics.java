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

/**
 * Created by LinShunkang on 2018/8/19
 */
public class JvmGcMetrics extends Metrics {

    private static final long serialVersionUID = -233095689152915892L;

    private final long youngGcCount;

    private final long youngGcTime;

    private final double avgYoungGcTime;

    private final long fullGcCount;

    private final long fullGcTime;

    private final long zGcTime;

    private final long zGcCount;

    private final double avgZGcTime;

    public JvmGcMetrics(long youngGcCount,
                        long youngGcTime,
                        long fullGcCount,
                        long fullGcTime,
                        long zGcCount,
                        long zGcTime) {
        this.youngGcCount = youngGcCount;
        this.youngGcTime = youngGcTime;
        this.avgYoungGcTime = getAvgTime(youngGcCount, youngGcTime);
        this.fullGcCount = fullGcCount;
        this.fullGcTime = fullGcTime;
        this.zGcCount = zGcCount;
        this.zGcTime = zGcTime;
        this.avgZGcTime = getAvgTime(zGcCount, zGcTime);
    }

    private double getAvgTime(long count, long time) {
        return count > 0L ? ((double) time) / count : 0D;
    }

    public double getAvgYoungGcTime() {
        return avgYoungGcTime;
    }

    public long getFullGcCount() {
        return fullGcCount;
    }

    public long getFullGcTime() {
        return fullGcTime;
    }

    public long getYoungGcCount() {
        return youngGcCount;
    }

    public long getYoungGcTime() {
        return youngGcTime;
    }

    public long getZGcTime() {
        return zGcTime;
    }

    public long getZGcCount() {
        return zGcCount;
    }

    public double getAvgZGcTime() {
        return avgZGcTime;
    }

    @Override
    public String toString() {
        return "JvmGcMetrics{" +
                "youngGcCount=" + youngGcCount +
                ", youngGcTime=" + youngGcTime +
                ", avgYoungGcTime=" + avgYoungGcTime +
                ", fullGcCount=" + fullGcCount +
                ", fullGcTime=" + fullGcTime +
                ", zGcTime=" + zGcTime +
                ", zGcCount=" + zGcCount +
                ", avgZGcTime=" + avgZGcTime +
                '}';
    }
}
