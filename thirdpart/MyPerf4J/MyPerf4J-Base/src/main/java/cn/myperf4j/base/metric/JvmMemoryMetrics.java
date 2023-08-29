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
 * Created by LinShunkang on 2018/8/19
 * <p>
 * 注意：以下成员变量的单位都是 KB
 */
public class JvmMemoryMetrics extends Metrics {

    private static final long serialVersionUID = -1501873958253505089L;

    private final long heapUsed;
    private final long heapMax;

    private final long nonHeapUsed;
    private final long nonHeapMax;

    private final long permGenUsed;
    private final long permGenMax;

    private final long metaspaceUsed;
    private final long metaspaceMax;

    private final long codeCacheUsed;
    private final long codeCacheMax;

    private final long oldGenUsed;
    private final long oldGenMax;

    private final long edenUsed;
    private final long edenMax;

    private final long survivorUsed;
    private final long survivorMax;

    public JvmMemoryMetrics(long heapUsed,
                            long heapMax,
                            long nonHeapUsed,
                            long nonHeapMax,
                            long permGenUsed,
                            long permGenMax,
                            long metaspaceUsed,
                            long metaspaceMax,
                            long codeCacheUsed,
                            long codeCacheMax,
                            long oldGenUsed,
                            long oldGenMax,
                            long edenUsed,
                            long edenMax,
                            long survivorUsed,
                            long survivorMax) {
        this.heapUsed = heapUsed;
        this.heapMax = heapMax;
        this.nonHeapUsed = nonHeapUsed;
        this.nonHeapMax = nonHeapMax;
        this.permGenUsed = permGenUsed;
        this.permGenMax = permGenMax;
        this.metaspaceUsed = metaspaceUsed;
        this.metaspaceMax = metaspaceMax;
        this.codeCacheUsed = codeCacheUsed;
        this.codeCacheMax = codeCacheMax;
        this.oldGenUsed = oldGenUsed;
        this.oldGenMax = oldGenMax;
        this.edenUsed = edenUsed;
        this.edenMax = edenMax;
        this.survivorUsed = survivorUsed;
        this.survivorMax = survivorMax;
    }

    public long getHeapUsed() {
        return heapUsed;
    }

    public double getHeapUsedPercent() {
        return NumUtils.getPercent(heapUsed, heapMax);
    }

    public long getNonHeapUsed() {
        return nonHeapUsed;
    }

    public double getNonHeapUsedPercent() {
        return NumUtils.getPercent(nonHeapUsed, nonHeapMax);
    }

    public long getPermGenUsed() {
        return permGenUsed;
    }

    public double getPermGenUsedPercent() {
        return NumUtils.getPercent(permGenUsed, permGenMax);
    }

    public long getMetaspaceUsed() {
        return metaspaceUsed;
    }

    public double getMetaspaceUsedPercent() {
        return NumUtils.getPercent(metaspaceUsed, metaspaceMax);
    }

    public long getCodeCacheUsed() {
        return codeCacheUsed;
    }

    public double getCodeCacheUsedPercent() {
        return NumUtils.getPercent(codeCacheUsed, codeCacheMax);
    }

    public long getOldGenUsed() {
        return oldGenUsed;
    }

    public double getOldGenUsedPercent() {
        return NumUtils.getPercent(oldGenUsed, oldGenMax);
    }

    public long getEdenUsed() {
        return edenUsed;
    }

    public double getEdenUsedPercent() {
        return NumUtils.getPercent(edenUsed, edenMax);
    }

    public long getSurvivorUsed() {
        return survivorUsed;
    }

    public double getSurvivorUsedPercent() {
        return NumUtils.getPercent(survivorUsed, survivorMax);
    }

    @Override
    public String toString() {
        return "JvmMemoryMetrics{" +
                "heapUsed=" + heapUsed +
                ", heapMax=" + heapMax +
                ", nonHeapUsed=" + nonHeapUsed +
                ", nonHeapMax=" + nonHeapMax +
                ", permGenUsed=" + permGenUsed +
                ", permGenMax=" + permGenMax +
                ", metaspaceUsed=" + metaspaceUsed +
                ", metaspaceMax=" + metaspaceMax +
                ", codeCacheUsed=" + codeCacheUsed +
                ", codeCacheMax=" + codeCacheMax +
                ", oldGenUsed=" + oldGenUsed +
                ", oldGenMax=" + oldGenMax +
                ", edenUsed=" + edenUsed +
                ", edenMax=" + edenMax +
                ", survivorUsed=" + survivorUsed +
                ", survivorMax=" + survivorMax +
                "} " + super.toString();
    }
}
