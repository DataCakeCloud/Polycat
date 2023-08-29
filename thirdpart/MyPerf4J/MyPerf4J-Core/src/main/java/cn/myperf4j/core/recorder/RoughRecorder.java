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
package cn.myperf4j.core.recorder;

import cn.myperf4j.base.buffer.IntBuf;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * Created by LinShunkang on 2018/3/25
 * <p>
 * MyPerf4J默认使用的是 AccurateRecorder，如果需要使用 RoughRecorder，则在配置文件里加上 RecorderMode=rough
 * <p>
 * 该类用于粗略存储某一个方法在指定时间片内的响应时间
 * 为了进一步减小内存占用，只利用数组方式:
 * 1、将小于等于 mostTimeThreshold 的响应时间记录在数组中；
 * 2、将大于 mostTimeThreshold 的响应时间记录到 timingArr[mostTimeThreshold+1]中。
 * <p>
 * 注意：由于该 Recorder 会将大于 mostTimeThreshold 的响应时间记录为 mostTimeThreshold+1
 * 所以为了保证 RoughRecorder 记录的准确性，请把 mostTimeThreshold 设置的偏大一些。
 */
public final class RoughRecorder extends Recorder {

    private final AtomicIntegerArray timingArr;

    private final AtomicInteger diffCount;

    public RoughRecorder(int methodTag, int mostTimeThreshold) {
        super(methodTag);
        this.timingArr = new AtomicIntegerArray(mostTimeThreshold + 2);
        this.diffCount = new AtomicInteger(0);
    }

    @Override
    public void recordTime(long startNanoTime, long endNanoTime) {
        if (startNanoTime > endNanoTime) {
            return;
        }

        int oldValue;
        int elapsedTime = (int) ((endNanoTime - startNanoTime) / 1000000);
        AtomicIntegerArray timingArr = this.timingArr;
        if (elapsedTime < timingArr.length() - 1) {
            oldValue = timingArr.getAndIncrement(elapsedTime);
        } else {
            oldValue = timingArr.getAndIncrement(timingArr.length() - 1);
        }

        if (oldValue <= 0) {
            diffCount.incrementAndGet();
        }
    }

    @Override
    public long fillSortedRecords(IntBuf intBuf) {
        long totalCount = 0L;
        AtomicIntegerArray timingArr = this.timingArr;
        for (int i = 0; i < timingArr.length(); ++i) {
            int count = timingArr.get(i);
            if (count > 0) {
                intBuf.write(i, count);
                totalCount += count;
            }
        }
        return totalCount;
    }

    @Override
    public int getDiffCount() {
        return diffCount.get();
    }

    @Override
    public void resetRecord() {
        if (!hasRecord()) {
            return;
        }

        AtomicIntegerArray timingArr = this.timingArr;
        for (int i = 0; i < timingArr.length(); ++i) {
            timingArr.set(i, 0);
        }

        diffCount.set(0);
    }

    @Override
    public boolean hasRecord() {
        return diffCount.get() > 0;
    }

    public static RoughRecorder getInstance(int methodTagId, int mostTimeThreshold) {
        return new RoughRecorder(methodTagId, mostTimeThreshold);
    }
}
