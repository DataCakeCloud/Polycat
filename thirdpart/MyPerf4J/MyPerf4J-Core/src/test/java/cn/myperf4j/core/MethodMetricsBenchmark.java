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
package cn.myperf4j.core;

import cn.myperf4j.base.MethodTag;
import cn.myperf4j.base.metric.MethodMetrics;
import cn.myperf4j.core.recorder.Recorder;
import cn.myperf4j.core.recorder.Recorders;
import cn.myperf4j.core.recorder.RoughRecorder;

import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Created by LinShunkang on 2019/06/22
 */
public final class MethodMetricsBenchmark {

    private MethodMetricsBenchmark() {
        //empty
    }

    public static void main(String[] args) {
        Recorders recorders = new Recorders(new AtomicReferenceArray<Recorder>(10));
        MethodTagMaintainer methodTagMaintainer = MethodTagMaintainer.getInstance();

        int methodId1 = methodTagMaintainer.addMethodTag(MethodTag.getGeneralInstance("", "Test", "Api", "m1", ""));
//        recorders.setRecorder(methodId1, AccurateRecorder.getInstance(0, 9000, 50));
        recorders.setRecorder(methodId1, RoughRecorder.getInstance(0, 10000));

        Recorder recorder = recorders.getRecorder(methodId1);
        recorders.setStartTime(System.currentTimeMillis());
        long start = System.nanoTime();
        for (long i = 0; i < 1000; ++i) {
            recorder.recordTime(start, start + i * 1000 * 1000);
        }
        recorders.setStopTime(System.currentTimeMillis());

        MethodTag methodTag = methodTagMaintainer.getMethodTag(methodId1);

        long tmp = 0L;
        start = System.nanoTime();
        for (int i = 0; i < 1000000; ++i) {
            MethodMetrics methodMetrics = MethodMetricsCalculator.calPerfStats(recorder, methodTag,
                    recorders.getStartTime(), recorders.getStopTime());
            tmp += methodMetrics.getRPS();
        }
        System.out.println("tmp=" + tmp + ", totalCost=" + (System.nanoTime() - start) / 1000_000 + "ms");
    }
}
