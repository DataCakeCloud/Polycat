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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Created by LinShunkang on 2018/7/7
 */
public class Recorders {

    private final AtomicReferenceArray<Recorder> recorderArr;

    private final AtomicInteger recorderCount;

    private volatile boolean writing;

    private volatile long startTime;

    private volatile long stopTime;

    public Recorders(AtomicReferenceArray<Recorder> recorderArr) {
        this.recorderArr = recorderArr;
        this.recorderCount = new AtomicInteger(0);
    }

    public Recorder getRecorder(int index) {
        return recorderArr.get(index);
    }

    public void setRecorder(int index, Recorder recorder) {
        recorderArr.set(index, recorder);
        recorderCount.incrementAndGet();
    }

    public int size() {
        return recorderArr.length();
    }

    public boolean isWriting() {
        return writing;
    }

    public void setWriting(boolean writing) {
        this.writing = writing;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getStopTime() {
        return stopTime;
    }

    public void setStopTime(long stopTime) {
        this.stopTime = stopTime;
    }

    public void resetRecorder() {
        int count = recorderCount.get();
        AtomicReferenceArray<Recorder> recorderArr = this.recorderArr;
        for (int i = 0; i < count; ++i) {
            Recorder recorder = recorderArr.get(i);
            if (recorder != null) {
                recorder.resetRecord();
            }
        }
    }

    @Override
    public String toString() {
        return "Recorders{" +
                "recorderArr=" + recorderArr +
                ", writing=" + writing +
                ", startTime=" + startTime +
                ", stopTime=" + stopTime +
                '}';
    }
}
