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

/**
 * Created by LinShunkang on 2019/07/23
 */
public class MethodMetricsInfo {

    private long tp95Sum;

    private long tp99Sum;

    private long tp999Sum;

    private long tp9999Sum;

    private int count;

    public MethodMetricsInfo(int tp95, int tp99, int tp999, int tp9999) {
        this.tp95Sum = tp95;
        this.tp99Sum = tp99;
        this.tp999Sum = tp999;
        this.tp9999Sum = tp9999;
        this.count = isValid(tp95, tp99, tp999, tp9999) ? 1 : 0;
    }

    private boolean isValid(int tp95, int tp99, int tp999, int tp9999) {
        return tp95 >= 0 && tp99 >= 0 && tp999 >= 0 && tp9999 >= 0;
    }

    public void add(int tp95, int tp99, int tp999, int tp9999) {
        if (isValid(tp95, tp99, tp999, tp9999)) {
            tp95Sum += tp95;
            tp99Sum += tp99;
            tp999Sum += tp999;
            tp9999Sum += tp9999;
            count++;
        }
    }

    public long getTp95Sum() {
        return tp95Sum;
    }

    public long getTp99Sum() {
        return tp99Sum;
    }

    public long getTp999Sum() {
        return tp999Sum;
    }

    public long getTp9999Sum() {
        return tp9999Sum;
    }

    public int getCount() {
        return count;
    }

    @Override
    public String toString() {
        return "MethodMetricsInfo{" +
                "tp95Sum=" + tp95Sum +
                ", tp99Sum=" + tp99Sum +
                ", tp999Sum=" + tp999Sum +
                ", tp9999Sum=" + tp9999Sum +
                ", count=" + count +
                '}';
    }
}
