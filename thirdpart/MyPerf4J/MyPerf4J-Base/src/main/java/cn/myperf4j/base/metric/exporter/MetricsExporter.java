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
package cn.myperf4j.base.metric.exporter;

import cn.myperf4j.base.metric.Metrics;

/**
 * Created by LinShunkang on 2018/8/19
 * <p>
 * 整体处理流程如下：
 * beforeProcess() -> process() -> process() -> *** -> process() -> afterProcess()
 * 即：每一轮统计均以beforeProcess()开始，以afterProcess()结束，中间会多次调用process()
 */
public interface MetricsExporter<T extends Metrics> {

    /**
     * 在每一轮统计指标开始处理前调用
     *
     * @param processId : 本轮统计的唯一ID
     */
    void beforeProcess(long processId, long startMillis, long stopMillis);

    /**
     * @param metrics     : 本轮统计的某一个指标
     * @param processId   : 本轮统计的唯一ID
     * @param startMillis : 本轮统计信息的起始时间，单位为ms
     * @param stopMillis  : 本轮统计信息的终止时间，单位为ms
     */
    void process(T metrics, long processId, long startMillis, long stopMillis);

    /**
     * 在每一轮统计指标开始处理前调用
     *
     * @param processId : 本轮统计的唯一ID
     */
    void afterProcess(long processId, long startMillis, long stopMillis);
}
