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

import cn.myperf4j.base.metric.JvmThreadMetrics;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

import static java.lang.Thread.State.BLOCKED;
import static java.lang.Thread.State.NEW;
import static java.lang.Thread.State.RUNNABLE;
import static java.lang.Thread.State.TERMINATED;
import static java.lang.Thread.State.TIMED_WAITING;
import static java.lang.Thread.State.WAITING;

/**
 * Created by LinShunkang on 2019/07/08
 */
public final class JvmThreadCollector {

    private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();

    private JvmThreadCollector() {
        //empty
    }

    public static JvmThreadMetrics collectThreadMetrics() {
        int news = 0;
        int runnable = 0;
        int blocked = 0;
        int waiting = 0;
        int timedWaiting = 0;
        int terminated = 0;
        ThreadMXBean mxBean = THREAD_MX_BEAN;
        ThreadInfo[] threadInfoArr = mxBean.getThreadInfo(mxBean.getAllThreadIds(), 0);
        for (int i = 0; i < threadInfoArr.length; ++i) {
            ThreadInfo threadInfo = threadInfoArr[i];
            if (threadInfo == null) {
                continue;
            }

            Thread.State state = threadInfo.getThreadState();
            if (state == NEW) {
                news++;
            } else if (state == RUNNABLE) {
                runnable++;
            } else if (state == BLOCKED) {
                blocked++;
            } else if (state == WAITING) {
                waiting++;
            } else if (state == TIMED_WAITING) {
                timedWaiting++;
            } else if (state == TERMINATED) {
                terminated++;
            }
        }

        return new JvmThreadMetrics(
                mxBean.getTotalStartedThreadCount(),
                mxBean.getThreadCount(),
                mxBean.getPeakThreadCount(),
                mxBean.getDaemonThreadCount(),
                news,
                runnable,
                blocked,
                waiting,
                timedWaiting,
                terminated);
    }
}
