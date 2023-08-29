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
public class JvmThreadMetrics extends Metrics {

    private static final long serialVersionUID = 8514109576224018139L;

    private final long totalStarted;

    private final int active;

    private final int peak;

    private final int daemon;

    private final int news;

    private final int runnable;

    private final int blocked;

    private final int waiting;

    private final int timedWaiting;

    private final int terminated;

    public JvmThreadMetrics(long totalStarted,
                            int active,
                            int peak,
                            int daemon,
                            int news,
                            int runnable,
                            int blocked,
                            int waiting,
                            int timedWaiting,
                            int terminated) {
        this.totalStarted = totalStarted;
        this.active = active;
        this.peak = peak;
        this.daemon = daemon;
        this.news = news;
        this.runnable = runnable;
        this.blocked = blocked;
        this.waiting = waiting;
        this.timedWaiting = timedWaiting;
        this.terminated = terminated;
    }

    public long getTotalStarted() {
        return totalStarted;
    }

    public int getActive() {
        return active;
    }

    public int getPeak() {
        return peak;
    }

    public int getDaemon() {
        return daemon;
    }

    public int getNews() {
        return news;
    }

    public int getRunnable() {
        return runnable;
    }

    public int getBlocked() {
        return blocked;
    }

    public int getWaiting() {
        return waiting;
    }

    public int getTimedWaiting() {
        return timedWaiting;
    }

    public int getTerminated() {
        return terminated;
    }

    @Override
    public String toString() {
        return "JvmThreadMetrics{" +
                "totalStarted=" + totalStarted +
                ", active=" + active +
                ", peak=" + peak +
                ", daemon=" + daemon +
                ", news=" + news +
                ", runnable=" + runnable +
                ", blocked=" + blocked +
                ", waiting=" + waiting +
                ", timedWaiting=" + timedWaiting +
                ", terminated=" + terminated +
                "} " + super.toString();
    }
}
