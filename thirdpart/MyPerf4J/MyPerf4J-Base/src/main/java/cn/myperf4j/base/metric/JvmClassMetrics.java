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
public class JvmClassMetrics extends Metrics {

    private static final long serialVersionUID = 5189910445931453667L;

    private final long total;

    private final long loaded;

    private final long unloaded;

    public JvmClassMetrics(long total, long loaded, long unloaded) {
        this.total = total;
        this.loaded = loaded;
        this.unloaded = unloaded;
    }

    public long getTotal() {
        return total;
    }

    public long getLoaded() {
        return loaded;
    }

    public long getUnloaded() {
        return unloaded;
    }

    @Override
    public String toString() {
        return "JvmClassMetrics{" +
                "total=" + total +
                ", loaded=" + loaded +
                ", unloaded=" + unloaded +
                '}';
    }
}
