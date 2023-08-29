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
package cn.myperf4j.base.config;

/**
 * Created by LinShunkang on 2018/5/12
 */
public final class ProfilingConfig {

    private static BasicConfig BASIC_CONFIG;

    private static MetricsConfig METRICS_CONFIG;

    private static FilterConfig FILTER_CONFIG;

    private static InfluxDbConfig INFLUX_DB_CONFIG;

    private static RecorderConfig RECORDER_CONFIG;

    private ProfilingConfig() {
        //empty
    }

    public static BasicConfig basicConfig() {
        return BASIC_CONFIG;
    }

    public static void basicConfig(BasicConfig basicConfig) {
        BASIC_CONFIG = basicConfig;
    }

    public static InfluxDbConfig influxDBConfig() {
        return INFLUX_DB_CONFIG;
    }

    public static void influxDBConfig(InfluxDbConfig influxDBConfig) {
        ProfilingConfig.INFLUX_DB_CONFIG = influxDBConfig;
    }

    public static MetricsConfig metricsConfig() {
        return METRICS_CONFIG;
    }

    public static void metricsConfig(MetricsConfig metricsConfig) {
        ProfilingConfig.METRICS_CONFIG = metricsConfig;
    }

    public static FilterConfig filterConfig() {
        return FILTER_CONFIG;
    }

    public static void filterConfig(FilterConfig filterConfig) {
        ProfilingConfig.FILTER_CONFIG = filterConfig;
    }

    public static RecorderConfig recorderConfig() {
        return RECORDER_CONFIG;
    }

    public static void recorderConfig(RecorderConfig recorderConfig) {
        RECORDER_CONFIG = recorderConfig;
    }
}
