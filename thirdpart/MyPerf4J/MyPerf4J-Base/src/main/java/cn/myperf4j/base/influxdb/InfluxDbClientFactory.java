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
package cn.myperf4j.base.influxdb;

import cn.myperf4j.base.config.InfluxDbConfig;
import cn.myperf4j.base.config.ProfilingConfig;

/**
 * Created by LinShunkang on 2020/05/18
 */
public final class InfluxDbClientFactory {

    private static final InfluxDbConfig CONFIG = ProfilingConfig.influxDBConfig();

    private static final InfluxDbClient CLIENT = new InfluxDbClient.Builder()
            .host(CONFIG.host())
            .port(CONFIG.port())
            .database(CONFIG.database())
            .username(CONFIG.username())
            .password(CONFIG.password())
            .connectTimeout(CONFIG.connectTimeout())
            .readTimeout(CONFIG.readTimeout())
            .build();

    private InfluxDbClientFactory() {
        //empty
    }

    public static InfluxDbClient getClient() {
        return CLIENT;
    }
}
