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

import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Created by LinShunkang on 2020/05/19
 */
public class InfluxDbClientTest {

    private final InfluxDbClient influxDbClient = new InfluxDbClient.Builder()
            .host("127.0.0.1")
            .port(8086)
            .connectTimeout(100)
            .readTimeout(1000)
            .database("test_db")
            .username("admin")
            .password("admin123")
            .build();

    @Test
    public void testBuildDatabase() {
        boolean database = influxDbClient.createDatabase();
        System.out.println(database);
    }

    @Test
    public void testWrite() throws InterruptedException {
        boolean write = influxDbClient.writeMetricsAsync(
                "cpu_load_short,host=server01,region=us-west value=0.64 1434055562000000000\n" +
                        "cpu_load_short,host=server02,region=us-west value=0.96 1434055562000000000");
        System.out.println(write);
        TimeUnit.SECONDS.sleep(3);
    }
}
