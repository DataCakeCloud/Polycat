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
package io.polycat.metrics;

import java.time.Duration;

import lombok.Data;

@Data
public class MetricsConfig {

    // the http port on which we will expose metrics to Prometheus Server. 0 means invalid
    private int port;

    // message that describes what these metrics are about
    private String helpMessage;

    //
    private String prefix;

    // If this string and the associated interval are set, we will setup a background task to
    // push metrics to the Prometheus PushGateWay.
    // The metrics will be pushed to the given pushAddress every pushIterval seconds.
    private String pushAddress;
    private Duration pushInterval = Duration.ofSeconds(0);

    MetricsConfig(MetricsConfig config) {
        this.port = config.getPort();
        this.helpMessage = config.getHelpMessage();
        this.prefix = config.getPrefix();
        this.pushAddress = config.getPushAddress();
        this.pushInterval = config.getPushInterval();
    }

    MetricsConfig(int port) {
        this.port = port;
        this.helpMessage = "default help message";
        this.prefix = "";
        this.pushAddress = "";
        this.pushInterval = Duration.ofSeconds(0);
    }
}
