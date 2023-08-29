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

import java.io.IOException;
import io.polycat.catalog.common.exception.CarbonSqlException;
import io.prometheus.client.exporter.HTTPServer;

public class PolyCatMetricsService {

    public static boolean disabled = false;

    private MetricsConfig metricsConfig;

    private HTTPServer metricsHttpServer;

    private boolean running = false;

    public PolyCatMetricsService(MetricsConfig config) {
        metricsConfig = new MetricsConfig(config);
    }

    public PolyCatMetricsService(int port) {
        metricsConfig = new MetricsConfig(port);
    }

    public boolean getStatus() {
        return running;
    }

    public void start() {

        try {
            running = true;
            metricsHttpServer = new HTTPServer(metricsConfig.getPort());

            if (!metricsConfig.getPushAddress().isEmpty()) {
                throw new CarbonSqlException("Prometheus push mode not supported");
            }

        } catch (IOException e) {
            e.printStackTrace();
            throw new CarbonSqlException("metricsHttpServer starting error");
        }
    }

    public void stop() {
        metricsHttpServer.stop();
        running = false;
    }
}
