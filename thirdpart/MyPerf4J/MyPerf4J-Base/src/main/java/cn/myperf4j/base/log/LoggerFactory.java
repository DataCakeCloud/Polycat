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
package cn.myperf4j.base.log;

import cn.myperf4j.base.config.MetricsConfig;
import cn.myperf4j.base.config.ProfilingConfig;
import cn.myperf4j.base.constant.PropertyValues.Metrics;

import java.util.HashMap;
import java.util.Map;

public final class LoggerFactory {

    private static final MetricsConfig METRICS_CONFIG = ProfilingConfig.metricsConfig();

    private static final Map<String, ILogger> LOGGER_MAP = new HashMap<>();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                for (ILogger writer : LOGGER_MAP.values()) {
                    writer.preCloseLog();
                }

                for (ILogger writer : LOGGER_MAP.values()) {
                    writer.closeLog();
                }
            }
        }));
    }

    private LoggerFactory() {
        //empty
    }

    public static synchronized ILogger getLogger(String logFile) {
        logFile = logFile.trim();

        if (logFile.equalsIgnoreCase(Metrics.NULL_FILE)) {
            return new NullLogger();
        } else if (logFile.equalsIgnoreCase(Metrics.STDOUT_FILE)) {
            return new StdoutLogger();
        }

        ILogger logger = LOGGER_MAP.get(logFile);
        if (logger != null) {
            return logger;
        }

        logger = new AutoRollingLogger(logFile, METRICS_CONFIG.logRollingTimeUnit(), METRICS_CONFIG.logReserveCount());
        LOGGER_MAP.put(logFile, logger);
        return logger;
    }
}
