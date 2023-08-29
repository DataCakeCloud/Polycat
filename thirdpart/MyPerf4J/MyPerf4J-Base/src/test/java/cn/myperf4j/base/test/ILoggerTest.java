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
package cn.myperf4j.base.test;

import cn.myperf4j.base.config.MetricsConfig;
import cn.myperf4j.base.config.ProfilingConfig;
import cn.myperf4j.base.constant.PropertyValues.Metrics;
import cn.myperf4j.base.log.ILogger;
import cn.myperf4j.base.log.LoggerFactory;
import org.junit.Assert;
import org.junit.Test;

public class ILoggerTest {

    @Test
    public void test() {
        ProfilingConfig.metricsConfig(MetricsConfig.loadMetricsConfig());
        ProfilingConfig.metricsConfig().logRollingTimeUnit(Metrics.LOG_ROLLING_MINUTELY);
        ProfilingConfig.metricsConfig().logReserveCount(Metrics.DEFAULT_LOG_RESERVE_COUNT);

        test(LoggerFactory.getLogger("/tmp/testLogger.log"));
        test(LoggerFactory.getLogger(Metrics.NULL_FILE));
    }

    private void test(ILogger logger) {
        logger.log("111111111");
        logger.log("222222222");
        logger.log("333333333");
        logger.log("444444444");
        logger.flushLog();

        logger.logAndFlush("555555");

        logger.preCloseLog();
        logger.closeLog();
    }

    public static void main(String[] args) {
        for (int i = 0; i < 100; ++i) {
            final String file = "/tmp/testLogger.log";
            new Thread(new Runnable() {
                @Override
                public void run() {
                    writeFile(file);
                }
            }).start();
        }
    }

    private static void writeFile(String file) {
        ILogger logger = LoggerFactory.getLogger(file);
        Assert.assertNotNull(logger);

        logger.log("111111111");
        logger.log("222222222");
        logger.log("333333333");
        logger.log("444444444");

        for (int i = 0; i < 1000000; ++i) {
            logger.log(Thread.currentThread().getName() + ": " + i);
        }
    }
}
