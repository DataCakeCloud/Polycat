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

import cn.myperf4j.base.constant.PropertyValues.Metrics;
import cn.myperf4j.base.file.AutoRollingFileWriter;
import cn.myperf4j.base.file.DailyRollingFileWriter;
import cn.myperf4j.base.file.HourlyRollingFileWriter;
import cn.myperf4j.base.file.MinutelyRollingFileWriter;

import static cn.myperf4j.base.util.SysProperties.LINE_SEPARATOR;

public class AutoRollingLogger implements ILogger {

    private final AutoRollingFileWriter writer;

    AutoRollingLogger(String logFile, String rollingTimeUnit, int reserveFileCount) {
        switch (rollingTimeUnit.toUpperCase()) {
            case Metrics.LOG_ROLLING_HOURLY:
                this.writer = new HourlyRollingFileWriter(logFile, reserveFileCount);
                break;
            case Metrics.LOG_ROLLING_MINUTELY:
                this.writer = new MinutelyRollingFileWriter(logFile, reserveFileCount);
                break;
            default:
                this.writer = new DailyRollingFileWriter(logFile, reserveFileCount);
        }
    }

    @Override
    public void log(String msg) {
        writer.write(msg + LINE_SEPARATOR);
    }

    @Override
    public void logAndFlush(String msg) {
        writer.writeAndFlush(msg + LINE_SEPARATOR);
    }

    @Override
    public void flushLog() {
        writer.flush();
    }

    @Override
    public void preCloseLog() {
        writer.preCloseFile();
    }

    @Override
    public void closeLog() {
        writer.closeFile(true);
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        writer.closeFile(true);
    }
}
