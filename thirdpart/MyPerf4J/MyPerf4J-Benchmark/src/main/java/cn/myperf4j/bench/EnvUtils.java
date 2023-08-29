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
package cn.myperf4j.bench;

import cn.myperf4j.asm.ASMBootstrap;
import cn.myperf4j.base.constant.PropertyKeys;
import cn.myperf4j.base.constant.PropertyValues.Metrics;
import cn.myperf4j.base.file.AutoRollingFileWriter;
import cn.myperf4j.base.file.MinutelyRollingFileWriter;

/**
 * Created by LinShunkang on 2019/08/31
 */
public final class EnvUtils {

    private EnvUtils() {
        //empty
    }

    public static synchronized boolean initASMBootstrap(long milliTimeSlice) {
        initProperties(milliTimeSlice);
        return ASMBootstrap.getInstance().initial();
    }

    private static void initProperties(long milliTimeSlice) {
        String propertiesFile = "/tmp/MyPerf4J.properties";
        buildPropertiesFile(propertiesFile, milliTimeSlice);
        System.setProperty(PropertyKeys.PRO_FILE_NAME, propertiesFile);
    }

    private static void buildPropertiesFile(String propertiesFile, long milliTimeSlice) {
        AutoRollingFileWriter writer = new MinutelyRollingFileWriter(propertiesFile, 1);
        writer.write("app_name=MyPerf4JTest\n");
        writer.write("metrics.exporter=" + Metrics.EXPORTER_LOG_STDOUT + "\n");
        writer.write("filter.packages.include=MyPerf4J\n");
        writer.write("metrics.time_slice.method=" + milliTimeSlice + "\n");
        writer.write("metrics.time_slice.jvm=" + milliTimeSlice + "\n");
        writer.closeFile(true);
    }
}
