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

import cn.myperf4j.base.config.MyProperties;
import cn.myperf4j.base.constant.PropertyKeys;
import cn.myperf4j.base.constant.PropertyValues;
import cn.myperf4j.base.constant.PropertyValues.Metrics;
import cn.myperf4j.base.util.IOUtils;
import cn.myperf4j.base.util.Logger;
import cn.myperf4j.base.file.AutoRollingFileWriter;
import cn.myperf4j.base.file.MinutelyRollingFileWriter;
import org.junit.After;
import org.junit.BeforeClass;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by LinShunkang on 2018/10/28
 */
public abstract class BaseTest {

    public static final String TEMP_FILE = "/tmp/MyPerf4J.properties";

    public static final String APP_NAME = "MyPerf4JBaseTest";

    public static final String METRICS_EXPORTER = Metrics.EXPORTER_LOG_STDOUT;

    public static final String INCLUDE_PACKAGES = "MyPerf4J";

    public static final int MILLI_TIMES_LICE = 1000;

    @BeforeClass
    public static void init() {
        System.setProperty(PropertyKeys.PRO_FILE_NAME, TEMP_FILE);
        AutoRollingFileWriter writer = new MinutelyRollingFileWriter(TEMP_FILE, 1);
        writer.write("AppName=" + APP_NAME + "\n");
        writer.write("metrics.exporter=" + METRICS_EXPORTER + "\n");
        writer.write("IncludePackages=" + INCLUDE_PACKAGES + "\n");
        writer.write("MilliTimeSlice=" + MILLI_TIMES_LICE + "\n");
        writer.closeFile(true);

        new File(TEMP_FILE).deleteOnExit();

        initProperties();
    }

    private static void initProperties() {
        InputStream in = null;
        try {
            in = new FileInputStream(System.getProperty(PropertyKeys.PRO_FILE_NAME, PropertyValues.DEFAULT_PRO_FILE));

            Properties properties = new Properties();
            properties.load(in);
            MyProperties.initial(properties);
        } catch (IOException e) {
            Logger.error("BaseTest.initProperties()", e);
        } finally {
            IOUtils.closeQuietly(in);
        }
    }

    @After
    public void clean() {
        new File(TEMP_FILE).delete();
    }

}
