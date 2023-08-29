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

import cn.myperf4j.base.util.StrUtils;

import static cn.myperf4j.base.config.MyProperties.getBoolean;
import static cn.myperf4j.base.config.MyProperties.getStr;
import static cn.myperf4j.base.constant.PropertyKeys.Basic.APP_NAME;
import static cn.myperf4j.base.constant.PropertyKeys.Basic.PROPERTIES_FILE_DIR;
import static cn.myperf4j.base.constant.PropertyKeys.Basic.DEBUG;

/**
 * Created by LinShunkang on 2020/05/24
 */
public class BasicConfig {

    private String appName;

    private String configFileDir;

    private boolean debug;

    public String appName() {
        return appName;
    }

    public void appName(String appName) {
        if (StrUtils.isBlank(appName)) {
            throw new IllegalArgumentException("AppName is required!!!");
        }
        this.appName = appName;
    }

    public String configFileDir() {
        return configFileDir;
    }

    public void configFileDir(String configFileDir) {
        this.configFileDir = configFileDir;
    }

    public boolean debug() {
        return debug;
    }

    public void debug(boolean debug) {
        this.debug = debug;
    }

    public String sysProfilingParamsFile() {
        return configFileDir + "." + appName + "_SysGenProfilingFile";
    }

    @Override
    public String toString() {
        return "BasicConfig{" +
                "appName='" + appName + '\'' +
                ", configFileDir='" + configFileDir + '\'' +
                ", debug=" + debug +
                '}';
    }

    public static BasicConfig loadBasicConfig() {
        String appName = getStr(APP_NAME);
        if (StrUtils.isBlank(appName)) {
            throw new IllegalArgumentException(APP_NAME.key() + "|" + APP_NAME.legacyKey() + " is required!!!");
        }

        BasicConfig config = new BasicConfig();
        config.appName(appName);
        config.debug(getBoolean(DEBUG, false));
        config.configFileDir(getStr(PROPERTIES_FILE_DIR));
        return config;
    }
}
