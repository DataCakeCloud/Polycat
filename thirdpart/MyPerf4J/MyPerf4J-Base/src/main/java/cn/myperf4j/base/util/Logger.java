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
package cn.myperf4j.base.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by LinShunkang on 2018/3/20
 */
public final class Logger {

    private static final ThreadLocal<DateFormat> TO_MILLS_DATE_FORMAT = new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        }
    };

    private static boolean debugEnable;

    private static final String PREFIX = " [MyPerf4J] ";

    private static final String INFO_LEVEL = "INFO ";

    private static final String DEBUG_LEVEL = "DEBUG ";

    private static final String WARN_LEVEL = "WARN ";

    private static final String ERROR_LEVEL = "ERROR ";

    private Logger() {
        //empty
    }

    public static void setDebugEnable(boolean debugEnable) {
        Logger.debugEnable = debugEnable;
    }

    public static boolean isDebugEnable() {
        return debugEnable;
    }

    public static void info(String msg) {
        System.out.println(getPrefix(INFO_LEVEL) + msg);
    }

    private static String getPrefix(String logLevel) {
        return getToMillisStr(new Date()) + PREFIX + logLevel + "[" + Thread.currentThread().getName() + "] ";
    }

    private static String getToMillisStr(Date date) {
        return TO_MILLS_DATE_FORMAT.get().format(date);
    }

    public static void debug(String msg) {
        if (debugEnable) {
            System.out.println(getPrefix(DEBUG_LEVEL) + msg);
        }
    }

    public static void warn(String msg) {
        System.out.println(getPrefix(WARN_LEVEL) + msg);
    }

    public static void error(String msg) {
        System.err.println(getPrefix(ERROR_LEVEL) + msg);
    }

    public static void error(String msg, Throwable throwable) {
        synchronized (System.err) {
            System.err.println(getPrefix(ERROR_LEVEL) + msg + " " + throwable.getMessage());
            throwable.printStackTrace();
        }
    }
}
