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

import cn.myperf4j.base.util.Logger;

import java.util.Properties;

/**
 * Created by LinShunkang on 2018/4/9
 */
public final class MyProperties {

    private static Properties properties;

    private MyProperties() {
        //empty
    }

    public static synchronized boolean initial(Properties prop) {
        if (properties != null || prop == null) {
            return false;
        }

        properties = prop;
        return true;
    }

    public static String getStr(ConfigKey confKey) {
        String str = getStr(confKey.key());
        if (str != null) {
            return str;
        }
        return getStr(confKey.legacyKey());
    }

    public static String getStr(String key) {
        checkState();

        String value = System.getProperty(key);
        if (value != null) {
            return value;
        }
        return properties.getProperty(key);
    }

    private static void checkState() {
        if (properties == null) {
            throw new IllegalStateException("MyProperties is not initial yet!!!");
        }
    }

    public static void setStr(String key, String value) {
        checkState();

        System.setProperty(key, value);
        properties.setProperty(key, value);
    }

    public static String getStr(ConfigKey confKey, String defaultValue) {
        checkState();

        String result = getStr(confKey);
        if (result != null) {
            return result;
        }
        return defaultValue;
    }

    public static String getStr(String key, String defaultValue) {
        checkState();

        String result = getStr(key);
        if (result != null) {
            return result;
        }
        return defaultValue;
    }

    public static int getInt(ConfigKey confKey, int defaultValue) {
        checkState();

        Integer result = getInt(confKey.key());
        if (result != null) {
            return result;
        }
        return getInt(confKey.legacyKey(), defaultValue);
    }

    public static Integer getInt(ConfigKey confKey) {
        checkState();

        Integer result = getInt(confKey.key());
        if (result != null) {
            return result;
        }
        return getInt(confKey.legacyKey());
    }

    public static Integer getInt(String key) {
        checkState();

        String result = getStr(key);
        if (result == null) {
            return null;
        }

        try {
            return Integer.valueOf(result);
        } catch (Exception e) {
            Logger.error("MyProperties.getInt(" + key + ")", e);
        }
        return null;
    }

    public static int getInt(String key, int defaultValue) {
        checkState();

        String result = getStr(key);
        if (result == null) {
            return defaultValue;
        }

        try {
            return Integer.parseInt(result);
        } catch (Exception e) {
            Logger.error("MyProperties.getInt(" + key + ", " + defaultValue + ")", e);
        }
        return defaultValue;
    }

    public static long getLong(ConfigKey confKey, long defaultValue) {
        checkState();

        Long l = getLong(confKey.key());
        if (l != null) {
            return l;
        }
        return getLong(confKey.legacyKey(), defaultValue);
    }

    public static Long getLong(String key) {
        checkState();

        String result = getStr(key);
        if (result == null) {
            return null;
        }

        try {
            return Long.valueOf(result);
        } catch (Exception e) {
            Logger.error("MyProperties.getLong(" + key + ")", e);
        }
        return null;
    }

    public static long getLong(String key, long defaultValue) {
        checkState();

        String result = getStr(key);
        if (result == null) {
            return defaultValue;
        }

        try {
            return Long.parseLong(result);
        } catch (Exception e) {
            Logger.error("MyProperties.getLong(" + key + ", " + defaultValue + ")", e);
        }
        return defaultValue;
    }

    public static long getLong(String key, long defaultValue, long minValue) {
        checkState();

        long result = getLong(key, defaultValue);
        if (result <= minValue) {
            return minValue;
        }
        return result;
    }

    public static boolean isSame(String key, String expectValue) {
        checkState();

        if (expectValue == null) {
            throw new IllegalArgumentException("isSame(" + key + ", null): expectValue must not null!!!");
        }
        return expectValue.equals(getStr(key));
    }

    public static boolean getBoolean(ConfigKey confKey, boolean defaultValue) {
        checkState();

        Boolean result = getBoolean(confKey.key());
        if (result != null) {
            return result;
        }
        return getBoolean(confKey.legacyKey(), defaultValue);
    }

    public static Boolean getBoolean(String key) {
        checkState();

        String result = getStr(key);
        if (result != null) {
            return result.equalsIgnoreCase("true");
        }
        return null;
    }

    public static boolean getBoolean(String key, boolean defaultValue) {
        checkState();

        String result = getStr(key);
        if (result != null) {
            return result.equalsIgnoreCase("true");
        }
        return defaultValue;
    }
}
