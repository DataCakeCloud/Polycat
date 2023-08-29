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

import java.net.InetAddress;

/**
 * Created by LinShunkang on 2020/05/23
 */
public final class IpUtils {

    private static String LOCALHOST_NAME = "";

    static {
        try {
            InetAddress localInetAddress = InetAddress.getLocalHost();
            LOCALHOST_NAME = localInetAddress.getHostName();
            Logger.info("IpUtils.static localHostName=" + LOCALHOST_NAME);
        } catch (Exception e) {
            Logger.error("IpUtils.static initializer()", e);
        }
    }

    private IpUtils() {
        //empty
    }

    public static String getLocalhostName() {
        return LOCALHOST_NAME;
    }
}
