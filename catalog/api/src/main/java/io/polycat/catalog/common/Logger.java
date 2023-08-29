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
package io.polycat.catalog.common;

public class Logger {

    private final org.slf4j.Logger logger;

    private final String name;

    private Logger(String name) {
        this.name = name;
        this.logger = org.slf4j.LoggerFactory.getLogger(name);
    }

    public static Logger getLogger(Class<?> clazz) {
        return new Logger(clazz.getName());
    }

    public static Logger getLogger(String name) {
        return new Logger(name);
    }

    public void debug(String msg) {
        logger.debug(msg);
    }

    public void debug(String msg, Object... args) {
        logger.debug(msg, args);
    }

    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }

    public boolean isInfoEnabled() {
        return logger.isInfoEnabled();
    }

    public void warn(String msg) {
        logger.warn(msg);
    }

    public void warn(String msg, Object... args) {
        logger.warn(msg, args);
    }

    public void warn(String msg, Throwable t) {
        logger.warn(msg, t);
    }

    public void warn(Throwable t) {
        logger.warn(t.getMessage());
    }

    public void info(String msg) {
        logger.info(msg);
    }

    public void info(String msg, Object... args) {
        logger.info(msg, args);
    }

    public void error(String msg) {
        logger.error(msg);
    }

    public void error(String msg, Object... args) {
        logger.error(msg, args);
    }
    
    public void error(String msg, Throwable t) {
        logger.error(msg, t);
    }

    public void error(Throwable t) {
        logger.error(t.getMessage(), t);
    }

    // 安全过程证明相关日志
    // todo:建议通过一个 CertificateLogger来打印，然后用log4j的配置来控制这个 CertificateLogger 类的输出格式 和 重定向等
    public void infoSafeCert(String msg) {
        info("[SafeCertificates] " + msg);
    }

    // 安全过程证明相关日志
    public void debugSafeCert(String msg) {
        debug("[SafeCertificates] " + msg);
    }
}
