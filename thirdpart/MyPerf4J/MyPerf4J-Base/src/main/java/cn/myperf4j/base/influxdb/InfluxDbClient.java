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
package cn.myperf4j.base.influxdb;

import cn.myperf4j.base.http.client.HttpClient;
import cn.myperf4j.base.http.HttpRequest;
import cn.myperf4j.base.http.HttpRespStatus;
import cn.myperf4j.base.http.HttpResponse;
import cn.myperf4j.base.util.Base64;
import cn.myperf4j.base.util.Logger;
import cn.myperf4j.base.util.StrUtils;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static cn.myperf4j.base.http.HttpStatusClass.INFORMATIONAL;
import static cn.myperf4j.base.http.HttpStatusClass.SUCCESS;
import static cn.myperf4j.base.util.ThreadUtils.newThreadFactory;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Created by LinShunkang on 2020/05/18
 */
public final class InfluxDbClient {

    private static final ThreadPoolExecutor ASYNC_EXECUTOR = new ThreadPoolExecutor(
            1,
            2,
            3,
            TimeUnit.MINUTES,
            new LinkedBlockingQueue<Runnable>(1024),
            newThreadFactory("MyPerf4J-InfluxDbClient_"),
            new ThreadPoolExecutor.DiscardOldestPolicy());

    private final String url;

    private final String writeReqUrl;

    private final String database;

    private final String username;

    private final String password;

    private final String authorization;

    protected final HttpClient httpClient;

    public InfluxDbClient(Builder builder) {
        this.url = "http://" + builder.host + ":" + builder.port;
        this.writeReqUrl = url + "/write?db=" + builder.database;
        this.database = builder.database;
        this.username = builder.username;
        this.password = builder.password;
        this.authorization = buildAuthorization(builder);
        this.httpClient = new HttpClient.Builder()
                .connectTimeout(builder.connectTimeout)
                .readTimeout(builder.readTimeout)
                .build();
    }

    private String buildAuthorization(Builder builder) {
        if (StrUtils.isNotBlank(builder.username) && StrUtils.isNotBlank(builder.password)) {
            String auth = username + ':' + password;
            return "Basic " + Base64.getEncoder().encodeToString(auth.getBytes(UTF_8));
        }
        return "";
    }

    public boolean createDatabase() {
        HttpRequest req = new HttpRequest.Builder()
                .url(url + "/query")
                .header("Authorization", authorization)
                .post("q=CREATE DATABASE " + database)
                .build();
        try {
            HttpResponse response = httpClient.execute(req);
            Logger.info("InfluxDbClient create database '" + database + "' response.status=" + response.getStatus());

            if (response.getStatus().statusClass() == SUCCESS) {
                return true;
            }
        } catch (IOException e) {
            Logger.error("InfluxDbClient.createDatabase(): e=" + e.getMessage(), e);
        }
        return false;
    }

    public boolean writeMetricsAsync(final String content) {
        try {
            ASYNC_EXECUTOR.execute(new ReqTask(content));
            return true;
        } catch (Throwable t) {
            Logger.error("InfluxDbClient.writeMetricsAsync(): t=" + t.getMessage(), t);
        }
        return false;
    }

    private class ReqTask implements Runnable {

        private final String content;

        ReqTask(String content) {
            this.content = content;
        }

        @Override
        public void run() {
            HttpRequest req = new HttpRequest.Builder()
                    .url(writeReqUrl)
                    .header("Authorization", authorization)
                    .post(content)
                    .build();
            try {
                HttpResponse response = httpClient.execute(req);
                HttpRespStatus status = response.getStatus();
                if (status.statusClass() == SUCCESS && Logger.isDebugEnable()) {
                    Logger.debug("ReqTask.run(): respStatus=" + status.simpleString() + ", reqBody=" + content);
                } else if (status.statusClass() != INFORMATIONAL && status.statusClass() != SUCCESS) {
                    Logger.warn("ReqTask.run(): respStatus=" + status.simpleString() + ", reqBody=" + content);
                }
            } catch (IOException e) {
                Logger.error("ReqTask.run(): ", e);
            }
        }
    }

    public static class Builder {

        private static final int DEFAULT_CONNECT_TIMEOUT = 1000;

        private static final int DEFAULT_READ_TIMEOUT = 3000;

        private String host;

        private int port;

        private String database;

        private String username;

        private String password;

        private int connectTimeout;

        private int readTimeout;

        public Builder() {
            this.connectTimeout = DEFAULT_CONNECT_TIMEOUT;
            this.readTimeout = DEFAULT_READ_TIMEOUT;
        }

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder database(String database) {
            this.database = database;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder connectTimeout(int connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public Builder readTimeout(int readTimeout) {
            this.readTimeout = readTimeout;
            return this;
        }

        public InfluxDbClient build() {
            return new InfluxDbClient(this);
        }
    }
}
