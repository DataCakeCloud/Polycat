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
package cn.myperf4j.base.http.client;

import cn.myperf4j.base.http.HttpHeaders;
import cn.myperf4j.base.http.HttpMethod;
import cn.myperf4j.base.http.HttpRequest;
import cn.myperf4j.base.http.HttpRespStatus;
import cn.myperf4j.base.http.HttpResponse;
import cn.myperf4j.base.util.ArrayUtils;
import cn.myperf4j.base.util.ListUtils;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

import static cn.myperf4j.base.http.HttpMethod.POST;
import static cn.myperf4j.base.http.HttpStatusClass.SUCCESS;
import static cn.myperf4j.base.util.InputStreamUtils.toBytes;

/**
 * Created by LinShunkang on 2020/05/15
 */
public final class HttpClient {

    private final int connectTimeout;

    private final int readTimeout;

    public HttpClient(Builder builder) {
        this.connectTimeout = builder.connectTimeout;
        this.readTimeout = builder.readTimeout;
    }

    public HttpResponse execute(HttpRequest request) throws IOException {
        HttpURLConnection urlConn = createConnection(request);
        urlConn.connect();

        HttpHeaders headers = new HttpHeaders(urlConn.getHeaderFields());
        HttpRespStatus status = HttpRespStatus.valueOf(urlConn.getResponseCode());
        if (SUCCESS.contains(status.code())) {
            return new HttpResponse(status, headers, toBytes(urlConn.getInputStream()));
        } else {
            return new HttpResponse(status, headers, toBytes(urlConn.getErrorStream()));
        }
    }

    private HttpURLConnection createConnection(HttpRequest request) throws IOException {
        URL url = new URL(request.getFullUrl());
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setConnectTimeout(connectTimeout);
        conn.setReadTimeout(readTimeout);

        HttpMethod method = request.getMethod();
        conn.setRequestMethod(method.getName());
        conn.setDoOutput(method == POST);
        conn.setDoInput(true);
        conn.setUseCaches(false);

        configureHeaders(request, conn);
        writeBody(request, conn);
        return conn;
    }

    private void configureHeaders(HttpRequest request, HttpURLConnection conn) {
        HttpHeaders headers = request.getHeaders();
        List<String> names = headers.names();
        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            List<String> values = headers.getValues(name);
            if (ListUtils.isEmpty(values)) {
                continue;
            }

            for (int k = 0; k < values.size(); k++) {
                conn.addRequestProperty(name, values.get(k));
            }
        }
    }

    private void writeBody(HttpRequest request, HttpURLConnection conn) throws IOException {
        HttpMethod method = request.getMethod();
        byte[] body = request.getBody();
        if (method.isPermitsBody() && ArrayUtils.isNotEmpty(body)) {
            try (BufferedOutputStream bufferedOs = new BufferedOutputStream(conn.getOutputStream())) {
                bufferedOs.write(body);
            }
        }
    }

    public static class Builder {

        private static final int DEFAULT_CONNECT_TIMEOUT = 1000;

        private static final int DEFAULT_READ_TIMEOUT = 3000;

        private int connectTimeout;

        private int readTimeout;

        public Builder() {
            this.connectTimeout = DEFAULT_CONNECT_TIMEOUT;
            this.readTimeout = DEFAULT_READ_TIMEOUT;
        }

        public Builder connectTimeout(int connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public Builder readTimeout(int readTimeout) {
            this.readTimeout = readTimeout;
            return this;
        }

        public HttpClient build() {
            return new HttpClient(this);
        }
    }
}
