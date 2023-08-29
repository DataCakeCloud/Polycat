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
package cn.myperf4j.base.http.server;

import cn.myperf4j.base.http.HttpHeaders;
import cn.myperf4j.base.http.HttpRequest;
import cn.myperf4j.base.http.HttpRequest.Builder;
import cn.myperf4j.base.http.HttpRespStatus;
import cn.myperf4j.base.http.HttpResponse;
import cn.myperf4j.base.http.client.HttpClient;
import cn.myperf4j.base.util.MapUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Created by LinShunkang on 2020/07/12
 */
public class SimpleHttpServerTest {

    private static final String RESP_STR = "Hello, SimpleHttpServer";

    private static final int PORT = 1024;

    private static final HttpClient httpClient = new HttpClient.Builder().build();

    private static final SimpleHttpServer server = new SimpleHttpServer(PORT, new Dispatcher() {
        @Override
        public HttpResponse dispatch(HttpRequest request) {
            return new HttpResponse(HttpRespStatus.OK, defaultHeaders(), RESP_STR.getBytes(UTF_8));
        }

        private HttpHeaders defaultHeaders() {
            HttpHeaders headers = new HttpHeaders(6);
            headers.set("User-Agent", "MyPerf4J");
            headers.set("Connection", "Keep-Alive");
            headers.set("Charset", UTF_8.name());
            return headers;
        }
    });

    @BeforeClass
    public static void start() {
        server.startAsync();
    }

    @AfterClass
    public static void stop() {
        server.stop();
    }

    @Test
    public void test() throws IOException {
        for (int i = 0; i < 10; i++) {
            final HttpResponse response = httpClient.execute(new Builder()
                    .url("127.0.0.1:" + PORT + "/test")
                    .get()
                    .params(MapUtils.of("k1", Collections.singletonList("v1")))
                    .build());
            Assert.assertEquals(RESP_STR, response.getBodyString());
        }
    }
}
