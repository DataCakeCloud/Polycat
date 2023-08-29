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
import cn.myperf4j.base.http.HttpRequest;
import cn.myperf4j.base.http.HttpResponse;
import cn.myperf4j.base.http.server.Dispatcher;
import cn.myperf4j.base.http.server.SimpleHttpServer;
import cn.myperf4j.base.util.MapUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static cn.myperf4j.base.http.HttpRespStatus.OK;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Created by LinShunkang on 2020/05/16
 */
public class HttpClientTest {

    private static final String RESPONSE_BODY = "Hello!";

    private static final HttpClient httpClient = new HttpClient.Builder().build();

    private static SimpleHttpServer server;

    @BeforeClass
    public static void init() {
        server = new SimpleHttpServer(8686, new Dispatcher() {
            @Override
            public HttpResponse dispatch(HttpRequest request) {
                System.out.println("Dispatcher.dispatch(): request.body=" + new String(request.getBody()));
                return new HttpResponse(OK, new HttpHeaders(0), RESPONSE_BODY.getBytes(UTF_8));
            }
        });
        server.startAsync();
    }

    @AfterClass
    public static void clean() {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void testGet() {
        HttpRequest req = new HttpRequest.Builder()
                .url("http://www.baidu.com")
//                .header("Connection", "close")
                .get()
                .build();
        try {
            HttpResponse resp = httpClient.execute(req);
            HttpHeaders headers = resp.getHeaders();
            System.out.println("Status=" + resp.getStatus());
            System.out.println("Connection=" + headers.get("Connection"));
            System.out.println(resp.getBodyString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testPost() {
        Map<String, List<String>> params = MapUtils.createHashMap(2);
        params.put("db", Collections.singletonList("http"));

        HttpRequest req = new HttpRequest.Builder()
                .url("localhost:8686/write")
                .params(params)
                .post("cpu_load_short,host=server01,region=us-west value=0.64 1434055562000000000\n" +
                        "cpu_load_short,host=server02,region=us-west value=0.96 1434055562000000000")
                .build();

        for (int i = 0; i < 10; i++) {
            try {
                HttpResponse resp = httpClient.execute(req);
                Assert.assertEquals(OK, resp.getStatus());
                Assert.assertEquals(RESPONSE_BODY, resp.getBodyString());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
