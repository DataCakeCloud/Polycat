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
package cn.myperf4j.base.http;

import cn.myperf4j.base.util.MapUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Created by LinShunkang on 2020/05/15
 */
public final class HttpHeaders {

    private final Map<String, List<String>> headers;

    public HttpHeaders(int size) {
        this.headers = MapUtils.createHashMap(size);
    }

    public HttpHeaders(Map<String, List<String>> headers) {
        this.headers = headers;
    }

    public String get(String name) {
        List<String> values = headers.get(name);
        if (values == null || values.isEmpty()) {
            return null;
        }
        return values.get(0);
    }

    public List<String> getValues(String name) {
        return headers.get(name);
    }

    public void set(String name, String value) {
        List<String> values = headers.get(name);
        if (values == null) {
            values = new ArrayList<>(1);
            headers.put(name, values);
        } else {
            values.clear();
        }
        values.add(value);
    }

    public void add(String name, String value) {
        List<String> values = headers.get(name);
        if (values == null) {
            values = new ArrayList<>(1);
            headers.put(name, values);
        }
        values.add(value);
    }

    public List<String> names() {
        return new ArrayList<>(headers.keySet());
    }

    public Map<String, List<String>> headers() {
        return headers;
    }

    public static HttpHeaders defaultHeaders() {
        HttpHeaders headers = new HttpHeaders(6);
        headers.set("User-Agent", "MyPerf4J");
        headers.set("Connection", "Keep-Alive");
        headers.set("Charset", UTF_8.name());
        return headers;
    }
}
