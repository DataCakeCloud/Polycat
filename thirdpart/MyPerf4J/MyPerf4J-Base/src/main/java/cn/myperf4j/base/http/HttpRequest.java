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

import cn.myperf4j.base.util.ArrayUtils;
import cn.myperf4j.base.util.MapUtils;
import cn.myperf4j.base.util.StrUtils;

import java.util.List;
import java.util.Map;

import static cn.myperf4j.base.http.HttpMethod.GET;
import static cn.myperf4j.base.http.HttpMethod.HEAD;
import static cn.myperf4j.base.http.HttpMethod.POST;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Created by LinShunkang on 2020/05/15
 */
public final class HttpRequest {

    private static final byte[] EMPTY_BODY = {};

    private static final ThreadLocal<StringBuilder> SB_TL = new ThreadLocal<StringBuilder>() {
        @Override
        protected StringBuilder initialValue() {
            return new StringBuilder(512);
        }
    };

    private final String url;

    private final HttpMethod method;

    private final HttpHeaders headers;

    private final Map<String, List<String>> params;

    private final byte[] body;

    private String fullUrl;

    public HttpRequest(Builder builder) {
        this.url = builder.url;
        this.method = builder.method;
        this.headers = builder.headers;
        this.params = builder.params;
        this.body = builder.body;
        this.fullUrl = "";
    }

    public HttpMethod getMethod() {
        return method;
    }

    public HttpHeaders getHeaders() {
        return headers;
    }

    public Map<String, List<String>> getParams() {
        return params;
    }

    public byte[] getBody() {
        return body;
    }

    public String getUrl() {
        return url;
    }

    public String getFullUrl() {
        if (StrUtils.isNotEmpty(fullUrl)) {
            return fullUrl;
        }
        return fullUrl = createFullUrl();
    }

    private String createFullUrl() {
        StringBuilder sb = SB_TL.get();
        try {
            if (!url.startsWith("http://") && !url.startsWith("https://")) {
                sb.append("http://");
            }
            sb.append(url);

            if (MapUtils.isEmpty(params)) {
                return sb.toString();
            }

            if (!StrUtils.isContains(url, '?')) {
                sb.append('?');
            } else if (!StrUtils.isEndWith(url, '?')) {
                sb.append('&');
            }

            for (Map.Entry<String, List<String>> param : params.entrySet()) {
                final List<String> values = param.getValue();
                for (int i = 0; i < values.size(); i++) {
                    sb.append(param.getKey()).append('=').append(values.get(i)).append('&');
                }
            }
            return sb.substring(0, sb.length() - 1);
        } finally {
            sb.setLength(0);
        }
    }

    @Override
    public String toString() {
        return "HttpRequest{" +
                "url='" + url + '\'' +
                ", method=" + method +
                ", headers=" + headers +
                ", params=" + params +
                '}';
    }

    public static class Builder {

        private String url;

        private HttpMethod method;

        private HttpHeaders headers;

        private Map<String, List<String>> params;

        private byte[] body;

        public Builder() {
            this.method = GET;
            this.headers = HttpHeaders.defaultHeaders();
            this.body = EMPTY_BODY;
        }

        public Builder url(String url) {
            if (StrUtils.isBlank(url)) {
                throw new IllegalArgumentException("url is blank!");
            }
            this.url = url;
            return this;
        }

        public Builder params(Map<String, List<String>> params) {
            this.params = params;
            return this;
        }

        public Builder headers(Map<String, List<String>> headers) {
            this.headers = new HttpHeaders(headers);
            return this;
        }

        public Builder header(String name, String value) {
            this.headers.set(name, value);
            return this;
        }

        public Builder addHeader(String name, String value) {
            this.headers.add(name, value);
            return this;
        }

        public Builder head() {
            return method(HEAD, EMPTY_BODY);
        }

        public Builder get() {
            return method(GET, EMPTY_BODY);
        }

        public Builder post(byte[] body) {
            return method(POST, body);
        }

        public Builder post(String body) {
            return method(POST, body.getBytes(UTF_8));
        }

        public Builder method(HttpMethod method, byte[] body) {
            if (method == null) {
                throw new IllegalArgumentException("method is null!");
            }

            if (ArrayUtils.isNotEmpty(body) && !method.isPermitsBody()) {
                throw new IllegalArgumentException("method " + method + " must not have a request body!");
            }

            if (ArrayUtils.isEmpty(body) && method.isPermitsBody()) {
                throw new IllegalArgumentException("method " + method + " must have a request body!");
            }

            this.method = method;
            this.body = body;
            return this;
        }

        public HttpRequest build() {
            if (url == null) {
                throw new IllegalStateException("url is null!");
            }
            return new HttpRequest(this);
        }
    }
}
