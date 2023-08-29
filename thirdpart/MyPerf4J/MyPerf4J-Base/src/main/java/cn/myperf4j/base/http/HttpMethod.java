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

/**
 * Created by LinShunkang on 2020/05/15
 */
public enum HttpMethod {

    UNKNOWN("UNKNOWN", false),
    HEAD("HEAD", false),
    GET("GET", false),
    POST("POST", true),
    ;

    private final String name;

    private final boolean permitsBody;

    HttpMethod(String name, boolean permitsBody) {
        this.name = name;
        this.permitsBody = permitsBody;
    }

    public String getName() {
        return name;
    }

    public boolean isPermitsBody() {
        return permitsBody;
    }

    public static HttpMethod parse(String name) {
        switch (name) {
            case "HEAD":
                return HEAD;
            case "GET":
                return GET;
            case "POST":
                return POST;
            default:
                return UNKNOWN;
        }
    }
}
