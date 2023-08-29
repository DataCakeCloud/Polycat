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

import cn.myperf4j.base.http.server.QueryStringDecoder;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Created by LinShunkang on 2020/07/12
 */
public class QueryStringDecoderTest {

    @Test
    public void testHasPath() {
        final String encodedURL = "/api/test?k1=v1&k2=v2&k3=v3";
        QueryStringDecoder appDecoder = new QueryStringDecoder(encodedURL, StandardCharsets.UTF_8, true);
        Map<String, List<String>> params = appDecoder.parameters();
        System.out.println(params);
    }

    @Test
    public void testNoPath() {
        final String encodedURL = "k1=v1&k2=v2&k3=v3";
        QueryStringDecoder appDecoder = new QueryStringDecoder(encodedURL, StandardCharsets.UTF_8, false);
        Map<String, List<String>> params = appDecoder.parameters();
        System.out.println(params);
    }
}
