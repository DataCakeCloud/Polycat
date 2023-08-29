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

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

/**
 * Created by LinShunkang on 2020/05/16
 */
public class HttpHeadersTest {

    @Test
    public void testSet() {
        HttpHeaders headers = new HttpHeaders(2);
        headers.set("Connection", "close");
        headers.set("Connection", "Keep-Alive");
        headers.set("Accept-Encoding", "gzip, deflate");

        Assert.assertEquals("Keep-Alive", headers.get("Connection"));
        Assert.assertEquals("gzip, deflate", headers.get("Accept-Encoding"));

        Assert.assertEquals(Collections.singletonList("Keep-Alive"), headers.getValues("Connection"));
        Assert.assertEquals(Collections.singletonList("gzip, deflate"), headers.getValues("Accept-Encoding"));
    }

    @Test
    public void testAdd() {
        HttpHeaders headers = new HttpHeaders(2);
        headers.set("Connection", "Keep-Alive");
        headers.add("Connection", "close");

        Assert.assertEquals("Keep-Alive", headers.get("Connection"));
        Assert.assertEquals(Arrays.asList("Keep-Alive", "close"), headers.getValues("Connection"));
    }

}
