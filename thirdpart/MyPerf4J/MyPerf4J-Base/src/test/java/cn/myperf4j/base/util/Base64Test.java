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
package cn.myperf4j.base.util;

import cn.myperf4j.base.util.Base64.Encoder;
import org.junit.Assert;
import org.junit.Test;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Created by LinShunkang on 2020/05/24
 */
public class Base64Test {

    private final Encoder encoder = Encoder.RFC4648;

    @Test
    public void test() {
        byte[] bytes = "Hello, Base64!".getBytes(UTF_8);
        Assert.assertEquals("SGVsbG8sIEJhc2U2NCE=", encoder.encodeToString(bytes));
        Assert.assertArrayEquals("SGVsbG8sIEJhc2U2NCE=".getBytes(UTF_8), encoder.encode(bytes));
    }
}
