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

import static cn.myperf4j.base.http.HttpStatusClass.CLIENT_ERROR;
import static cn.myperf4j.base.http.HttpStatusClass.INFORMATIONAL;
import static cn.myperf4j.base.http.HttpStatusClass.REDIRECTION;
import static cn.myperf4j.base.http.HttpStatusClass.SERVER_ERROR;
import static cn.myperf4j.base.http.HttpStatusClass.SUCCESS;
import static cn.myperf4j.base.http.HttpStatusClass.UNKNOWN;

/**
 * Created by LinShunkang on 2020/05/16
 */
public class HttpStatusClassTest {

    @Test
    public void testValueOf() {
        for (int i = -100; i < 100; i++) {
            Assert.assertEquals(UNKNOWN, HttpStatusClass.valueOf(i));
        }

        for (int i = 100; i < 200; i++) {
            Assert.assertEquals(INFORMATIONAL, HttpStatusClass.valueOf(i));
        }

        for (int i = 200; i < 300; i++) {
            Assert.assertEquals(SUCCESS, HttpStatusClass.valueOf(i));
        }

        for (int i = 300; i < 400; i++) {
            Assert.assertEquals(REDIRECTION, HttpStatusClass.valueOf(i));
        }

        for (int i = 400; i < 500; i++) {
            Assert.assertEquals(CLIENT_ERROR, HttpStatusClass.valueOf(i));
        }

        for (int i = 500; i < 600; i++) {
            Assert.assertEquals(SERVER_ERROR, HttpStatusClass.valueOf(i));
        }

        for (int i = 600; i < 1024; i++) {
            Assert.assertEquals(UNKNOWN, HttpStatusClass.valueOf(i));
        }
    }
}
