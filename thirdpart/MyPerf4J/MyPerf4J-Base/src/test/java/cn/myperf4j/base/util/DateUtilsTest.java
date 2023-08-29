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

import org.junit.Assert;
import org.junit.Test;

import java.util.Date;

import static cn.myperf4j.base.util.DateUtils.isSameDay;
import static cn.myperf4j.base.util.DateUtils.isSameHour;
import static cn.myperf4j.base.util.DateUtils.isSameMinute;

/**
 * Created by LinShunkang on 2020/07/26
 */
public class DateUtilsTest {

    @Test
    public void testSameDay() {
        Assert.assertTrue(isSameDay(new Date(), new Date()));
        Assert.assertFalse(isSameDay(new Date(), new Date(System.currentTimeMillis() + 24 * 60 * 60 * 1000)));
    }

    @Test
    public void testSameMinute() {
        Assert.assertTrue(isSameMinute(new Date(), new Date()));
        Assert.assertFalse(isSameMinute(new Date(), new Date(System.currentTimeMillis() + 60 * 1000)));
    }

    @Test
    public void testSameHour() {
        Assert.assertTrue(isSameHour(new Date(), new Date()));
        Assert.assertFalse(isSameHour(new Date(), new Date(System.currentTimeMillis() + 60 * 60 * 1000)));
    }

}
