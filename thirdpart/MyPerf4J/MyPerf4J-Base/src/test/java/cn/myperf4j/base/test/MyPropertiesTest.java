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
package cn.myperf4j.base.test;

import cn.myperf4j.base.config.MyProperties;
import cn.myperf4j.base.constant.PropertyKeys;
import cn.myperf4j.base.constant.PropertyKeys.Basic;
import cn.myperf4j.base.constant.PropertyKeys.Filter;
import cn.myperf4j.base.constant.PropertyKeys.Metrics;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by LinShunkang on 2018/10/28
 */
public class MyPropertiesTest extends BaseTest {

    @Test
    public void test() {
        Assert.assertEquals(MyProperties.getStr(PropertyKeys.PRO_FILE_NAME), BaseTest.TEMP_FILE);
        Assert.assertEquals(MyProperties.getStr(Basic.APP_NAME), BaseTest.APP_NAME);
        Assert.assertEquals(MyProperties.getStr(Metrics.EXPORTER), BaseTest.METRICS_EXPORTER);
        Assert.assertEquals(MyProperties.getStr(Filter.PACKAGES_INCLUDE), BaseTest.INCLUDE_PACKAGES);

        MyProperties.setStr("key", "value");
        Assert.assertEquals(MyProperties.getStr("key"), "value");
        Assert.assertTrue(MyProperties.isSame("key", "value"));

        MyProperties.setStr("long", "1000");
        Assert.assertEquals(MyProperties.getLong("long", 1), 1000);
        Assert.assertEquals(MyProperties.getLong("long", 1, 10000), 10000);
    }
}
