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

import cn.myperf4j.base.config.ProfilingParams;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by LinShunkang on 2018/10/28
 */
public class ProfilingParamsTest {

    @Test
    public void test() {
        ProfilingParams params = ProfilingParams.of(1000, 10);
        Assert.assertEquals(params.getMostTimeThreshold(), 1000);
        Assert.assertNotEquals(params.getMostTimeThreshold(), -1000);

        Assert.assertEquals(params.getOutThresholdCount(), 10);
        Assert.assertNotEquals(params.getOutThresholdCount(), -10);
    }
}
