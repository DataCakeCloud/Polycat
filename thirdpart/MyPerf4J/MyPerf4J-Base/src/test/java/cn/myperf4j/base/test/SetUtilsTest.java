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

import cn.myperf4j.base.util.SetUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by LinShunkang on 2019-01-01
 */
public class SetUtilsTest {

    @Test
    public void test() {
        Assert.assertTrue(SetUtils.of(1).contains(1));
        Assert.assertTrue(SetUtils.of(1, 2, 3).contains(1));
        Assert.assertTrue(SetUtils.of(1, 2, 3).contains(2));
        Assert.assertTrue(SetUtils.of(1, 2, 3).contains(3));
    }
}
