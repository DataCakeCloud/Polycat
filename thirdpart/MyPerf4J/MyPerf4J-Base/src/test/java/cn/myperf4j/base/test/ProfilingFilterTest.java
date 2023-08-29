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

import cn.myperf4j.base.config.ProfilingFilter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static cn.myperf4j.base.config.ProfilingFilter.isNotNeedInjectClassLoader;
import static cn.myperf4j.base.config.ProfilingFilter.isNotNeedInjectMethod;

/**
 * Created by LinShunkang on 2018/10/28
 */
public class ProfilingFilterTest {

    @Before
    public void init() {
        ProfilingFilter.addIncludePackage("org.junit");
        ProfilingFilter.addExcludePackage("org.junit.rules");
        ProfilingFilter.addExcludeMethods("hello");
        ProfilingFilter.addExcludeClassLoader("org.apache.catalina.loader.WebappClassLoader");
        ProfilingFilter.addExcludeMethods("Demo.getId1(long)");
        ProfilingFilter.addExcludeMethods("Demo.getId1(long,int)");
        ProfilingFilter.addExcludeMethods("Demo.getId1()");
        ProfilingFilter.addExcludeMethods("Demo.getId1(ClassA$ClassB,long)");
    }

    @Test
    public void test() {
        Assert.assertFalse(ProfilingFilter.isNeedInject("org.junit.Before"));
        Assert.assertTrue(ProfilingFilter.isNeedInject("org/junit/Before"));
        Assert.assertTrue(ProfilingFilter.isNotNeedInject("org/junit/rules/ErrorCollector"));

        Assert.assertTrue(isNotNeedInjectMethod("toString"));
        Assert.assertTrue(isNotNeedInjectMethod("hello"));
        Assert.assertFalse(isNotNeedInjectMethod("assertFalse"));

        Assert.assertFalse(isNotNeedInjectMethod("Demo.getId1(ClassA$ClassB)"));
        Assert.assertTrue(isNotNeedInjectMethod("Demo.getId1()"));
        Assert.assertTrue(isNotNeedInjectMethod("Demo.getId1(ClassA$ClassB,long)"));
        Assert.assertTrue(isNotNeedInjectMethod("Demo.getId1(long)"));
        Assert.assertTrue(isNotNeedInjectMethod("Demo.getId1(long,int)"));

        Assert.assertTrue(isNotNeedInjectClassLoader("org.apache.catalina.loader.WebappClassLoader"));
        Assert.assertFalse(isNotNeedInjectClassLoader("org.springframework.boot.loader.LaunchedURLClassLoader"));
    }

    @Test
    public void testWildcardMatch() {
        Assert.assertFalse(ProfilingFilter.isNeedInject("cn/junit/test/a"));
        Assert.assertFalse(ProfilingFilter.isNeedInject("cn/junit/test2"));

        ProfilingFilter.addIncludePackage("cn.junit.test.*");
        Assert.assertTrue(ProfilingFilter.isNeedInject("cn/junit/test/a"));
        Assert.assertTrue(ProfilingFilter.isNeedInject("cn/junit/test/2"));

        Assert.assertFalse(ProfilingFilter.isNotNeedInject("com/junit/test/a"));
        Assert.assertFalse(ProfilingFilter.isNotNeedInject("com/junit/test2"));

        ProfilingFilter.addExcludePackage("com.junit.test.*");
        Assert.assertTrue(ProfilingFilter.isNotNeedInject("com/junit/test/a"));
        Assert.assertTrue(ProfilingFilter.isNotNeedInject("com/junit/test/2"));
    }

}
