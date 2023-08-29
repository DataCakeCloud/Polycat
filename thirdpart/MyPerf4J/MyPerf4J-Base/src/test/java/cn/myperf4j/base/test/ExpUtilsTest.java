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

import cn.myperf4j.base.util.PkgExpUtils;
import cn.myperf4j.base.util.StrMatchUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Created by LinShunkang on 2019-01-01
 */
public class ExpUtilsTest {

    @Test
    public void testParse() {
        Assert.assertTrue(PkgExpUtils.parse("cn.myperf4j.util.[Logger,DateUtils]")
                .containsAll(Arrays.asList("cn.myperf4j.util.Logger;cn.myperf4j.util.DateUtils".split(";"))));

        Assert.assertTrue(PkgExpUtils.parse("cn.myperf4j.util.[Logger,DateUtils].abc")
                .containsAll(Arrays.asList("cn.myperf4j.util.Logger.abc;cn.myperf4j.util.DateUtils.abc".split(";"))));

        Assert.assertTrue(PkgExpUtils.parse("cn.myperf4j.util.[Logger,DateUtils].abc.[e,f]")
                .containsAll(Arrays.asList(("cn.myperf4j.util.Logger.abc.e;cn.myperf4j.util.DateUtils.abc.f;" +
                        "cn.myperf4j.util.Logger.abc.e;cn.myperf4j.util.DateUtils.abc.f").split(";"))));
    }

    @Test
    public void testMatch() {
        Assert.assertTrue(StrMatchUtils.isMatch("abc", "abc"));
        Assert.assertTrue(StrMatchUtils.isMatch("abc", "a*"));
        Assert.assertTrue(StrMatchUtils.isMatch("abc", "*abc"));
        Assert.assertTrue(StrMatchUtils.isMatch("abc", "a*bc"));
        Assert.assertTrue(StrMatchUtils.isMatch("abc", "a*c"));
        Assert.assertTrue(StrMatchUtils.isMatch("abc", "ab*"));
        Assert.assertTrue(StrMatchUtils.isMatch("abc", "ab**"));
        Assert.assertTrue(StrMatchUtils.isMatch("abcdef", "ab****c*"));
        Assert.assertTrue(StrMatchUtils.isMatch("abcdef", "ab*c*f"));

        Assert.assertFalse(StrMatchUtils.isMatch("abcdef", "ab*c*eg"));
        Assert.assertFalse(StrMatchUtils.isMatch("abcdef", "abcdefg"));
    }
}
