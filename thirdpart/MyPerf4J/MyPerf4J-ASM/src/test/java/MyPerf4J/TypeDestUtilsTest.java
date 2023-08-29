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
package MyPerf4J;

import cn.myperf4j.base.util.TypeDescUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;

import static cn.myperf4j.base.util.TypeDescUtils.getMethodParamsDesc;
import static cn.myperf4j.base.util.TypeDescUtils.getSimpleClassName;

/**
 * Created by LinShunkang on 2018/10/19
 */
public class TypeDestUtilsTest {

    @Test
    public void test() {
        Assert.assertEquals(getMethodParamsDesc("()V"), "()");
        Assert.assertEquals(getMethodParamsDesc("(IF)V"), "(int, float)");
        Assert.assertEquals(getMethodParamsDesc("(ILjava/lang/Object;F)V"), "(int, Object, float)");
        Assert.assertEquals(getMethodParamsDesc("(Ljava/lang/Object;)I"), "(Object)");
        Assert.assertEquals(getMethodParamsDesc("(ILjava/lang/String;)[I"), "(int, String)");
        Assert.assertEquals(getMethodParamsDesc("(ILjava/lang/Map;)[I"), "(int, Map)");
        Assert.assertEquals(getMethodParamsDesc("([I)Ljava/lang/Object;"), "(int[])");
        Assert.assertEquals(getMethodParamsDesc(
                "([ILjava/lang/Object;[Ljava/lang/Object;[Ljava/lang/String;)Ljava/lang/Object;"),
                "(int[], Object, Object[], String[])");
        Assert.assertEquals(getMethodParamsDesc(
                "([[ILjava/lang/Object;[[[Ljava/lang/Object;[[[[[Ljava/lang/String;)Ljava/lang/Object;"),
                "(int[][], Object, Object[][][], String[][][][][])");
    }

    @Test
    public void testMethod() throws NoSuchMethodException {
        Method method0 = TypeDestUtilsTest.class.getDeclaredMethod("testMethod");
        Assert.assertEquals(getMethodParamsDesc(method0), "()");

        Method method1 = TypeDescUtils.class.getDeclaredMethod("getMethodParamsDesc", Method.class);
        Assert.assertEquals(getMethodParamsDesc(method1), "(Method)");
    }

    @Test
    public void testSimpleClassName() {
        Assert.assertEquals(getSimpleClassName("java/lang/String"), "String");
        Assert.assertEquals(getSimpleClassName("java/lang/Object"), "Object");
    }

}
