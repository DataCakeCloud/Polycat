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

import cn.myperf4j.base.config.LevelMappingFilter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

/**
 * Created by LinShunkang on 2019/05/12
 */
public class ClassLevelMapping {

    @Before
    public void init() {
        LevelMappingFilter.putLevelMapping("Controller", Collections.singletonList("*Controller"));
        LevelMappingFilter.putLevelMapping("Api", Collections.singletonList("*Api*"));
        LevelMappingFilter.putLevelMapping("Service", Arrays.asList("*Service", "*ServiceImpl"));
    }

    @Test
    public void test() {
        Assert.assertEquals(LevelMappingFilter.getClassLevel("com.google.UserController"), "Controller");
        Assert.assertEquals(LevelMappingFilter.getClassLevel("com.google.UserApi"), "Api");
        Assert.assertEquals(LevelMappingFilter.getClassLevel("com.google.UserApiImpl"), "Api");
        Assert.assertEquals(LevelMappingFilter.getClassLevel("com.google.UserApiService"), "Api");
        Assert.assertEquals(LevelMappingFilter.getClassLevel("com.google.UserApiServiceImpl"), "Api");
        Assert.assertEquals(LevelMappingFilter.getClassLevel("com.google.UserService"), "Service");
        Assert.assertEquals(LevelMappingFilter.getClassLevel("com.google.UserServiceImpl"), "Service");
    }
}
