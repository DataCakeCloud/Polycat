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
package cn.myperf4j.base;

import cn.myperf4j.base.config.LevelMappingFilter;

/**
 * Created by LinShunkang on 2018/7/13
 */
public final class MethodTag {

    private static final String TYPE_GENERAL = "General";

    private static final String TYPE_DYNAMIC_PROXY = "DynamicProxy";

    private final String simpleClassName;

    private final String methodName;

    private final String methodParamDesc;

    private final String fullDescription;

    private final String description;

    private final String type;

    private final String level;

    private MethodTag(String fullClassName,
                      String simpleClassName,
                      String methodName,
                      String methodParamDesc,
                      String type,
                      String level) {
        this.simpleClassName = simpleClassName;
        this.methodName = methodName;
        this.methodParamDesc = methodParamDesc;
        this.fullDescription = fullClassName + '.' + methodName;
        this.description = simpleClassName + '.' + methodName + methodParamDesc;
        this.type = type;
        this.level = level;
    }

    public String getSimpleClassName() {
        return simpleClassName;
    }

    public String getMethodName() {
        return methodName;
    }

    public String getMethodParamDesc() {
        return methodParamDesc;
    }

    public String getSimpleDesc() {
        return description;
    }

    public String getFullDesc() {
        return fullDescription;
    }

    public String getType() {
        return type;
    }

    public String getLevel() {
        return level;
    }

    @Override
    public String toString() {
        return "MethodTag{" +
                "simpleClassName='" + simpleClassName + '\'' +
                ", methodName='" + methodName + '\'' +
                ", methodParamDesc='" + methodParamDesc + '\'' +
                ", description='" + description + '\'' +
                ", type='" + type + '\'' +
                '}';
    }

    public static MethodTag getGeneralInstance(String fullClassName,
                                               String simpleClassName,
                                               String classLevel,
                                               String methodName,
                                               String methodParamDesc) {
        return new MethodTag(fullClassName, simpleClassName, methodName, methodParamDesc, TYPE_GENERAL, classLevel);
    }

    public static MethodTag getDynamicProxyInstance(String fullClassName,
                                                    String className,
                                                    String methodName,
                                                    String methodParamDesc) {
        String classLevel = LevelMappingFilter.getClassLevel(className);
        return new MethodTag(fullClassName, className, methodName, methodParamDesc, TYPE_DYNAMIC_PROXY, classLevel);
    }
}
