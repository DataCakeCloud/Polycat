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

import cn.myperf4j.base.constant.PropertyValues.Separator;

import java.util.List;
import java.util.Set;

/**
 * Created by LinShunkang on 2018-12-31
 * 该类用于解析 MyPerf4J 自定义的包路径表达式解析
 * 规则：
 * 1、可以用 [] 代表集合的概念，集合 [e1,e2,e3] 中包含多个元素，每个元素由英文逗号分隔，元素可以是 package 和 class。但 [] 不可嵌套出现。
 * 例如：
 * a、cn.myperf4j.util.[Logger,DateUtils] -> cn.myperf4j.util.Logger;cn.myperf4j.util.DateUtils
 * b、cn.myperf4j.metric.[formatter,processor] -> cn.myperf4j.metric.formatter;cn.myperf4j.metric.processor
 * c、cn.myperf4j.metric.[formatter,MethodMetrics] -> cn.myperf4j.metric.formatter;cn.myperf4j.metric.MethodMetrics
 * <p>
 * 2、可以用 * 代表贪心匹配，可以匹配多个字符。
 */
public final class PkgExpUtils {

    private PkgExpUtils() {
        //empty
    }

    public static Set<String> parse(String expStr) {
        int leftIdx = expStr.indexOf('[');
        if (leftIdx < 0) {
            return SetUtils.of(expStr);
        }

        int rightIdx = expStr.indexOf(']', leftIdx);
        if (rightIdx < 0) {
            throw new IllegalArgumentException("PkgExpUtils.parse(\"" + expStr + "\"): '[' always paired with ']'");
        }

        String prefixStr = expStr.substring(0, leftIdx);
        String suffixStr = rightIdx + 1 < expStr.length() ? expStr.substring(rightIdx + 1) : "";

        String elementsStr = expStr.substring(leftIdx + 1, rightIdx);
        List<String> elements = StrUtils.splitAsList(elementsStr, Separator.ARR_ELE);
        Set<String> result = SetUtils.createHashSet(elements.size());
        for (int i = 0; i < elements.size(); ++i) {
            String subExpStr = prefixStr.concat(elements.get(i)).concat(suffixStr);
            result.addAll(parse(subExpStr));
        }
        return result;
    }
}
