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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * Created by LinShunkang on 2018/9/6
 */
public final class LineProtocolUtils {

    private static final Pattern SPACE_PATTERN = Pattern.compile(" ");

    private static final Pattern COMMA_PATTERN = Pattern.compile(",");

    private static final Pattern EQUAL_SIGN_PATTERN = Pattern.compile("=");

    private static final Map<String, String> methodNameMap = new ConcurrentHashMap<>(1024);

    private LineProtocolUtils() {
        //empty
    }

    /**
     * 用于把tagOrField里的 ','  ' '  '=' 转义为符合LineProtocol的格式
     *
     * @param tagOrField : tag key、tag value、field key
     * @return: 符合LineProtocol格式的文本
     */
    public static String processTagOrField(String tagOrField) {
        String lineProtocol = methodNameMap.get(tagOrField);
        if (lineProtocol != null) {
            return lineProtocol;
        }

        lineProtocol = SPACE_PATTERN.matcher(tagOrField).replaceAll("\\\\ ");
        lineProtocol = COMMA_PATTERN.matcher(lineProtocol).replaceAll("\\\\,");
        lineProtocol = EQUAL_SIGN_PATTERN.matcher(lineProtocol).replaceAll("\\\\=");
        methodNameMap.put(tagOrField, lineProtocol);
        return lineProtocol;
    }
}
