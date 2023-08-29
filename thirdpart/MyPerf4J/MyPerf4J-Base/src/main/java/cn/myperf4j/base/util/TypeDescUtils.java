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

import java.lang.reflect.Method;

/**
 * Created by LinShunkang on 2018/9/6
 */
public final class TypeDescUtils {

    private static final ThreadLocal<StringBuilder> SB_TL = new ThreadLocal<StringBuilder>() {
        @Override
        protected StringBuilder initialValue() {
            return new StringBuilder(128);
        }
    };

    //类型􏰀述符
    private static final char[] TYPE_DESCRIPTOR = {'Z', 'C', 'B', 'S', 'I', 'F', 'J', 'D', '[', 'L'};

    //Java 类型
    private static final String[] JAVA_TYPE_DESC = {"boolean", "char", "byte", "short", "int", "float",
            "long", "double", "[]", "Object"};

    private static final byte[] TYPE_DESCRIPTOR_BIT_MAP = new byte[128];

    private static final String[] JAVA_TYPE_DESC_MAP = new String[128];

    static {
        for (int i = 0; i < TYPE_DESCRIPTOR.length; ++i) {
            char ch = TYPE_DESCRIPTOR[i];
            TYPE_DESCRIPTOR_BIT_MAP[ch] = 1;
            JAVA_TYPE_DESC_MAP[ch] = JAVA_TYPE_DESC[i];
        }
    }

    private TypeDescUtils() {
        //empty
    }

    /**
     * (IF)V -> int, float
     * (Ljava/lang/Object;)I -> Object
     * (ILjava/lang/String;)[I ->  int, String
     * ([I)Ljava/lang/Object; -> int[]
     *
     * @param descriptor: 方法描述符
     * @return : 源文件中的方法声明的参数部分
     */

    public static String getMethodParamsDesc(String descriptor) {
        descriptor = descriptor.trim();

        int roundTimes = 0;
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < descriptor.length() && roundTimes++ <= descriptor.length(); ) {
            char ch = descriptor.charAt(i);
            if (ch == '(') {
                ++i;
                continue;
            } else if (ch == ')') {
                break;
            }

            if (isTypeDescriptor(ch)) {
                i = processDescriptor(descriptor, i, sb);
                if (i < 0) {
                    return descriptor;
                }
            } else {
                Logger.error("TypeDescUtils.processDescriptor(" + descriptor + ") Should never arrive here!!! 0");
                return descriptor;
            }
        }

        if (sb.length() < 2) {
            sb.append(')');
            return sb.toString();
        }

        sb.replace(sb.length() - 2, sb.length() - 1, ")");
        return sb.substring(0, sb.length() - 1);
    }

    private static boolean isTypeDescriptor(char ch) {
        return TYPE_DESCRIPTOR_BIT_MAP[ch] == 1;
    }

    private static int processDescriptor(String descriptor, int idx, StringBuilder sb) {
        int startIdx = idx;
        boolean isObjectDesc = false;
        int arrayLevel = 0;
        int roundTimes = 0;
        for (; idx < descriptor.length() && roundTimes++ <= descriptor.length(); ++idx) {
            char ch = descriptor.charAt(idx);
            if (ch == '[') {
                ++arrayLevel;
            } else if (ch == 'L') {
                isObjectDesc = true;
            } else if (ch == ';') {
                sb.append(getSimpleClassName(descriptor, startIdx, idx));
                appendArrDesc(sb, arrayLevel);
                sb.append(", ");
                return idx + 1;
            } else if (isTypeDescriptor(ch) && !isObjectDesc) {
                sb.append(JAVA_TYPE_DESC_MAP[ch]);
                appendArrDesc(sb, arrayLevel);
                sb.append(", ");
                return idx + 1;
            } else if (ch == ')') {
                //理论上永远走不到这里
                Logger.error("TypeDescUtils.processDescriptor(" + descriptor + ", " + idx + ", " + sb
                        + ") Should never arrive here!!! 1");
                return -1;
            }
        }

        //理论上永远走不到这里
        Logger.error("TypeDescUtils.processDescriptor(" + descriptor + ", " + idx + ", " + sb
                + ") Should never arrive here!!! 2");
        return -1;
    }

    private static String getSimpleClassName(String descriptor, int startIdx, int endIdx) {
        int lastIdx = startIdx;
        for (int i = endIdx; i >= startIdx; --i) {
            if (descriptor.charAt(i) == '/') {
                lastIdx = i;
                break;
            }
        }
        return descriptor.substring(lastIdx + 1, endIdx);
    }

    private static void appendArrDesc(StringBuilder sb, int arrayLevel) {
        for (int k = 0; k < arrayLevel; ++k) {
            sb.append("[]");
        }
    }

    public static String getMethodParamsDesc(Method method) {
        Class<?>[] parameterTypes = method.getParameterTypes();
        if (parameterTypes.length <= 0) {
            return "()";
        }

        StringBuilder sb = SB_TL.get();
        try {
            sb.append('(');
            int i = 0;
            for (; i < parameterTypes.length - 1; ++i) {
                sb.append(parameterTypes[i].getSimpleName()).append(", ");
            }
            sb.append(parameterTypes[i].getSimpleName()).append(')');

            return sb.toString();
        } finally {
            sb.setLength(0);
        }
    }

    /**
     * @param innerClassName : 形如：java/lang/String
     */
    public static String getSimpleClassName(String innerClassName) {
        int idx = innerClassName.lastIndexOf('/');
        return innerClassName.substring(idx + 1);
    }
}
