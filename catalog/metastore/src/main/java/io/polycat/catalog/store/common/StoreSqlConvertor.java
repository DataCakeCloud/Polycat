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
package io.polycat.catalog.store.common;

import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StoreSqlConvertor {

    private static final Pattern HUMP_FLAG_PATTERN = Pattern.compile("[A-Z]");
    private static final Pattern UNDERLINE_FLAG_PATTERN = Pattern.compile("_[a-z]");
    private static final String SPACE = " ";
    private static final String APOSTROPHE = "'";
    private static final String LIKE_PLACEHOLDER = "%";

    private boolean existsCondition = false;
    private boolean previousConditionEstablish = false;

    private StringBuffer stringBuffer = new StringBuffer(" ");

    public String getFilterSql() {
        if (!existsCondition || !previousConditionEstablish) {
            this.stringBuffer.append(" 1=1 ");
        }
        return this.stringBuffer.toString();
    }

    public StoreSqlConvertor equals(String field, Object value) {
        appendConditional(field, value, "=");
        return this;
    }

    public StoreSqlConvertor greaterThanOrEquals(String field, Object value) {
        appendConditional(field, value, ">=");
        return this;
    }

    public StoreSqlConvertor greaterThan(String field, Object value) {
        appendConditional(field, value, ">");
        return this;
    }

    public StoreSqlConvertor lessThanOrEquals(String field, Object value) {
        appendConditional(field, value, "<=");
        return this;
    }

    public StoreSqlConvertor lessThan(String field, Object value) {
        appendConditional(field, value, "<");
        return this;
    }

    public <T> StoreSqlConvertor in(String field, Collection<T> value) {
        appendConditional(field, value, "IN");
        return this;
    }

    public <T> StoreSqlConvertor notIn(String field, Collection<T> value) {
        appendConditional(field, value, "NOT IN");
        return this;
    }

    public StoreSqlConvertor likeLeft(String field, String value) {
        if (!StringUtils.isBlank(value)) {
            value += LIKE_PLACEHOLDER + value;
        } else {
            value = null;
        }
        appendConditional(field, value, "LIKE");
        return this;
    }

    public StoreSqlConvertor likeRight(String field, String value) {
        if (!StringUtils.isBlank(value)) {
            value = value + LIKE_PLACEHOLDER;
        } else {
            value = null;
        }
        appendConditional(field, value, "LIKE");
        return this;
    }

    public StoreSqlConvertor like(String field, String value) {
        if (!StringUtils.isBlank(value)) {
            value = LIKE_PLACEHOLDER + value + LIKE_PLACEHOLDER;
        } else {
            value = null;
        }
        appendConditional(field, value, "LIKE");
        return this;
    }

    public StoreSqlConvertor likeSpec(String field, String value) {
        if (StringUtils.isBlank(value)) {
            value = null;
        }
        appendConditional(field, value, "LIKE");
        return this;
    }

    public StoreSqlConvertor AND() {
        if (previousConditionEstablish) {
            stringBuffer.append(SPACE).append("AND").append(SPACE);
        }
        return this;
    }

    public StoreSqlConvertor OR() {
        if (previousConditionEstablish) {
            stringBuffer.append(SPACE).append("OR").append(SPACE);
        }
        return this;
    }

    private void appendConditional(String field, Object value, String conditionalSymbol) {
        if (value != null) {
            if (value instanceof String) {
                stringBuffer.append(SPACE).append(hump2underline(field)).append(SPACE).append(conditionalSymbol).append(SPACE).append(APOSTROPHE).append(value).append(APOSTROPHE);
            } else if (value instanceof Collection) {
                if (!((Collection)value).isEmpty()) {
                    Set<String> set = new HashSet<>();
                    for (Object o : ((Collection) value)) {
                        if (o instanceof String) {
                            set.add(APOSTROPHE + (String)o + APOSTROPHE);
                        } else {
                            set.add((String)o);
                        }
                    }
                    String join = String.join(",", set);
                    this.stringBuffer.append(SPACE).append(hump2underline(field)).append(SPACE).append(conditionalSymbol).append(" (").append(join).append(") ");
                } else {
                    previousConditionEstablish = false;
                    return;
                }
            } else {
                stringBuffer.append(SPACE).append(hump2underline(field)).append(SPACE).append(conditionalSymbol).append(SPACE).append(value);
            }
            previousConditionEstablish = true;
            existsCondition = true;
            return;
        }
        previousConditionEstablish = false;
    }

    public static StoreSqlConvertor get() {
        return new StoreSqlConvertor();
    }

    private static String hump2underline(String str) {
        Matcher matcher = HUMP_FLAG_PATTERN.matcher(str);
        StringBuffer sb = new StringBuffer();
        while(matcher.find()) {
            matcher.appendReplacement(sb,  "_" + matcher.group(0).toLowerCase());
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    private static String underline2hump(String str) {
        str = str.toLowerCase();
        Matcher matcher = UNDERLINE_FLAG_PATTERN.matcher(str);
        StringBuffer sb = new StringBuffer();
        while(matcher.find()) {
            matcher.appendReplacement(sb,  matcher.group(0).toUpperCase().replace("_",""));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

}
