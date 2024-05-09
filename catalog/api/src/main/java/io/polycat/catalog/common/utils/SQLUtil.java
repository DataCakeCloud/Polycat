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
package io.polycat.catalog.common.utils;

import org.apache.hadoop.hive.common.FileUtils;

public class SQLUtil {

    public static final String SINGLE_QUOTES = "'";
    public static final String DOUBLE_QUOTES = "\"";
    public static final String OPERATOR_LIKE = "%";
    public static final String HIVE_WILDCARD_IDENTIFIER = ".*";
    public static final String HIVE_WILDCARD_IDENTIFIER_REGEX = "\\.\\*";

    public static String likeEscapeForHiveQuotesValue(String quotesValue) {
        if (containSqlStringQuotes(quotesValue)) {
            char quotes = quotesValue.charAt(0);
            return quotes + likeEscapeForHive(parseSqlQuotesValue(quotesValue)) + quotes;
        }
        return likeEscapeForHive(quotesValue);
    }

    public static String escapeForHiveQuotesValue(String quotesValue) {
        if (containSqlStringQuotes(quotesValue)) {
            char quotes = quotesValue.charAt(0);
            return quotes + escapeSqlValueForHive(parseSqlQuotesValue(quotesValue)) + quotes;
        }
        return escapeSqlValueForHive(quotesValue);
    }

        /**
         * Obtain hive type path wildcard escape to RDS , Allow multiple wildcards to merge into one
         * @param value apostrophe
         * @return
         */
    public static String likeEscapeForHive(String value) {
        if (value != null) {
            if (value.contains(HIVE_WILDCARD_IDENTIFIER)) {
                StringBuilder res = new StringBuilder();
                String[] split = value.split(HIVE_WILDCARD_IDENTIFIER_REGEX);
                boolean tail = false;
                for (int i = 0; i < split.length; i++) {
                    if (i == 0 && split[i].length() == 0) {
                        res.append(OPERATOR_LIKE).append(getRdsWildcardEscape(split[0]));
                    } else {
                        res.append(getRdsWildcardEscape(split[i])).append(OPERATOR_LIKE);
                        if (i == split.length - 1) {
                            tail = true;
                        }
                    }
                }
                if (tail) {
                    res.deleteCharAt(res.length() - 1);
                }
                if (value.endsWith(HIVE_WILDCARD_IDENTIFIER)) {
                    res.append(OPERATOR_LIKE);
                }
                return res.toString();
            }
            return getRdsWildcardEscape(value);
        }
        return null;
    }

    public static String escapeSqlValueForHive(String s) {
        if (s != null && !"".equals(s)) {
            return escapeSqlValueForHive(s, null, null);
        }
        return s;
    }

    public static String escapeSqlValueForHive(String s, String wildcard, String wildcardReplaceValue) {
        if (s != null && !"".equals(s)) {
            if (wildcard == null || "".equals(wildcard)) {
                return FileUtils.escapePathName(s, "");
            }
            return FileUtils.escapePathName(s, "").replaceAll(wildcard, wildcardReplaceValue);
        }
        return s;
    }

    public static String getRdsWildcardEscape(String s) {
        if (s != null && !"".equals(s)) {
            return escapeSqlValueForHive(s, OPERATOR_LIKE, "\\\\%");
        }
        return s;
    }

    /**
     * parse sql value inside quotes
     * @param s
     * @return
     */
    public static String parseSqlQuotesValue(String s) {
        if (containSqlStringQuotes(s)) {
            return new StringBuffer(s).deleteCharAt(s.length() - 1).delete(0, 1).toString();
        }
        return s;
    }

    /**
     * Whether to include SQL string quotes
     * @param s
     * @return boolean
     */
    public static boolean containSqlStringQuotes(String s) {
        if (s != null) {
            return s.startsWith(SINGLE_QUOTES) && s.endsWith(SINGLE_QUOTES)
                    || s.startsWith(DOUBLE_QUOTES) && s.endsWith(DOUBLE_QUOTES);
        }
        return false;
    }
}
