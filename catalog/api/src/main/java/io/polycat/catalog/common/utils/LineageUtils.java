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

import io.polycat.catalog.common.lineage.EDbType;
import io.polycat.catalog.common.lineage.ELineageObjectType;
import io.polycat.catalog.common.lineage.ELineageType;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;

public class LineageUtils {

    public static final char UNAME_SEP_DELIMITER = '@';
    public static final char QNAME_SEP_ENTITY_NAME = '.';
    public static final char UNAME_SEP_TYPE = ':';
    public final static String DEFAULT_CATALOG = "default_catalog";

    public final static String DEFAULT_DATABASE = "default";
    public final static String DEFAULT_TMP_TABLE_PREFIX = "Tmp_tbl_";

    private static final String NAME_PATTERN = "([^0-9][\\w]+)";
    private static final String NAME_PATTERN_COLUMN = "(.*+)";
    private static final String NAME_PATTERN_DASHED_LINE = "([^0-9-][\\w-]+)";
    private static final String REGEX_CATALOG = NAME_PATTERN_DASHED_LINE;
    private static final String REGEX_DATABASE = String.format("%s\\.%s", REGEX_CATALOG, NAME_PATTERN);
    private static final String REGEX_TABLE = String.format("%s\\.%s", REGEX_DATABASE, NAME_PATTERN);
    private static final String REGEX_COLUMN = String.format("%s\\.%s", REGEX_TABLE, NAME_PATTERN_COLUMN);

    public static String getTmpTableQualifiedName() {
        return DEFAULT_TMP_TABLE_PREFIX + System.currentTimeMillis();
    }

    public static String getDatabaseQualifiedName(String catalogName, String databaseName) {
        if (catalogName == null) {
            catalogName = DEFAULT_CATALOG;
        }
        if (databaseName == null) {
            databaseName = DEFAULT_DATABASE;
        }
        return String.format("%s%c%s", catalogName, QNAME_SEP_ENTITY_NAME, databaseName);
    }

    public static String getTableQualifiedName(String catalogName, String databaseName, String tableName) {
        return String.format("%s%c%s", getDatabaseQualifiedName(catalogName, databaseName), QNAME_SEP_ENTITY_NAME,
                tableName);
    }

    public static String getColumnQualifiedName(String catalogName, String databaseName, String tableName,
                                                String columnName) {
        return getColumnQualifiedName(getTableQualifiedName(catalogName, databaseName, tableName), columnName);
    }

    public static String getColumnQualifiedName(String tableQualifiedName, String columnName) {
        if (!columnName.matches(NAME_PATTERN)) {
            columnName = escapeCharacters(columnName);
        }
        return String.format("%s%c%s", tableQualifiedName, QNAME_SEP_ENTITY_NAME, columnName);
    }

    public static String getLineageRsUniqueName(ELineageType lineageType, Object downstream) {
        return getLineageRsUniqueName(lineageType.getNum(), downstream);
    }

    public static String getLineageRsUniqueName(Integer lineageType, Object downstream) {
        return String.format("%d%c%s", lineageType, UNAME_SEP_TYPE, String.valueOf(downstream));
    }

    public static String getLineageUniqueName(EDbType dbType, ELineageObjectType objectType, String qualifiedName) {
        return getLineageUniqueName(dbType.getNum(), objectType.getNum(), qualifiedName);
    }

    public static String getLineageUniqueName(Integer dbType, Integer objectType, String qualifiedName) {
        return String.format("%d%c%d%c%s", dbType, UNAME_SEP_TYPE, objectType, UNAME_SEP_DELIMITER, qualifiedName);
    }

    public static void checkQualifiedName(String qualifiedName, EDbType dbType, ELineageObjectType objectType) {
        boolean pass = true;
        switch (dbType) {
            case HIVE:
            case MYSQL:
            case SPARKSQL:
                pass = checkQualifiedNameHive(qualifiedName, objectType);
                break;
            default:
                break;
        }
        if (!pass) {
            throw new IllegalArgumentException(
                    "Check qualifiedName:[" + qualifiedName + "] is failed, please construct a correct name.");
        }
    }

    public static boolean checkQualifiedNameHive(String qualifiedName, ELineageObjectType objectType) {
        switch (objectType) {
            case CATALOG:
            case DATABASE:
                break;
            case TABLE:
            case VIEW:
                return qualifiedName.matches(REGEX_TABLE);
            case COLUMN:
                return qualifiedName.matches(REGEX_COLUMN);
            default:
                break;
        }
        return true;
    }

    public static String escapeCharacters(String input) {
        try {
            return URLEncoder.encode(escapeSpecialCharacters(input), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return input;
        }
    }

    public static String unescapeCharacters(String input) {
        try {
            return unescapeSpecialCharacters(URLDecoder.decode(input, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return input;
        }
    }

    public static String escapeSpecialCharacters(String input) {
        return input.replaceAll("\\.", "\\\\u002E").replaceAll("\\*", "\\\\u002A");
    }

    public static String unescapeSpecialCharacters(String input) {
        return input.replaceAll("\\\\u002E", "\\.").replaceAll("\\\\u002A", "\\*");
    }


}
