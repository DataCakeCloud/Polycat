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
package io.polycat.catalog.common.model.stats;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public enum ColumnStatisticsDataType {

    /**
     * boolean statistics data type
     */
    BOOLEAN_STATS((short)1, BooleanColumnStatsData.class, "booleanStats", new HashSet<String>(Arrays.asList("boolean"))),
    LONG_STATS((short)2, LongColumnStatsData.class, "longStats", new HashSet<String>(Arrays.asList("bigint", "int", "smallint", "tinyint", "timestamp"))),
    DOUBLE_STATS((short)3, DoubleColumnStatsData.class, "doubleStats", new HashSet<String>(Arrays.asList("double", "float"))),
    STRING_STATS((short)4, StringColumnStatsData.class, "stringStats", new HashSet<String>(Arrays.asList("string", "varchar", "char"))),
    BINARY_STATS((short)5, BinaryColumnStatsData.class, "binaryStats", new HashSet<String>(Arrays.asList("binary"))),
    DECIMAL_STATS((short)6, DecimalColumnStatsData.class, "decimalStats", new HashSet<String>(Arrays.asList("decimal"))),
    DATE_STATS((short)7, DateColumnStatsData.class, "dateStats", new HashSet<String>(Arrays.asList("date")));
    private final short typeId;
    private final Class<?> typeClass;
    private final String typeName;
    private final Set<String> dataTypeSet;
    private static final Map<String, ColumnStatisticsDataType> byNameMap = new HashMap<String, ColumnStatisticsDataType>();
    private static final Map<String, ColumnStatisticsDataType> dataTypeSetMap = new HashMap<String, ColumnStatisticsDataType>();

    static {
        for (ColumnStatisticsDataType statisticsDataType : ColumnStatisticsDataType.values()) {
            byNameMap.put(statisticsDataType.typeName, statisticsDataType);
            for (String type: statisticsDataType.dataTypeSet) {
                dataTypeSetMap.put(type, statisticsDataType);
            }
        }
    }

    public static ColumnStatisticsDataType findType(int typeId) {
        switch(typeId) {
            case 1:
                return BOOLEAN_STATS;
            case 2:
                return LONG_STATS;
            case 3:
                return DOUBLE_STATS;
            case 4:
                return STRING_STATS;
            case 5:
                return BINARY_STATS;
            case 6:
                return DECIMAL_STATS;
            case 7:
                return DATE_STATS;
            default:
                return null;
        }
    }

    public static ColumnStatisticsDataType findByName(String name) {
        ColumnStatisticsDataType statisticsDataType = byNameMap.get(name);
        if (statisticsDataType == null) {
            throw new IllegalArgumentException("Field statistic type " + statisticsDataType + " doesn't exist!");
        }
        return statisticsDataType;
    }

    public static ColumnStatisticsDataType findByColumnType(String colType) {
        if (colType != null) {
            colType = colType.toLowerCase();
            if (colType.contains("(")) {
                colType = parseDataType(colType);
            }
        }
        ColumnStatisticsDataType statisticsDataType = dataTypeSetMap.get(colType);
        if (statisticsDataType == null) {
            throw new IllegalArgumentException("Column type [" + colType + "] no matching statistic type!");
        }
        return statisticsDataType;
    }

    private static String parseDataType(String colType) {
        if (colType != null) {
            colType = colType.toLowerCase();
            if (colType.contains("(")) {
                int i = colType.indexOf("(");
                colType = colType.substring(0, i).trim();
            }
        }
        return colType;
    }

    public static ColumnStatisticsDataType findByThriftIdOrThrow(int fieldId) {
        ColumnStatisticsDataType statisticsDataType = findType(fieldId);
        if (statisticsDataType == null) {
            throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
        }
        return statisticsDataType;
    }

    ColumnStatisticsDataType(short typeId, Class<?> typeClass, String typeName, Set<String> dataTypeSet) {
        this.typeId = typeId;
        this.typeClass = typeClass;
        this.typeName = typeName;
        this.dataTypeSet = dataTypeSet;
    }

    public short getTypeId() {
        return typeId;
    }

    public String getTypeName() {
        return typeName;
    }

    public Set<String> getDataTypeSet() {
        return dataTypeSet;
    }

    public Class<?> getTypeClass() {
        return typeClass;
    }
}
