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
package io.polycat.catalog.common.types;

import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.model.record.Field;
import io.polycat.catalog.common.model.record.NullWritable;

public class DataTypes {
    public static final BooleanType BOOLEAN = new BooleanType();
    public static final TinyIntType TINYINT = new TinyIntType(DataTypeObject.TINYINT.name());;
    public static final TinyIntType BYTE = new TinyIntType(DataTypeObject.BYTE.name());
    public static final SmallIntType SMALLINT = new SmallIntType(DataTypeObject.SMALLINT.name());
    public static final SmallIntType SHORT = new SmallIntType(DataTypeObject.SHORT.name());
    public static final IntegerType INTEGER = new IntegerType(DataTypeObject.INTEGER.name());
    public static final IntegerType INT = new IntegerType(DataTypeObject.INT.name());
    public static final BigIntType BIGINT =  new BigIntType(DataTypeObject.BIGINT.name());
    public static final BigIntType LONG =  new BigIntType(DataTypeObject.LONG.name());
    public static final FloatType FLOAT = new FloatType();
    public static final DoubleType DOUBLE = new DoubleType();
    public static final DecimalType DECIMAL = new DecimalType(38, 18);
    public static final StringType STRING = new StringType(DataTypeObject.STRING.name());
    public static final StringType VARCHAR = new StringType(DataTypeObject.VARCHAR.name());
    public static final TimestampType TIMESTAMP = new TimestampType();
    public static final DateType DATE = new DateType();
    public static final BlobType BLOB = new BlobType(DataTypeObject.BLOB.name());
    public static final BlobType BINARY = new BlobType(DataTypeObject.BINARY.name());
    public static final NullType NULL = new NullType();
    public static final ObjectType OBJECT = new ObjectType();
    public static final IntervalType INTERVAL = new IntervalType();
    public static final MapAvgDataType MAP_AVG_DATA = new MapAvgDataType();

    public static final int BOOLEAN_SQL_TYPE = BOOLEAN.getSqlType();
    public static final int TINYINT_SQL_TYPE = TINYINT.getSqlType();
    public static final int SMALLINT_SQL_TYPE = SMALLINT.getSqlType();
    public static final int INTEGER_SQL_TYPE = INTEGER.getSqlType();
    public static final int BIGINT_SQL_TYPE = BIGINT.getSqlType();
    public static final int FLOAT_SQL_TYPE = FLOAT.getSqlType();
    public static final int DOUBLE_SQL_TYPE = DOUBLE.getSqlType();
    public static final int DECIMAL_SQL_TYPE = DECIMAL.getSqlType();
    public static final int STRING_SQL_TYPE = STRING.getSqlType();
    public static final int TIMESTAMP_SQL_TYPE = TIMESTAMP.getSqlType();
    public static final int DATE_SQL_TYPE = DATE.getSqlType();
    public static final int BLOB_SQL_TYPE = BLOB.getSqlType();
    public static final int NULL_SQL_TYPE = NULL.getSqlType();
    public static final int OBJECT_SQL_TYPE = OBJECT.getSqlType();
    public static final int INTERVAL_SQL_TYPE = INTERVAL.getSqlType();
    public static final int MAP_AVG_DATA_SQL_TYPE = MAP_AVG_DATA.getSqlType();

    public static boolean isIntegerType(DataType dataType) {
        return dataType == DataTypes.INTEGER || dataType == DataTypes.INT
            || dataType == DataTypes.BIGINT || dataType == DataTypes.LONG
            || dataType == DataTypes.SMALLINT || dataType == DataTypes.SHORT
            || dataType == DataTypes.TINYINT || dataType == DataTypes.BYTE;
    }

    public static boolean isFloatingPointType(DataType dataType) {
        return dataType == DataTypes.FLOAT
            || dataType == DataTypes.DOUBLE
            || dataType instanceof DecimalType;
    }

    public static boolean isNumberType(DataType dataType) {
        return isIntegerType(dataType) || isFloatingPointType(dataType);
    }

    public static DataType getHighType(DataType left, DataType right) {
        if (isIntegerType(left) && isIntegerType(right)) {
            return DataTypes.BIGINT;
        } else if (isNumberType(left) && isNumberType(right)) {
            return DataTypes.DECIMAL;
        } else {
            return left;
        }
    }

    public static DataType valueOf(String dataTypeName) {
        dataTypeName = dataTypeName.trim().toUpperCase();
        if (BOOLEAN.getName().equals(dataTypeName)) {
            return BOOLEAN;
        } else if (TINYINT.getName().equals(dataTypeName)) {
            return TINYINT;
        } else if (BYTE.getName().equals(dataTypeName)) {
            return BYTE;
        } else if (SMALLINT.getName().equals(dataTypeName)) {
            return SMALLINT;
        }else if (SHORT.getName().equals(dataTypeName)) {
            return SHORT;
        } else if (INTEGER.getName().equals(dataTypeName)) {
            return INTEGER;
        } else if (INT.getName().equals(dataTypeName)) {
            return INT;
        } else if (BIGINT.getName().equals(dataTypeName)) {
            return BIGINT;
        } else if (LONG.getName().equals(dataTypeName)) {
            return LONG;
        } else if (FLOAT.getName().equals(dataTypeName)) {
            return FLOAT;
        } else if (DOUBLE.getName().equals(dataTypeName)) {
            return DOUBLE;
        } else if (DECIMAL.getName().equals(dataTypeName)) {
            return DECIMAL;
        } else if (STRING.getName().equals(dataTypeName)) {
            return STRING;
        } else if (VARCHAR.getName().equals(dataTypeName) ||
                (dataTypeName.startsWith("VARCHAR(") && dataTypeName.endsWith(")"))) {
            return VARCHAR;
        } else if (TIMESTAMP.getName().equals(dataTypeName)) {
            return TIMESTAMP;
        } else if (DATE.getName().equals(dataTypeName)) {
            return DATE;
        } else if (BLOB.getName().equals(dataTypeName)) {
            return BLOB;
        } else if (BINARY.getName().equals(dataTypeName)) {
            return BINARY;
        } else if (NULL.getName().equals(dataTypeName)) {
            return NULL;
        } else if (OBJECT.getName().equals(dataTypeName)) {
            return OBJECT;
        } else if (INTERVAL.getName().equals(dataTypeName)) {
            return INTERVAL;
        } else if (dataTypeName.trim().startsWith("DECIMAL(") && dataTypeName.trim().endsWith(")")) {
            return parseDecimal(dataTypeName);
        } else if (dataTypeName.trim().startsWith("ARRAY<") && dataTypeName.trim().endsWith(">")) {
            return parseArray(dataTypeName);
        } else if (dataTypeName.trim().startsWith("MAP<") && dataTypeName.trim().endsWith(">")) {
            return parseMap(dataTypeName);
        } else if (dataTypeName.trim().startsWith("STRUCT<") && dataTypeName.trim().endsWith(">")) {
            // Example for parsing STRUCT: STRUCT<SELECT:STRING,DMP:STRING,PROFILE:STRING,OTHER:STRING,TYPE:INT>
            return new StructType(dataTypeName);
        } else if (dataTypeName.startsWith("VARCHAR(") && dataTypeName.endsWith(")")) {
            return parseVarcharWithLen(dataTypeName);
        }
        throw new IllegalArgumentException("Invalid data type: " + dataTypeName);
    }

    private static DataType parseVarcharWithLen(String dataTypeName) {
        int i = dataTypeName.indexOf("(");
        int k = dataTypeName.indexOf(")");
        if (k != dataTypeName.length() - 1) {
            throw new IllegalArgumentException("Invalid sql type: " + dataTypeName);
        }
        String scaleStr = dataTypeName.substring(i + 1, k);
        int scale = Integer.parseInt(scaleStr);
        return new VarcharType(scale);
    }

    private static DataType parseDecimal(String dataTypeName) {
        int i = dataTypeName.indexOf("(");
        int j = dataTypeName.indexOf(",");
        int k = dataTypeName.indexOf(")");
        if (k != dataTypeName.length() - 1) {
            throw new IllegalArgumentException("Invalid sql type: " + dataTypeName);
        }
        // Example for parsing decimal: Decimal(1,2)
        // i = 7, j = 9, k = 11
        // the presion and scale in this example will be at index 8 and 10
        // therefore the below code takes substring from 8 to 9 and 10 to 11 respectively to get presision and scale.
        String precisionStr = dataTypeName.substring(i + 1, j);
        String scaleStr = dataTypeName.substring(j + 1, k);

        int precision = Integer.parseInt(precisionStr);
        int scale = Integer.parseInt(scaleStr);

        return new DecimalType(precision, scale);
    }

    private static DataType parseArray(String dataTypeName) {
        int i = dataTypeName.indexOf("<");
        int k = dataTypeName.lastIndexOf(">");
        if (k != dataTypeName.length() - 1) {
            throw new IllegalArgumentException("Invalid sql type: " + dataTypeName);
        }
        // Example for parsing decimal: Array<string>
        DataType elementType = DataTypes.valueOf(dataTypeName.substring(i + 1, k));
        return new ArrayType(elementType);
    }

    private static DataType parseMap(String dataTypeName) {
        int i = dataTypeName.indexOf("<");
        int j = dataTypeName.indexOf(",");
        int k = dataTypeName.lastIndexOf(">");
        if (k != dataTypeName.length() - 1) {
            throw new IllegalArgumentException("Invalid sql type: " + dataTypeName);
        }

        // Example for parsing decimal: Map<string, int>
        String keyType = dataTypeName.substring(i + 1, j);
        String valueType = dataTypeName.substring(j + 1, k);

        return new MapType(DataTypes.valueOf(keyType), DataTypes.valueOf(valueType));
    }



    public static DataType valueOf(int sqlType) {
        if (sqlType == BOOLEAN.getSqlType()) {
            return BOOLEAN;
        } else if (sqlType == TINYINT.getSqlType()) {
            return TINYINT;
        } else if (sqlType == SMALLINT.getSqlType()) {
            return SMALLINT;
        } else if (sqlType == INTEGER.getSqlType()) {
            return INTEGER;
        } else if (sqlType == BIGINT.getSqlType()) {
            return BIGINT;
        } else if (sqlType == FLOAT.getSqlType()) {
            return FLOAT;
        } else if (sqlType == DOUBLE.getSqlType()) {
            return DOUBLE;
        } else if (sqlType == DECIMAL.getSqlType()) {
            return DECIMAL;
        } else if (sqlType == STRING.getSqlType()) {
            return STRING;
        } else if (sqlType == TIMESTAMP.getSqlType()) {
            return TIMESTAMP;
        } else if (sqlType == DATE.getSqlType()) {
            return DATE;
        } else if (sqlType == BLOB.getSqlType()) {
            return BLOB;
        } else if (sqlType == NULL.getSqlType()) {
            return NULL;
        } else if (sqlType == OBJECT.getSqlType()) {
            return OBJECT;
        } else if (sqlType == INTERVAL.getSqlType()) {
            return INTERVAL;
        }
        throw new CatalogException("Invalid sql type: " + sqlType);
    }

    public static DecimalType createDecimalType(int precision, int scale) {
        return new DecimalType(precision, scale);
    }

    public static DecimalType createDefaultDecimalType() {
        return new DecimalType(38, 18);
    }

    public static boolean isDecimal(DataType dataType) {
        return dataType.getSqlType() == DECIMAL.getSqlType();
    }


    public static Field createEmptyField(int sqlType) {
        if (sqlType == DataTypes.NULL.getSqlType()) {
            return NullWritable.getInstance();
        }
        return valueOf(sqlType).createEmptyField();
    }

    public static boolean isArray(DataType dataType) {
        return dataType.getSqlType() == ArrayType.ARRAY.getSqlType();
    }
}
