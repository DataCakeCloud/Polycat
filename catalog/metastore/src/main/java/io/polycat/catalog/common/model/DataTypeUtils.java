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
package io.polycat.catalog.common.model;

import io.polycat.catalog.common.model.StatsTransform.BlobTransform;
import io.polycat.catalog.common.model.StatsTransform.BooleanTransform;
import io.polycat.catalog.common.model.StatsTransform.ByteTransform;
import io.polycat.catalog.common.model.StatsTransform.DateTransform;
import io.polycat.catalog.common.model.StatsTransform.DecimalTransform;
import io.polycat.catalog.common.model.StatsTransform.DoubleTransform;
import io.polycat.catalog.common.model.StatsTransform.FloatTransform;
import io.polycat.catalog.common.model.StatsTransform.IntTransform;
import io.polycat.catalog.common.model.StatsTransform.LongTransform;
import io.polycat.catalog.common.model.StatsTransform.NullTransform;
import io.polycat.catalog.common.model.StatsTransform.ShortTransform;
import io.polycat.catalog.common.model.StatsTransform.StringTransform;
import io.polycat.catalog.common.model.StatsTransform.TimestampTransform;
import io.polycat.catalog.common.model.record.BlobWritable;
import io.polycat.catalog.common.model.record.BooleanWritable;
import io.polycat.catalog.common.model.record.ByteWritable;
import io.polycat.catalog.common.model.record.DateWritable;
import io.polycat.catalog.common.model.record.DecimalWritable;
import io.polycat.catalog.common.model.record.DoubleWritable;
import io.polycat.catalog.common.model.record.Field;
import io.polycat.catalog.common.model.record.FloatWritable;
import io.polycat.catalog.common.model.record.IntWritable;
import io.polycat.catalog.common.model.record.LongWritable;
import io.polycat.catalog.common.model.record.ShortWritable;
import io.polycat.catalog.common.model.record.StringWritable;
import io.polycat.catalog.common.model.record.TimestampWritable;
import io.polycat.catalog.common.types.DataType;
import io.polycat.catalog.common.types.DataTypes;



public class DataTypeUtils {

    public static Field toField(DataType dataType) {
        if (dataType.getName().equals(DataTypes.BOOLEAN.getName())) {
            return new BooleanWritable();
        } else if (dataType.getName().equals(DataTypes.BYTE.getName())) {
            return new ByteWritable();
        } else if (dataType.getName().equals(DataTypes.SHORT.getName())) {
            return new ShortWritable();
        } else if (dataType.getName().equals(DataTypes.INT.getName())) {
            return new IntWritable();
        } else if (dataType.getName().equals(DataTypes.LONG.getName())) {
            return new LongWritable();
        } else if (dataType.getName().equals(DataTypes.FLOAT.getName())) {
            return new FloatWritable();
        } else if (dataType.getName().equals(DataTypes.DOUBLE.getName())) {
            return new DoubleWritable();
        } else if (dataType.getName().equals(DataTypes.DECIMAL.getName())) {
            return new DecimalWritable();
        } else if (dataType.getName().equals(DataTypes.TIMESTAMP.getName())) {
            return new TimestampWritable();
        } else if (dataType.getName().equals(DataTypes.DATE.getName())) {
            return new DateWritable();
        } else if (dataType.getName().equals(DataTypes.STRING.getName())
            || dataType.getName().equals(DataTypes.VARCHAR.getName())) {
            return new StringWritable();
        } else if (dataType.getName().equals(DataTypes.BINARY.getName())) {
            return new BlobWritable();
        } else {
            throw new IllegalStateException("Unexpected value: " + dataType);
        }
    }

   public static StatsTransform createTransform(Field field) {
        int sqlType = field.getType().getSqlType();
        if (sqlType == DataTypes.BOOLEAN_SQL_TYPE) {
            return new BooleanTransform(field);
        } else if (sqlType == DataTypes.TINYINT_SQL_TYPE) {
            return new ByteTransform(field);
        } else if (sqlType == DataTypes.SMALLINT_SQL_TYPE) {
            return new ShortTransform(field);
        } else if (sqlType == DataTypes.INTEGER_SQL_TYPE) {
            return new IntTransform(field);
        } else if (sqlType == DataTypes.BIGINT_SQL_TYPE) {
            return new LongTransform(field);
        } else if (sqlType == DataTypes.FLOAT_SQL_TYPE) {
            return new FloatTransform(field);
        } else if (sqlType == DataTypes.DOUBLE_SQL_TYPE) {
            return new DoubleTransform(field);
        } else if (sqlType == DataTypes.DECIMAL_SQL_TYPE) {
            return new DecimalTransform(field);
        } else if (sqlType == DataTypes.STRING_SQL_TYPE) {
            return new StringTransform(field);
        } else if (sqlType == DataTypes.TIMESTAMP_SQL_TYPE) {
            return new TimestampTransform(field);
        } else if (sqlType == DataTypes.DATE_SQL_TYPE) {
            return new DateTransform(field);
        } else if (sqlType == DataTypes.BLOB_SQL_TYPE) {
            return new BlobTransform(field);
        } else if (sqlType == DataTypes.NULL_SQL_TYPE) {
            return new NullTransform(field);
        }
        throw new IllegalStateException("Unexpected value: " + field.getType());
    }
}
