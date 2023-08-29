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
package io.polycat.common.index.record;

import io.polycat.catalog.common.types.DataTypes;
import io.polycat.common.index.record.StatsTransform.BlobTransform;
import io.polycat.common.index.record.StatsTransform.BooleanTransform;
import io.polycat.common.index.record.StatsTransform.ByteTransform;
import io.polycat.common.index.record.StatsTransform.DateTransform;
import io.polycat.common.index.record.StatsTransform.DecimalTransform;
import io.polycat.common.index.record.StatsTransform.DoubleTransform;
import io.polycat.common.index.record.StatsTransform.FloatTransform;
import io.polycat.common.index.record.StatsTransform.IntTransform;
import io.polycat.common.index.record.StatsTransform.LongTransform;
import io.polycat.common.index.record.StatsTransform.NullTransform;
import io.polycat.common.index.record.StatsTransform.ShortTransform;
import io.polycat.common.index.record.StatsTransform.StringTransform;
import io.polycat.common.index.record.StatsTransform.TimestampTransform;
import io.polycat.catalog.common.model.record.Field;


public class DataTypeUtils {

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
