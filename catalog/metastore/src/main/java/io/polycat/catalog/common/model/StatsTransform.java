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

import io.polycat.catalog.common.model.record.Field;

import com.google.protobuf.ByteString;

public abstract class StatsTransform {

    final Field field;

    StatsTransform(Field field) {
        this.field = field;
    }

    public abstract void transform(ByteString value);

    public abstract ByteString getValue();

    static class BlobTransform extends StatsTransform {

        BlobTransform(Field field) {
            super(field);
        }

        @Override
        public void transform(ByteString value) {
            field.setValue(ByteStringUtils.toBlob(value));
        }

        @Override
        public ByteString getValue() {
            return ByteStringUtils.fromBlob(field.getBlob());
        }
    }

    static class BooleanTransform extends StatsTransform {

        BooleanTransform(Field field) {
            super(field);
        }

        @Override
        public void transform(ByteString value) {
            field.setValue(ByteStringUtils.toBoolean(value));
        }

        @Override
        public ByteString getValue() {
            return ByteStringUtils.fromBoolean(field.getBoolean());
        }
    }

    static class ByteTransform extends StatsTransform {

        ByteTransform(Field field) {
            super(field);
        }

        @Override
        public void transform(ByteString value) {
            field.setValue(ByteStringUtils.toByte(value));
        }

        @Override
        public ByteString getValue() {
            return ByteStringUtils.fromByte(field.getByte());
        }
    }

    static class DateTransform extends StatsTransform {

        DateTransform(Field field) {
            super(field);
        }

        @Override
        public void transform(ByteString value) {
            field.setValue(ByteStringUtils.toDate(value));
        }

        @Override
        public ByteString getValue() {
            return ByteStringUtils.fromLong(field.getLong());
        }
    }

    static class DecimalTransform extends StatsTransform {

        DecimalTransform(Field field) {
            super(field);
        }

        @Override
        public void transform(ByteString value) {
            field.setValue(ByteStringUtils.toDecimal(value));
        }

        @Override
        public ByteString getValue() {
            return ByteStringUtils.fromDecimal(field.getDecimal());
        }
    }

    static class DoubleTransform extends StatsTransform {

        DoubleTransform(Field field) {
            super(field);
        }

        @Override
        public void transform(ByteString value) {
            field.setValue(ByteStringUtils.toDouble(value));
        }

        @Override
        public ByteString getValue() {
            return ByteStringUtils.fromDouble(field.getDouble());
        }
    }

    static class FloatTransform extends StatsTransform {

        FloatTransform(Field field) {
            super(field);
        }

        @Override
        public void transform(ByteString value) {
            field.setValue(ByteStringUtils.toFloat(value));
        }

        @Override
        public ByteString getValue() {
            return ByteStringUtils.fromFloat(field.getFloat());
        }
    }

    static class IntTransform extends StatsTransform {

        IntTransform(Field field) {
            super(field);
        }

        @Override
        public void transform(ByteString value) {
            field.setValue(ByteStringUtils.toInt(value));
        }

        @Override
        public ByteString getValue() {
            return ByteStringUtils.fromInt(field.getInteger());
        }
    }

    static class LongTransform extends StatsTransform {

        LongTransform(Field field) {
            super(field);
        }

        @Override
        public void transform(ByteString value) {
            field.setValue(ByteStringUtils.toLong(value));
        }

        @Override
        public ByteString getValue() {
            return ByteStringUtils.fromLong(field.getLong());
        }
    }

    static class MapAvgTransform extends StatsTransform {

        MapAvgTransform(Field field) {
            super(field);
        }

        @Override
        public void transform(ByteString value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ByteString getValue() {
            throw new UnsupportedOperationException();
        }
    }

    static class NullTransform extends StatsTransform {

        NullTransform(Field field) {
            super(field);
        }

        @Override
        public void transform(ByteString value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ByteString getValue() {
            throw new UnsupportedOperationException();
        }
    }

    static class ShortTransform extends StatsTransform {

        ShortTransform(Field field) {
            super(field);
        }

        @Override
        public void transform(ByteString value) {
            field.setValue(ByteStringUtils.toShort(value));
        }

        @Override
        public ByteString getValue() {
            return ByteStringUtils.fromShort(field.getShort());
        }
    }

    static class StringTransform extends StatsTransform {

        StringTransform(Field field) {
            super(field);
        }

        @Override
        public void transform(ByteString value) {
            field.setValues(ByteStringUtils.toStringUtf8(value));
        }

        @Override
        public ByteString getValue() {
            return ByteStringUtils.fromStringUtf8(field.getString());
        }
    }

    static class TimestampTransform extends StatsTransform {

        TimestampTransform(Field field) {
            super(field);
        }

        @Override
        public void transform(ByteString value) {
            field.setValue(ByteStringUtils.toTimestamp(value));
        }

        @Override
        public ByteString getValue() {
            return ByteStringUtils.fromLong(field.getLong());
        }
    }
}
