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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;

import com.google.protobuf.ByteString;
import io.polycat.catalog.common.model.record.Record;
import io.polycat.catalog.common.utils.CodecUtil;

public class ByteStringUtils {

    public static ByteString fromBlob(byte[] value) {
        return ByteString.copyFrom(value);
    }

    public static byte[] toBlob(ByteString value) {
        return value.toByteArray();
    }

    public static ByteString fromBoolean(boolean value) {
        return fromBlob(ByteUtils.fromBoolean(value));
    }

    public static boolean toBoolean(ByteString value) {
        return ByteUtils.toBoolean(value.toByteArray());
    }

    public static ByteString fromByte(byte value) {
        return fromBlob(new byte[] {value});
    }

    public static byte toByte(ByteString value) {
        return value.byteAt(0);
    }

    public static ByteString fromDate(Date value) {
        return fromLong(value.getTime());
    }

    public static long toDate(ByteString value) {
        return toLong(value);
    }

    public static ByteString fromDecimal(BigDecimal value) {
        return fromStringUtf8(value.toString());
    }

    public static BigDecimal toDecimal(ByteString value) {
        return new BigDecimal(toStringUtf8(value));
    }

    public static ByteString fromDouble(double value) {
        return ByteString.copyFrom(ByteBuffer.allocate(8).putDouble(value));
    }

    public static double toDouble(ByteString value) {
        return value.asReadOnlyByteBuffer().getDouble();
    }

    public static ByteString fromFloat(float value) {
        return ByteString.copyFrom(ByteBuffer.allocate(4).putFloat(value));
    }

    public static float toFloat(ByteString value) {
        return value.asReadOnlyByteBuffer().getFloat();
    }

    public static ByteString fromInt(int value) {
        return ByteString.copyFrom(ByteBuffer.allocate(4).putInt(value));
    }

    public static int toInt(ByteString value) {
        return value.asReadOnlyByteBuffer().getInt();
    }

    public static ByteString fromLong(long value) {
        return ByteString.copyFrom(ByteBuffer.allocate(8).putLong(value));
    }

    public static long toLong(ByteString value) {
        return value.asReadOnlyByteBuffer().getLong();
    }

    public static ByteString fromShort(short value) {
        return ByteString.copyFrom(ByteBuffer.allocate(2).putShort(value));
    }

    public static short toShort(ByteString value) {
        return value.asReadOnlyByteBuffer().getShort();
    }

    public static ByteString fromStringUtf8(String value) {
        return ByteString.copyFromUtf8(value);
    }

    public static String toStringUtf8(ByteString value) {
        return value.toStringUtf8();
    }

    public static ByteString fromTimestamp(Timestamp value) {
        return fromLong(value.getTime());
    }

    public static long toTimestamp(ByteString value) {
        return toLong(value);
    }

    public static String[] encodeStats(Record record) {
        return record.fields.stream()
                .map(DataTypeUtils::createTransform)
                .map(StatsTransform::getValue)
                .map(CodecUtil::byteString2Base64)
                .toArray(String[]::new);
    }
}

