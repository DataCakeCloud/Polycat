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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;

public class ByteUtils {

    static byte[] TRUE = new byte[]{1};
    static byte[] FALSE = new byte[]{0};

    public static byte[] fromBoolean(boolean value) {
        return value ? ByteUtils.TRUE : ByteUtils.FALSE;
    }

    public static boolean toBoolean(byte[] value) {
        return value[0] == ByteUtils.TRUE[0];
    }

    public static byte[] fromByte(byte value) {
        return new byte [] {value};
    }

    public static byte toByte(byte[] value) {
        return value[0];
    }

    public static byte[] fromShort(short value) {
        return ByteBuffer.allocate(2).putShort(value).array();
    }

    public static short toShort(byte[] value) {
        return ByteBuffer.wrap(value).getShort();
    }

    public static byte[] fromInt(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }

    public static int toInt(byte[] value) {
        return ByteBuffer.wrap(value).getInt();
    }

    public static byte[] fromLong(long value) {
        return ByteBuffer.allocate(8).putLong(value).array();
    }

    public static long toLong(byte[] value) {
        return ByteBuffer.wrap(value).getLong();
    }

    public static byte[] fromFloat(float value) {
        return ByteBuffer.allocate(4).putFloat(value).array();
    }

    public static float toFloat(byte[] value) {
        return ByteBuffer.wrap(value).getFloat();
    }

    public static byte[] fromDouble(double value) {
        return ByteBuffer.allocate(8).putDouble(value).array();
    }

    public static double toDouble(byte[] value) {
        return ByteBuffer.wrap(value).getDouble();
    }

    public static byte[] fromDate(Date value) {
        return fromLong(value.getTime());
    }

    public static Date toDate(byte[] value) {
        return new Date(toLong(value));
    }

    public static byte[] fromTimestamp(Timestamp value) {
        return fromLong(value.getTime());
    }

    public static Timestamp toTimestamp(byte[] value) {
        return new Timestamp(toLong(value));
    }

    public static byte[] fromDecimal(BigDecimal value) {
        return fromString(value.toString());
    }

    public static BigDecimal toDecimal(byte[] value) {
        return new BigDecimal(toString(value));
    }

    public static byte[] fromString(String value) {
        return value.getBytes();
    }

    public static String toString(byte[] value) {
        return new String(value);
    }
}
