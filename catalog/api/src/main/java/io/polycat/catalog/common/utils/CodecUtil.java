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

import java.util.Base64;

import com.google.protobuf.ByteString;

public class CodecUtil {

    public static byte[] longToBytes(long values) {
        byte[] buffer = new byte[8];
        for (int i = 0; i < 8; i++) {
            int offset = 64 - (i + 1) * 8;
            buffer[i] = (byte) ((values >> offset) & 0xff);
        }
        return buffer;
    }

    public static long bytesToLong(byte[] buffer) {
        long  values = 0;
        for (int i = 0; i < 8; i++) {
            values <<= 8; values|= (buffer[i] & 0xff);
        }
        return values;
    }

    public static String byteString2Hex(ByteString byteString) {
        return bytes2Hex(byteString.toByteArray());
    }

    public static ByteString hex2ByteString(String encodedText) {
        return ByteString.copyFrom(hex2Bytes(encodedText));
    }

    public static String bytes2Hex(byte[] bytes) {
        StringBuilder builder = new StringBuilder(bytes.length << 1);
        String temp;
        for (byte value : bytes) {
            temp = Integer.toHexString(value & 0xFF);
            if (temp.length() == 1) {
                builder.append("0");
            }
            builder.append(temp);
        }
        return builder.toString();
    }

    public static byte[] hex2Bytes(String hexString) {
        byte[] result = new byte[hexString.length() / 2];
        for (int len = hexString.length(), index = 0; index <= len - 1; index += 2) {
            String subString = hexString.substring(index, index + 2);
            int intValue = Integer.parseInt(subString, 16);
            result[index / 2] = (byte)intValue;
        }
        return result;
    }

    public static String byteString2Base64(ByteString byteString) {
        return bytes2Base64(byteString.toByteArray());
    }

    public static ByteString base642ByteString(String value) {
        return ByteString.copyFrom(base642Bytes(value));
    }

    public static String bytes2Base64(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }

    public static byte[] base642Bytes(String value) {
        return  Base64.getDecoder().decode(value);
    }
}
