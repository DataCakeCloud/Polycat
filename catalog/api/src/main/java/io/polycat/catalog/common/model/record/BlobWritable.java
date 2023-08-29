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
package io.polycat.catalog.common.model.record;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import io.polycat.catalog.common.serialization.Writable;
import io.polycat.catalog.common.types.DataType;
import io.polycat.catalog.common.types.DataTypes;
import io.polycat.catalog.common.utils.CodecUtil;

/**
 * 二进制字节数组类型，暂时用于传递storm消息封装后的二进制字节流（因为storm的结构限制，暂时无法完全解耦成多行的Records传输
 * 需要把proto转换后的字节数组当做Record的一个字段值来传递
 */
public class BlobWritable extends Field {

    private byte[] value;

    public BlobWritable() {
    }

    public BlobWritable(byte[] value) {
        Objects.requireNonNull(value);
        this.value = value;
    }

    @Override
    public DataType getType() {
        return DataTypes.BLOB;
    }

    @Override
    public void setValue(byte[] value) {
        Objects.requireNonNull(value);
        this.value = value;
    }

    @Override
    public byte[] getBlob() {
        return value;
    }

    @Override
    public String getString() {
        return Arrays.toString(value);
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        Writable.writeByteArray(out, value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        value = Writable.readByteArray(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BlobWritable that = (BlobWritable) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public String toString() {
        return CodecUtil.bytes2Hex(value);
    }

    @Override
    public int compareTo(Field o) {
        BlobWritable blobWritable = (BlobWritable)o;
        if (value.length > blobWritable.getBlob().length) {
            return 1;
        } else if (value.length < blobWritable.getBlob().length) {
            return -1;
        } else {
            for (int i = 0;i < value.length;  i++) {
                return Byte.compare(value[i], blobWritable.getBlob()[i]);
            }
        }
        return 0;
    }
}
