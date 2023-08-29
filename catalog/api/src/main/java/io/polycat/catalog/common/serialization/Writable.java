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
package io.polycat.catalog.common.serialization;

import io.polycat.catalog.common.exception.ErrorItem;
import io.polycat.catalog.common.exception.ExceptionUtil;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;

import io.polycat.catalog.common.exception.CarbonSqlException;

/**
 * 序列化接口，避免使用Serializable导致低性能
 */
@WritableSerDe
public interface Writable {

    /**
     * 序列化，将类成员写入到out中
     */
    void write(DataOutput out) throws IOException;

    /**
     * 反序列化，从in中读出类成员
     */
    void readFields(DataInput in) throws IOException;

    /**
     * 将输入序列化。
     * 当反序列化时明确知道类定义时，使用此接口做序列化。与{@link #deserialize(byte[], Class)}配合使用
     */
    static byte[] serialize(Writable writable) {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                ObjectOutputStream outputStream = new ObjectOutputStream(bytes)) {
            writable.write(outputStream);
            outputStream.flush();
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new CarbonSqlException(ErrorItem.SERIALIZATION_ERROR,
                    writable.getClass().getName(), ExceptionUtil.getMessageExcludeException(e));
        }
    }

    /**
     * 将输入反序列化为指定对象。
     * 与{@link #serialize(Writable)} 配合使用。
     */
    static <T extends Writable> T deserialize(byte[] input, Class<T> cls) {
        try (ByteArrayInputStream bytes = new ByteArrayInputStream(input);
                ObjectInputStream inputStream = new ObjectInputStream(bytes)) {
            Writable instance = cls.newInstance();
            instance.readFields(inputStream);
            return (T) instance;
        } catch (IllegalAccessException | InstantiationException | IOException e) {
            throw new CarbonSqlException(ErrorItem.DESERIALIZATION_ERROR, cls.getName(), ExceptionUtil.getMessageExcludeException(e));
        }
    }

    static <T extends Writable> byte[] serializeList(List<T> writables) {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
             ObjectOutputStream outputStream = new ObjectOutputStream(bytes)) {
            outputStream.writeInt(writables.size());
            for (Writable writable : writables) {
                writable.write(outputStream);
            }
            outputStream.flush();
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new CarbonSqlException(ErrorItem.SERIALIZATION_ERROR,
                    writables.get(0).getClass().getName(), ExceptionUtil.getMessageExcludeException(e));
        }
    }

    static <T extends Writable> List<T> deserializeList(byte[] input, Class<T> cls) {
        try (ByteArrayInputStream bytes = new ByteArrayInputStream(input);
             ObjectInputStream inputStream = new ObjectInputStream(bytes)) {
            int size = inputStream.readInt();
            List<Writable> list = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                Writable instance = cls.newInstance();
                instance.readFields(inputStream);
                list.add(instance);
            }
            return (List<T>) list;
        } catch (IllegalAccessException | InstantiationException e) {
            throw new CarbonSqlException(ErrorItem.SERIALIZATION_ERROR, cls.getName(), ExceptionUtil.getMessageExcludeException(e));
        } catch (IOException e) {
            throw new CarbonSqlException(ErrorItem.SERIALIZATION_ERROR, cls.getName(), ExceptionUtil.getMessageExcludeException(e));
        }
    }

    /**
     * 将输入序列化。
     * 当反序列化时不知道类定义时，使用此接口做序列化。与{@link #deserializeWithClassName(byte[])}配合使用
     */
    static byte[] serializeWithClassName(Writable writable) {
        String className = "";
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                ObjectOutputStream outputStream = new ObjectOutputStream(bytes)) {
            className = writable.getClass().getName();
            outputStream.writeUTF(writable.getClass().getName());
            writable.write(outputStream);
            outputStream.flush();
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new CarbonSqlException(ErrorItem.SERIALIZATION_ERROR, className, ExceptionUtil.getMessageExcludeException(e));
        }
    }

    /**
     * 将输入反序列化。
     * 与{@link #serializeWithClassName(Writable)} )} 配合使用。
     */
    static <T extends Writable> T deserializeWithClassName(byte[] input) {
        String className = "";
        try (ByteArrayInputStream bytes = new ByteArrayInputStream(input);
                ObjectInputStream inputStream = new ObjectInputStream(bytes)) {
            className = inputStream.readUTF();
            Object object = Class.forName(className).newInstance();
            if (object instanceof Writable) {
                Writable instance = (Writable)object; 
                instance.readFields(inputStream);
                return (T) instance;
            } else {
                throw new CarbonSqlException(ErrorItem.DESERIALIZATION_ERROR, className, "object is not writable:" + object.getClass().getName());
            }
        } catch (IllegalAccessException | InstantiationException | ClassNotFoundException | IOException e) {
            throw new CarbonSqlException(ErrorItem.DESERIALIZATION_ERROR, className, e.getMessage());
        }
    }

    // 以下为工具函数，为List，Array等类型提供序列化/反序列化工具函数

    static void writeByteArray(DataOutput out, byte[] bytes) throws IOException {
        out.writeInt(bytes.length);
        for (byte aByte : bytes) {
            out.writeByte(aByte);
        }
    }

    static byte[] readByteArray(DataInput in) throws IOException {
        int size = in.readInt();
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
            bytes[i] = in.readByte();
        }
        return bytes;
    }

    static void writeMap(DataOutput out, Map<? extends Writable, ? extends Writable> map) throws IOException {
        writeList(out, new ArrayList<>(map.keySet()));
        writeList(out, new ArrayList<>(map.values()));
    }

    static <K extends Writable, V extends Writable> Map<K, V> readMap(DataInput in, Class<K> keyClass,
            Class<V> valueClass) throws IOException {
        List<K> keyList = readList(in, keyClass);
        List<V> valueList = readList(in, valueClass);
        if (keyList.size() != valueList.size()) {
            throw new CarbonSqlException(
                    String.format("deserialize map fail,key count(%d) not equals value count(%d):", keyList.size(),
                            valueList.size()));
        }

        Map<K, V> map = new HashMap<>();
        for (int i = 0; i < keyList.size(); i++) {
            map.put(keyList.get(i), valueList.get(i));
        }
        return map;
    }

    static void writeStringKeyMap(DataOutput out, Map<String, ? extends Writable> map) throws IOException {
        out.writeInt(map.size());
        for (Map.Entry<String, ? extends Writable> entry : map.entrySet()) {
            out.writeUTF(entry.getKey());
            entry.getValue().write(out);
        }
    }

    static <T extends Writable> Map<String, T> readStringKeyMap(DataInput in, Class<T> clz) throws IOException {
        int size = in.readInt();
        Map<String, T> map = new HashMap<>();
        for (int i = 0; i < size; i++) {
            String key = in.readUTF();
            T value = null;
            try {
                value = clz.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                throw new CarbonSqlException(e.getMessage(), e);
            }
            value.readFields(in);
            map.put(key, value);
        }
        return map;
    }

    // 序列化可能后续会进行继承和扩展的类对象
    static void writeByUnKnowClass(DataOutput out, Writable writable) throws IOException {
        out.writeUTF(writable.getClass().getName());
        writable.write(out);
    }

    // 反序列化位置的类
    static <T extends Writable> T readByUnKnowClass(DataInput in) throws IOException {
        T t;
        String className = "";
        try {
            className = in.readUTF();
            Class<T> clz = (Class<T>) Class.forName(className);
            t = clz.newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new CarbonSqlException(ErrorItem.DESERIALIZATION_ERROR, className, ExceptionUtil.getMessageExcludeException(e));
        }
        t.readFields(in);
        return t;
    }

    static void writeList(DataOutput out, List<? extends Writable> list) throws IOException {
        out.writeInt(list.size());
        for (Writable writable : list) {
            writable.write(out);
        }
    }

    static <T extends Writable> List<T> readList(DataInput in, Class<T> clz) throws IOException {
        int size = in.readInt();
        List<T> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            T t = null;
            try {
                t = clz.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                throw new CarbonSqlException(ErrorItem.DESERIALIZATION_ERROR, clz.getName(), e.getMessage());
            }
            t.readFields(in);
            list.add(t);
        }
        return list;
    }

    static void writeIntegerList(DataOutput out, List<Integer> list) throws IOException {
        out.writeInt(list.size());
        for (int i : list) {
            out.writeInt(i);
        }
    }

    static List<Integer> readIntegerList(DataInput in) throws IOException {
        int size = in.readInt();
        List<Integer> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            list.add(in.readInt());
        }
        return list;
    }

    static void writeStringList(DataOutput out, List<String> list) throws IOException {
        out.writeInt(list.size());
        for (String string : list) {
            out.writeUTF(string);
        }
    }

    static List<String> readStringList(DataInput in) throws IOException {
        int size = in.readInt();
        List<String> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            list.add(in.readUTF());
        }
        return list;
    }

    static void writeFloatArray(DataOutput output, float[] floats) throws IOException {
        output.writeInt(floats.length);
        for (float aFloat : floats) {
            output.writeFloat(aFloat);
        }
    }

    static float[] readFloatArray(DataInput input) throws IOException {
        int size = input.readInt();
        float[] floats = new float[size];
        for (int i = 0; i < size; i++) {
            floats[i] = input.readFloat();
        }
        return floats;
    }

    static void writeStringIfNotNull(DataOutput out, String str) throws IOException {
        out.writeBoolean(str != null);
        if (str != null) {
            out.writeUTF(str);
        }
    }

    static String readStringIfNotNull(DataInput in) throws IOException {
        if (in.readBoolean()) {
            return in.readUTF();
        }
        return null;
    }

    static void writeFloatIfNotNull(DataOutput out, Float f) throws IOException {
        out.writeBoolean(f != null);
        if (f != null) {
            out.writeFloat(f);
        }
    }

    static Float readFloatIfNotNull(DataInput in) throws IOException {
        if (in.readBoolean()) {
            return in.readFloat();
        }
        return null;
    }
}
