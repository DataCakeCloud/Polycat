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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.types.DataType;
import io.polycat.catalog.common.types.DataTypes;
import lombok.Getter;

public class ObjectWritable extends Field {

    private static final Logger logger = Logger.getLogger(ObjectWritable.class);

    @Getter
    private Object value;

    public ObjectWritable() {

    }

    public ObjectWritable(Object value) {
        this.value = value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public DataType getType() {
        return DataTypes.OBJECT;
    }

    @Override
    public Object getObject() {
        return value;
    }

    @Override
    public String getString() {
        return value.toString();
    }

    @Override
    public int compareTo(Field o) {
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        ((ObjectOutputStream)out).writeObject(value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        try {
            value = ((ObjectInputStream)in).readObject();
        } catch (ClassNotFoundException e) {
            logger.error(e);
            throw new IOException(e);
        }
    }

    @Override
    public String toString() {
        return getString();
    }
}
