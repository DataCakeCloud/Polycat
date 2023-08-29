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

import java.sql.Types;

import io.polycat.catalog.common.model.record.Field;
import io.polycat.catalog.common.model.record.StringWritable;

public class ArrayType extends DataType {
    static final ArrayType ARRAY = new ArrayType();

    private DataType elementType;

    private ArrayType() {
        super(DataTypeObject.ARRAY.name(), Types.ARRAY);
    }

    public ArrayType(DataType elementType) {
        super(DataTypeObject.ARRAY.name() + "<" + elementType.toString() + ">", Types.ARRAY);
        this.elementType = elementType;
    }


    @Override
    public Field createEmptyField() {
        return new StringWritable();
    }

    private Object readResolve() {
        return ARRAY;
    }
}
