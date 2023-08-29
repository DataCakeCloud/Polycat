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
package io.polycat.catalog.common.http;

import java.lang.reflect.Type;

import io.polycat.catalog.common.types.BigIntType;
import io.polycat.catalog.common.types.BlobType;
import io.polycat.catalog.common.types.BooleanType;
import io.polycat.catalog.common.types.DataType;
import io.polycat.catalog.common.types.DataTypes;
import io.polycat.catalog.common.types.DateType;
import io.polycat.catalog.common.types.DecimalType;
import io.polycat.catalog.common.types.DoubleType;
import io.polycat.catalog.common.types.FloatType;
import io.polycat.catalog.common.types.IntegerType;
import io.polycat.catalog.common.types.IntervalType;
import io.polycat.catalog.common.types.MapAvgDataType;
import io.polycat.catalog.common.types.NullType;
import io.polycat.catalog.common.types.ObjectType;
import io.polycat.catalog.common.types.SmallIntType;
import io.polycat.catalog.common.types.StringType;
import io.polycat.catalog.common.types.TimestampType;
import io.polycat.catalog.common.types.TinyIntType;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class InterfaceAdapter<T> implements JsonSerializer<T>, JsonDeserializer<T> {

    @Override
    public T deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext ctx)
        throws JsonParseException {
        JsonObject member = (JsonObject) jsonElement;
        String typeString = getElement(member, "type").getAsString();
        Class<?> actualType = classForName(typeString);
        if (typeString.equals(BigIntType.class.getName())) {
            return (T) DataTypes.BIGINT;
        } else if (typeString.equals(BlobType.class.getName())) {
            return (T) DataTypes.BLOB;
        } else if (typeString.equals(BooleanType.class.getName())) {
            return (T) DataTypes.BOOLEAN;
        } else if (typeString.equals(DateType.class.getName())) {
            return (T) DataTypes.DATE;
        } else if (typeString.equals(DoubleType.class.getName())) {
            return (T) DataTypes.DOUBLE;
        } else if (typeString.equals(FloatType.class.getName())) {
            return (T) DataTypes.FLOAT;
        } else if (typeString.equals(IntegerType.class.getName())) {
            return (T) DataTypes.INTEGER;
        } else if (typeString.equals(IntervalType.class.getName())) {
            return (T) DataTypes.INTERVAL;
        } else if (typeString.equals(MapAvgDataType.class.getName())) {
            return (T) DataTypes.MAP_AVG_DATA;
        } else if (typeString.equals(NullType.class.getName())) {
            return (T) DataTypes.NULL;
        } else if (typeString.equals(ObjectType.class.getName())) {
            return (T) DataTypes.OBJECT;
        } else if (typeString.equals(SmallIntType.class.getName())) {
            return (T) DataTypes.SMALLINT;
        } else if (typeString.equals(StringType.class.getName())) {
            return (T) DataTypes.STRING;
        } else if (typeString.equals(TimestampType.class.getName())) {
            return (T) DataTypes.TIMESTAMP;
        } else if (typeString.equals(TinyIntType.class.getName())) {
            return (T) DataTypes.TINYINT;
        } else {
            JsonElement data = getElement(member, "data");
            return ctx.deserialize(data, actualType);
        }
    }

    @Override
    public JsonElement serialize(T dataObject, Type type, JsonSerializationContext ctx) {
        JsonObject member = new JsonObject();
        member.addProperty("type", dataObject.getClass().getName());
        if (dataObject instanceof DataType) {
            if (dataObject instanceof DecimalType) {
                member.add("data", ctx.serialize(dataObject));
            }
        } else {
            member.add("data", ctx.serialize(dataObject));
        }
        return member;
    }

    private Class<?> classForName(String typeString) {
        try {
            return Class.forName(typeString);
        } catch (ClassNotFoundException e) {
            throw new JsonParseException(e);
        }
    }

    private JsonElement getElement(JsonObject jsonObject, String memberName) {
        JsonElement jsonElement = jsonObject.get(memberName);
        if (jsonElement == null) {
            throw new JsonParseException("member not found: " + memberName);
        }
        return jsonElement;
    }
}
