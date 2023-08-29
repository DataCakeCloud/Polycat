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

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.net.URLEncoder;

import io.polycat.catalog.common.http.InterfaceAdapter;
import io.polycat.catalog.common.types.DataType;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class GsonUtil {

    public static Gson create() {
        return create(true);
    }

    public static Gson create(boolean prettyPrint) {
        if (prettyPrint) {
            return new GsonBuilder()
                    .registerTypeAdapter(DataType.class, new InterfaceAdapter<DataType>())
                    .setPrettyPrinting()
                    .create();
        }
        return new GsonBuilder()
                .registerTypeAdapter(DataType.class, new InterfaceAdapter<DataType>())
                .create();
    }

    public static String toJson(Object object) {
        return create().toJson(object);
    }

    public static String toJson(Object object, boolean prettyPrint) {
        return create(prettyPrint).toJson(object);
    }

    public static String toURLJson(Object object) throws UnsupportedEncodingException {
        return URLEncoder.encode(toJson(object), "UTF-8");
    }

    public static <T> T fromJson(String json, Class<T> classOfT) {
        return create().fromJson(json, classOfT);
    }

    public static <T> T fromJson(String json, Type typeOfT) {
        return create().fromJson(json, typeOfT);
    }
}
