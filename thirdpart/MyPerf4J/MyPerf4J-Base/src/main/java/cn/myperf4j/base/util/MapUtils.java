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
package cn.myperf4j.base.util;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by LinShunkang on 2018/3/11
 */
public final class MapUtils {

    private static final float DEFAULT_LOAD_FACTOR = 0.75f;

    private MapUtils() {
        //empty
    }

    public static <K, V> Map<K, V> of(K k, V v) {
        Map<K, V> map = createHashMap(1);
        map.put(k, v);
        return map;
    }

    public static <K, V> Map<K, V> createHashMap(int keyNum) {
        return new HashMap<>(getFitCapacity(keyNum));
    }

    public static <K, V> Map<K, V> createHashMap(int keyNum, float loadFactor) {
        return new HashMap<>(getFitCapacity(keyNum, loadFactor));
    }

    public static <K, V> ConcurrentHashMap<K, V> createConcHashMap(int keyNum, float loadFactor) {
        return new ConcurrentHashMap<>(getFitCapacity(keyNum, loadFactor));
    }

    public static <K, V> Map<K, V> createLinkedHashMap(int keyNum) {
        return new LinkedHashMap<>(getFitCapacity(keyNum));
    }

    public static int getFitCapacity(int keyNum) {
        return getFitCapacity(keyNum, DEFAULT_LOAD_FACTOR);
    }

    public static int getFitCapacity(int keyNum, float loadFactor) {
        return (int) (keyNum / loadFactor) + 1;
    }

    public static <K, V> boolean isEmpty(Map<K, V> map) {
        return map == null || map.isEmpty();
    }

    public static <K, V> boolean isNotEmpty(Map<K, V> map) {
        return !isEmpty(map);
    }
}
