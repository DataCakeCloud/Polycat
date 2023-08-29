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

import java.util.ArrayList;
import java.util.List;

/**
 * Created by LinShunkang on 2020/05/16
 */
public final class ListUtils {

    private ListUtils() {
        //empty
    }

    public static <T> boolean isEmpty(List<T> list) {
        return list == null || list.isEmpty();
    }

    public static <T> boolean isNotEmpty(List<T> list) {
        return !isEmpty(list);
    }

    public static <T> List<List<T>> partition(List<T> list, int size) {
        if (size <= 0) {
            throw new IllegalArgumentException(size + " <= 0");
        }

        if (list == null || list.size() == 0) {
            return new ArrayList<>(0);
        }

        List<List<T>> result = new ArrayList<>(list.size() / size + 1);
        for (int fromIndex = 0; fromIndex < list.size(); ) {
            List<T> subList = getSubList(list, fromIndex, size);
            result.add(subList);
            fromIndex += size;
        }
        return result;
    }

    public static <T> List<T> getSubList(List<T> list, int fromIndex, int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException(limit + " <= 0");
        }

        int toIndex = Math.min(list.size(), fromIndex + limit);
        return list.subList(fromIndex, toIndex);
    }
}
