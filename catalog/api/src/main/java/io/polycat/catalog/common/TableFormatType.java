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
package io.polycat.catalog.common;

import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * table format type enum
 */
public enum TableFormatType {

    /**
     * Iceberg type
     */
    ICEBERG,
    HUDI,
    DELTA,
    HIVE,
    PAIMON
    ;

    private static Set<String> tableFormatSet;

    static {
        tableFormatSet = Arrays.stream(TableFormatType.values()).map(Enum::name).collect(Collectors.toSet());
    }


    public static boolean isTableFormatType(String name) {
        if (name != null && name.length() > 0) {
            return tableFormatSet.contains(name.toUpperCase(Locale.ROOT));
        }
        return false;
    }
}
