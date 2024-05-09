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
package io.polycat.catalog.common.lineage;

/**
 * Enum to track dependency. This enum has the following values:
 * 1. SIMPLE - Indicates that the column is derived from another table column
 * with no transformations e.g. T2.c1 = T1.c1.
 * 2. EXPRESSION - Indicates that the column is derived from a UDF, UDAF, UDTF or
 * set operations like union on columns on other tables
 * e.g. T2.c1 = T1.c1 + T3.c1.
 * 3. SCRIPT - Indicates that the column is derived from the output
 * of a user script through a TRANSFORM, MAP or REDUCE syntax
 * or from the output of a PTF chain execution.
 */
public enum EDependWay {
    /**
     * SIMPLE
     */
    SIMPLE(0),
    EXPRESSION(1),
    SCRIPT(2),
    ;

    private final int num;

    EDependWay(int num) {
        this.num = num;
    }

    public static EDependWay forNum(int num) {
        switch (num) {
            case 0:
                return SIMPLE;
            case 1:
                return EXPRESSION;
            case 2:
                return SCRIPT;
            default:
                throw new IllegalArgumentException("Unsupported depend way.");
        }
    }

    public int getNum() {
        return num;
    }
}
