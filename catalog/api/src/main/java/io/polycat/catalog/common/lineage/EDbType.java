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


import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * database vendor type.
 */
public enum EDbType {
    /**
     * hive sql
     */
    HIVE(0),
    MYSQL(1),
    CLICKHOUSE(2),
    REDSHIFT(3),
    POSTGRESQL(4),
    SPARKSQL(5),
    ;
    private final int num;

    EDbType(int num) {
        this.num = num;
    }

    public static EDbType forNum(int num) {
        switch (num) {
            case 0:
                return HIVE;
            case 1:
                return MYSQL;
            case 2:
                return CLICKHOUSE;
            case 3:
                return REDSHIFT;
            case 4:
                return POSTGRESQL;
            case 5:
                return SPARKSQL;
            default:
                throw new IllegalArgumentException("Unsupported DB type, currently supported: " + Arrays.stream(EDbType.values()).collect(
                        Collectors.toList()));
        }
    }

    public int getNum() {
        return num;
    }
}
