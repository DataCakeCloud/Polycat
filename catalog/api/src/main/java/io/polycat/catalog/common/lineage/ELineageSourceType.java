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
 * Lineage entity source type：
 * 1. DDL -->  create table d1.t1(id int, name string);
 * 2. LINEAGE --> create table d1.t1 as select ...;
 *      --> insert into table d1.t1 select ...;
 */
public enum ELineageSourceType {
    /**
     * DDL
     */
    DDL(0),
    LINEAGE(1),
    ;

    private final int num;

    ELineageSourceType(int num) {
        this.num = num;
    }

    public static ELineageSourceType forNum(int num) {
        switch (num) {
            case 0:
                return DDL;
            case 1:
                return LINEAGE;
            default:
                throw new IllegalArgumentException("Unsupported lineage source type.");
        }
    }

    public int getNum() {
        return num;
    }
}
