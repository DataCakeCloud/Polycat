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
 * Lineage relationship type: the value and description are as follows：
 * FIELD_DEPEND_FIELD：insert into table new_t1 select a1 from t1;
 * TABLE_DEPEND_TABLE：insert into table new_t1 select a1 from t1;
 * FIELD_INFLU_TABLE：WHERE、GROUP BY、ORDER BY 、JOIN influence table
 * FIELD_INFLU_FIELD：WHERE、GROUP BY、ORDER BY 、JOIN influence field
 * TABLE_INFLU_FIELD：insert into table new_t1 select count(*) from t1;
 * FIELD_JOIN_FIELD：insert into table new_t1 select t1.a1 from t1 join t2 on t1.a1=t2.a1
 */
public enum ELineageType {
    /**
     * TABLE_DEPEND_TABLE
     */
    TABLE_DEPEND_TABLE(0),
    FIELD_DEPEND_FIELD(1),
    FIELD_INFLU_TABLE(2),
    FIELD_INFLU_FIELD(3),
    TABLE_INFLU_FIELD(4),
    FIELD_JOIN_FIELD(5),
    ;

    private final int num;

    ELineageType(int num) {
        this.num = num;
    }

    public static ELineageType forNum(int num) {
        switch (num) {
            case 0:
                return TABLE_DEPEND_TABLE;
            case 1:
                return FIELD_DEPEND_FIELD;
            case 2:
                return FIELD_INFLU_TABLE;
            case 3:
                return FIELD_INFLU_FIELD;
            case 4:
                return TABLE_INFLU_FIELD;
            case 5:
                return FIELD_JOIN_FIELD;
            default:
                throw new IllegalArgumentException("Unsupported lineage type.");
        }
    }

    public int getNum() {
        return num;
    }
}
