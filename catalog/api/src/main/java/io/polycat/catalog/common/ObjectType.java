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

import lombok.Getter;

@Getter
public enum ObjectType {
    CATALOG(0),
    DATABASE(1),
    TABLE(2),
    SHARE(3),
    ROLE(4),
    VIEW(5),
    BRANCH(6),
    COLUMN(7),
    ROW(8),
    STREAM(9),
    ACCELERATOR(10),
    DELEGATE(11),
    FUNCTION(12),
    MATERIALIZED_VIEW(13),
    USER(14),
    GROUP(15),
    OTHERS(16);

    private final int num;

    ObjectType(int num) {
        this.num = num;
    }

    public static ObjectType forNum(int value) {
        switch (value) {
            case 0: return CATALOG;
            case 1: return DATABASE;
            case 2: return TABLE;
            case 3: return SHARE;
            case 4: return ROLE;
            case 5: return VIEW;
            case 6: return BRANCH;
            case 7: return COLUMN;
            case 8: return ROW;
            case 9: return STREAM;
            case 10: return ACCELERATOR;
            case 11: return DELEGATE;
            case 12: return FUNCTION;
            case 13: return MATERIALIZED_VIEW;
            case 14: return USER;
            case 15: return GROUP;
            default: return null;
        }
    }
}