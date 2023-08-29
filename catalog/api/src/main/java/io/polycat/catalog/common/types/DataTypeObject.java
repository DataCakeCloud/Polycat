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
package io.polycat.catalog.common.types;

import lombok.Getter;

public enum DataTypeObject {
    BOOLEAN(1),
    BYTE(2),
    TINYINT(2),
    SMALLINT(3),
    SHORT(3),
    INT(4),
    INTEGER(4),
    LONG (5),
    BIGINT(5),
    FLOAT (6),
    DOUBLE (7),
    DECIMAL (8),
    TIMESTAMP (9),
    DATE (10),
    STRING (11),
    VARCHAR (12),
    BINARY (13),
    BLOB(14),
    NULL(15),
    OBJECT(16),
    INTERVAL(17),
    MAP_AVG_DATA(18),
    STRUCT (20),
    ARRAY (21),
    MAP (23);

    @Getter
    int num;

    DataTypeObject(int num) {
        this.num = num;
    }
}
