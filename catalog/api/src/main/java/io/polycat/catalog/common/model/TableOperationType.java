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
package io.polycat.catalog.common.model;

import lombok.Getter;

public enum TableOperationType {
    DDL_CREATE_TABLE(1),
    DDL_DROP_TABLE(2),
    DDL_UNDROP_TABLE(3),
    DDL_RESTORE_TABLE(4),
    DDL_ADD_COLUMN(5),
    DDL_DELETE_COLUMN(6),
    DDL_RENAME_COLUMN(7),
    DDL_MODIFY_DATA_TYPE(8),
    DDL_SET_PROPERTIES(9),
    DDL_UNSET_PROPERTIES(10),
    DDL_MERGE_BRANCH(11),
    DML_INSERT(12),
    DML_INSERT_OVERWRITE(13),
    DML_UPDATE(14),
    DML_DROP_PARTITION(15),
    DDL_ALTER_PARTITION(16),
    DDL_CREATE_INDEX(17),
    DDL_DROP_INDEX(18);

    @Getter
    int num;

    TableOperationType(int num) {
        this.num = num;
    }
}
