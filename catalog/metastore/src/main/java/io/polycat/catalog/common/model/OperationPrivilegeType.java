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

public enum OperationPrivilegeType {

    CREATE(1),
    DROP(2),
    ALTER(3),
    UNDROP(4),
    RESTORE(5),
    SHOW(6),
    SELECT(7),
    UPDATE(8),
    INSERT(9),
    DELETE(10),
    CREATE_TABLE(11),
    CREATE_VIEW(12),
    CREATE_DATABASE(13),
    CREATE_BRANCH(14),
    SHOW_TABLE(15),
    SHOW_VIEW(16),
    SHOW_DATABASE(17),
    SHOW_BRANCH(18),
    USE(19),
    DESC(20),
    OVERWRITE(21),
    CREATE_STREAM(22),
    MERGE_BRANCH(23),
    CREATE_ACCELERATOR(24),
    SHOW_ACCELERATORS(25),
    DROP_ACCELERATOR(26),
    CHANGE_SCHEMA(27),
    SHOW_ACCESSSTATS(28),
    DESC_ACCESSSTATS(29),
    SHOW_DATALINEAGE(30),
    COPY_INTO(31),
    OWNER(32),
    SHARE_PRIVILEGE_USAGE(33),
    SHARE_PRIVILEGE_SELECT(34);


    @Getter
    private final int type;


    OperationPrivilegeType(int type) {
        this.type = type;
    }

    public OperationPrivilegeType getOperationPrivilegeType(int type) {
        for (OperationPrivilegeType t : OperationPrivilegeType.values()) {
            if (t.getType() == type) {
                return t;
            }
        }
        return null;
    }
}
