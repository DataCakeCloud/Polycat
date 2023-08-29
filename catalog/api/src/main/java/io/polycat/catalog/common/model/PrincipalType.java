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

public enum PrincipalType {

    USER(1),
    GROUP(2),
    ROLE(3),
    SHARE(4),
    OTHER(5);

    @Getter
    int num;

    PrincipalType(int num) {
        this.num = num;
    }


    public static PrincipalType getPrincipalType(int type) {
        for (PrincipalType t : PrincipalType.values()) {
            if (t.getNum() == type) {
                return t;
            }
        }
        return null;
    }

    public static PrincipalType getPrincipalType(String typeStr) {
        if (typeStr == null) {
            return USER;
        }
        String typeUpper = typeStr.toUpperCase();
        if (typeUpper.equals("USER") || typeUpper.equals("GROUP")
                || typeUpper.equals("ROLE") || typeUpper.equals("SHARE")) {
            return  PrincipalType.valueOf(typeUpper);
        }
        return PrincipalType.valueOf("OTHER");
    }
}
