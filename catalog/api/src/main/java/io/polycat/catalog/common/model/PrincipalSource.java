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

public enum PrincipalSource {
    IAM(1),
    SAML(2),
    LDAP(3),
    LOCAL(4),
    OTHER(5);

    @Getter
    int num;

    PrincipalSource(int num) {
        this.num = num;
    }

    public static PrincipalSource getPrincipalSource(int type) {
        for (PrincipalSource t : PrincipalSource.values()) {
            if (t.getNum() == type) {
                return t;
            }
        }
        return null;
    }

    public static PrincipalSource getPrincipalSource(String typeStr) {
        String typeUpper = typeStr.toUpperCase();
        if (typeUpper.equals("IAM") || typeUpper.equals("SAML") || typeUpper.equals("LDAP")) {
            return  PrincipalSource.valueOf(typeUpper);
        }
        return PrincipalSource.valueOf("OTHER");
    }
}