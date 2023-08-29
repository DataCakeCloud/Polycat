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
package io.polycat.catalog.store.common;

import io.polycat.catalog.common.model.FunctionType;
import io.polycat.catalog.common.model.PrincipalType;
import io.polycat.catalog.common.plugin.request.input.FunctionResourceType;

public class StoreTypeConvertor {

    public static FunctionResourceType toFunctionResourceType(String funcType) {
        return FunctionResourceType.getFunctionResourceType(funcType);
    }

    public static PrincipalType toOwnerType(String ownerType) {
        return PrincipalType.getPrincipalType(ownerType);
    }

    public static FunctionType toFunctionType(String funcType) {
        return FunctionType.getPrincipalType(funcType);
    }
}
