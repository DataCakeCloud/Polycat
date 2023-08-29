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
package io.polycat.catalog.common.plugin.request;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.polycat.catalog.common.Constants;
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.plugin.request.base.ProjectRequestBase;

import lombok.Data;

@Data
public class GetTokenWithRenewerRequest extends ProjectRequestBase<Void> {

    String tokenType;
    String owner;
    String renewer;
    @Override
    public Operation getOperation() {
        return Operation.GET_TOKEN;
    }

    public void setDelegationToken(String owner, String renewer) {
        tokenType = Constants.MRS_TOKEN;
        this.owner = owner;
        this.renewer = renewer;
    }

    public Map<String, String> getParams() {
        Map<String, String> paramMap = new HashMap<>();
        if (tokenType.equals(Constants.MRS_TOKEN)) {
            if (owner != null) {
                paramMap.put(Constants.OWNER_PARAM, owner);
            }
            if (renewer != null) {
                paramMap.put(Constants.RENEWER_PARAM, renewer);
            }
            return paramMap;
        } else {
            return Collections.EMPTY_MAP;
        }
    }
}
