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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.polycat.catalog.common.plugin.request.input.AuthorizationInput;

import lombok.Getter;
import lombok.Setter;

public class AuthenticationRequest {

    @Getter
    private final String projectId;
    @Getter
    @Setter
    private final Boolean ignoreUnknownObj;
    @Getter
    private final List<AuthorizationInput> authorizationInputList;

    public AuthenticationRequest(String projectId, List<AuthorizationInput> authorizationInputList) {
        this(projectId, false, authorizationInputList);
    }

    public AuthenticationRequest(String projectId, Boolean ignoreUnknownObj,
        List<AuthorizationInput> authorizationInputList) {
        this.projectId = projectId;
        this.ignoreUnknownObj = ignoreUnknownObj;
        this.authorizationInputList = authorizationInputList;
    }

    public Map<String, String> getHeader(String token) {
        Map<String, String> header = new HashMap<>();
        header.put("Authorization", token);
        return header;
    }

}
