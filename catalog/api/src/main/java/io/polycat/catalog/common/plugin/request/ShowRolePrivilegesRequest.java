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

import io.polycat.catalog.common.plugin.request.base.PageListRequestBase;
import io.polycat.catalog.common.plugin.request.input.ShowRolePrivilegesInput;

import io.swagger.annotations.ApiModel;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
@ApiModel(description = "Show role privileges request")
public class ShowRolePrivilegesRequest extends PageListRequestBase<ShowRolePrivilegesInput> {

    public ShowRolePrivilegesRequest() {
        super();
    }

    public ShowRolePrivilegesRequest(String projectId,
        ShowRolePrivilegesInput input) {
        super(projectId, input);
    }

    public ShowRolePrivilegesRequest(String projectId,
        ShowRolePrivilegesInput input, Integer maxResults, String pageToken) {
        super(projectId, input, maxResults, pageToken);
    }
}