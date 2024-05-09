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
package io.polycat.catalog.common.plugin.request.input;

import java.util.List;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@ApiModel(description = "Show role privilege input body")
@Data
public class ShowRolePrivilegesInput {

    @ApiModelProperty(value = "userId", required = false)
    private String userId;

    @ApiModelProperty(value = "object type", required = false)
    private String objectType;

    @ApiModelProperty(value = "Exact object qualified names", required = false)
    private List<String> exactObjectNames;

    @ApiModelProperty(value = "Object qualified name prefix", required = false)
    private String objectNamePrefix;

    @ApiModelProperty(value = "Exact role names, best option cobnditions", required = false)
    private List<String> exactRoleNames;

    @ApiModelProperty(value = "Exclude role names with fixed prefix", required = false)
    private String excludeRolePrefix;

    @ApiModelProperty(value = "Include role names with fixed prefix", required = false)
    private String includeRolePrefix;

}