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

import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.model.TableUsageProfile;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@ApiModel(description = "policy input body")
@Data
public class PolicyInput {

    @ApiModelProperty(value = "principal id")
    private String principalId;

    @ApiModelProperty(value = "principal type", required = true)
    private String principalType;

    @ApiModelProperty(value = "principal source", required = true)
    private String principalSource;

    @ApiModelProperty(value = "principal name", required = true)
    private String principalName;

    @ApiModelProperty(value = "operation", required = true)
    private List<Operation> operationList;

    @ApiModelProperty(value = "object type", required = true)
    private String objectType;

    @ApiModelProperty(value = "object name", required = true)
    private String objectName;

    @ApiModelProperty(value = "effect", required = true)
    private Boolean effect;

    @ApiModelProperty(value = "owner", required = true)
    private Boolean owner;

    @ApiModelProperty(value = "condition")
    private String condition;

    @ApiModelProperty(value = "obligation")
    private String obligation;

    @ApiModelProperty(value = "grantAble", required = true)
    private Boolean grantAble;

    @ApiModelProperty(value = "policy id List")
    private List<String> policyIdList;
}
