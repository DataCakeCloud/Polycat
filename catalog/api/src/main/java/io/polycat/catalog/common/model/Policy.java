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

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@ApiModel(description = "policy")
@Data
public class Policy {

    @ApiModelProperty(value = "policy id", required = true)
    private String policyId;

    @ApiModelProperty(value = "project id", required = true)
    private String projectId;

    @ApiModelProperty(value = "principal type", required = true)
    private PrincipalType principalType;

    @ApiModelProperty(value = "principal source", required = true)
    private PrincipalSource principalSource;

    @ApiModelProperty(value = "principal id", required = true)
    private String principalId;

    @ApiModelProperty(value = "privilege", required = true)
    private String privilege;

    @ApiModelProperty(value = "resource type", required = true)
    private String objectType;

    @ApiModelProperty(value = "resource name", required = true)
    private String objectName;

    @ApiModelProperty(value = "condition")
    private String condition;

    @ApiModelProperty(value = "obligation")
    private String obligation;

    @ApiModelProperty(value = "create time", required = true)
    private long createdTime;

    @ApiModelProperty(value = "owner", required = true)
    private boolean isOwner;

    @ApiModelProperty(value = "grantAble")
    private String grantAble;
}
