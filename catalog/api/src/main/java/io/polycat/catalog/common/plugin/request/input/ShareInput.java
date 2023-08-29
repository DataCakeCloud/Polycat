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

import io.polycat.catalog.common.Operation;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@ApiModel(description = "role input body")
@Data
public class ShareInput {

    @ApiModelProperty(value = "account id", required = true)
    private String accountId;

    @ApiModelProperty(value = "project id", required = true)
    private String projectId;

    @ApiModelProperty(value = "share name", required = true)
    private String shareName;

    @ApiModelProperty(value = "share id", required = true)
    private String shareId;

    @ApiModelProperty(value = "operation", required = true)
    private Operation operation;

    @ApiModelProperty(value = "object name", required = true)
    private String objectName;

    @ApiModelProperty(value = "account ids", required = true)
    private String[] accountIds;

    @ApiModelProperty(value = "user id", required = true)
    private String userId;

    @ApiModelProperty(value = "users", required = true)
    private String[] users;
}