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

import java.util.Map;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@Data
@ApiModel(description = "database input")
public class DatabaseInput {
    @ApiModelProperty(value = "catalog name", required = true, example = "c1")
    private String catalogName;

    @ApiModelProperty(value = "database name", required = true, example = "db1")
    private String databaseName;

    @ApiModelProperty(value = "description")
    private String description;

    @ApiModelProperty(value = "location uri")
    private String locationUri;

    @ApiModelProperty(value = "parameters")
    private Map<String, String> parameters;

    @ApiModelProperty(value = "create time")
    private long createTime;

    @ApiModelProperty(value = "authentication source type", required = true, example = "IAM")
    private String authSourceType;

    @ApiModelProperty(value = "account", required = true)
    private String accountId;

    @ApiModelProperty(value = "owner", required = true, example = "zhangsan")
    private String owner;

    @ApiModelProperty(value = "ownerType", required = true, example = "USER")
    private String ownerType;
}
