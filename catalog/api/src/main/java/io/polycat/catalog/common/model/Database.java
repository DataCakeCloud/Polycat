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

import java.io.Serializable;
import java.util.Map;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.experimental.Accessors;

@ApiModel(description = "database")
@Data
@Accessors(chain = true)
public class Database implements Serializable {

    @ApiModelProperty(value = "version")
    private String version;

    @ApiModelProperty(value = "catalog name", required = true)
    private String catalogName;

    @ApiModelProperty(value = "database name", required = true)
    private String databaseName;

    @ApiModelProperty(value = "description")
    private String description;

    @ApiModelProperty(value = "location url", required = true)
    private String locationUri;

    @ApiModelProperty(value = "parameters")
    private Map<String, String> parameters;

    @ApiModelProperty(value = "create time")
    private long createTime;

    @ApiModelProperty(value = "authentication source type")
    private String authSourceType;

    @ApiModelProperty(value = "account")
    private String accountId;

    @ApiModelProperty(value = "owner type")
    private String ownerType;

    @ApiModelProperty(value = "owner")
    private String owner;

    @ApiModelProperty(value = "dropped", reference = "database is dropped")
    private boolean dropped;

    @ApiModelProperty(value = "dropped time")
    private long droppedTime;

    @ApiModelProperty(value = "database Id")
    private String databaseId;

}
