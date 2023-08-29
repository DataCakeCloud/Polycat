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

import io.polycat.catalog.common.Constants;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldNameConstants;
import org.hibernate.validator.constraints.Length;

@ApiModel(description = "catalog response")
@Data
@NoArgsConstructor
@AllArgsConstructor
@FieldNameConstants
public class Catalog implements Serializable {
    @ApiModelProperty(value = "catalog version", required = true)
    private String version;

    @ApiModelProperty(value = "catalog name", required = true)
    @NotNull
    @Length(max = Constants.RESOURCE_MAX_LENGTH, min = Constants.RESOURCE_MIN_LENGTH)
    protected String catalogName;

    @ApiModelProperty(value = "create time", required = true)
    @NotNull
    private Long createTime;

    @ApiModelProperty(value = "parent name", required = true)
    private String parentName;

    @ApiModelProperty(value = "parent version", required = true)
    private String parentVersion;

    @ApiModelProperty(value = "owner", required = true)
    @NotNull
    private String owner;

    @ApiModelProperty(value = "owner type", required = true)
    @NotNull
    private String ownerType;

    @ApiModelProperty(value = "authorization Source Type", required = true)
    @NotNull
    private String authSourceType;

    @ApiModelProperty(value = "account id", required = true)
    @NotNull
    private String accountId;

    @ApiModelProperty(value = "dropped time", required = true)
    private Long droppedTime;

    @ApiModelProperty(value = "description info")
    private String description;

    @ApiModelProperty(value = "location URL", required = true)
    @NotNull
    private String location;

    public Catalog(String version, String catalogName, String owner, String ownerType, String authSourceType,
        String accountId, String location) {
        this.version = version;
        this.catalogName = catalogName;
        this.owner = owner;
        this.ownerType = ownerType;
        this.authSourceType = authSourceType;
        this.accountId = accountId;
        this.location = location;
    }
}
