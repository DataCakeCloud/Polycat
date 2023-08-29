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

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import lombok.experimental.Accessors;
import org.hibernate.validator.constraints.Length;

@ApiModel(description = "catalog input")
@Data
@Accessors(chain = true)
public class CatalogInput {

    @ApiModelProperty(value = "catalog name", required = true, example = "c1")
    @NotBlank(message = "catalogName is empty")
    @Length(max = 60, message = "catalogName too long")
    private String catalogName;

    @ApiModelProperty(value = "parent branch current name")
    private String parentName;

    @ApiModelProperty(value = "parent branch version")
    private String parentVersion;

    @ApiModelProperty(value = "description")
    private String description;

    @ApiModelProperty(value = "owner", required = true, example = "mike")
    @NotBlank(message = "owner is empty")
    private String owner;

    @ApiModelProperty(value = "ownerType", required = true, example = "USER")
    @NotBlank(message = "ownerType is empty")
    private String ownerType;

    @ApiModelProperty(value = "authSourceType", required = true, example = "IAM")
    @NotBlank(message = "authSourceType is empty")
    private String authSourceType;

    @ApiModelProperty(value = "accountId", required = true, example = "account1")
    @NotBlank(message = "accountId is empty")
    private String accountId;

    @ApiModelProperty(value = "locationUri", example = "/location/uri/")
    private String location;
}
