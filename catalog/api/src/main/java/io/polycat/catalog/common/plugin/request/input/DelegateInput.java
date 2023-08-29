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
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@ApiModel(description = "delegate input")
@Data
public class DelegateInput {
    @NotNull
    @ApiModelProperty(value = "delegate name", required = true)
    private String delegateName;

    @NotNull
    @ApiModelProperty(value = "user id", required = true)
    private String userId;

    @NotNull
    @ApiModelProperty(value = "storage provider", required = true)
    private String storageProvider;

    @ApiModelProperty(value = "provider domain name")
    private String providerDomainName;

    @ApiModelProperty(value = "agency name")
    private String agencyName;

    @ApiModelProperty(value = "allowed location list")
    private List<String> allowedLocationList;

    @ApiModelProperty(value = "blocked location list")
    private List<String> blockedLocationList;

}
