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
import io.polycat.catalog.common.model.AuthorizationType;
import io.polycat.catalog.common.model.CatalogInnerObject;
import io.polycat.catalog.common.model.GrantObject;
import io.polycat.catalog.common.model.User;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@ApiModel(description = "authorization input")
@Data
public class AuthorizationInput {

    @ApiModelProperty(value = "authorization type", required = true)
    private AuthorizationType authorizationType;

    @ApiModelProperty(value = "user", required = true)
    private User user;

    @ApiModelProperty(value = "operation", required = true)
    private Operation operation;

    @ApiModelProperty(value = "catalog object", required = true)
    private CatalogInnerObject catalogInnerObject;

    @ApiModelProperty(value = "grant object", required = true)
    private GrantObject grantObject;

    @ApiModelProperty(value = "token", required = true)
    private String token;
}

