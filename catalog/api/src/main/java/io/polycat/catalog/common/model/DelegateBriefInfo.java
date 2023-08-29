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

@ApiModel(description = "delegate brief info")
@Data
public class DelegateBriefInfo {
    @ApiModelProperty(value = "delegate name", required = true)
    private String delegateName;

    @ApiModelProperty(value = "provider", required = true)
    private String storageProvider;

    @ApiModelProperty(value = "user id", required = true)
    private String userId;

    public DelegateBriefInfo(String delegateName, String storageProvider, String userId) {
        this.delegateName = delegateName;
        this.storageProvider = storageProvider;
        this.userId = userId;
    }
}
