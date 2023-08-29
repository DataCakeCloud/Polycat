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

import io.swagger.annotations.ApiModelProperty;
import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class UserProfileContext {
    @ApiModelProperty(value = "authentication source type")
    private String authSourceType;

    @ApiModelProperty(value = "account")
    private String accountId;

    @ApiModelProperty(value = "ownerType", required = true, example = "USER")
    private String ownerType;

    @ApiModelProperty(value = "owner", required = true)
    private String owner;

    @ApiModelProperty(value = "total capacity", required = true)
    private long totalCapacity;

    @ApiModelProperty(value = "total database", required = true)
    private long totalDatabase;

    @ApiModelProperty(value = "total table", required = true)
    private long totalTable;

    @ApiModelProperty(value = "api invoke count", required = true)
    private long apiInvokeCount;
}
