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

import io.polycat.catalog.common.ObjectType;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;


@ApiModel(description = "grant object input")
@Data
public class GrantObject {
    @ApiModelProperty(value = "project id", required = true)
    private String projectId;

    @ApiModelProperty(value = "object type", required = true)
    private ObjectType objectType;

    @ApiModelProperty(value = "object name", required = true)
    private String objectName;

    public GrantObject() {

    }

    public GrantObject(String projectId, ObjectType objectType, String objectName) {
        this.projectId = projectId;
        this.objectType = objectType;
        this.objectName = objectName;
    }
}