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

import io.polycat.catalog.common.model.base.DatabaseBase;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@ApiModel(description = "catalog object input")
@Data
public class CatalogInnerObject extends DatabaseBase {
    @ApiModelProperty(value = "object name", required = true)
    private String objectName;

    @ApiModelProperty(value = "object id", required = true)
    private String objectId;

    public CatalogInnerObject() {
    }

    public CatalogInnerObject(String projectId, String catalogName, String databaseName, String objectName) {
        this.projectId = projectId;
        this.catalogName = catalogName;
        this.databaseName = databaseName;
        this.objectName = objectName;
    }

    @Override
    public String toString() {
        return "CatalogInnerObject{" +
                "objectName='" + objectName + '\'' +
                ", objectId='" + objectId + '\'' +
                ", projectId='" + projectId + '\'' +
                ", databaseName='" + databaseName + '\'' +
                '}';
    }
}