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
import lombok.NoArgsConstructor;

@ApiModel(description = "table usage profile")
@Data
@NoArgsConstructor
public class TableSource {

    @ApiModelProperty(value = "project id", required = true)
    private String projectId;

    @ApiModelProperty(value = "catalog name", required = true)
    private String catalogName;

    @ApiModelProperty(value = "database name", required = true)
    private String databaseName;

    @ApiModelProperty(value = "table name", required = true)
    private String tableName;


    public TableSource(String projectId, Table table) {
        this.projectId = projectId;
        this.catalogName = table.getCatalogName();
        this.databaseName = table.getDatabaseName();
        this.tableName = table.getTableName();
    }


    public TableSource(String projectId, String catalogName, String databaseName, String tableName) {
        this.projectId = projectId;
        this.catalogName = catalogName;
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    @Override
    public String toString() {
        return "TableSource{" +
            "projectId='" + projectId + '\'' +
            ", catalogName='" + catalogName + '\'' +
            ", databaseName='" + databaseName + '\'' +
            ", tableName='" + tableName + '\'' +
            '}';
    }

    public String fullTableName() {
        return catalogName + "." + databaseName + "." +  tableName;
    }
}
