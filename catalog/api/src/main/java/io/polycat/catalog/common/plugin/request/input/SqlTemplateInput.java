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

import java.util.Objects;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@ApiModel(description = "sql template input")
@Data
public class SqlTemplateInput {

    @ApiModelProperty(value = "catalog id", required = true)
    private String catalogId;

    @ApiModelProperty(value = "database id", required = true)
    private String databaseId;

    @ApiModelProperty(value = "table id", required = true)
    private String tableId;

    @ApiModelProperty(value = "hash code", required = true)
    private int hashCode;

    @ApiModelProperty(value = "sql template", required = true)
    private String sqlTemplate;

    @ApiModelProperty(value = "bin file path", required = true)
    private String binFilePath;

    public SqlTemplateInput() {

    }

    public SqlTemplateInput(String catalogId, String databaseId, String tableId, int hashCode, String sqlTemplate,
            String binFilePath) {
        this.catalogId = catalogId;
        this.databaseId = databaseId;
        this.tableId = tableId;
        this.hashCode = hashCode;
        this.sqlTemplate = sqlTemplate;
        this.binFilePath = binFilePath;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SqlTemplateInput that = (SqlTemplateInput) o;
        return hashCode == that.hashCode && Objects.equals(catalogId, that.catalogId) && Objects
                .equals(databaseId, that.databaseId) && Objects.equals(tableId, that.tableId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, hashCode);
    }
}
