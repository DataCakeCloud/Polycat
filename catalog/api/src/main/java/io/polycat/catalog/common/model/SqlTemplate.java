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

import io.polycat.catalog.common.model.base.TableBase;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@ApiModel(description = "sql template")
@Data
public class SqlTemplate extends TableBase {

    @ApiModelProperty(value = "hash code", required = true)
    private int hashCode;

    @ApiModelProperty(value = "sql template", required = true)
    private String sqlTemplate;

    @ApiModelProperty(value = "compiled", required = true)
    private boolean compiled;

    @ApiModelProperty(value = "bin file path", required = true)
    private String binFilePath;

    public SqlTemplate(String catalogId, String databaseId, String tableId, int hashCode, String sqlTemplate,
            boolean compiled, String binFilePath) {
        this.catalogId = catalogId;
        this.databaseId = databaseId;
        this.tableId = tableId;
        this.hashCode = hashCode;
        this.sqlTemplate = sqlTemplate;
        this.compiled = compiled;
        this.binFilePath = binFilePath;
    }
}
