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

import java.util.List;

import io.polycat.catalog.common.model.base.AcceleratorBase;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@ApiModel(description = "accelerator body")
@Data
public class AcceleratorObject extends AcceleratorBase {

    @ApiModelProperty(value = "library path", required = true)
    private String path;

    @ApiModelProperty(value = "library name", required = true)
    private String libraryName;

    @ApiModelProperty(value = "sql statement", required = true)
    private String sqlStatement;

    @ApiModelProperty(value = "compiled", required = true)
    private boolean compiled;

    @ApiModelProperty(value = "sql template list", required = true)
    private List<SqlTemplate> sqlTemplateList;

    public AcceleratorObject(String path, String libraryName, String sqlStatement, boolean compiled,
        List<SqlTemplate> sqlTemplateList) {
        this.path = path;
        this.libraryName = libraryName;
        this.sqlStatement = sqlStatement;
        this.compiled = compiled;
        this.sqlTemplateList = sqlTemplateList;
    }
}
