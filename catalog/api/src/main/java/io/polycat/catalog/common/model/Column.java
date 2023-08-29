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

@ApiModel(description = "column output")
@Data
@NoArgsConstructor
public class Column {

    @ApiModelProperty(value = "column name", required = true, example = "c1")
    private String columnName;

    @ApiModelProperty(value = "colType", required = true)
    private String colType;

    @ApiModelProperty(value = "comments")
    private String comment;

    public Column(String name, String colType) {
        this(name, colType, "");
    }

    public Column(String columnName, String colType, String comment) {
        this.columnName = columnName;
        this.colType = colType;
        this.comment = comment;
    }

    public Column(Column other) {
        if (other != null) {
            this.columnName = other.getColumnName();
            this.colType = other.getColType();
            this.comment = other.getComment();
        }
    }
}
