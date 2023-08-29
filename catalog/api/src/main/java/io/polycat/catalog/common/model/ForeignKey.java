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

@ApiModel("polycat foreign key base info")
@Data
public class ForeignKey {
    @ApiModelProperty(value = "parent Key Database Name")
    private String pkTableDb;

    @ApiModelProperty(value = "parent Key Table Name", required = true)
    private String pkTableName; // required

    @ApiModelProperty(value = "parent Key Column Name", required = true)
    private String pkColumnName; // required

    @ApiModelProperty(value = "foreign Key Database Name", required = true)
    private String fkTableDb; // required

    @ApiModelProperty(value = "foreign Key Table Name", required = true)
    private String fkTableName; // required

    @ApiModelProperty(value = "foreign Key Column Name", required = true)
    private String fkColumnName; // required

    @ApiModelProperty(value = "foreign Key Seq", required = true)
    private int keySeq; // required

    @ApiModelProperty(value = "foreign Key update rule when parent key update", required = true)
    private int updateRule; // required

    @ApiModelProperty(value = "foreign Key delete rule when parent key delete", required = true)
    private int deleteRule; // required

    @ApiModelProperty(value = "foreign Key name", required = true)
    private String fkName; // required

    @ApiModelProperty(value = "parent Key name", required = true)
    private String pkName; // required

    @ApiModelProperty(value = "enable constraint", required = true)
    private boolean enable_cstr; // required

    @ApiModelProperty(value = "is foreign Key validate", required = true)
    private boolean validate_cstr; // required

    @ApiModelProperty(value = "is foreign Key rely", required = true)
    private boolean rely_cstr; // required

    @ApiModelProperty(value = "catalog Name", required = true)
    private String catName; // optional
}
