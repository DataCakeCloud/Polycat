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

@Data
@ApiModel("Primary Key")
public class PrimaryKey {
    @ApiModelProperty(value = "database Name", required = true)
    private String dbName; // required

    @ApiModelProperty(value = "table Name", required = true)
    private String tableName; // required

    @ApiModelProperty(value = "column Name", required = true)
    private String columnName; // required

    @ApiModelProperty(value = "key sequence", required = true)
    private int keySeq; // required

    @ApiModelProperty(value = "primary key Name", required = true)
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
