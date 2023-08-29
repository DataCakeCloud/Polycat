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
@ApiModel(value = "constraint resource")
public class Constraint {
    @ApiModelProperty(value = "catalog Name", required = true)
    private String catName; // required

    @ApiModelProperty(value = "database name", required = true)
    private String dbName; // required

    @ApiModelProperty(value = "table Name", required = true)
    private String table_name; // required

    @ApiModelProperty(value = "column Name", required = true)
    private String column_name; // required

    @ApiModelProperty(value = "constraint Name", required = true)
    private String cstr_name; // required

    @ApiModelProperty(value = "constraint type", required = true)
    private ConstraintType cstr_type; // required

    @ApiModelProperty(value = "constraint info like key_seq, check_expression, default_value", required = true)
    private String cstr_info; // required

    @ApiModelProperty(value = "enable constraint", required = true)
    private boolean enable_cstr; // required

    @ApiModelProperty(value = "constraint is validated", required = true)
    private boolean validate_cstr; // required

    @ApiModelProperty(value = "constraint is rely when Query", required = true)
    private boolean rely_cstr; // required
}
