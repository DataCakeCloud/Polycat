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

import io.polycat.catalog.common.model.base.PartitionInput;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@ApiModel(description = "add partition input")
@Data
public class AddPartitionInput {
    @ApiModelProperty(value = "based version", required = true)
    String basedVersion;

    @ApiModelProperty(value = "file format", required = true)
    String fileFormat;

    @ApiModelProperty(value = "overwrite")
    boolean overwrite;

    @ApiModelProperty(value = "if skip exist partition")
    boolean ifNotExist;

    @ApiModelProperty(value = "if need result")
    boolean needResult;

    @ApiModelProperty(value = "partitions", required = true)
    PartitionInput[] partitions;
}
