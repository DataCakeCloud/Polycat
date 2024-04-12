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
package io.polycat.catalog.common.model.stats;

import java.nio.ByteBuffer;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@ApiModel(description = "Boolean Column stats")
@Data
public class BooleanColumnStatsData{
    @ApiModelProperty(value = "The number of True in the column", required = true)
    protected long numTrues;

    @ApiModelProperty(value = "The number of False in the column", required = true)
    protected long numFalses; // required

    @ApiModelProperty(value = "The number of null values in the column.", required = true)
    protected long numNulls; // required

    @ApiModelProperty(value = "bit vectors")
    protected byte[] bitVectors; // optional
}
