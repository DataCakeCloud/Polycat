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

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@ApiModel(description = "table usage profile")
@Data
public class TableUsageProfile {

    @ApiModelProperty(value = "source table", required = true)
    private TableSource table;

    @ApiModelProperty(value = "op types", required = false)
    private List<String> opTypes;

    @ApiModelProperty(value = "origin options ", required = false)
    private List<String> originOpTypes;

    @ApiModelProperty(value = "create day timestamp", required = false)
    private long createDayTimestamp;

    @ApiModelProperty(value = "create timestamp", required = true)
    private long createTimestamp;

    @ApiModelProperty(value = "sum count", required = true)
    private BigInteger sumCount;

    @ApiModelProperty(value = "avg count", required = false)
    private long avgCount;

    @ApiModelProperty(value = "user id", required = true)
    private String userId;

    @ApiModelProperty(value = "user group", required = false)
    private String userGroup;

    @ApiModelProperty(value = "task id", required = false)
    private String taskId;

    @ApiModelProperty(value = "user tag", required = false)
    private String tag;

    @ApiModelProperty(value = "exec statement", required = false)
    private String statement;

    public TableUsageProfile() {
    }

    public TableUsageProfile(TableSource table, String opType, long startTimestamp, BigInteger sumCount) {
        this(table, Collections.singletonList(opType), startTimestamp, sumCount, 0);
    }

    public TableUsageProfile(String projectId, Table table, String opType, long startTimestamp, BigInteger sumCount) {
        this(new TableSource(projectId, table), Collections.singletonList(opType), startTimestamp, sumCount, 0);
    }

    public TableUsageProfile(TableSource table, List<String> opTypes, BigInteger sumCount, long avgCount) {
        this(table, opTypes, 0L, sumCount, avgCount);
    }

    public TableUsageProfile(TableSource table,
                             List<String> opTypes, long createDayTimestamp, BigInteger sumCount, long avgCount) {
        this.table = table;
        this.opTypes = opTypes;
        this.createDayTimestamp = createDayTimestamp;
        this.sumCount = sumCount;
        this.avgCount = avgCount;
    }
}