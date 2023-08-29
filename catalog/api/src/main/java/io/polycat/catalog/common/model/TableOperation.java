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

@ApiModel(description = "table operation")
@Data
public class TableOperation {

    @ApiModelProperty(value = "operation type", required = true)
    private String operType;

    @ApiModelProperty(value = "add nums", required = true)
    private long addedNums;

    @ApiModelProperty(value = "delete nums", required = true)
    private long deleteNums;

    @ApiModelProperty(value = "update nums", required = true)
    private long updatedNums;

    @ApiModelProperty(value = "file count", required = true)
    private int fileCount;

    @Override
    public String toString() {
        return "(operation=" + operType + ", fileCount=" + fileCount + ", addedRows=" + addedNums + ")";
    }

    public TableOperation(String operType, long addedNums, long deleteNums, long updatedNums, int fileCount) {
        this.operType = operType;
        this.addedNums = addedNums;
        this.deleteNums = deleteNums;
        this.updatedNums = updatedNums;
        this.fileCount = fileCount;
    }
}
