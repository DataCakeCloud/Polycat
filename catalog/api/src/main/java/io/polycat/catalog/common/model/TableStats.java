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

@ApiModel(description = "table stats")
@Data
public class TableStats {

    @ApiModelProperty(value = "byte size", required = true)
    private long byteSizeMB;

    @ApiModelProperty(value = "num rows", required = true)
    private long numRows;

    @ApiModelProperty(value = "num files", required = true)
    private int numFiles;

    public TableStats(long byteSizeMB, long numRows, int numFiles) {
        this.byteSizeMB = byteSizeMB;
        this.numRows = numRows;
        this.numFiles = numFiles;
    }

    public void merge(long byteSizeMB, long numRows, int numFiles) {
        this.byteSizeMB = this.byteSizeMB + byteSizeMB;
        this.numRows = this.numRows + numRows;
        this.numFiles = this.numFiles + numFiles;
    }
}
