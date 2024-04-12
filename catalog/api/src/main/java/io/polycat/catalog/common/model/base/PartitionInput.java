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
package io.polycat.catalog.common.model.base;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.polycat.catalog.common.plugin.request.input.FileInput;
import io.polycat.catalog.common.plugin.request.input.FileStatsInput;
import io.polycat.catalog.common.model.StorageDescriptor;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@ApiModel(description = "partition base object")
@Data
public class PartitionInput implements Serializable {
    @ApiModelProperty(value = "catalog name", required = true, example = "c1")
    private String catalogName;

    @ApiModelProperty(value = "database name", required = true, example = "db1")
    private String databaseName;

    @ApiModelProperty(value = "table name", required = true, example = "t1")
    private String tableName;

    @ApiModelProperty(value = "partition values", required = true)
    private List<String> partitionValues;

    @ApiModelProperty(value = "storage descriptor", required = true)
    private StorageDescriptor storageDescriptor;

    @ApiModelProperty(value = "parameters", required = true)
    private Map<String, String> parameters = new HashMap<>();

    @ApiModelProperty(value = "create time", required = true)
    Long createTime = 0L;

    @ApiModelProperty(value = "last access time", required = true)
    Long lastAccessTime = 0L;

    @ApiModelProperty(value = "files list", required = true)
    protected FileInput[] files;

    @ApiModelProperty(value = "index", required = true)
    protected FileStatsInput[] index;

    @ApiModelProperty(value = "partition index file url", required = true)
    private String fileIndexUrl;

    public Map<String, String> getParameters() {
        if (parameters == null) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(parameters);
    }
}
