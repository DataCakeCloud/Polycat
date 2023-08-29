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

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ApiModel(description = "Partition")
public class Partition implements Serializable {

    @ApiModelProperty(value = "partition version", required = true, example = "c1")
    private String version;

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

    @ApiModelProperty(value = "data files", required = true)
    private List<DataFile> dataFiles;

    @ApiModelProperty(value = "partition index file url", required = true)
    private String fileIndexUrl;

    @ApiModelProperty(value = "parameters", required = true)
    private Map<String, String> parameters = Collections.emptyMap();

    @ApiModelProperty(value = "create time", required = true)
    Long createTime = 0L;

    @ApiModelProperty(value = "last access time", required = true)
    Long lastAccessTime = 0L;

    public void setParameters(Map<String, String> parameters) {
        if (parameters != null) {
            this.parameters = parameters;
            return;
        }

        this.parameters = Collections.emptyMap();
    }
}


