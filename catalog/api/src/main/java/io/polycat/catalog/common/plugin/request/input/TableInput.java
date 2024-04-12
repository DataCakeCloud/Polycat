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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import io.polycat.catalog.common.Constants;
import io.polycat.catalog.common.model.StorageDescriptor;
import io.polycat.catalog.common.model.Column;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@Data
@ApiModel(description = "table input")
public class TableInput {
    @ApiModelProperty(value = "catalog name", required = true, example = "c1")
    private String catalogName;

    @ApiModelProperty(value = "database name", required = true, example = "db1")
    private String databaseName;

    @ApiModelProperty(value = "table name", required = true, example = "t1")
    private String tableName;

    @ApiModelProperty(value = "description")
    private String description;

    @ApiModelProperty(value = "authentication source type")
    private String authSourceType;

    @ApiModelProperty(value = "account")
    private String accountId;

    @ApiModelProperty(value = "owner", required = true, example = "zhangsan")
    private String owner;

    @ApiModelProperty(value = "owner type", required = true, example = "USER")
    private String ownerType;

    @ApiModelProperty(value = "the table create time")
    private Long createTime; // Unit: second

    @ApiModelProperty(value = "the last time that table was accessed")
    private Long lastAccessTime; // Unit: second

    @ApiModelProperty(value = "the last time that table was update/create")
    private Long updateTime; // Unit: second

    @ApiModelProperty(value = "the last time that column statistics were computed for this table")
    private Long LastAnalyzedTime; // Unit: second，最后进行列级统计时间

    @ApiModelProperty(value = "the retention time for this table")
    private Integer retention;

    @ApiModelProperty(value = "partitions")
    private List<Column> partitionKeys;

    @ApiModelProperty(value = "table type")
    private String tableType;

    @ApiModelProperty(value = "storage input", required = true)
    private StorageDescriptor storageDescriptor;

    @ApiModelProperty(value = "properties")
    private Map<String, String> parameters = new HashMap<>();

    @ApiModelProperty(value = "lmsMvcc")
    private boolean lmsMvcc;

    @ApiModelProperty(value = "view Original test")
    private String viewOriginalText;

    @ApiModelProperty(value = "view Original test")
    private String viewExpandedText;

    public void setParameters(Map<String, String> parameters) {
        if (Objects.nonNull(parameters)) {
            this.parameters = new HashMap<>(parameters.size());
            this.parameters.putAll(parameters);
        }
    }

    public String getProperty(String name) {
        if (this.parameters != null) {
            return this.getParameters().get(name);
        }
        return null;
    }

    public String getDescription() {
        if (description == null) {
            return getProperty(Constants.COMMENT);
        }
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
        // Compatible with hive comment
        if (this.parameters == null) {
            this.parameters = new HashMap<>();
            this.parameters.put(Constants.COMMENT, description);
        }
    }
}
