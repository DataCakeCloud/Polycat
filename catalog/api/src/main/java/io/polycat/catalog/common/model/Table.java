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
import lombok.Data;

@ApiModel(description = "table")
@Data
public class Table implements Serializable {
    @ApiModelProperty(value = "table version", required = true, example = "db1")
    private String version;

    @ApiModelProperty(value = "catalog name", required = true, example = "c1")
    private String catalogName;

    @ApiModelProperty(value = "database name", required = true, example = "db1")
    private String databaseName;

    @ApiModelProperty(value = "table name", required = true, example = "t1")
    private String tableName;

    @ApiModelProperty(value = "description")
    private String description;

    @ApiModelProperty(value = "partitions", required = true)
    private List<Column> partitionKeys;

    @ApiModelProperty(value = "table type", required = true)
    private String tableType;

    // for undrop
    private String tableId;

    @ApiModelProperty(value = "table storage", required = true)
    private StorageDescriptor storageDescriptor;

    @ApiModelProperty(value = "table stats", required = true)
    private TableStats tableStats;

    @ApiModelProperty(value = "create time", required = true)
    private Long createTime;

    @ApiModelProperty(value = "last access time", required = true)
    private Long lastAccessTime;

    @ApiModelProperty(value = "authentication source type")
    private String authSourceType;

    @ApiModelProperty(value = "account")
    private String accountId;

    @ApiModelProperty(value = "ownerType", required = true, example = "USER")
    private String ownerType;

    @ApiModelProperty(value = "owner", required = true)
    private String owner;

    @ApiModelProperty(value = "lms mvcc", required = true)
    private boolean lmsMvcc;

    @ApiModelProperty(value = "the retention time of table")
    private Long retention;

    @ApiModelProperty(value = "dropped time", required = true)
    private Long droppedTime = 0L;

    @ApiModelProperty(value = "parameters")
    private Map<String, String> parameters = Collections.emptyMap();

    @ApiModelProperty(value = "If the table is a view, the original text of the view")
    private String viewOriginalText;

    @ApiModelProperty(value = "If the table is a view, the expanded text of the view")
    private String viewExpandedText;

    public List<Column> getFields() {
        if (storageDescriptor != null && storageDescriptor.getColumns() != null) {
            return storageDescriptor.getColumns();
        }
        return Collections.emptyList();
    }
}