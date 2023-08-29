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

import java.util.ArrayList;
import java.util.List;

import io.polycat.catalog.common.utils.UuidUtil;

import lombok.Data;


@Data
public class TableCommitObject {
    private String commitId = "";
    private String projectId = "";
    private String catalogId = "";
    private String databaseId = "";
    private String tableId = "";
    private String tableName = "";
    private long createTime = 0;
    private long commitTime = 0;
    private List<OperationObject> operations = new ArrayList<>();
    private long droppedTime = 0;
    private String version = "";

    public TableCommitObject(String projectId, String catalogId, String databaseId,
        String tableId, String tableName, long createTime, long commitTime,
        List<OperationObject> operations, long droppedTime, String version) {
        this.commitId = UuidUtil.generateUUID32();
        this.projectId = projectId;
        this.catalogId = catalogId;
        this.databaseId = databaseId;
        this.tableId = tableId;
        this.tableName = tableName;
        this.createTime = createTime;
        this.commitTime = commitTime;
        this.operations = operations;
        this.droppedTime = droppedTime;
        this.version = version;
    }

}
