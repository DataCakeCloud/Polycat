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

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldNameConstants;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
@NoArgsConstructor
@FieldNameConstants
public class UsageProfileObject {
    private String id;
    private String projectId;
    private String catalogName;
    private String databaseName;
    private String tableName;
    private String tableId;
    private long createDayTime;
    private long createTime;
    private String opType;
    private String originOpType;
    private long count;
    private String userId;
    private String userGroup;
    private String taskId;
    private String tag;
    private String statement;

    public UsageProfileObject(String projectId, String catalogName, String databaseName, String tableName, long createTime,
                              String opType, long count) {
        this.projectId = projectId;
        this.catalogName = catalogName;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.createTime = createTime;
        this.opType = opType;
        this.count = count;
    }

}
