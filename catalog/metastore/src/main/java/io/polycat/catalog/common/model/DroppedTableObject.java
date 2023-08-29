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

import io.polycat.catalog.store.gaussdb.pojo.DroppedTableRecord;

import lombok.Data;

@Data
public class DroppedTableObject {
    private String name;
    private String objectId;
    private long createTime;
    private long droppedTime;
    private boolean dropPurge;

    public DroppedTableObject(String name, String objectId, long createTime, long droppedTime, boolean dropPurge) {
        this.name = name;
        this.objectId = objectId;
        this.createTime = createTime;
        this.droppedTime = droppedTime;
        this.dropPurge = dropPurge;
    }

    public DroppedTableObject(DroppedTableRecord droppedTableRecord) {
        this.name = droppedTableRecord.getTableName();
        this.objectId = droppedTableRecord.getTableId();
        this.createTime = droppedTableRecord.getCreateTime();
        this.droppedTime = droppedTableRecord.getDroppedTime();
        this.dropPurge = droppedTableRecord.isPurge();
    }
}
