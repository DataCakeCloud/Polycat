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


import io.polycat.catalog.store.protos.common.PartitionSetType;
import io.polycat.catalog.store.protos.common.TableDataInfo;
import lombok.Data;


@Data
public class TableHistoryObject {
    private String eventId = "";
    private TablePartitionSetType partitionSetType;
    private String curSetId = "";
    private List<String> setIds = new ArrayList<>();
    private String tableIndexUrl = "";
    private String version = "";

    public TableHistoryObject() {

    }

    public TableHistoryObject(TableHistoryObject src) {
        this.eventId = src.getEventId();
        this.partitionSetType = src.getPartitionSetType();
        this.curSetId = src.getCurSetId();
        this.setIds = src.getSetIds();
        this.tableIndexUrl = src.getTableIndexUrl();
        this.version = src.getVersion();
    }

    private TablePartitionSetType trans2TablePartitionSetType(PartitionSetType partitionSetType) {
        switch (partitionSetType) {
            case INIT:
                return TablePartitionSetType.INIT;
            case DATA:
                return TablePartitionSetType.DATA;
            case INDEX:
                return TablePartitionSetType.INDEX;
            default:
                throw new UnsupportedOperationException("failed to convert " + partitionSetType.name());
        }
    }

    public TableHistoryObject(TableDataInfo tableDataInfo, String eventId, String version) {
        this.eventId = eventId;
        this.version = version;
        this.partitionSetType = trans2TablePartitionSetType(tableDataInfo.getPartitionType());
        this.curSetId = tableDataInfo.getCurSetId();
        this.setIds = tableDataInfo.getSetIdsList();
        this.tableIndexUrl = tableDataInfo.getTableIndexUrl();
    }

    public void clearSetIds() {
        this.setIds = new ArrayList<>();
    }

    public void addSetId(String setId) {
        this.setIds.add(setId);
    }

}
