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
import java.util.stream.Collectors;

import io.polycat.catalog.store.protos.common.TableDataPartitionSetInfo;
import lombok.Data;

@Data
public class DataPartitionSetObject {
    private String setId;
    private String catalogId;
    private String databaseId;
    private String tableId;
    private long partitionsSize;
    private List<PartitionObject> dataPartitions;

    public DataPartitionSetObject() {
        this.dataPartitions = new ArrayList<>();
    }

    public DataPartitionSetObject(DataPartitionSetObject src) {
        this.setId = src.getSetId();
        this.catalogId = src.getCatalogId();
        this.databaseId = src.getDatabaseId();
        this.tableId = src.getTableId();
        this.partitionsSize = src.getPartitionsSize();
        this.dataPartitions = src.getDataPartitions();
    }

    public DataPartitionSetObject(String setId, String catalogId, String databaseId, String tableId,
        TableDataPartitionSetInfo setInfo) {
        this.setId = setId;
        this.catalogId = catalogId;
        this.databaseId = databaseId;
        this.tableId = tableId;
        if (setInfo != null) {
            this.dataPartitions = new ArrayList<>();
            this.partitionsSize = setInfo.getPartitionsSize();
            this.dataPartitions
                .addAll(setInfo.getDataPartitionsList().stream().map(partition -> new PartitionObject(partition)).collect(
                    Collectors.toList()));
        }
    }

    public void addDataPartition(PartitionObject partitionObject) {
      this.dataPartitions.add(partitionObject);
    }
}
