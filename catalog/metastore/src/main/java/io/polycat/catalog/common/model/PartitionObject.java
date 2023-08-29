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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.polycat.catalog.common.utils.CodecUtil;
import io.polycat.catalog.store.protos.common.DataFile;
import io.polycat.catalog.store.protos.common.FileStats;
import io.polycat.catalog.store.protos.common.Partition;
import io.polycat.catalog.store.protos.common.PartitionType;
import lombok.Data;

import static java.util.stream.Collectors.toList;

@Data
public class PartitionObject {
    private String name = "";
    private TablePartitionType type;
    private String partitionId = "";
    private String schemaVersion = "";  // link to TableSchemaHistory
    private boolean invisible;     // indicate from which subspace to find schema
    private String location = "";
    private List<ColumnObject> partitionKeys = new ArrayList<>();
    private List<ColumnObject> column = new ArrayList<>();
    private List<DataFileObject> file = new ArrayList<>();
    private List<FileStatsObject> stats = new ArrayList<>();
    private String partitionIndexUrl = "";
    private String fileFormat = "";
    private String inputFormat = "";
    private String outputFormat = "";
    private String serde = "";
    private Map<String, String> properties = Collections.emptyMap();
    private long startTime = 0;
    private long endTime = 0;

    public PartitionObject() {
        this.properties = new HashMap<>();
    }

    public PartitionObject(PartitionObject src) {
        this.name = src.getName();
        this.type = src.getType();
        this.partitionId = src.getPartitionId();
        this.schemaVersion = src.getSchemaVersion();
        this.invisible = src.isInvisible();
        this.location = src.getLocation();
        this.partitionKeys = src.getPartitionKeys();
        this.column = src.getColumn();
        this.file = src.getFile();
        this.stats = src.getStats();
        this.partitionIndexUrl = src.getPartitionIndexUrl();
        this.fileFormat = src.getFileFormat();
        this.inputFormat = src.getInputFormat();
        this.outputFormat = src.getOutputFormat();
        this.serde = src.getSerde();
        this.properties = new HashMap<>();
        this.properties.putAll(src.getProperties());
        this.startTime = src.getStartTime();
        this.endTime = src.getEndTime();
    }

    public PartitionObject(Partition partition) {
        this.name = partition.getName();
        this.type = TablePartitionType.getTablePartitionType(partition.getType());
        this.partitionId = partition.getPartitionId();
        this.schemaVersion = CodecUtil.byteString2Hex(partition.getSchemaVersion());
        this.invisible = partition.getInvisible();
        this.location = partition.getLocation();
        this.column = partition.getColumnList().stream().map(column -> new ColumnObject(column)).collect(toList());
        this.file = partition.getFileList().stream().map(file -> new DataFileObject(file)).collect(toList());
        this.stats = partition.getStatsList().stream().map(fileStats -> new FileStatsObject(fileStats)).collect(toList());
        this.partitionIndexUrl = partition.getPartitionIndexUrl();
        this.fileFormat = partition.getFileFormat();
        this.inputFormat = partition.getInputFormat();
        this.outputFormat = partition.getOutputFormat();
        this.serde = partition.getSerde();
        this.properties = new HashMap<>();
        this.properties = partition.getPropertiesMap();
        this.startTime = partition.getStartTime();
        this.endTime = partition.getEndTime();
    }

}
