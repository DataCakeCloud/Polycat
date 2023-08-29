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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import io.polycat.catalog.common.utils.CodecUtil;
import io.polycat.common.expression.Expression;
import io.polycat.common.expression.bool.FalseExpression;
import io.polycat.common.expression.bool.TrueExpression;
import io.polycat.common.expression.rewrite.TableMinMaxFilterRewriter;
import io.polycat.catalog.common.model.record.Record;

import com.google.protobuf.ByteString;


public class MinMaxIndex {

    /**
     * get all hit splits of partitions
     */
    public List<FileGroupObject> prune(final List<ColumnObject> tableSchema, final List<PartitionObject> partitions,
            final Map<String, List<ColumnObject>> partitionSchemaMap, final Expression filter) {
        if (filter == null) {
            return listFileOfPartitions(partitions, partitionSchemaMap);
        }
        return pruneFileOfPartitions(tableSchema, partitions, partitionSchemaMap, filter);
    }

    /**
     * list all splits of partitions without filter
     */
    private List<FileGroupObject> listFileOfPartitions(List<PartitionObject> partitions,
            Map<String, List<ColumnObject>> partitionSchemaMap) {
        return partitions.stream()
                .map(partition -> listFileOfPartition(partition, getPartitionSchema(partition, partitionSchemaMap)))
                .collect(Collectors.toList());
    }

    /**
     * list all splits of one partition without filter
     */
    private FileGroupObject listFileOfPartition(PartitionObject partition, List<ColumnObject> partitionSchema) {
        FileGroupObject fileGroupObject = new FileGroupObject();
        fileGroupObject.setName(partition.getName());
        fileGroupObject.setBaseLocation(partition.getLocation());
        fileGroupObject.setDataFile(partition.getFile());
        fileGroupObject.setColumn(partitionSchema);
        fileGroupObject.setPartitionIndexUrl(partition.getPartitionIndexUrl());

        return fileGroupObject;
    }

    /**
     * get some splits of partitions which the min/max match the filter
     */
    private List<FileGroupObject> pruneFileOfPartitions(List<ColumnObject> tableSchema, List<PartitionObject> partitions,
            Map<String, List<ColumnObject>> partitionSchemaMap, Expression filter) {
        final Expression minMaxFilter = TableMinMaxFilterRewriter.getInstance().execute(filter);
        if (minMaxFilter.equals(TrueExpression.getInstance())) {
            return listFileOfPartitions(partitions, partitionSchemaMap);
        }
        if (minMaxFilter.equals(FalseExpression.getInstance())) {
            return Collections.emptyList();
        }
        return partitions.stream()
                .map(partition -> {
                    List<ColumnObject> partitionSchema = getPartitionSchema(partition, partitionSchemaMap);
                    return pruneFileOfPartition(tableSchema, partition, partitionSchema, minMaxFilter);
                }).filter(Objects::nonNull).collect(Collectors.toList());
    }

    /**
     * get some splits of one partition which the min/max match the filter
     */
    private FileGroupObject pruneFileOfPartition(List<ColumnObject> tableSchema, PartitionObject partition,
            List<ColumnObject> partitionSchema, Expression minMaxFilter) {
        PartitionMinMaxFilter partitionMinMaxFilter = new PartitionMinMaxFilter(tableSchema, partitionSchema, minMaxFilter);
        MinMaxRecordBuilder builder = new MinMaxRecordBuilder(partitionMinMaxFilter);
        Expression partitionExpression = partitionMinMaxFilter.getPartitionExpression();

        FileGroupObject fileGroupObject = new FileGroupObject();
        fileGroupObject.setColumn(partitionSchema);
        fileGroupObject.setName(partition.getName());
        fileGroupObject.setBaseLocation(partition.getLocation());

        if (partition.getType() == TablePartitionType.EXTERNAL || partitionExpression.equals(TrueExpression.getInstance())) {
            fileGroupObject.setDataFile(partition.getFile());
            fileGroupObject.setPartitionIndexUrl(partition.getPartitionIndexUrl());
            return fileGroupObject;
        }
        if(partitionExpression.equals(FalseExpression.getInstance())) {
            return null;
        }
        int numberOfFiles = partition.getFile().size();
        if (numberOfFiles == 0) {
            return null;
        }
        int numberOfStats = partition.getStats().size();
        if (numberOfStats == 0) {
            fileGroupObject.setDataFile(partition.getFile());
            return fileGroupObject;
        } else {
            List<DataFileObject> matchedDataFiles = new ArrayList<>(numberOfFiles);
            for (int i = 0; i < numberOfFiles; i++) {
                Record record = builder.build(partition.getStats().get(i));
                if (partitionExpression.evaluateToBool(record)) {
                    matchedDataFiles.add(partition.getFile().get(i));
                }
            }
            if (matchedDataFiles.isEmpty()) {
                return null;
            }

            fileGroupObject.setDataFile(matchedDataFiles);
            return fileGroupObject;
        }
    }


    private List<ColumnObject> getPartitionSchema(PartitionObject partition, Map<String, List<ColumnObject>> schemaMap) {
        List<ColumnObject> columnList = partition.getColumn();
        if (!partition.getSchemaVersion().isEmpty()) {
            columnList = schemaMap.get(partition.getSchemaVersion());
        }
        if (columnList.isEmpty()) {
            throw new RuntimeException("failed to get schema for partition:" + partition.getPartitionId());
        }
        return columnList;
    }
}
