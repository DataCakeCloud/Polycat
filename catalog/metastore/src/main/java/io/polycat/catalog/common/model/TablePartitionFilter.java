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

import io.polycat.catalog.common.model.record.Record;
import io.polycat.common.expression.Expression;
import io.polycat.common.index.record.TablePartitionRecordBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class TablePartitionFilter {
    public List<PartitionObject> prune(final List<PartitionObject> partitions, final Expression filter,
        List<Column> schemaFields) {
        if (filter == null) {
            return partitions;
        }
        List<PartitionObject> newPartitions = new ArrayList<>(partitions.size());
        HashMap<String, String> schemaMap = new HashMap<>();
        schemaFields.forEach(schemaField -> {
            schemaMap.put(schemaField.getColumnName(), schemaField.getColType());
        });
        TablePartitionRecordBuilder builder = new TablePartitionRecordBuilder(filter, schemaMap, schemaFields);
        partitions.forEach(tablePartition -> {
            if (whetherPartitionMatchFilter(builder, tablePartition, schemaMap)) {
                newPartitions.add(tablePartition);
            }
        });
        return newPartitions;
    }

    private boolean whetherPartitionMatchFilter(TablePartitionRecordBuilder builder, PartitionObject partition,
        HashMap<String, String> schemaMap) {
        HashMap<String, String> columnMap = getPartitionColumnMap(partition);
        Record record = builder.build(columnMap, schemaMap);
        return builder.evaluateToBool(record);
    }

    private HashMap<String, String> getPartitionColumnMap(PartitionObject partition) {
        HashMap<String, String> keyValue = new HashMap<>();
        Arrays.stream(partition.getName().split("/")).forEach(item -> {
            String[] subPartition = item.split("=");
            keyValue.put(subPartition[0], subPartition[1]);
        });
        return keyValue;
    }
}
