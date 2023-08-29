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


import io.polycat.catalog.common.model.record.Field;
import io.polycat.catalog.common.model.record.NullWritable;
import io.polycat.catalog.common.model.record.Record;
import io.polycat.catalog.common.types.DataType;

public class MinMaxRecordBuilder {

    final Record record;
    final List<StatsLoad> statsLoads;

    public MinMaxRecordBuilder(PartitionMinMaxFilter filter) {
        int numberOfColumn = filter.getNumberOfMinMaxColumn();
        record = new Record(numberOfColumn);
        for (int i = 0; i < numberOfColumn; i++) {
            record.add(NullWritable.getInstance());
        }
        statsLoads = new ArrayList<>(numberOfColumn);
        initialize(filter);
    }

    public Record build(FileStatsObject fileStats) {
        for (StatsLoad statsLoad : statsLoads) {
            statsLoad.load(fileStats);
        }
        return record;
    }

    private void initialize(PartitionMinMaxFilter filter) {
        int numberOfColumn = filter.getNumberOfColumn();
        int minIndex, maxIndex;
        for (int i = 0; i < numberOfColumn; i++) {
            if (filter.isFieldPresentInPartition(i)) {
                minIndex = i * 2;
                maxIndex = minIndex + 1;
                if (filter.isMinMaxPresent(minIndex)) {
                    addMinIndexStatLoad(minIndex, filter.dataTypeOfColumn(i), filter.fieldIndexInPartition(i));
                }
                if (filter.isMinMaxPresent(maxIndex)) {
                    addMaxIndexStatLoad(maxIndex, filter.dataTypeOfColumn(i), filter.fieldIndexInPartition(i));
                }
            }
        }
    }

    private void addMinIndexStatLoad(int fieldIndexInRecord, DataType dataType, int fieldIndexInPartition) {
        Field field = DataTypeUtils.toField(dataType);
        record.setValue(fieldIndexInRecord, field);
        statsLoads.add(new StatsLoad(field, new StatsExtractor.MinExtractor(fieldIndexInPartition)));
    }

    private void addMaxIndexStatLoad(int fieldIndexInRecord, DataType dataType, int fieldIndexInPartition) {
        Field field = DataTypeUtils.toField(dataType);
        record.setValue(fieldIndexInRecord, field);
        statsLoads.add(new StatsLoad(field, new StatsExtractor.MaxExtractor(fieldIndexInPartition)));
    }
}
