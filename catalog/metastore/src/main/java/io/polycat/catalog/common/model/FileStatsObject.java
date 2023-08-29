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

import io.polycat.catalog.store.protos.common.FileStats;
import lombok.Data;

import static java.util.stream.Collectors.toList;

@Data
public class FileStatsObject {
    private List<byte[]> minValue;
    private List<byte[]> maxValue;

    public FileStatsObject() {
        this.minValue = new ArrayList<>();
        this.maxValue = new ArrayList<>();
    }

    public FileStatsObject(FileStats fileStats) {
        this.minValue = fileStats.getMinValueList().stream().map(bytes -> bytes.toByteArray()).collect(toList());
        this.maxValue = fileStats.getMaxValueList().stream().map(bytes -> bytes.toByteArray()).collect(toList());
    }

    public FileStatsObject(List<byte[]> minValue, List<byte[]> maxValue) {
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    public void addMinValue(byte[] min) {
        this.minValue.add(min);
    }

    public void addMaxValue(byte[] max) {
        this.maxValue.add(max);
    }

    public void addAllMinValue(List<byte[]> minList) {
        this.minValue.addAll(minList);
    }

    public void addAllMaxValue(List<byte[]> maxList) {
        this.maxValue.addAll(maxList);
    }
}
