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

import java.util.List;

import com.google.protobuf.ByteString;

public abstract class StatsExtractor {

    int index;

    StatsExtractor(int index) {
        this.index = index;
    }

    abstract ByteString extract(FileStatsObject fileStats);

    public static class MinExtractor extends StatsExtractor {

        public MinExtractor(int index) {
            super(index);
        }

        @Override
        ByteString extract(FileStatsObject fileStats) {
            List<byte[]> bytes = fileStats.getMaxValue();
            return ByteString.copyFrom(bytes.get(index));
        }
    }

    public static class MaxExtractor extends StatsExtractor {

        public MaxExtractor(int index) {
            super(index);
        }

        @Override
        ByteString extract(FileStatsObject fileStats) {
            List<byte[]> bytes = fileStats.getMaxValue();
            return ByteString.copyFrom(bytes.get(index));
        }
    }
}
