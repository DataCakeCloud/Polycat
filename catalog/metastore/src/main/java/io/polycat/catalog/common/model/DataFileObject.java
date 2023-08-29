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

import io.polycat.catalog.store.protos.common.DataFile;
import lombok.Data;

@Data
public class DataFileObject {
    private String fileName;
    private long offset;
    private long length;
    private long rowCount;

    public DataFileObject(DataFile dataFile) {
        this.fileName = dataFile.getFileName();
        this.offset = dataFile.getOffset();
        this.length = dataFile.getLength();
        this.rowCount = dataFile.getRowCount();
    }

    public DataFileObject(String fileName, long offset, long length, long rowCount) {
        this.fileName = fileName;
        this.offset = offset;
        this.length = length;
        this.rowCount = rowCount;
    }

}
