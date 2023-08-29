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

import io.polycat.catalog.store.protos.common.ColumnInfo;
import io.polycat.catalog.store.protos.common.SchemaInfo;

import lombok.Data;


@Data
public class TableSchemaObject {

    private List<ColumnObject> columns = new ArrayList<>();
    private List<ColumnObject> partitionKeys = new ArrayList<>();

    private List<ColumnObject> trans2ColumnObjectList(List<ColumnInfo> columnInfoList) {
        List<ColumnObject> columnObjects = new ArrayList<>(columnInfoList.size());
        columnInfoList.forEach(columnInfo -> {
            columnObjects.add(new ColumnObject(columnInfo.getOrdinal(), columnInfo.getName(),
                columnInfo.getType(), columnInfo.getComment()));
        });
        return columnObjects;
    }

    public TableSchemaObject(SchemaInfo schemaInfo) {
        this.columns = trans2ColumnObjectList(schemaInfo.getColumnsList());
        this.partitionKeys = trans2ColumnObjectList(schemaInfo.getPartitionKeysList());
    }

    public TableSchemaObject(List<ColumnObject> columns, List<ColumnObject> partitionKeys) {
        this.columns = columns;
        this.partitionKeys = partitionKeys;
    }
}
