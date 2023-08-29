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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;


import io.polycat.catalog.common.types.DataTypes;
import io.polycat.catalog.store.protos.common.ColumnInfo;
import lombok.AllArgsConstructor;
import io.polycat.catalog.common.types.DataType;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ColumnObject {
    private int ordinal = -1;
    private String name = "";
    private DataType dataType;
    private String comment = "";

    public ColumnObject(int ordinal, String name, String type, String comment) {
        this.ordinal = ordinal;
        this.name = name;
        this.dataType = DataTypes.valueOf(type);
        this.comment = (comment == null ? "" : comment);
    }

    public void updateColumnObject(ColumnObject columnObject, String name, String type, String comment) {
        columnObject.name = name;
        columnObject.dataType = DataTypes.valueOf(type);
        if (comment != null) {
            columnObject.comment = comment;
        }
    }

    public ColumnObject(ColumnObject columnObject) {
        this.name = columnObject.getName();
        this.dataType = columnObject.getDataType();
        this.ordinal = columnObject.getOrdinal();
        this.comment = columnObject.getComment();
    }

    public ColumnObject(ColumnInfo columnInfo) {
        this.ordinal = columnInfo.getOrdinal();
        this.name = columnInfo.getName();
        this.dataType = DataTypes.valueOf(columnInfo.getType());
        this.comment = (columnInfo.getComment() == null ? "" : columnInfo.getComment());
    }
}
