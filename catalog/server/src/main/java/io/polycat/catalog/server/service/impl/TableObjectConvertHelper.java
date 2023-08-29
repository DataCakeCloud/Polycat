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
package io.polycat.catalog.server.service.impl;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.polycat.catalog.common.model.ColumnObject;
import io.polycat.catalog.common.model.SchemaUtil;
import io.polycat.catalog.common.model.SerDeInfo;
import io.polycat.catalog.common.model.SkewedInfo;
import io.polycat.catalog.common.model.Table;
import io.polycat.catalog.common.model.TableIdent;
import io.polycat.catalog.common.model.TableName;
import io.polycat.catalog.common.model.TableBaseObject;
import io.polycat.catalog.common.model.TableObject;
import io.polycat.catalog.common.model.StorageDescriptor;
import io.polycat.catalog.common.model.TableSchemaObject;
import io.polycat.catalog.common.model.TableStorageObject;
import io.polycat.catalog.common.model.Column;

import io.polycat.catalog.common.plugin.request.input.TableTypeInput;
import io.polycat.catalog.common.types.DataTypes;
import org.apache.logging.log4j.util.Strings;

public class TableObjectConvertHelper {
    private static final String dateFormat = "yyyy-MM-dd HH:mm:ss";
    private static final SimpleDateFormat SDF = new SimpleDateFormat(dateFormat);

    public static Table toTableModel(TableObject tableObject) {
        if (null == tableObject) {
            return null;
        }

        Table table = new Table();
        table.setCatalogName(tableObject.getCatalogName());
        table.setTableId(tableObject.getTableId());
        table.setDatabaseName(tableObject.getDatabaseName());
        table.setTableName(tableObject.getName());
        table.setOwner(tableObject.getTableBaseObject().getOwner());
        table.setLmsMvcc(tableObject.getTableBaseObject().isLmsMvcc());
        table.setTableType(tableObject.getTableBaseObject().getTableType());
        table.setCreateTime(tableObject.getTableBaseObject().getCreateTime());
        table.setAccountId(Strings.EMPTY);
        table.setAuthSourceType(Strings.EMPTY);
        table.setOwnerType("USER");
        table.setRetention(0L);
        table.setViewExpandedText(tableObject.getTableBaseObject().getViewExpandedText());
        table.setViewOriginalText(tableObject.getTableBaseObject().getViewOriginalText());
        table.setLastAccessTime(System.currentTimeMillis());
        if (tableObject.getDroppedTime() != 0) {
            table.setDroppedTime(tableObject.getDroppedTime());
        } else {
            table.setPartitionKeys(convertColumn(tableObject.getTableSchemaObject().getPartitionKeys()));
            table.setStorageDescriptor(toStorageDescriptor(tableObject));
            table.setParameters(tableObject.getTableBaseObject().getParameters());
        }
        return table;
    }


    private static List<Column> convertColumn(List<ColumnObject> columns) {
        List<Column> columnOutputs = new ArrayList<>(columns.size());
        for (ColumnObject column : columns) {
            String dataType;
            dataType = column.getDataType().toString();
            columnOutputs.add(new Column(column.getName(), dataType, column.getComment()));
        }
        return columnOutputs;
    }

    private static StorageDescriptor toStorageDescriptor(TableObject tableObject) {
        TableStorageObject tableStorageObject = tableObject.getTableStorageObject();
        if (tableObject.getTableSchemaObject().getColumns() == null && tableStorageObject == null) {
            return null;
        }

        StorageDescriptor tableStorage =  new StorageDescriptor();
        tableStorage.setColumns(convertColumn(tableObject.getTableSchemaObject().getColumns()));
        if (tableStorageObject != null) {
            // compatibility history proto storage_info.proto field=location: view location=null
            if (TableTypeInput.VIRTUAL_VIEW.name().equals(tableObject.getTableBaseObject().getTableType())) {
                tableStorage.setLocation(null);
            } else {
                tableStorage.setLocation(tableStorageObject.getLocation());
            }
            tableStorage.setSourceShortName(tableStorageObject.getSourceShortName());
            tableStorage.setFileFormat(tableStorageObject.getFileFormat());
            tableStorage.setInputFormat(tableStorageObject.getInputFormat());
            tableStorage.setOutputFormat(tableStorageObject.getOutputFormat());
            tableStorage.setSerdeInfo(toSerDeInfo(tableStorageObject));
            tableStorage.setParameters(tableStorageObject.getParameters());

            // todo: set based on stored table object once underlying structure complement these fields
            tableStorage.setCompressed(tableStorageObject.getCompressed());
            tableStorage.setNumberOfBuckets(tableStorageObject.getNumberOfBuckets());
            tableStorage.setBucketColumns(tableStorageObject.getBucketColumns());
            tableStorage.setSortColumns(tableStorageObject.getSortColumns());
            tableStorage.setSkewedInfo(
                    new SkewedInfo(Collections.emptyList(), Collections.emptyList(), Collections.emptyMap()));
            tableStorage.setStoredAsSubDirectories(false);
        }

        return tableStorage;
    }

    private static SerDeInfo toSerDeInfo(TableStorageObject tableStorageObject) {
        SerDeInfo serDeInfo = null;
        if (tableStorageObject != null) {
            serDeInfo = tableStorageObject.getSerdeInfo();
        }
        return serDeInfo;
    }
}
