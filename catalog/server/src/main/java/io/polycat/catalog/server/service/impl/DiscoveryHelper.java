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

import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.model.ColumnObject;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.DatabaseObject;
import io.polycat.catalog.common.model.TableBaseObject;
import io.polycat.catalog.common.model.TableName;
import io.polycat.catalog.common.model.TableSchemaObject;
import io.polycat.catalog.common.model.TableStorageObject;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.discovery.DatabaseSearch;
import io.polycat.catalog.common.model.discovery.TableSearch;
import io.polycat.catalog.common.utils.SegmenterUtils;
import io.polycat.catalog.common.utils.UuidUtil;
import io.polycat.catalog.store.api.DiscoveryStore;
import io.polycat.catalog.store.gaussdb.pojo.DiscoverySearchRecord;
import java.util.List;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Slf4j(topic = "discoveryLogger")
@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class DiscoveryHelper {

    private static DiscoveryStore discoveryStore;

    @Autowired
    public void setDiscoveryStore(DiscoveryStore discoveryStore) {
        this.discoveryStore = discoveryStore;
    }

    public static void updateDatabaseDiscoveryInfo(TransactionContext context, DatabaseName databaseName, DatabaseObject databaseObject) {
        DiscoverySearchRecord record = new DiscoverySearchRecord();
        record.setId(UuidUtil.generateUUID32());
        record.setCatalogName(databaseName.getCatalogName());
        record.setQualifiedName(databaseName.getQualifiedName());
        record.setOwner(databaseObject.getUserId());
        record.setObjectType(ObjectType.DATABASE.getNum());
        long currentTimeMillis = System.currentTimeMillis();
        record.setCreateTime(currentTimeMillis);
        record.setUpdateTime(currentTimeMillis);
        final DatabaseSearch databaseSearch = new DatabaseSearch();
        databaseSearch.setDatabaseName(databaseObject.getName());
        databaseSearch.setDescription(databaseObject.getDescription());
        databaseSearch.setCreateTime(databaseObject.getCreateTime());
        databaseSearch.setQualifiedName(databaseName.getQualifiedName());
        databaseSearch.setObjectType(ObjectType.DATABASE.name());
        databaseSearch.setOwner(databaseObject.getUserId());
        databaseSearch.setCatalogName(databaseName.getCatalogName());
        databaseSearch.setLocation(databaseObject.getLocation());
        record.setSearchInfo(databaseSearch);
        setTsATokens(record, databaseName);
        record.setTsBTokens(databaseObject.getUserId());
        record.setTsCTokens(hanLPSegment(databaseObject.getDescription()));
        record.setKeywords(String.format("%s %s %s %s", databaseName.getQualifiedName(), databaseObject.getUserId(), databaseObject.getDescription(), databaseObject.getLocation()));
        discoveryStore.updateDiscoveryInfo(context, databaseName.getProjectId(), record);
    }

    public static void updateTableDiscoveryInfo(TransactionContext context, TableName tableName,
            TableBaseObject tableBaseObject, TableSchemaObject tableSchema,
            TableStorageObject tableStorage) {
        try {
            DiscoverySearchRecord record = new DiscoverySearchRecord();
            record.setId(UuidUtil.generateUUID32());
            record.setCatalogName(tableName.getCatalogName());
            record.setQualifiedName(tableName.getQualifiedName());
            record.setOwner(tableBaseObject.getOwner());
            record.setObjectType(ObjectType.TABLE.getNum());
            long currentTimeMillis = System.currentTimeMillis();
            record.setCreateTime(currentTimeMillis);
            record.setUpdateTime(currentTimeMillis);
            TableSearch tableSearch = new TableSearch();
            tableSearch.setDescription(tableBaseObject.getDescription());
            tableSearch.setCreateTime(tableBaseObject.getCreateTime());
            tableSearch.setQualifiedName(tableName.getQualifiedName());
            tableSearch.setObjectType(ObjectType.TABLE.name());
            tableSearch.setOwner(tableBaseObject.getOwner());
            tableSearch.setCatalogName(tableName.getCatalogName());
            tableSearch.setDatabaseName(tableName.getDatabaseName());
            tableSearch.setTableName(tableName.getTableName());
            tableSearch.setLocation(tableStorage.getLocation());
            tableSearch.setSdFileFormat(tableStorage.getFileFormat());
            if (tableSchema.getPartitionKeys() != null) {
                tableSearch.setPartitionKeys(TableObjectConvertHelper.convertColumn(tableSchema.getPartitionKeys()));
            }
            if (tableBaseObject.getParameters() != null) {
                String ddlTime = tableBaseObject.getParameters().get("transient_lastDdlTime");
                if (ddlTime != null) {
                    tableSearch.setTransientLastDdlTime(Long.parseLong(ddlTime));
                } else {
                    tableSearch.setTransientLastDdlTime(tableBaseObject.getCreateTime());
                }
            }
            record.setSearchInfo(tableSearch);
            setTsATokens(record, tableName);
            record.setTsBTokens(tableBaseObject.getOwner());
            record.setTsCTokens(hanLPSegment(tableBaseObject.getDescription()));
            String fieldTsVectors = getFieldTsVectors(tableSchema);
            record.setTsDTokens(fieldTsVectors);
            record.setKeywords(String.format("%s %s %s %s %s", tableName.getQualifiedName(), tableBaseObject.getOwner(), tableBaseObject.getDescription(), fieldTsVectors, tableStorage.getLocation()));
            discoveryStore.updateDiscoveryInfo(context, tableName.getProjectId(), record);
        } catch (Exception e) {
            log.error("Discovery store upsert record: {}, cause: {}.", tableName.getQualifiedName(), e.getMessage());
        }

    }

    private static void setTsATokens(DiscoverySearchRecord record, TableName tableName) {
        List<String> segment = SegmenterUtils.simpleSegment(tableName.getQualifiedName(), "\\.");
        record.setTsATokens(getTsVector(segment));
        segment.add(String.format("%s.%s", tableName.getDatabaseName(), tableName.getTableName()));
        segment.add(tableName.getQualifiedName());
        setTsFixedIndexTokens(segment);
        record.setTsASetIndexTokens(getTsVector(segment));
    }

    private static void setTsATokens(DiscoverySearchRecord record, DatabaseName databaseName) {
        List<String> segment = SegmenterUtils.simpleSegment(databaseName.getQualifiedName(), "\\.");
        record.setTsATokens(getTsVector(segment));
        segment.add(databaseName.getQualifiedName());
        setTsFixedIndexTokens(segment);
        record.setTsASetIndexTokens(getTsVector(segment));
    }

    private static void setTsFixedIndexTokens(List<String> segment) {
        if (segment != null && segment.size() > 0) {
            for (int i = 0; i < segment.size(); i++) {
                segment.set(i, segment.get(i).concat(":").concat(String.valueOf(i + 1)));
            }
        }
    }

    private static String getFieldTsVectors(TableSchemaObject tableSchema) {
        StringBuffer buffer = new StringBuffer();
        appendFieldTsVector(tableSchema.getColumns(), buffer);
        appendFieldTsVector(tableSchema.getPartitionKeys(), buffer);
        return buffer.toString();
    }

    private static void appendFieldTsVector(List<ColumnObject> columns, StringBuffer buffer) {
        if (CollectionUtils.isNotEmpty(columns)) {
            for (ColumnObject column: columns) {
                buffer.append(column.getName()).append(" ").append(hanLPSegment(column.getComment())).append(" ");
            }
        }
    }

    private static String simpleSegment(String content) {
        List<String> segment = SegmenterUtils.simpleSegment(content, "\\.");
        return getTsVector(segment);
    }

    private static String hanLPSegment(String content) {
        List<String> segment = SegmenterUtils.hanLPSegment(content);
        return getTsVector(segment);
    }

    private static String getTsVector(List<String> segment) {
        return segment.stream().filter(x -> x.trim().length() > 0).collect(Collectors.joining(" "));
    }

    public static void dropTableDiscoveryInfo(TransactionContext context, TableName tableName) {
        try {
            discoveryStore.dropDiscoveryInfo(context, tableName.getProjectId(), tableName.getQualifiedName(), ObjectType.TABLE);
        } catch (Exception e) {
            log.error("Discovery store drop record: {}, cause: {}.", tableName.getQualifiedName(), e.getMessage());
        }
    }

    public static void dropDatabaseDiscoveryInfo(TransactionContext context, DatabaseName databaseName) {
        try {
            discoveryStore.dropDiscoveryInfo(context, databaseName.getProjectId(), databaseName.getQualifiedName(), ObjectType.DATABASE);
        } catch (Exception e) {
            log.error("Discovery store drop record: {}, cause: {}.", databaseName.getQualifiedName(), e.getMessage());
        }
    }


}
