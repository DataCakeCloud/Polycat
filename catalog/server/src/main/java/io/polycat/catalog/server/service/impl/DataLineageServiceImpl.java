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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.DataLineageType;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.model.AppSource;
import io.polycat.catalog.common.model.DataLineage;
import io.polycat.catalog.common.model.DataLineageObject;
import io.polycat.catalog.common.model.DataSource;
import io.polycat.catalog.common.model.DataSourceType;
import io.polycat.catalog.common.model.FileSource;
import io.polycat.catalog.common.model.StreamerSource;
import io.polycat.catalog.common.model.TableObject;
import io.polycat.catalog.common.model.TableSource;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.plugin.request.input.DataLineageInput;
import io.polycat.catalog.service.api.DataLineageService;
import io.polycat.catalog.store.api.DataLineageStore;
import io.polycat.catalog.store.api.Transaction;
import io.polycat.catalog.store.common.StoreConvertor;
import io.polycat.catalog.common.model.TableIdent;
import io.polycat.catalog.common.model.TableName;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class DataLineageServiceImpl implements DataLineageService {
    private static final Logger logger = Logger.getLogger(DataLineageServiceImpl.class);
    @Autowired
    private Transaction storeTransaction;
    @Autowired
    private DataLineageStore dataLineageStore;

    @Override
    public void recordDataLineage(DataLineageInput dataLineageInput) {
        if (Objects.isNull(dataLineageInput.getDataLineage().getDataOutput())) {
            throw new CatalogServerException(ErrorCode.DATA_LINEAGE_OUTPUT_ERROR);
        }

        TableSource tableSource = dataLineageInput.getDataLineage().getDataOutput().getTableSource();
        TableObject table = TableObjectHelper.getTableObject(StoreConvertor.tableName(tableSource.getProjectId(),
            tableSource.getCatalogName(), tableSource.getDatabaseName(), tableSource.getTableName()));
        if (Objects.isNull(table)) {
            throw new CatalogServerException(ErrorCode.DATA_LINEAGE_SOURCE_ERROR, tableSource);
        }
        TableIdent targetTableIdent = StoreConvertor.tableIdent(table.getProjectId(),
            table.getCatalogId(), table.getDatabaseId(), table.getTableId(), null);
        DataLineageObject dataLineageObject = new DataLineageObject(targetTableIdent, DataSourceType.SOURCE_TABLE,
            "", dataLineageInput.getDataLineage().getOperation().toString());
        try (TransactionContext ctx = storeTransaction.openTransaction()) {
            for (DataSource dataSource : dataLineageInput.getDataLineage().getDataInput()) {
                dataLineageObject.setDataSourceType(dataSource.getSourceType());
                switch (dataSource.getSourceType()) {
                    case SOURCE_TABLE:
                        TableObject dataSourceTable = TableObjectHelper.getTableObject(
                            StoreConvertor.tableName(dataSource.getTableSource().getProjectId(),
                                dataSource.getTableSource().getCatalogName(),
                                dataSource.getTableSource().getDatabaseName(),
                                dataSource.getTableSource().getTableName()));
                        if (dataSourceTable == null) {
                            continue;
                        }
                        String strSourceTable = dataSourceTable.getProjectId() + ":"
                            + dataSourceTable.getCatalogId() + ":"
                            + dataSourceTable.getDatabaseId() + ":"
                            + dataSourceTable.getTableId();
                        dataLineageObject.setDataSourceContent(strSourceTable);
                        break;
                    case SOURCE_STREAM:
                        dataLineageObject.setDataSourceContent(dataSource.getStreamerSource().getStreamer());
                        break;
                    case SOURCE_FILE:
                        dataLineageObject.setDataSourceContent(dataSource.getFileSource().getPath());
                        break;
                    case SOURCE_APP:
                        dataLineageObject.setDataSourceContent(dataSource.getAppSource().getAppName());
                        break;
                    default:
                        throw new CatalogServerException(ErrorCode.DATA_LINEAGE_SOURCE_TYPE_ILLEGAL,
                            dataSource.getSourceType());
                }

                dataLineageStore.upsertDataLineage(ctx, dataLineageObject);
                ctx.commit();
            }
        }
    }

    @Override
    public List<DataLineage> getDataLineageByTable(String projectId, String catalogName, String databaseName,
        String tableName, DataLineageType dataLineageType) {
        TableName name = new TableName(projectId, catalogName, databaseName, tableName);
        TableObject table = TableObjectHelper.getTableObject(name);
        if (table == null) {
            return Collections.emptyList();
        }

        List<DataLineageObject> dataLineageObjects;
        try (TransactionContext ctx = storeTransaction.openTransaction()) {
            if (dataLineageType == DataLineageType.UPSTREAM) {
                dataLineageObjects = dataLineageStore.listDataLineageByTableId(ctx, projectId,
                    table.getCatalogId(), table.getDatabaseId(), table.getTableId());
            } else {
                String dataSourceContent = table.getProjectId() + ":" + table.getCatalogId() + ":"
                    + table.getDatabaseId() + ":" + table.getTableId();
                dataLineageObjects = dataLineageStore.listDataLineageByDataSource(ctx, table.getProjectId(),
                    DataSourceType.SOURCE_TABLE, dataSourceContent);
            }
        }


        List<DataLineage> dataLineageList = new ArrayList<>();
        for (DataLineageObject value : dataLineageObjects) {
            DataSource dataSource = null;
            switch (value.getDataSourceType()) {
                case SOURCE_TABLE:
                    String[] strSourceTable = value.getDataSourceContent().split(":");
                    if (strSourceTable.length < 4) {
                        break;
                    }
                    TableObject sourceTableRecord = TableObjectHelper.getTableObject(new TableIdent(strSourceTable[0],
                        strSourceTable[1], strSourceTable[2], strSourceTable[3]));
                    if (sourceTableRecord == null) {
                        break;
                    }
                    TableSource tableSource = new TableSource(projectId,
                        TableObjectConvertHelper.toTableModel(sourceTableRecord));
                    dataSource = new DataSource(tableSource);
                    break;
                case SOURCE_STREAM:
                    StreamerSource streamSource = new StreamerSource(value.getDataSourceContent());
                    dataSource = new DataSource(streamSource);
                    break;
                case SOURCE_FILE:
                    FileSource fileSource = new FileSource(value.getDataSourceContent());
                    dataSource = new DataSource(fileSource);
                    break;
                case SOURCE_APP:
                    AppSource appSource = new AppSource(value.getDataSourceContent());
                    dataSource = new DataSource(appSource);
                    break;
                default:
                    throw new CatalogServerException(ErrorCode.DATA_LINEAGE_SOURCE_TYPE_ILLEGAL,
                        value.getDataSourceType().toString());
            }
            if (dataSource != null) {
                List<DataSource> dataInputs = new ArrayList<>();
                dataInputs.add(dataSource);
                TableObject sourceTableRecord = TableObjectHelper
                    .getTableObject(new TableIdent(value.getProjectId(),
                        value.getCatalogId(), value.getDatabaseId(), value.getTableId()));
                TableSource tableSource = new TableSource(projectId,
                    TableObjectConvertHelper.toTableModel(sourceTableRecord));
                DataSource dataOutput = new DataSource();
                dataOutput.setTableSource(tableSource);
                dataOutput.setSourceType(value.getDataSourceType());
                DataLineage dataLineage = new DataLineage();
                dataLineage.setOperation(Operation.valueOf(value.getOperation()));
                dataLineage.setDataOutput(dataOutput);
                dataLineage.setDataInput(dataInputs);
                dataLineageList.add(dataLineage);
            }
        }
        return dataLineageList;
    }
}
