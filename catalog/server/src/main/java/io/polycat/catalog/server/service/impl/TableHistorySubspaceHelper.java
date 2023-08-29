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

import io.polycat.catalog.common.model.TableIdent;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.store.api.TableDataStore;
import io.polycat.catalog.store.api.TableMetaStore;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class TableHistorySubspaceHelper {
    private static final int baseHistorySubspaceFlag = 0x1;

    private static final int schemaHistorySubspaceFlag = 0x1 << 1;

    private static final int storageHistorySubspaceFlag = 0x1 << 2;

    private static final int commitSubspaceFlag = 0x1 << 3;

    private static final int dataHistorySubspaceFlag = 0x1 << 4;

    private static final int indexHistorySubspaceFlag = 0x1 << 5;

    private static TableMetaStore tableMetaStore;

    private static TableDataStore tableDataStore;

    @Autowired
    public void setTableMetaStore(TableMetaStore tableMetaStore) {
        this.tableMetaStore = tableMetaStore;
    }

    @Autowired
    public void setTableDataStore(TableDataStore tableDataStore) {
        this.tableDataStore = tableDataStore;
    }

    public static int createHistorySubspace(TransactionContext context, TableIdent tableIdent) {
        int historySubspaceFlag = baseHistorySubspaceFlag | schemaHistorySubspaceFlag
            | storageHistorySubspaceFlag | commitSubspaceFlag | dataHistorySubspaceFlag | indexHistorySubspaceFlag;
        return historySubspaceFlag;
    }

    public static int createHistorySubspace(TransactionContext context, TableIdent tableIdent, int historySubspaceFlag) {
        historySubspaceFlag = createBaseHistorySubspace(context,tableIdent, historySubspaceFlag);
        historySubspaceFlag = createSchemaHistorySubspace(context,tableIdent, historySubspaceFlag);
        historySubspaceFlag = createStorageHistorySubspace(context,tableIdent, historySubspaceFlag);
        historySubspaceFlag = createCommitSubspace(context, tableIdent, historySubspaceFlag);
        historySubspaceFlag = createDataHistorySubspace(context, tableIdent, historySubspaceFlag);
        historySubspaceFlag = createIndexHistorySubspace(context, tableIdent, historySubspaceFlag);
        return historySubspaceFlag;
    }

    public static boolean baseHistorySubspaceExist(int historySubspaceFlag) {
        return (historySubspaceFlag & baseHistorySubspaceFlag) == baseHistorySubspaceFlag;
    }

    public static int createIndexHistorySubspace(TransactionContext context, TableIdent tableIdent,
                                                int historySubspaceFlag) {
        if (!baseHistorySubspaceExist(historySubspaceFlag)) {
            tableDataStore.createTableIndexHistorySubspace(context, tableIdent.getProjectId());
            historySubspaceFlag |= baseHistorySubspaceFlag;
        }
        return historySubspaceFlag;
    }

    public static int createBaseHistorySubspace(TransactionContext context, TableIdent tableIdent,
        int historySubspaceFlag) {
        if (!baseHistorySubspaceExist(historySubspaceFlag)) {
            tableMetaStore.createTableBaseHistorySubspace(context, tableIdent.getProjectId());
            historySubspaceFlag |= baseHistorySubspaceFlag;
        }
        return historySubspaceFlag;
    }

    public static boolean schemaHistorySubspaceFlagExist(int historySubspaceFlag) {
        return (historySubspaceFlag & schemaHistorySubspaceFlag) == schemaHistorySubspaceFlag;
    }

    public static int createSchemaHistorySubspace(TransactionContext context, TableIdent tableIdent,
        int historySubspaceFlag) {
        if (!schemaHistorySubspaceFlagExist(historySubspaceFlag)) {
            tableMetaStore.createTableSchemaHistorySubspace(context, tableIdent.getProjectId());
            historySubspaceFlag |= schemaHistorySubspaceFlag;
        }
        return historySubspaceFlag;
    }

    public static boolean storageHistorySubspaceExist(int historySubspaceFlag) {
        return (historySubspaceFlag & storageHistorySubspaceFlag) == storageHistorySubspaceFlag;
    }

    public static int createStorageHistorySubspace(TransactionContext context, TableIdent tableIdent,
        int historySubspaceFlag) {
        if (!storageHistorySubspaceExist(historySubspaceFlag)) {
            tableMetaStore.createTableStorageHistorySubspace(context, tableIdent.getProjectId());
            historySubspaceFlag |= storageHistorySubspaceFlag;
        }
        return historySubspaceFlag;
    }

    public static boolean commitSubspaceExist(int historySubspaceFlag) {
        return (historySubspaceFlag & commitSubspaceFlag) == commitSubspaceFlag;
    }

    public static int createCommitSubspace(TransactionContext context, TableIdent tableIdent,
        int historySubspaceFlag) {
        if (!commitSubspaceExist(historySubspaceFlag)) {
            //tableMetaStore.createTableCommitSubspace(context, tableIdent.getProjectId());
            historySubspaceFlag |= commitSubspaceFlag;
        }
        return historySubspaceFlag;
    }

    public static boolean dataHistorySubspaceExist(int historySubspaceFlag) {
        return (historySubspaceFlag & dataHistorySubspaceFlag) == dataHistorySubspaceFlag;
    }

    public static int createDataHistorySubspace(TransactionContext context, TableIdent tableIdent,
        int historySubspaceFlag) {
        if (!dataHistorySubspaceExist(historySubspaceFlag)) {
            tableDataStore.createTableHistorySubspace(context, tableIdent.getProjectId());
            historySubspaceFlag |= dataHistorySubspaceFlag;
        }
        return historySubspaceFlag;
    }
}
