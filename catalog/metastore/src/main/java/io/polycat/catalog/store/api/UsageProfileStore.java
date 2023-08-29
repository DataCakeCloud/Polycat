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
package io.polycat.catalog.store.api;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.*;

public interface UsageProfileStore {

    void createUsageProfileSubspace(TransactionContext context, String projectId);

    void dropUsageProfileSubspace(TransactionContext context, String projectId);

    void updateUsageProfile(TransactionContext context, String projectId, UsageProfileObject usageProfileObject) throws MetaStoreException;

    Optional<UsageProfileObject> getUsageProfile(TransactionContext context, String projectId, String catalogId,
                                                 String databaseId, String tableId, long startTime, String opType) throws MetaStoreException;

    void deleteUsageProfile(TransactionContext context, String projectId, long startTime, long endTime) throws MetaStoreException;

    ScanRecordCursorResult<List<UsageProfileObject>> listUsageProfile(TransactionContext context, String projectId, String tableId, String catalogName, String databaseName, List<String> opTypes, long startTime, long endTime, byte[] continuation)  throws MetaStoreException;

    void recordUsageProfile(TransactionContext ctx, UsageProfileObject usageProfileObject);

    /**
     * TODO Statistical accuracy is accurate to the day level
     * @param context
     * @param projectId
     * @param catalogName
     * @param databaseName
     * @param opTypes
     * @param startTime
     * @param endTime
     * @param continuation
     * @return
     */
    ScanRecordCursorResult<List<UsageProfileObject>> listUsageProfilePreStatByFilter(TransactionContext context, String projectId, String catalogName, String databaseName, String tableName, List<String> opTypes, long startTime, long endTime, int maxBatchRowNum, byte[] continuation);

    List<UsageProfileAccessStatObject> getUsageProfileAccessStatList(String projectId, UsageProfileAccessStatObject accessStatObject, Set<String> opTypesSet, boolean sortAsc);

    List<String> getTableAccessUsers(TransactionContext context, String projectId, String catalogName, String databaseName, String tableName);

    List<UsageProfileObject> getUsageProfileDetailsByCondition(TransactionContext context, String projectId, UsageProfileObject upo, long startTime, long endTime, int rowCount);
}
