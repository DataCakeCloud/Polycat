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

import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.lock.LockInfo;
import io.polycat.catalog.common.model.lock.LockLevel;
import io.polycat.catalog.common.model.lock.LockState;

import java.util.List;

/**
 * @author liangyouze
 * @date 2024/2/19
 */
public interface LockStore {

    LockInfo createLock(TransactionContext context, String projectId, String objectName, LockLevel lockLevel, String user, String hostname, LockState state);

    List<LockInfo> getLocksByObjectName(TransactionContext context, String projectId, String objectName);

    LockInfo getLockById(TransactionContext context, String projectId, Long lockId);

    int updateLockState(TransactionContext context, String projectId, Long lockId, LockState lockState);

    int updateHeartbeat(TransactionContext context, String projectId, Long lockId);

    int deleteLock(TransactionContext context, String projectId, Long lockId);

    List<Long> getTimeoutLocks(TransactionContext context, Integer timeout);

    void deleteLocks(TransactionContext context, List<Long> timeoutLocks);

    List<LockInfo> getLocks(TransactionContext context, String projectId);
}
