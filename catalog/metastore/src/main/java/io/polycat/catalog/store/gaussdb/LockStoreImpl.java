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
package io.polycat.catalog.store.gaussdb;

import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.store.api.LockStore;
import io.polycat.catalog.store.api.SystemSubspaceStore;
import io.polycat.catalog.store.gaussdb.pojo.LockRecord;
import io.polycat.catalog.store.mapper.LockMapper;
import io.polycat.catalog.common.model.lock.LockInfo;
import io.polycat.catalog.common.model.lock.LockLevel;
import io.polycat.catalog.common.model.lock.LockState;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import java.sql.Timestamp;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author liangyouze
 * @date 2024/2/19
 */

@Slf4j
@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "gauss")
public class LockStoreImpl implements LockStore, SystemSubspaceStore {

    @Autowired
    private LockMapper lockMapper;
    @Override
    public LockInfo createLock(TransactionContext context, String projectId, String objectName, LockLevel lockLevel, String user, String hostname, LockState state) {
        final long currentTime = System.currentTimeMillis();
        final LockRecord lockRecord = new LockRecord(null, objectName, state.name(), lockLevel.name(),
                new Timestamp(currentTime), new Timestamp(currentTime), user, hostname, projectId);
        lockMapper.insertLockInfo(lockRecord);
        return toLockInfo(lockRecord);
    }

    @Override
    public List<LockInfo> getLocksByObjectName(TransactionContext context, String projectId, String objectName) {
        final List<LockRecord> lockRecords = lockMapper.getLocksByObjectName(projectId, objectName);
        return lockRecords.stream()
                .map(this::toLockInfo)
                .collect(Collectors.toList());
    }

    @Override
    public LockInfo getLockById(TransactionContext context, String projectId, Long lockId) {
        final LockRecord lockRecord = lockMapper.getLockById(projectId, lockId);
        return toLockInfo(lockRecord);
    }

    @Override
    public int updateLockState(TransactionContext context, String projectId, Long lockId, LockState lockState) {
        return lockMapper.updateLockState(projectId, lockId, lockState.name());
    }

    @Override
    public int updateHeartbeat(TransactionContext context, String projectId, Long lockId) {
        final long lastHeartbeat = System.currentTimeMillis();
        return lockMapper.updateLastHeartbeat(projectId, lockId, lastHeartbeat);
    }

    @Override
    public int deleteLock(TransactionContext context, String projectId, Long lockId) {
        return lockMapper.deleteLock(projectId, lockId);
    }

    @Override
    public List<Long> getTimeoutLocks(TransactionContext context, Integer timeout) {
        return lockMapper.getTimeoutLocks(timeout);
    }

    @Override
    public void deleteLocks(TransactionContext context, List<Long> timeoutLocks) {
        if (timeoutLocks == null || timeoutLocks.isEmpty()) {
            return;
        }
        lockMapper.deleteLocks(timeoutLocks);
    }

    @Override
    public List<LockInfo> getLocks(TransactionContext context, String projectId) {
        final List<LockRecord> lockRecords = lockMapper.getLocks(projectId);
        return lockRecords.stream().map(this::toLockInfo).collect(Collectors.toList());
    }

    private LockInfo toLockInfo(LockRecord lockRecord) {
        if (lockRecord == null) {
            return null;
        }
        return new LockInfo(lockRecord.getLockId(), lockRecord.getObjectName(),
                LockState.valueOf(lockRecord.getLockState()), LockLevel.valueOf(lockRecord.getLockLevel()),
                lockRecord.getCreateTime().getTime(), lockRecord.getLastHeartbeat().getTime(), lockRecord.getUserName(),
                lockRecord.getHostname(), lockRecord.getProjectId());
    }

    @Override
    public void createSystemSubspace(TransactionContext context) {
        lockMapper.createLockSubspace();
    }
}
