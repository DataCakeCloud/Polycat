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

import io.polycat.catalog.server.util.TransactionRunnerUtil;
import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.model.lock.LockInfo;
import io.polycat.catalog.common.model.lock.LockLevel;
import io.polycat.catalog.common.model.lock.LockState;
import io.polycat.catalog.common.plugin.request.input.LockInput;
import io.polycat.catalog.service.api.LockService;
import io.polycat.catalog.store.api.LockStore;
import io.polycat.catalog.util.CheckUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author liangyouze
 * @date 2024/2/19
 */

@Slf4j
@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class LockServiceImpl implements LockService {

    @Autowired
    private LockStore lockStore;

    @Value("${lock.timeout.sec:300}")
    private Integer timeout;
    private ScheduledExecutorService heartbeatPool = Executors.newScheduledThreadPool(1, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, this.getClass().getName());
        }
    });

    @Override
    public LockInfo createLock(String projectId, LockInput lockInput) {
        final String objectName = lockInput.getObjectName();
        CheckUtil.checkStringParameter(objectName);
        final LockLevel lockLevel = LockLevel.valueOf(lockInput.getLockLevel());
        return TransactionRunnerUtil.transactionRunThrow(context -> {
            switch (lockLevel) {
                case TABLE:
                    final List<LockInfo> locks = lockStore.getLocksByObjectName(context, projectId, objectName);
                    if (locks.isEmpty()) {
                        return lockStore.createLock(context, projectId, objectName, lockLevel, lockInput.getUser(), lockInput.getHostname(), LockState.ACQUIRED);
                    } else {
                        return lockStore.createLock(context, projectId, objectName, lockLevel, lockInput.getUser(), lockInput.getHostname(), LockState.WAITING);
                    }
                case DB:
                case PARTITION:
                default:
                    throw new UnsupportedOperationException(String.format("Unsupported lock level: %s", lockInput.getLockLevel()));
            }
        }).getResult();
    }

    @Override
    public LockInfo checklock(String projectId, Long lockId) {
        return TransactionRunnerUtil.transactionRunThrow(context -> {
            final LockInfo lock = lockStore.getLockById(context, projectId, lockId);
            if (lock == null) {
                throw new CatalogServerException(ErrorCode.NO_SUCH_LOCK, lockId.toString());
            }
            if (lock.getLockState() == LockState.ACQUIRED) {
                return lock;
            } else {
                final List<LockInfo> locks = lockStore.getLocksByObjectName(context, projectId, lock.getObjectName());
                for (LockInfo info : locks) {
                    if (info.getLockState() == LockState.ACQUIRED) {
                        return lock;
                    }
                }
                final int num = lockStore.updateLockState(context, projectId, lockId, LockState.ACQUIRED);
                if (num == 1) {
                    lock.setLockState(LockState.ACQUIRED);
                    return lock;
                } else {
                    throw new CatalogServerException(ErrorCode.INNER_SERVER_ERROR,
                            String.format("Failed to update lock %s state", lockId.toString()));
                }
            }
        }).getResult();
    }

    @Override
    public void heartbeat(String projectId, Long lockId) {
        TransactionRunnerUtil.transactionRunThrow(context -> {
            final int num = lockStore.updateHeartbeat(context, projectId, lockId);
            if (num < 1) {
                throw new CatalogServerException(String.format("No such lock : %s in project: %s", lockId.toString(), projectId), ErrorCode.NO_SUCH_LOCK);
            }
            return null;
        });
    }

    @Override
    public void unlock(String projectId, Long lockId) {
        TransactionRunnerUtil.transactionRunThrow(context -> {
            final int num = lockStore.deleteLock(context, projectId, lockId);
            if (num < 1) {
                throw new CatalogServerException(String.format("No such lock : %s in project: %s", lockId.toString(), projectId), ErrorCode.NO_SUCH_LOCK);
            }
            return null;
        });
    }

    @Override
    public List<LockInfo> showLocks(String projectId) {
        return lockStore.getLocks(null, projectId);
    }

    @Override
    public void monitorLock() {
        heartbeatPool.scheduleWithFixedDelay(this::timeoutLock, 0, 120, TimeUnit.SECONDS);
    }

    private void timeoutLock() {
        TransactionRunnerUtil.transactionRunThrow(context -> {
            try {
                final List<Long> timeoutLocks = lockStore.getTimeoutLocks(context, timeout);
                log.debug("get timeout locks: {}", timeoutLocks);
                if (!timeoutLocks.isEmpty()) {
                    lockStore.deleteLocks(context, timeoutLocks);
                    final String lockIds = timeoutLocks.stream().map(Object::toString).collect(Collectors.joining(","));
                    log.debug("Timeout locks: {}", lockIds);
                }
            } catch (Exception e) {
                log.error("Failed timeout lock: ", e);
            }
            return null;
        }).getResult();
    }
}
