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
package io.polycat.catalog.iceberg;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.polycat.catalog.client.Client;
import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.exception.LockException;
import io.polycat.catalog.common.model.lock.LockInfo;
import io.polycat.catalog.common.model.lock.LockLevel;
import io.polycat.catalog.common.model.lock.LockState;
import io.polycat.catalog.common.plugin.request.CheckLockRequest;
import io.polycat.catalog.common.plugin.request.CreateLockRequest;
import io.polycat.catalog.common.plugin.request.LockHeartbeatRequest;
import io.polycat.catalog.common.plugin.request.UnlockRequest;
import io.polycat.catalog.common.plugin.request.input.LockInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author liangyouze
 * @date 2024/2/6
 */
public class PolyCatLock {
    private static final String POLYCAT_ACQUIRE_LOCK_TIMEOUT_MS = "iceberg.polycat.lock-timeout-ms";
    private static final String POLYCAT_LOCK_CHECK_MIN_WAIT_MS = "iceberg.polycat.lock-check-min-wait-ms";
    private static final String POLYCAT_LOCK_CHECK_MAX_WAIT_MS = "iceberg.polycat.lock-check-max-wait-ms";
    private static final String POLYCAT_LOCK_CREATION_TIMEOUT_MS =
            "iceberg.polycat.lock-creation-timeout-ms";
    private static final String POLYCAT_LOCK_CREATION_MIN_WAIT_MS =
            "iceberg.polycat.lock-creation-min-wait-ms";
    private static final String POLYCAT_LOCK_CREATION_MAX_WAIT_MS =
            "iceberg.polycat.lock-creation-max-wait-ms";
    private static final String POLYCAT_LOCK_HEARTBEAT_INTERVAL_MS =
            "iceberg.polycat.lock-heartbeat-interval-ms";
    private static final String POLYCAT_TABLE_LEVEL_LOCK_EVICT_MS =
            "iceberg.polycat.table-level-lock-evict-ms";

    private static final long POLYCAT_ACQUIRE_LOCK_TIMEOUT_MS_DEFAULT = 3 * 60 * 1000; // 3 minutes
    private static final long POLYCAT_LOCK_CHECK_MIN_WAIT_MS_DEFAULT = 50; // 50 milliseconds
    private static final long POLYCAT_LOCK_CHECK_MAX_WAIT_MS_DEFAULT = 5 * 1000; // 5 seconds
    private static final long POLYCAT_LOCK_CREATION_TIMEOUT_MS_DEFAULT = 3 * 60 * 1000; // 3 minutes
    private static final long POLYCAT_LOCK_CREATION_MIN_WAIT_MS_DEFAULT = 50; // 50 milliseconds
    private static final long POLYCAT_LOCK_CREATION_MAX_WAIT_MS_DEFAULT = 5 * 1000; // 5 seconds
    private static final long POLYCAT_LOCK_HEARTBEAT_INTERVAL_MS_DEFAULT = 4 * 60 * 1000; // 4 minutes
    private static final long POLYCAT_TABLE_LEVEL_LOCK_EVICT_MS_DEFAULT = TimeUnit.MINUTES.toMillis(10);
    private static volatile Cache<String, ReentrantLock> commitLockCache;
    private static final Logger LOG = LoggerFactory.getLogger(PolyCatLock.class);

    private final String catalogName;
    private final String databaseName;
    private final String tableName;
    private final String fullName;
    private final Client polycatClient;

    private final long lockAcquireTimeout;
    private final long lockCheckMinWaitTime;
    private final long lockCheckMaxWaitTime;
    private final long lockCreationTimeout;
    private final long lockCreationMinWaitTime;
    private final long lockCreationMaxWaitTime;
    private final long lockHeartbeatIntervalTime;
    private final ScheduledExecutorService exitingScheduledExecutorService;

    private Optional<Long> polycatLockId = Optional.empty();

    private ReentrantLock jvmLock = null;
    private Heartbeat heartbeat = null;

    public PolyCatLock(Configuration conf, String catalogName, String databaseName, String tableName, String fullName, Client polycatClient) {
        this.catalogName = catalogName;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.fullName = fullName;
        this.polycatClient = polycatClient;

        this.lockAcquireTimeout =
                conf.getLong(POLYCAT_ACQUIRE_LOCK_TIMEOUT_MS, POLYCAT_ACQUIRE_LOCK_TIMEOUT_MS_DEFAULT);
        this.lockCheckMinWaitTime =
                conf.getLong(POLYCAT_LOCK_CHECK_MIN_WAIT_MS, POLYCAT_LOCK_CHECK_MIN_WAIT_MS_DEFAULT);
        this.lockCheckMaxWaitTime =
                conf.getLong(POLYCAT_LOCK_CHECK_MAX_WAIT_MS, POLYCAT_LOCK_CHECK_MAX_WAIT_MS_DEFAULT);
        this.lockCreationTimeout =
                conf.getLong(POLYCAT_LOCK_CREATION_TIMEOUT_MS, POLYCAT_LOCK_CREATION_TIMEOUT_MS_DEFAULT);
        this.lockCreationMinWaitTime =
                conf.getLong(POLYCAT_LOCK_CREATION_MIN_WAIT_MS, POLYCAT_LOCK_CREATION_MIN_WAIT_MS_DEFAULT);
        this.lockCreationMaxWaitTime =
                conf.getLong(POLYCAT_LOCK_CREATION_MAX_WAIT_MS, POLYCAT_LOCK_CREATION_MAX_WAIT_MS_DEFAULT);
        this.lockHeartbeatIntervalTime =
                conf.getLong(POLYCAT_LOCK_HEARTBEAT_INTERVAL_MS, POLYCAT_LOCK_HEARTBEAT_INTERVAL_MS_DEFAULT);
        long tableLevelLockCacheEvictionTimeout =
                conf.getLong(POLYCAT_TABLE_LEVEL_LOCK_EVICT_MS, POLYCAT_TABLE_LEVEL_LOCK_EVICT_MS_DEFAULT);

        this.exitingScheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor(
                        new ThreadFactoryBuilder()
                                .setDaemon(true)
                                .setNameFormat("iceberg-hive-lock-heartbeat-" + fullName + "-%d")
                                .build());
        initTableLevelLockCache(tableLevelLockCacheEvictionTimeout);
    }

    private static void initTableLevelLockCache(long evictionTimeout) {
        if (commitLockCache == null) {
            synchronized (PolyCatLock.class) {
                if (commitLockCache == null) {
                    commitLockCache =
                            Caffeine.newBuilder()
                                    .expireAfterAccess(evictionTimeout, TimeUnit.MILLISECONDS)
                                    .build();
                }
            }
        }
    }

    public void lock() throws LockException{
        acquireJvmLock();

        // Getting lock
        polycatLockId = Optional.of(acquireLock());

        // Starting heartbeat for the HMS lock
        heartbeat = new Heartbeat(polycatClient, polycatLockId.get(), lockHeartbeatIntervalTime);
        heartbeat.schedule(exitingScheduledExecutorService);
    }

    private void acquireJvmLock() {
        if (jvmLock != null) {
            throw new IllegalStateException(
                    String.format("Cannot call acquireLock twice for %s", fullName));
        }

        jvmLock = commitLockCache.get(fullName, t -> new ReentrantLock(true));
        jvmLock.lock();
    }

    private long acquireLock() {
        AtomicReference<LockInfo> lockInfo = new AtomicReference<>(createLock());

        final long start = System.currentTimeMillis();
        long duration = 0;
        boolean timeout = false;
        CatalogException catalogException = null;

        try {
            if (lockInfo.get().getLockState().equals(LockState.WAITING)) {
                // Retry count is the typical "upper bound of retries" for Tasks.run() function. In fact,
                // the maximum number of
                // attempts the Tasks.run() would try is `retries + 1`. Here, for checking locks, we use
                // timeout as the
                // upper bound of retries. So it is just reasonable to set a large retry count. However, if
                // we set
                // Integer.MAX_VALUE, the above logic of `retries + 1` would overflow into
                // Integer.MIN_VALUE. Hence,
                // the retry is set conservatively as `Integer.MAX_VALUE - 100` so it doesn't hit any
                // boundary issues.
                Tasks.foreach(lockInfo.get().getLockId())
                        .retry(Integer.MAX_VALUE - 100)
                        .exponentialBackoff(lockCheckMinWaitTime, lockCheckMaxWaitTime, lockAcquireTimeout, 1.5)
                        .throwFailureWhenFinished()
                        .onlyRetryOn(WaitingForLockException.class)
                        .run(
                                id -> {
                                    final LockInfo newLockInfo = polycatClient.checkLock(new CheckLockRequest(polycatClient.getProjectId(), id));
                                    lockInfo.set(newLockInfo);
                                    if (newLockInfo.getLockState().equals(LockState.WAITING)) {
                                        throw new WaitingForLockException(
                                                String.format(
                                                        "Waiting for lock on table %s.%s.%s", catalogName, databaseName, tableName));
                                    }

                                },
                                CatalogException.class);
            }
        } catch (WaitingForLockException e) {
            timeout = true;
            duration = System.currentTimeMillis() - start;
        } catch (CatalogException e) {
            catalogException = e;
        } finally {
            if (!lockInfo.get().getLockState().equals(LockState.ACQUIRED)) {
                unlock(Optional.of(lockInfo.get().getLockId()));
            }
        }

        if (!lockInfo.get().getLockState().equals(LockState.ACQUIRED)) {
            if (timeout) {
                throw new LockException(
                        "Timed out after %s ms waiting for lock on %s.%s.%s", duration, catalogName, databaseName, tableName);
            }

            if (catalogException != null) {
                throw new LockException(
                        catalogException, "Check lock failed for %.%s.%s", catalogName, databaseName, tableName);
            }

            // Just for safety. We should not get here.
            throw new LockException(
                    "Could not acquire the lock on %s.%s.%s, lock request ended in state %s",
                    catalogName, databaseName, tableName, lockInfo.get().getLockState());
        } else {
            return lockInfo.get().getLockId();
        }
    }

    private LockInfo createLock() throws LockException {
        String hostName;
        AtomicReference<LockInfo> lockInfo = new AtomicReference<>();
        try {
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException uhe) {
            throw new LockException(uhe, "Error generating host name");
        }
        final LockInput lockInput = new LockInput(fullName, LockLevel.TABLE.toString(), polycatClient.getUserName(), hostName);

        final CreateLockRequest createLockRequest = new CreateLockRequest();
        createLockRequest.setInput(lockInput);
        createLockRequest.setProjectId(polycatClient.getProjectId());

        Tasks.foreach(createLockRequest)
                .retry(Integer.MAX_VALUE - 100)
                .exponentialBackoff(
                        lockCreationMinWaitTime, lockCreationMaxWaitTime, lockCreationTimeout, 2.0)
                .shouldRetryTest(
                        e -> e instanceof LockException
                )
                .throwFailureWhenFinished()
                .run(
                        request -> {
                            try {
                                lockInfo.set(polycatClient.lock(request));
                            } catch (CatalogException e) {
                                LOG.warn("Failed to create lock {}", request, e);
                                throw new LockException(
                                        e, "Failed to create lock on table %s.%s.%s", catalogName, databaseName, tableName);
                            }
                        },
                        LockException.class);

        // This should be initialized always, or exception should be thrown.
        LOG.debug("Lock {} created for table {}.{}.{}", lockInfo, catalogName, databaseName, tableName);
        return lockInfo.get();
    }

    private void unlock(Optional<Long> lockId) {
        Long id = null;
        try {
            if (!lockId.isPresent()) {
                LOG.warn("Could not find lock");
                return;
            } else {
                id = lockId.get();
            }

            polycatClient.unlock(new UnlockRequest(polycatClient.getProjectId(), id));
        } catch (Exception e) {
            LOG.warn("Failed to unlock {}.{}.{}", catalogName, databaseName, tableName, e);
        }
    }

    public void unlock() {
        if (heartbeat != null) {
            heartbeat.cancel();
            exitingScheduledExecutorService.shutdown();
        }

        try {
            unlock(polycatLockId);
        } finally {
            releaseJvmLock();
        }
    }

    private void releaseJvmLock() {
        if (jvmLock != null) {
            jvmLock.unlock();
            jvmLock = null;
        }
    }

    public void ensureActive() throws LockException {
        if (heartbeat == null) {
            throw new LockException("Lock is not active");
        }

        if (heartbeat.encounteredException != null) {
            throw new LockException(
                    heartbeat.encounteredException,
                    "Failed to heartbeat for hive lock. %s",
                    heartbeat.encounteredException.getMessage());
        }
        if (!heartbeat.active()) {
            throw new LockException("Hive lock heartbeat thread not active");
        }
    }

    private static class Heartbeat implements Runnable {
        private final Client polyCatClient;
        private final long lockId;
        private final long intervalMs;
        private ScheduledFuture<?> future;
        private volatile Exception encounteredException = null;

        Heartbeat(Client polyCatClient, long lockId, long intervalMs) {
            this.polyCatClient = polyCatClient;
            this.lockId = lockId;
            this.intervalMs = intervalMs;
            this.future = null;
        }

        @Override
        public void run() {
            try {
                polyCatClient.heartbeat(new LockHeartbeatRequest(polyCatClient.getProjectId(), lockId));
            } catch (CatalogException e) {
                this.encounteredException = e;
                throw new CommitFailedException(e, "Failed to heartbeat for lock: %d", lockId);
            }
        }

        public void schedule(ScheduledExecutorService scheduler) {
            future = scheduler.scheduleAtFixedRate(this, intervalMs / 2, intervalMs, TimeUnit.MILLISECONDS);
        }

        boolean active() {
            return future != null && !future.isCancelled();
        }

        public void cancel() {
            if (future != null) {
                future.cancel(false);
            }
        }
    }

    private static class WaitingForLockException extends RuntimeException {
        WaitingForLockException(String message) {
            super(message);
        }
    }

}
