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
package io.polycat.catalog.audit.impl;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.polycat.catalog.audit.api.AuditLogService;
import io.polycat.catalog.audit.api.AuditLogStore;
import io.polycat.catalog.common.model.AuditLog;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.ThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Locale;
import java.util.concurrent.*;

@Service("auditLogService")
public class AuditLogServiceImpl implements AuditLogService {

    private static final Logger logger = LoggerFactory.getLogger(AuditLogServiceImpl.class);
    private final AuditLogStore auditLogStore = new AuditLogStoreFileImpl();
    private ExecutorService executorService;
    private static final int MAX_POOL_SIZE = 4;
    private static final int POOL_SIZE = 1;
    private static final int KEEP_ALIVE_TIME = 10;
    private static final String AUDITLOG_POOL_NAME = "auditlog-pool-%d";
    private static final String LOG_ROUTING_KEY = "LOG_ROUTING";
    private static final int QUEUE_SIZE = 10000;

    public AuditLogServiceImpl() {
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat(AUDITLOG_POOL_NAME).build();
        executorService = new ThreadPoolExecutor(
                POOL_SIZE,
                MAX_POOL_SIZE,
                KEEP_ALIVE_TIME,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(QUEUE_SIZE),
                namedThreadFactory, new ThreadPoolExecutor.AbortPolicy());
    }

    /**
     * addAuditLog
     *
     * @param auditLog auditLog
     */
    @Override
    public void addAuditLog(AuditLog auditLog) {
        executorService.execute(() -> {
            try {
                auditLogStore.addAuditLog(auditLog, false);
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
        });
    }

    @Override
    public void addAuditLogRoute(AuditLog auditLog, String routeKey) {
        executorService.execute(() -> {
            try {
                if (StringUtils.isNotEmpty(routeKey)) {
                    ThreadContext.put(LOG_ROUTING_KEY, routeKey.toLowerCase(Locale.ROOT));
                }
                auditLogStore.addAuditLog(auditLog, false);
            } catch (Exception e) {
                logger.error(e.getMessage());
            } finally {
                if (StringUtils.isNotEmpty(routeKey)) {
                    ThreadContext.remove(LOG_ROUTING_KEY);
                }
            }
        });
    }
}
