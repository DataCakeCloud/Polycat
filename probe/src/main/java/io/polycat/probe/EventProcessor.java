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
package io.polycat.probe;

import io.polycat.catalog.client.PolyCatClient;
import io.polycat.catalog.common.model.TableUsageProfile;
import io.polycat.catalog.common.plugin.request.InsertUsageProfileRequest;
import io.polycat.catalog.common.plugin.request.input.TableUsageProfileInput;
import io.polycat.probe.model.TableMetaInfo;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;


import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventProcessor implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(EventProcessor.class);

    private static final int MAX_POOL_SIZE = 4;
    private static final String EVENT_PROCESS_POOL_NAME = "event-process-pool-%d";
    private static final String EVENT_PROCESS_POOL_MAX_SIZE_CONFIG =
            "polycat.event.process.maxPoolSize";

    private PolyCatClient polyCatClient;
    private ExecutorService eventProcessorPool;

    public EventProcessor(Configuration conf) {
        polyCatClient = PolyCatClientUtil.buildPolyCatClientFromConfig(conf);
        LOG.info(String.format("polyCatClient context info:%s", polyCatClient.getContext()));

        int maxPoolSize = conf.getInt(EVENT_PROCESS_POOL_MAX_SIZE_CONFIG, MAX_POOL_SIZE);
        LOG.info(String.format("EventProcessor maxPoolSize: %s", maxPoolSize));
        ThreadFactory namedThreadFactory =
                new ThreadFactoryBuilder().setNameFormat(EVENT_PROCESS_POOL_NAME).build();

        this.eventProcessorPool =
                new ThreadPoolExecutor(
                        0,
                        maxPoolSize,
                        5L,
                        TimeUnit.SECONDS,
                        new LinkedBlockingQueue<Runnable>(),
                        namedThreadFactory);
    }

    public void pushEvent(TableMetaInfo metaInfo) throws Exception {
        List<TableUsageProfile> tableUsageProfiles = metaInfo.getTableUsageProfiles();
        if (Objects.isNull(tableUsageProfiles) || tableUsageProfiles.isEmpty()) {
            LOG.info("tableUsageProfiles list is empty!");
            return;
        }

        for (TableUsageProfile tableUsageProfile : tableUsageProfiles) {
            tableUsageProfile.getTable().setProjectId(polyCatClient.getProjectId());
            tableUsageProfile.setUserId(polyCatClient.getUserName());
            tableUsageProfile.setTaskId(metaInfo.getTaskId());
        }

        LOG.info(String.format("push table meta info: %s", metaInfo));
        eventProcessorPool.execute(
                () -> {
                    try {
                        InsertUsageProfileRequest request =
                                new InsertUsageProfileRequest(
                                        new TableUsageProfileInput(tableUsageProfiles));
                        request.setProjectId(polyCatClient.getProjectId());
                        polyCatClient.insertUsageProfile(request);
                    } catch (Exception e) {
                        LOG.error("", e);
                    }
                });
    }

    @Override
    public void close() throws IOException {
        if (!Objects.isNull(eventProcessorPool)) {
            eventProcessorPool.shutdown();
        }
    }
}
