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
import io.polycat.catalog.common.plugin.request.UpdateDataLineageRequest;
import io.polycat.catalog.common.plugin.request.input.LineageInfoInput;
import io.polycat.catalog.common.plugin.request.input.TableUsageProfileInput;

import java.util.List;
import java.util.Objects;


import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(EventProcessor.class);
    private PolyCatClient polyCatClient;

    public EventProcessor(Configuration conf) {
        polyCatClient = PolyCatClientUtil.buildPolyCatClientFromConfig(conf);
        LOG.info(String.format("polyCatClient context info:%s", polyCatClient.getContext()));
    }

    public void pushTableUsageProfile(List<TableUsageProfile> tableUsageProfiles) {
        if (Objects.isNull(tableUsageProfiles) || tableUsageProfiles.isEmpty()) {
            LOG.info("tableUsageProfiles list is empty!");
            return;
        }
        LOG.info(String.format("will push table usageProfiles info: %s", tableUsageProfiles));
        try {
            InsertUsageProfileRequest request =
                    new InsertUsageProfileRequest(new TableUsageProfileInput(tableUsageProfiles));
            request.setProjectId(polyCatClient.getProjectId());
            polyCatClient.insertUsageProfile(request);
        } catch (Exception e) {
            LOG.error("insertUsageProfile error!", e);
        }
    }

    public void pushSqlLineage(LineageInfoInput lineageInfoInput) {
        LOG.info("will push sql lineage:{}", lineageInfoInput.toString());
        try {
            UpdateDataLineageRequest updateDataLineageRequest =
                    new UpdateDataLineageRequest(polyCatClient.getProjectId(), lineageInfoInput);
            polyCatClient.updateDataLineage(updateDataLineageRequest);
        } catch (Exception e) {
            LOG.error("push sql lineage error!", e);
        }
    }
}
