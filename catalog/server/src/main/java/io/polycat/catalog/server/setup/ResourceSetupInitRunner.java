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
package io.polycat.catalog.server.setup;

import io.polycat.catalog.service.api.CatalogResourceService;
import io.polycat.catalog.service.api.LockService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

@Slf4j
@Order(Ordered.LOWEST_PRECEDENCE)
@Component
public class ResourceSetupInitRunner implements ApplicationRunner {

    @Value("${tenant.project.name}")
    private String TENANT_PROJECT_NAME;
    @Value("${tenant.resource.check:true}")
    private boolean resourceCheck;

    @Autowired
    private CatalogResourceService catalogResourceService;

    @Autowired
    private LockService lockService;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        log.info("Start check resource exists...");
        if (TENANT_PROJECT_NAME == null || "".equals(TENANT_PROJECT_NAME)) {
            TENANT_PROJECT_NAME = "default_project";
        }
        if (!catalogResourceService.doesExistResource(TENANT_PROJECT_NAME)) {
            catalogResourceService.createResource(TENANT_PROJECT_NAME);
            log.info("Create resource success.");
        } else {
            log.info("Resource exists...");
        }
        // TODO tenant's schema、table initialization check.
        if (resourceCheck) {
            log.info("Start resource checking ......");
            catalogResourceService.resourceCheck();
            log.info("Resource check completed successfully ......");
        }
        catalogResourceService.init();
        lockService.monitorLock();
    }
}
