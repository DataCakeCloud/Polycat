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
package io.polycat.catalog.store.fdb.record.impl;

import com.google.common.collect.Lists;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.store.api.ResourceStore;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "fdb")
public class ResourceStoreImpl implements ResourceStore {

    @Override
    public void createResource(TransactionContext context, String projectId) throws MetaStoreException {

    }

    @Override
    public Boolean doesExistResource(TransactionContext context, String projectId) throws MetaStoreException {
        return true;
    }

    @Override
    public void dropResource(TransactionContext context, String projectId) throws MetaStoreException {

    }

    @Override
    public List<String> listProjects(TransactionContext context) {
        return Lists.newArrayList();
    }

    @Override
    public void resourceCheck(TransactionContext context) {

    }
}
