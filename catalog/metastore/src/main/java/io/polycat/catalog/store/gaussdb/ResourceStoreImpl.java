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
import io.polycat.catalog.store.api.ResourceStore;
import io.polycat.catalog.store.api.SubspaceStore;
import io.polycat.catalog.store.api.SystemSubspaceStore;
import io.polycat.catalog.store.gaussdb.common.MetaTableConsts;
import io.polycat.catalog.util.ResourceUtil;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.store.mapper.*;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.stream.Collectors;

@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "gauss")
public class ResourceStoreImpl implements ResourceStore {

    private static final Logger log = Logger.getLogger(ResourceStoreImpl.class);

    /**
     * Count the number of milliseconds used by the most recent table.
     * //@Value("${discovery.table.hotstat.millisecond:30}")
     */
    private Long tableUsageProfileHotStatMillisecond = 30 * 24 *3600 * 1000L;

    @Autowired
    private SchemaMapper schemaMapper;

    @Autowired
    private CatalogMapper catalogMapper;

    @Autowired
    List<SubspaceStore> subspaceStores;

    @Autowired
    List<SystemSubspaceStore> systemSubspaceStores;

    @Override
    public void createResource(TransactionContext context, String projectId) throws MetaStoreException {
        try {
            schemaMapper.createSchema(projectId);
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR, e);
        }
    }

    @Override
    public Boolean doesExistResource(TransactionContext context, String projectId) throws MetaStoreException {
        try {
            List<Integer> schemas = schemaMapper.doesExistSchema(projectId);
            return schemas.size() > 0;
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR, e);
        }
    }

    @Override
    public void dropResource(TransactionContext context, String projectId) throws MetaStoreException {
        try {
            schemaMapper.dropSchema(projectId);
        } catch (Exception e) {
            log.info(e.getMessage(), e);
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR, e);
        }
    }

    @Override
    public List<String> listProjects(TransactionContext context) {
        return listProjectsInternal(context).stream().filter(x -> x.startsWith(MetaTableConsts.PG_SCHEMA_PREFIX)).map(x -> x.replaceFirst(
            MetaTableConsts.PG_SCHEMA_PREFIX, "")).collect(
                Collectors.toList());
    }

    private List<String> listProjectsInternal(TransactionContext context) {
        return catalogMapper.listProjects();
    }

    @Override
    public void resourceCheck(TransactionContext context) {
        schemaMapper.createSystemSchema();
        List<String> schemaLists = listProjectsInternal(context);
        systemSubspaceStores.forEach(store -> store.createSystemSubspace(context));
        //TODO Improve resource checking in the future
        for (String schema: schemaLists) {
            log.debug("Start checking schema={}.", schema);
            subspaceStores.forEach(store -> store.createSubspace(context, ResourceUtil.getProjectId(schema)));
            log.info("Success checked schema={}.", schema);
        }

    }

}
