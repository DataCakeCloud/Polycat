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
package io.polycat.catalog.store;

import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.util.ResourceUtil;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.store.mapper.ResourceMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ResourceStoreHelper {

    private static ResourceMapper resourceMapper;

    @Autowired
    public void setResourceMapper(ResourceMapper resourceMapper) {
        ResourceStoreHelper.resourceMapper = resourceMapper;
    }

    public static boolean doesExistsTable(TransactionContext context, String projectId, String tableName) {
        if (tableName == null || tableName.length() == 0) {
            throw new MetaStoreException("Resource table is empty. please input correct table name");
        }
        return resourceMapper.doesExistTable(ResourceUtil.getSchema(projectId), tableName);
    }

    public static boolean doesExistsFunction(TransactionContext context, String functionName) {
        if (functionName == null || functionName.length() == 0) {
            throw new MetaStoreException("Resource function is empty. please input correct name");
        }
        return resourceMapper.doesExistsFunction(functionName);
    }

    public static boolean doesExistsView(TransactionContext context, String projectId, String viewName) {
        if (viewName == null || viewName.length() == 0) {
            throw new MetaStoreException("Resource viewName is empty. please input correct name");
        }
        return resourceMapper.doesExistsView(ResourceUtil.getSchema(projectId), viewName);
    }
}
