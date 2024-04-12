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
package io.polycat.catalog.store.api;

import java.util.List;
import java.util.Optional;

import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.model.ObjectNameMap;
import io.polycat.catalog.common.model.TransactionContext;

public interface ObjectNameMapStore {
    void createObjectNameMapSubspace(TransactionContext context, String projectId);

    void dropObjectNameMapSubspace(TransactionContext context, String projectId);

    void insertObjectNameMap(TransactionContext context, ObjectNameMap objectNameMap) throws MetaStoreException;

    Optional<ObjectNameMap> getObjectNameMap(TransactionContext context, String projectId,
        ObjectType objectType, String upperObjectName, String objectName) throws MetaStoreException;

    void deleteObjectNameMap(TransactionContext context, String projectId, ObjectType objectType,
        String upperObjectName, String objectName) throws MetaStoreException;

    List<ObjectNameMap> listObjectNameMap(TransactionContext context, String projectId,
        ObjectType objectType, String upperObjectName, String topObjectId);
}
