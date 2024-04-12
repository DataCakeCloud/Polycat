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

import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.ObjectNameMap;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.store.api.ObjectNameMapStore;
import io.polycat.catalog.store.gaussdb.pojo.ObjectNameMapRecord;
import io.polycat.catalog.store.mapper.ObjectNameMapMapper;
import io.polycat.catalog.common.ObjectType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "gauss")
public class ObjectNameMapStoreImpl implements ObjectNameMapStore {
    @Autowired
    private ObjectNameMapMapper objectNameMapMapper;

    @Override
    public void createObjectNameMapSubspace(TransactionContext context, String projectId) {
        objectNameMapMapper.createObjectNameMapSubspace(projectId);
    }

    @Override
    public void dropObjectNameMapSubspace(TransactionContext context, String projectId) {
        objectNameMapMapper.dropObjectNameMapSubspace(projectId);
    }

    @Override
    public void insertObjectNameMap(TransactionContext context, ObjectNameMap objectNameMap) throws MetaStoreException {
        objectNameMapMapper.insertObjectNameMap(objectNameMap.getProjectId(), objectNameMap.getObjectType().name(),
                objectNameMap.getUpperObjectName(), objectNameMap.getObjectName(), objectNameMap.getTopObjectId(),
                objectNameMap.getUpperObjectId(), objectNameMap.getObjectId());
    }

    @Override
    public Optional<ObjectNameMap> getObjectNameMap(TransactionContext context, String projectId,
                                             ObjectType objectType, String upperObjectName, String objectName) throws MetaStoreException {
        ObjectNameMapRecord objectNameMapRecord = objectNameMapMapper.getObjectNameMap(projectId, objectType.name(),
                upperObjectName, objectName);
        if (objectNameMapRecord == null) {
            return Optional.empty();
        }

        return Optional.of(new ObjectNameMap(projectId, ObjectType.valueOf(objectNameMapRecord.getObjectType()),
                objectNameMapRecord.getUpperObjectName(),
                objectNameMapRecord.getObjectName(), objectNameMapRecord.getTopObjectId(),
                objectNameMapRecord.getUpperObjectId(), objectNameMapRecord.getObjectId()));
    }

    @Override
    public void deleteObjectNameMap(TransactionContext context, String projectId, ObjectType objectType,
                             String upperObjectName, String objectName) throws MetaStoreException {
        objectNameMapMapper.deleteObjectNameMap(projectId, objectType.name(), upperObjectName, objectName);
    }

    @Override
    public List<ObjectNameMap> listObjectNameMap(TransactionContext context, String projectId,
                                          ObjectType objectType, String upperObjectName, String topObjectId) {
        List<ObjectNameMapRecord> objectNameMapRecords = objectNameMapMapper.listObjectNameMap(projectId, objectType.name(),
                upperObjectName, topObjectId);

        List<ObjectNameMap> objectNameMaps = objectNameMapRecords.stream()
                .map(objectNameMapRecord -> new ObjectNameMap(projectId, ObjectType.valueOf(objectNameMapRecord.getObjectType()),
                objectNameMapRecord.getUpperObjectName(),
                objectNameMapRecord.getObjectName(), objectNameMapRecord.getTopObjectId(),
                objectNameMapRecord.getUpperObjectId(), objectNameMapRecord.getObjectId())).collect(Collectors.toList());

        return objectNameMaps;
    }
}
