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

import com.google.protobuf.InvalidProtocolBufferException;
import io.polycat.catalog.common.model.FunctionObject;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.store.api.FunctionStore;
import io.polycat.catalog.store.gaussdb.pojo.FunctionInfoRecord;
import io.polycat.catalog.store.mapper.FunctionMapper;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.CatalogIdent;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.plugin.request.input.FunctionInput;
import io.polycat.catalog.common.plugin.request.input.FunctionResourceUri;
import io.polycat.catalog.store.protos.ResourceUri;
import io.polycat.catalog.store.protos.ResourceUris;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "gauss")
public class FunctionStoreImpl implements FunctionStore {
    @Resource
    FunctionMapper functionMapper;

    @Override
    public void createFunctionSubspace(TransactionContext context, String projectId) {
        functionMapper.createFunctionSubspace(projectId);
    }

    @Override
    public void dropFunctionSubspace(TransactionContext context, String projectId) {
        functionMapper.dropFunctionSubspace(projectId);
    }

    @Override
    public Boolean functionExist(TransactionContext context, DatabaseIdent databaseIdent, String functionName)
            throws MetaStoreException {
        return functionMapper.functionExist(databaseIdent.getProjectId(), databaseIdent.getCatalogId(), databaseIdent.getDatabaseId(), functionName);
    }

    @Override
    public FunctionObject getFunction(TransactionContext context, DatabaseIdent databaseIdent, String functionName)
            throws MetaStoreException {
        FunctionInfoRecord record = functionMapper.getFunction(databaseIdent.getProjectId(), databaseIdent.getCatalogId(), databaseIdent.getDatabaseId(), functionName);
        if (record == null) {
            return null;
        }
        try {
            ResourceUris resourceUris = ResourceUris.parseFrom(record.getResourceUris());
            return new FunctionObject(functionName, record.getClassName(), record.getOwnerName(), record.getOwnerType(),
                    record.getFunctionType(), record.getCreateTime(), resourceUris.getResourceList().stream().map(x -> new FunctionResourceUri(x.getType(), x.getUri())).collect(Collectors.toList()));
        } catch (InvalidProtocolBufferException e) {
            log.warn("getFunction() error: {}", e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void insertFunction(TransactionContext context, DatabaseIdent databaseIdent, String funcName,
                               FunctionInput funcInput, long createTime) throws MetaStoreException {

        FunctionInfoRecord record = new FunctionInfoRecord();
        record.setCatalogId(databaseIdent.getCatalogId());
        record.setDatabaseId(databaseIdent.getDatabaseId());
        record.setFunctionName(funcName);
        record.setClassName(funcInput.getClassName());
        record.setOwnerName(funcInput.getOwner());
        record.setOwnerType(funcInput.getOwnerType());
        record.setFunctionType(funcInput.getFuncType());
        record.setCreateTime(createTime);
        List<ResourceUri> resourceUris = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(funcInput.getResourceUris())) {
            resourceUris = funcInput.getResourceUris().stream().map(x -> ResourceUri.newBuilder().setUri(x.getUri()).setType(x.getType().name()).build()).collect(Collectors.toList());
        }
        record.setResourceUris(ResourceUris.newBuilder().addAllResource(resourceUris).build().toByteArray());
        functionMapper.insertFunction(databaseIdent.getProjectId(), record);
    }

    @Override
    public List<String> listFunctions(TransactionContext context, DatabaseIdent databaseIdent, String pattern) {
        List<FunctionInfoRecord> functionInfoRecords = functionMapper.listFunction(databaseIdent.getProjectId(), databaseIdent.getCatalogId(), databaseIdent.getDatabaseId(), pattern);
        return functionInfoRecords.stream().map(FunctionInfoRecord::getFunctionName).collect(Collectors.toList());
    }

    @Override
    public List<FunctionObject> listAllFunctions(TransactionContext context, CatalogIdent catalogIdent) {
        List<FunctionInfoRecord> functionInfoRecords = functionMapper.listAllFunctions(catalogIdent.getProjectId(), catalogIdent.getCatalogId());
        return functionInfoRecords.stream().map(record -> {
            ResourceUris resourceUris = null;
            try {
                resourceUris = ResourceUris.parseFrom(record.getResourceUris());
            } catch (InvalidProtocolBufferException e) {
                throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
            }

            return new FunctionObject(record.getFunctionName(), record.getDatabaseName(), record.getClassName(),
                record.getOwnerName(), record.getOwnerType(), record.getFunctionType(), record.getCreateTime(),
                resourceUris.getResourceList().stream().map(x -> new FunctionResourceUri(x.getType(), x.getUri())).collect(Collectors.toList()));

        }).collect(Collectors.toList());
    }

    @Override
    public void dropFunction(TransactionContext context, DatabaseIdent databaseIdent, String functionName) {
        functionMapper.deleteFunction(databaseIdent.getProjectId(), databaseIdent.getCatalogId(), databaseIdent.getDatabaseId(), functionName);
    }
}
