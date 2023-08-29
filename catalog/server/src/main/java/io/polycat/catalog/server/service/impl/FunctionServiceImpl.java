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
package io.polycat.catalog.server.service.impl;

import java.util.*;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.model.*;
import io.polycat.catalog.common.plugin.request.input.FunctionInput;
import io.polycat.catalog.common.utils.CatalogStringUtils;
import io.polycat.catalog.common.utils.UuidUtil;
import io.polycat.catalog.server.util.TransactionFrameRunner;
import io.polycat.catalog.server.util.TransactionRunner;
import io.polycat.catalog.server.util.TransactionRunnerUtil;
import io.polycat.catalog.service.api.FunctionService;
import io.polycat.catalog.store.api.*;
import io.polycat.catalog.store.common.StoreConvertor;
import io.polycat.catalog.store.common.StoreTypeConvertor;
import io.polycat.catalog.store.common.StoreValidator;
import io.polycat.catalog.util.CheckUtil;
import jakarta.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import static io.polycat.catalog.common.Operation.CREATE_FUNCTION;
import static io.polycat.catalog.common.Operation.DROP_FUNCTION;

@Slf4j
@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class FunctionServiceImpl implements FunctionService {
    private final int MAX_RETRY_NUM = 256;

    @Autowired
    private FunctionStore functionStore;
    @Autowired
    private Transaction storeTransaction;
    @Autowired
    private CatalogStore catalogStore;
    @Autowired
    private RetriableException retriableException;
    @Autowired
    private UserPrivilegeStore userPrivilegeStore;

    private String functionOperateDetail(String catalogName, String databaseName, String functionName) {
        return new StringBuilder()
                .append("catalog name: ").append(catalogName).append(", ")
                .append("database name: ").append(databaseName).append(", ")
                .append("function name: ").append(functionName)
                .toString();
    }

    private boolean isFunctionExist(CatalogIdent catalogIdent, String eventId) {
        Optional<CatalogCommitObject> catalogCommit = CatalogCommitHelper
                .getCatalogCommit(catalogIdent, eventId);
        return catalogCommit.isPresent();
    }

    private DatabaseIdent createFunctionInternal(TransactionContext context, String projectId, String catalogNameStr,
                                                 String databaseNameStr, FunctionInput funcInput, String catalogCommitEventId) {
        DatabaseName databaseName = StoreConvertor.databaseName(projectId, catalogNameStr, databaseNameStr);
        DatabaseIdent databaseIdent = DatabaseObjectHelper.getDatabaseIdent(databaseName);
        CheckUtil.assertNotNull(databaseIdent, ErrorCode.DATABASE_NOT_FOUND, databaseName);
        // 1. check function not exists
        if (functionStore.functionExist(context, databaseIdent, funcInput.getFunctionName())) {
            throw new CatalogServerException(ErrorCode.FUNCTION_ALREADY_EXIST, funcInput.getFunctionName());
        }

        // 2. insert
        long createTime = System.currentTimeMillis();
        functionStore.insertFunction(context, databaseIdent, funcInput.getFunctionName(), funcInput, createTime);

        // 3. insert User Privilege store, TODO
        // Replace objectId with function fully qualified name
//        String fullFunctionName = getFullFunctionName(catalogNameStr, databaseNameStr, funcInput.getFunctionName());
//        userPrivilegeStore.insertUserPrivilege(context, projectId, funcInput.getOwner(),
//            ObjectType.FUNCTION.name(), fullFunctionName, true, 0);


        // 4. insert catalog commit
        CatalogName catalogName = StoreConvertor.catalogName(projectId, catalogNameStr);
        CatalogObject catalog = CatalogObjectHelper.getCatalogObject(catalogName);
        String version = VersionManagerHelper.getNextVersion(context, projectId, catalog.getRootCatalogId());
        catalogStore.insertCatalogCommit(context, projectId, catalog.getCatalogId(), catalogCommitEventId, createTime, CREATE_FUNCTION,
                functionOperateDetail(catalogNameStr, databaseNameStr, funcInput.getFunctionName()),
                version);

        return databaseIdent;
    }

    private String getFullFunctionName(String catalogName, String databaseName, @NotNull String functionName) {
        return String.format("%s.%s.%s", catalogName, databaseName == null ? "default" : databaseName, functionName).toLowerCase(Locale.ROOT);
    }

    @Override
    public void createFunction(String projectId, String catalogName, String databaseName, FunctionInput funcInput) {
        validFuncInput(funcInput);
        String catalogCommitEventId = UuidUtil.generateCatalogCommitId();
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(MAX_RETRY_NUM);
        DatabaseIdent databaseIdent = runner.run(context -> {
            return createFunctionInternal(context, projectId, CatalogStringUtils.normalizeIdentifier(catalogName),
                    CatalogStringUtils.normalizeIdentifier(databaseName), funcInput,
                    catalogCommitEventId);
        }).getResultAndCheck(ret -> CatalogCommitHelper.catalogCommitExist(ret, catalogCommitEventId));
    }

    private void validFuncInput(FunctionInput funcInput) {
        FunctionType functionType = StoreTypeConvertor.toFunctionType(funcInput.getFuncType());
        CheckUtil.checkNameLegality("functionName", funcInput.getFunctionName());
        if (functionType == null) {
            funcInput.setFuncType(null);
        } else {
            funcInput.setFuncType(functionType.name());
        }
        funcInput.setOwnerType(StoreTypeConvertor.toOwnerType(funcInput.getOwnerType()).name());
    }

    private void dropFunctionInternal(TransactionContext context, String projectId, String catalogName,
                                      String databaseName, String functionName, String catalogCommitEventId) {
        DatabaseIdent databaseIdent = DatabaseObjectHelper.getDatabaseIdent(
                StoreConvertor.databaseName(projectId, catalogName, databaseName));
        CheckUtil.assertNotNull(databaseIdent, ErrorCode.DATABASE_NOT_FOUND, databaseName);
        FunctionObject function = functionStore.getFunction(context, databaseIdent, functionName);
        if (function == null) {
            throw new MetaStoreException(ErrorCode.FUNCTION_NOT_FOUND, functionName);
        }

        functionStore.dropFunction(context, databaseIdent, functionName);

        // insert CatalogCommit
        CatalogName cat = StoreConvertor.catalogName(projectId, catalogName);
        CatalogObject catalog = CatalogObjectHelper.getCatalogObject(cat);
        String version = VersionManagerHelper.getNextVersion(context, projectId, catalog.getRootCatalogId());
        catalogStore.insertCatalogCommit(context, projectId, catalog.getCatalogId(), catalogCommitEventId, new Date().getTime(),
                DROP_FUNCTION, functionOperateDetail(catalogName, databaseName, functionName),
                version);
    }

    @Override
    public void dropFunction(String projectId, String catalogName, String databaseName, String functionName) {
        String catalogCommitEventId = UuidUtil.generateCatalogCommitId();
        CatalogObject catalogObject = CatalogObjectHelper.getCatalogObject(StoreConvertor.catalogName(projectId, catalogName));
        CatalogIdent catalogIdent = StoreConvertor.catalogIdent(catalogObject.getProjectId(),
                catalogObject.getCatalogId(), catalogObject.getRootCatalogId());

        try (TransactionRunner runner = new TransactionRunner()) {
            runner.setMaxAttempts(MAX_RETRY_NUM);
            runner.run(context -> {
                dropFunctionInternal(context, projectId,
                        CatalogStringUtils.normalizeIdentifier(catalogName),
                        CatalogStringUtils.normalizeIdentifier(databaseName),
                        functionName, catalogCommitEventId);
                return null;
            });
        } catch (RuntimeException e) {
            // The Event ID for the Catalog commit is assigned before the transaction starts,
            // and the same event ID is used for each retry transaction.
            // the Event ID is used as the catalog commit primary key.
            // key checks whether the previous transaction has been successfully inserted.
            // If the transaction is success, reports drop function successful.
            if (!isFunctionExist(catalogIdent, catalogCommitEventId)) {
                throw e;
            }
        }
    }

    @Override
    public FunctionInput getFunction(String projectId, String catalogName, String databaseName, String functionName) {
        return TransactionRunnerUtil.transactionRunThrow(context -> {
            DatabaseIdent databaseIdent = DatabaseObjectHelper.getDatabaseIdent(context,
                    StoreConvertor.databaseName(projectId, catalogName, databaseName));
            CheckUtil.assertNotNull(databaseIdent, ErrorCode.DATABASE_NOT_FOUND, databaseName);
            // 1. check function not exists
            FunctionObject functionObject = functionStore.getFunction(context, databaseIdent, functionName);
            CheckUtil.assertNotNull(functionObject, ErrorCode.FUNCTION_NOT_FOUND, functionName);
            return convertorToFunctionInput(catalogName, databaseName, functionObject);
        }).getResult();
    }

    public static FunctionInput convertorToFunctionInput(String catalogName, String databaseName,
                                                         FunctionObject functionObject) {
        FunctionInput funcIn = new FunctionInput();
        funcIn.setCatalogName(catalogName);
        funcIn.setDatabaseName(databaseName);
        funcIn.setFunctionName(functionObject.getFunctionName());
        funcIn.setClassName(functionObject.getClassName());
        funcIn.setOwner(functionObject.getOwnerName());
        funcIn.setOwnerType(functionObject.getOwnerType());
        funcIn.setFuncType(functionObject.getFunctionType());
        funcIn.setCreateTime(functionObject.getCreateTime());
        funcIn.setResourceUris(functionObject.getResourceUriObjectList());
        return funcIn;
    }

    @Override
    public List<String> listFunctions(String projectId, String catalogName, String databaseName,
                                      String pattern) {
        return TransactionRunnerUtil.transactionRunThrow(context -> {
            DatabaseIdent databaseIdent = DatabaseObjectHelper.getDatabaseIdent(
                    StoreConvertor.databaseName(projectId, catalogName, databaseName));
            CheckUtil.assertNotNull(databaseIdent, ErrorCode.DATABASE_NOT_FOUND, databaseName);
            return functionStore.listFunctions(context, databaseIdent, pattern);
        }).getResult();
    }

    @Override
    public void alterFunction(String projectId, String catalogName, String databaseName, String funcName,
                              FunctionInput newFunction) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "alterFunction");
    }

    @Override
    public List<FunctionInput> getAllFunctions(String projectId, String catalogName) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getAllFunctions");
    }
}
