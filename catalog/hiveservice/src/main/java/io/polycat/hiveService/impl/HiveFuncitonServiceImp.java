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
package io.polycat.hiveService.impl;

import java.util.List;
import java.util.stream.Collectors;

import io.polycat.catalog.common.plugin.request.input.FunctionInput;
import io.polycat.catalog.service.api.FunctionService;
import io.polycat.hiveService.common.HiveServiceHelper;
import io.polycat.hiveService.data.accesser.HiveDataAccessor;
import io.polycat.hiveService.data.accesser.PolyCatDataAccessor;
import io.polycat.hiveService.hive.HiveMetaStoreClientUtil;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "hive")
public class HiveFuncitonServiceImp implements FunctionService {

    @Override
    public void alterFunction(String projectId, String catName, String dbName, String funcName, FunctionInput newFunc) {
        HiveServiceHelper.HiveExceptionHandler(
            () -> HiveMetaStoreClientUtil.getHMSClient()
                .alterFunction(catName, dbName, funcName, HiveDataAccessor.toFunction(newFunc)));
    }

    @Override
    public List<FunctionInput> getAllFunctions(String projectId, String catalogName) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> HiveMetaStoreClientUtil.getHMSClient().getAllFunctions().getFunctions().stream()
                .map(PolyCatDataAccessor::toFunctionBase)
                .collect(Collectors.toList()));
    }

    @Override
    public void createFunction(String projectId, String catName, String dbName, FunctionInput function) {
        HiveServiceHelper.HiveExceptionHandler(
            () -> HiveMetaStoreClientUtil.getHMSClient().createFunction(HiveDataAccessor.toFunction(function)));
    }

    @Override
    public void dropFunction(String projectId, String catName, String dbName, String funcName) {
        HiveServiceHelper.HiveExceptionHandler(
            () -> HiveMetaStoreClientUtil.getHMSClient().dropFunction(catName, dbName, funcName));
    }

    @Override
    public FunctionInput getFunction(String projectId, String catName, String dbName, String funcName) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> PolyCatDataAccessor.toFunctionBase(
                HiveMetaStoreClientUtil.getHMSClient().getFunction(catName, dbName, funcName)));
    }

    @Override
    public List<String> listFunctions(String projectId, String catName, String dbName, String pattern) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> HiveMetaStoreClientUtil.getHMSClient().getFunctions(catName, dbName, pattern));
    }
}
