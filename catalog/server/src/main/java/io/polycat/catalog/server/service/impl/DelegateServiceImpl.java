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

import java.util.List;
import java.util.Optional;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.model.DelegateBriefInfo;
import io.polycat.catalog.common.model.DelegateOutput;
import io.polycat.catalog.common.model.DelegateStorageProvider;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.plugin.request.input.DelegateInput;
import io.polycat.catalog.service.api.DelegateService;
import io.polycat.catalog.store.api.DelegateStore;
import io.polycat.catalog.store.api.Transaction;

import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import io.polycat.catalog.store.fdb.record.impl.UserPrivilegeStoreImpl;
import org.apache.commons.lang3.EnumUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class DelegateServiceImpl implements DelegateService {
    private static final Logger logger = Logger.getLogger(DelegateServiceImpl.class);
    private static final UserPrivilegeStoreImpl userPrivilegeStore = UserPrivilegeStoreImpl.getInstance();

    @Autowired
    private Transaction storeTransaction;

    @Autowired
    private DelegateStore delegateStore;

    @Override
    public DelegateOutput createDelegate(String projectId, DelegateInput delegateBody) {
        if (projectId.isEmpty()) {
            throw new CatalogServerException(ErrorCode.ARGUMENT_ILLEGAL);
        }
        String storageProvider = delegateBody.getStorageProvider().toUpperCase();
        if (!EnumUtils.isValidEnum(DelegateStorageProvider.class, storageProvider)) {
            throw new CatalogServerException(ErrorCode.DELEGATE_PROVIDER_ILLEGAL, delegateBody.getDelegateName());
        }
        try (TransactionContext context = storeTransaction.openTransaction()) {
            // check whether the storage delegate exists
            if (delegateStore.delegateExist(context, projectId, delegateBody.getDelegateName())) {
                throw new MetaStoreException(ErrorCode.DELEGATE_ALREADY_EXIST, delegateBody.getDelegateName());
            }

            userPrivilegeStore.insertUserPrivilege(context, projectId, delegateBody.getUserId(),
                ObjectType.DELEGATE.name(), delegateBody.getDelegateName(), true, 0);

            delegateStore.insertDelegate(context, projectId, delegateBody);
            context.commit();
            return new DelegateOutput(storageProvider, delegateBody.getProviderDomainName(),
                delegateBody.getAgencyName(), delegateBody.getAllowedLocationList(), delegateBody.getBlockedLocationList());
        }
    }

    @Override
    public DelegateOutput getDelegate(String projectId, String delegateName) {
        try (TransactionContext context = storeTransaction.openTransaction()) {
            Optional<DelegateOutput> optional = delegateStore.getDelegate(context, projectId, delegateName);
            if (!optional.isPresent()) {
                throw new CatalogServerException(ErrorCode.DELEGATE_NOT_FOUND, delegateName);
            }
            context.commit();
            return optional.get();
        }
    }

    @Override
    public void dropDelegate(String projectId, String delegateName) {
        try (TransactionContext context = storeTransaction.openTransaction()) {
            // check whether the storage delegate exists
            if (!delegateStore.delegateExist(context, projectId, delegateName)) {
                throw new CatalogServerException(ErrorCode.DELEGATE_NOT_FOUND, delegateName);
            }

            delegateStore.deleteDelegate(context, projectId, delegateName);

            context.commit();
        }
    }

    @Override
    public List<DelegateBriefInfo> listDelegates(String projectId, String pattern) {
        QueryComponent filter = null;
        if (pattern != null && !pattern.trim().isEmpty()) {
            filter = Query.field("delegate_name").startsWith(pattern);
        }

        try (TransactionContext context = storeTransaction.openTransaction()) {
            return delegateStore.listDelegates(context, projectId, filter);
        }
    }
}
