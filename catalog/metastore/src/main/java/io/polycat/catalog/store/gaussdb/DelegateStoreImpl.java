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

import java.util.List;
import java.util.Optional;

import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.store.api.DelegateStore;
import io.polycat.catalog.store.mapper.DelegateMapper;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.DelegateBriefInfo;
import io.polycat.catalog.common.model.DelegateOutput;
import io.polycat.catalog.common.plugin.request.input.DelegateInput;

import com.apple.foundationdb.record.query.expressions.QueryComponent;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "gauss")
public class DelegateStoreImpl implements DelegateStore {

    private static final Logger log = Logger.getLogger(DelegateStoreImpl.class);

    private DelegateMapper delegateMapper;

    @Override
    public Boolean delegateExist(TransactionContext context, String projectId, String delegateName)
        throws MetaStoreException {
        try {
            return delegateMapper.delegateExist(projectId, delegateName);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public Optional<DelegateOutput> getDelegate(TransactionContext context, String projectId, String delegateName)
        throws MetaStoreException {
        return Optional.empty();
    }

    @Override
    public void insertDelegate(TransactionContext context, String projectId, DelegateInput delegateBody)
        throws MetaStoreException {
        try {
            delegateMapper.insertDelegate(projectId, delegateBody);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void deleteDelegate(TransactionContext context, String projectId, String delegateName)
        throws MetaStoreException {

    }

    @Override
    public List<DelegateBriefInfo> listDelegates(TransactionContext context, String projectId, QueryComponent filter)
        throws MetaStoreException {
        return null;
    }
}
