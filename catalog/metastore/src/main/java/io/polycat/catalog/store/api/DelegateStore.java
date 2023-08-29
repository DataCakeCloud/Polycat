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
import io.polycat.catalog.common.model.DelegateBriefInfo;
import io.polycat.catalog.common.model.DelegateOutput;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.plugin.request.input.DelegateInput;

import com.apple.foundationdb.record.query.expressions.QueryComponent;

public interface DelegateStore {
    Boolean delegateExist(TransactionContext context, String projectId, String delegateName) throws MetaStoreException;

    Optional<DelegateOutput> getDelegate(TransactionContext context, String projectId, String delegateName) throws MetaStoreException;

    void insertDelegate(TransactionContext context, String projectId, DelegateInput delegateBody) throws MetaStoreException;

    void deleteDelegate(TransactionContext context, String projectId, String delegateName) throws MetaStoreException;

    List<DelegateBriefInfo> listDelegates(TransactionContext context, String projectId, QueryComponent filter)
        throws MetaStoreException;
}
