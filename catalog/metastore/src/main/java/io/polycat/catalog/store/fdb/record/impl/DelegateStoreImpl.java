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
package io.polycat.catalog.store.fdb.record.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.store.api.DelegateStore;
import io.polycat.catalog.common.model.DelegateBriefInfo;
import io.polycat.catalog.common.model.DelegateOutput;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.plugin.request.input.DelegateInput;
import io.polycat.catalog.store.common.StoreMetadata;
import io.polycat.catalog.store.fdb.record.DirectoryStoreHelper;
import io.polycat.catalog.store.fdb.record.RecordStoreHelper;
import io.polycat.catalog.store.fdb.record.TransactionContextUtil;
import io.polycat.catalog.store.protos.DelegateRecord;

import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "fdb")
public class DelegateStoreImpl implements DelegateStore {

    private static class DelegateStoreImplHandler {
        private static final DelegateStoreImpl INSTANCE = new DelegateStoreImpl();
    }

    public static DelegateStore getInstance() {
        return DelegateStoreImpl.DelegateStoreImplHandler.INSTANCE;
    }

    private Tuple delegatePrimaryKey(String delegateName) {
        return Tuple.from(delegateName);
    }

    @Override
    public Boolean delegateExist(TransactionContext context, String projectId, String delegateName) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore delegateFDBRecordStore = DirectoryStoreHelper.getDelegateStore(fdbRecordContext, projectId);
        FDBStoredRecord<Message> record = delegateFDBRecordStore.loadRecord(delegatePrimaryKey(delegateName));
        return (record != null);
    }

    @Override
    public Optional<DelegateOutput> getDelegate(TransactionContext context, String projectId, String delegateName)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore delegateFDBRecordStore = DirectoryStoreHelper.getDelegateStore(fdbRecordContext, projectId);
        FDBStoredRecord<Message> record = delegateFDBRecordStore.loadRecord(delegatePrimaryKey(delegateName));
        if (record == null) return Optional.empty();
        DelegateRecord.Builder builder = DelegateRecord.newBuilder().mergeFrom(record.getRecord());
        return Optional.of(new DelegateOutput(builder.getStorageProvider(), builder.getProviderDomainName(),
            builder.getAgencyName(), builder.getStorageAllowedLocationsList(), builder.getStorageBlockedLocationsList()));
    }

    @Override
    public void insertDelegate(TransactionContext context, String projectId, DelegateInput delegateBody)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore delegateFDBRecordStore = DirectoryStoreHelper.getDelegateStore(fdbRecordContext, projectId);
        DelegateRecord delegateRecord = DelegateRecord.newBuilder()
            .setDelegateName(delegateBody.getDelegateName())
            .setUserId(delegateBody.getUserId())
            .setStorageProvider(delegateBody.getStorageProvider().toUpperCase())
            .setProviderDomainName(delegateBody.getProviderDomainName())
            .setAgencyName(delegateBody.getAgencyName())
            .addAllStorageAllowedLocations(delegateBody.getAllowedLocationList())
            .addAllStorageBlockedLocations(delegateBody.getBlockedLocationList())
            .build();
        delegateFDBRecordStore.insertRecord(delegateRecord);
    }

    @Override
    public void deleteDelegate(TransactionContext context, String projectId, String delegateName) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore delegateFDBRecordStore = DirectoryStoreHelper.getDelegateStore(fdbRecordContext, projectId);
        delegateFDBRecordStore.deleteRecord(delegatePrimaryKey(delegateName));
    }

    public List<DelegateBriefInfo> listDelegates(TransactionContext context, String projectId, QueryComponent filter)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore store = DirectoryStoreHelper.getDelegateStore(fdbRecordContext, projectId);
        List<FDBQueriedRecord<Message>> recordList = RecordStoreHelper
            .getRecords(store, StoreMetadata.DELEGATE_RECORD, filter);

        List<DelegateBriefInfo> delegateBriefInfos = new ArrayList<>();
        for (FDBQueriedRecord<Message> record : recordList) {
            DelegateRecord.Builder builder = DelegateRecord.newBuilder().mergeFrom(record.getRecord());
            DelegateBriefInfo dBriefInfo = new DelegateBriefInfo(builder.getDelegateName(),
                builder.getStorageProvider(), builder.getUserId());
            delegateBriefInfos.add(dBriefInfo);
        }
        return delegateBriefInfos;
    }

}
