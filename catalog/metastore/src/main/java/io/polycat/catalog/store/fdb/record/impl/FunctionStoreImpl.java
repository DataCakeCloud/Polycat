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

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.tuple.Tuple;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.FunctionObject;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.plugin.request.input.FunctionResourceUri;
import io.polycat.catalog.common.plugin.request.input.FunctionInput;
import io.polycat.catalog.store.api.FunctionStore;
import io.polycat.catalog.store.common.StoreMetadata;
import io.polycat.catalog.store.fdb.record.DirectoryStoreHelper;
import io.polycat.catalog.store.fdb.record.TransactionContextUtil;
import io.polycat.catalog.store.protos.FunctionRecord;
import io.polycat.catalog.store.protos.ResourceUri;

import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.RecordQuery.Builder;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.google.protobuf.Message;
import org.apache.commons.lang3.StringUtils;

public class FunctionStoreImpl implements FunctionStore {

    private static final Logger logger = Logger.getLogger(FunctionStoreImpl.class);

    @Override
    public void createFunctionSubspace(TransactionContext context, String projectId) {

    }

    @Override
    public void dropFunctionSubspace(TransactionContext context, String projectId) {

    }

    @Override
    public Boolean functionExist(TransactionContext context, DatabaseIdent databaseIdent, String functionName)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore functionStore = DirectoryStoreHelper.getFunctionStore(fdbRecordContext, databaseIdent);
        FDBStoredRecord<Message> record = functionStore.loadRecord(buildFunctionKey(functionName));
        return (record != null);
    }

    @Override
    public FunctionObject getFunction(TransactionContext context, DatabaseIdent databaseIdent, String functionName)
        throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore functionStore = DirectoryStoreHelper.getFunctionStore(fdbRecordContext, databaseIdent);
        FDBStoredRecord<Message> record = functionStore.loadRecord(buildFunctionKey(functionName));
        if (record == null) {
            return null;
        }
        FunctionRecord.Builder builder = FunctionRecord.newBuilder().mergeFrom(record.getRecord());
        List<FunctionResourceUri> functionResourceUris = new ArrayList<>();
        for (ResourceUri ru: builder.getResourceUrisList()) {
            FunctionResourceUri fru = new FunctionResourceUri(ru.getType(), ru.getUri());
            functionResourceUris.add(fru);
        }
        return new FunctionObject(functionName, builder.getClassName(), builder.getOwnerName(), builder.getOwnerType(),
            builder.getFunctionType(), builder.getCreateTime(), functionResourceUris);
    }

    @Override
    public void insertFunction(TransactionContext context, DatabaseIdent databaseIdent, String funcName,
        FunctionInput funcInput, long createTime) throws MetaStoreException {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore functionStore = DirectoryStoreHelper.getFunctionStore(fdbRecordContext, databaseIdent);
        FunctionRecord.Builder builder = FunctionRecord.newBuilder()
            .setFunctionName(funcName)
            .setClassName(funcInput.getClassName())
            .setOwnerName(funcInput.getOwner())
            .setOwnerType(funcInput.getOwnerType())
            .setFunctionType(funcInput.getFuncType())
            .setCreateTime(createTime);
        if (funcInput.getResourceUris() != null && funcInput.getResourceUris().size() != 0) {
            List<ResourceUri> resourceUriList = new ArrayList<>();
            for (FunctionResourceUri functionResourceUri: funcInput.getResourceUris()) {
                ResourceUri resourceUri = ResourceUri.newBuilder()
                    .setType(functionResourceUri.getType().name())
                    .setUri(functionResourceUri.getUri())
                    .build();
                resourceUriList.add(resourceUri);
            }
            builder.addAllResourceUris(resourceUriList);
        }

        functionStore.insertRecord(builder.build());
    }

    @Override
    public List<String> listFunctions(TransactionContext context, DatabaseIdent databaseIdent, String pattern) {
        List<String> functionNames = new ArrayList<>();
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore store = DirectoryStoreHelper.getFunctionStore(fdbRecordContext, databaseIdent);
        RecordCursor<FDBQueriedRecord<Message>> cursor = store.executeQuery(buildQueryWithPattern(pattern)).limitRowsTo(50);

        RecordCursorIterator<FDBQueriedRecord<Message>> iter = cursor.asIterator();
        while (iter.hasNext()) {
            FunctionRecord.Builder builder = FunctionRecord.newBuilder().mergeFrom(iter.next().getRecord());
            functionNames.add(builder.getFunctionName());
        }

        return functionNames;
    }

    public RecordQuery buildQueryWithPattern(String pattern) throws MetaStoreException {
        // todo gonna to support sql wildcard character '_' and '%';
        //  But now we implement beginWith() and limit 50
        Builder queryBuilder = RecordQuery.newBuilder()
            .setRecordType(StoreMetadata.FUNCTION_RECORD.getRecordTypeName());

        if (StringUtils.isNotBlank(pattern)) {
            QueryComponent filter = Query.field("function_name").startsWith(pattern);
            return queryBuilder.setFilter(filter).build();
        }
        return queryBuilder.build();
    }

    @Override
    public void dropFunction(TransactionContext context, DatabaseIdent databaseIdent, String functionName) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore store = DirectoryStoreHelper.getFunctionStore(fdbRecordContext, databaseIdent);
        FDBStoredRecord<Message> record = store.loadRecord(buildFunctionKey(functionName));
        if (record == null) {
            throw new MetaStoreException(ErrorCode.DELEGATE_NOT_FOUND, functionName);
        }
        store.deleteRecord(buildFunctionKey(functionName));
    }

    private Tuple buildFunctionKey(String funcName) {
        return Tuple.from(funcName);
    }
}
