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


import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.tuple.Tuple;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.ViewFilterObject;
import io.polycat.catalog.common.model.ViewIdent;
import io.polycat.catalog.common.model.ViewNameObject;
import io.polycat.catalog.common.model.ViewPolicyObject;
import io.polycat.catalog.common.model.ViewRecordObject;
import io.polycat.catalog.store.api.Transaction;
import io.polycat.catalog.store.api.ViewStore;
import io.polycat.catalog.store.common.StoreConvertor;
import io.polycat.catalog.store.fdb.record.DirectoryStoreHelper;
import io.polycat.catalog.store.fdb.record.RecordStoreHelper;
import io.polycat.catalog.store.fdb.record.TransactionContextUtil;


import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.google.protobuf.Message;
import io.polycat.catalog.store.protos.View;
import io.polycat.catalog.store.protos.ViewFilter;
import io.polycat.catalog.store.protos.ViewObjectName;
import io.polycat.catalog.store.protos.ViewPolicy;
import io.polycat.catalog.store.protos.ViewRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;


@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "fdb")
public class ViewStoreImpl implements ViewStore {

    @Autowired
    private Transaction storeTransaction;

    public ViewStoreImpl() {

    }

    private static class ViewStoreImplHandler {
        private static final ViewStoreImpl INSTANCE = new ViewStoreImpl();
    }

    public static ViewStoreImpl getInstance() {
        return ViewStoreImpl.ViewStoreImplHandler.INSTANCE;
    }


    private Tuple buildViewObjectNameKey(String viewName) {
        return Tuple.from(viewName);
    }

    @Override
    public Boolean viewExist(TransactionContext context, DatabaseIdent databaseIdent, String name) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore objectNameStore = DirectoryStoreHelper.getViewObjectNameStore(fdbRecordContext, databaseIdent);
        FDBStoredRecord<Message> objectRecord = objectNameStore
            .loadRecord(buildViewObjectNameKey(name));
        return (objectRecord != null);
    }

    @Override
    public void insertViewObjectName(TransactionContext context, DatabaseIdent databaseIdent, String viewName, String objectId) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore objectNameStore = DirectoryStoreHelper.getViewObjectNameStore(fdbRecordContext, databaseIdent);
        ViewObjectName viewObjectName = ViewObjectName.newBuilder()
            .setName(viewName)
            .setObjectId(objectId).build();
        objectNameStore.insertRecord(viewObjectName);
    }

    public ViewNameObject getViewObjectName(TransactionContext context, DatabaseIdent databaseIdent, String viewName) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore objectNameStore = DirectoryStoreHelper.getViewObjectNameStore(fdbRecordContext, databaseIdent);
        FDBStoredRecord<Message> storedRecord =
            objectNameStore.loadRecord(buildViewObjectNameKey(viewName));
        if (storedRecord == null) {
            return null;
        }

        ViewObjectName viewObjectName = ViewObjectName.newBuilder().mergeFrom(storedRecord.getRecord()).build();

        return new ViewNameObject(viewObjectName.getName(), viewObjectName.getObjectId());
    }

    @Override
    public void dropViewObjectName(TransactionContext context, DatabaseIdent databaseIdent, String viewName) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore objectNameStore = DirectoryStoreHelper.getViewObjectNameStore(fdbRecordContext, databaseIdent);
        objectNameStore.deleteRecord(buildViewObjectNameKey(viewName));
    }

    private ViewPolicy trans2ViewPolicy(ViewPolicyObject viewPolicyObject) {
        ViewPolicy.Builder builder = ViewPolicy.newBuilder()
            .setCatalogId(viewPolicyObject.getCatalogId())
            .setDatabaseId(viewPolicyObject.getDatabaseId())
            .setObjectType(viewPolicyObject.getObjectType())
            .setObjectId(viewPolicyObject.getObjectId())
            .setColumnFlag(viewPolicyObject.isColumnFlag())
            .addAllColumnName(viewPolicyObject.getColumnName())
            .addAllExpression(viewPolicyObject.getExpression())
            .setFilterFlag(viewPolicyObject.isFilterFlag());

        for (ViewFilterObject viewFilterObject : viewPolicyObject.getViewFilter()) {
            ViewFilter viewFilter = ViewFilter.newBuilder().setOperator(viewFilterObject.getOperator())
                .setColumnName(viewFilterObject.getColumnName()).setValue(viewFilterObject.getValue()).build();
            builder.addViewFilter(viewFilter);
        }

        return builder.build();
    }

    private View trans2View(ViewRecordObject viewRecordObject) {
        View.Builder builder = View.newBuilder()
            .setPrimaryKey(0)
            .setName(viewRecordObject.getName())
            .setComment(viewRecordObject.getComment())
            .setCreatedTime(viewRecordObject.getCreatedTime());

        for(ViewPolicyObject viewPolicyObject: viewRecordObject.getViewPolicy()) {
            ViewPolicy viewPolicy = trans2ViewPolicy(viewPolicyObject);
            builder.addViewPolicy(viewPolicy);
        }

        return builder.build();
    }

    @Override
    public void insertView(TransactionContext context, DatabaseIdent databaseIdent, String viewId, ViewRecordObject view) {
        ViewIdent viewIdent = StoreConvertor.viewIdent(databaseIdent.getProjectId(), databaseIdent.getCatalogId(),
            databaseIdent.getDatabaseId(), viewId);
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore viewStore = DirectoryStoreHelper.getViewReferenceStore(fdbRecordContext, viewIdent);
        ViewRecordObject viewRecordObject = new ViewRecordObject(view);
        viewRecordObject.setProjectId(databaseIdent.getProjectId());
        viewRecordObject.setCatalogId(databaseIdent.getCatalogId());
        viewRecordObject.setDatabaseId(databaseIdent.getDatabaseId());
        viewRecordObject.setViewId(viewId);
        viewRecordObject.setCreatedTime(RecordStoreHelper.getCurrentTime());

        View newView = trans2View(viewRecordObject);
        viewStore.insertRecord(newView);
    }

    @Override
    public void updateView(TransactionContext context, ViewIdent viewIdent, ViewRecordObject view, String newName) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore viewStore = DirectoryStoreHelper.getViewReferenceStore(fdbRecordContext, viewIdent);
        ViewRecordObject viewRecordObject = new ViewRecordObject(view);
        viewRecordObject.setName(newName);
        viewStore.updateRecord(trans2View(viewRecordObject));
    }

    @Override
    public ViewRecordObject getView(TransactionContext context, ViewIdent viewIdent) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBStoredRecord<Message> viewRecord = DirectoryStoreHelper.getViewReferenceStore(fdbRecordContext, viewIdent)
            .loadRecord(buildViewReferenceKey());
        View view = View.newBuilder().mergeFrom(viewRecord.getRecord()).build();
        return trans2ViewRecordObject(view, viewIdent);
    }

    @Override
    public void dropView(TransactionContext context, ViewIdent viewIdent) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        FDBRecordStore viewStore = DirectoryStoreHelper.getViewReferenceStore(fdbRecordContext, viewIdent);
        viewStore.deleteRecord(buildViewReferenceKey());
    }

    private ViewFilterObject trans2ViewFilterObject(ViewFilter viewFilter) {
        ViewFilterObject viewFilterObject = new ViewFilterObject();
        viewFilterObject.setOperator(viewFilter.getOperator());
        viewFilterObject.setValue(viewFilter.getValue());
        viewFilterObject.setColumnName(viewFilter.getColumnName());

        return viewFilterObject;
    }

    private ViewPolicyObject trans2ViewPolicyObject(ViewPolicy viewPolicy, ViewIdent viewIdent) {
        ViewPolicyObject viewPolicyObject = new ViewPolicyObject();
        viewPolicyObject.setCatalogId(viewIdent.getCatalogId());
        viewPolicyObject.setDatabaseId(viewIdent.getDatabaseId());
        viewPolicyObject.setObjectType(viewPolicy.getObjectType());
        viewPolicyObject.setObjectId(viewPolicy.getObjectId());
        viewPolicyObject.setColumnFlag(viewPolicy.getColumnFlag());
        viewPolicyObject.getColumnName().addAll(viewPolicy.getColumnNameList());
        viewPolicyObject.getExpression().addAll(viewPolicy.getExpressionList());
        viewPolicyObject.setFilterFlag(viewPolicy.getFilterFlag());

        for (ViewFilter viewFilter : viewPolicy.getViewFilterList()) {
            viewPolicyObject.getViewFilter().add(trans2ViewFilterObject(viewFilter));
        }

        return viewPolicyObject;
    }

    private ViewRecordObject trans2ViewRecordObject(View view, ViewIdent viewIdent) {
        ViewRecordObject viewRecordObject = new ViewRecordObject();
        viewRecordObject.setProjectId(viewIdent.getProjectId());
        viewRecordObject.setCatalogId(viewIdent.getCatalogId());
        viewRecordObject.setDatabaseId(viewIdent.getDatabaseId());
        viewRecordObject.setViewId(viewIdent.getViewId());
        viewRecordObject.setName(view.getName());
        viewRecordObject.setComment(view.getComment());
        viewRecordObject.setCreatedTime(view.getCreatedTime());

        for (ViewPolicy viewPolicy : view.getViewPolicyList()) {
            viewRecordObject.getViewPolicy().add(trans2ViewPolicyObject(viewPolicy, viewIdent));
        }

        return viewRecordObject;
    }

    @Override
    public ViewRecordObject getViewById(ViewIdent viewIdent) throws MetaStoreException {
        try (TransactionContext context = storeTransaction.openTransaction()) {
            FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
            FDBRecordStore viewStore = DirectoryStoreHelper.getViewReferenceStore(fdbRecordContext, viewIdent);
            FDBStoredRecord<Message> storedRecord = viewStore.loadRecord(buildViewReferenceKey());
            if (storedRecord == null) {
                throw new MetaStoreException(ErrorCode.VIEW_NOT_FOUND);
            }
            View view = View.newBuilder().mergeFrom(storedRecord.getRecord()).build();
            return trans2ViewRecordObject(view, viewIdent);
        }
    }


    private static String getViewParentId(String projectId, String catalogId, String databaseId) {
        return projectId + "." + catalogId + "." + databaseId;
    }

    private Tuple buildViewReferenceKey() {
        return Tuple.from(0);
    }
}
