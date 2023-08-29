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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.model.CatalogInnerObject;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.ViewColumnFilter;
import io.polycat.catalog.common.model.ViewFilterObject;
import io.polycat.catalog.common.model.ViewIdent;
import io.polycat.catalog.common.model.ViewName;
import io.polycat.catalog.common.model.ViewNameObject;
import io.polycat.catalog.common.model.ViewPolicyObject;
import io.polycat.catalog.common.model.ViewRecordObject;
import io.polycat.catalog.common.model.ViewStatement;
import io.polycat.catalog.common.plugin.request.input.ViewInput;
import io.polycat.catalog.common.utils.UuidUtil;
import io.polycat.catalog.service.api.ViewService;
import io.polycat.catalog.store.api.CatalogStore;
import io.polycat.catalog.store.api.Transaction;
import io.polycat.catalog.store.api.ViewStore;
import io.polycat.catalog.store.common.StoreConvertor;
import io.polycat.catalog.store.fdb.record.RecordStoreHelper;
import io.polycat.catalog.util.CheckUtil;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import static io.polycat.catalog.common.Operation.CREATE_VIEW;
import static io.polycat.catalog.common.Operation.DROP_VIEW;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class ViewServiceImpl implements ViewService {
    @Autowired
    private ViewStore viewStore;
    @Autowired
    private CatalogStore catalogStore;
    @Autowired
    private Transaction storeTransaction;

    private String createCommitDetail(ViewName viewName) {
        return new StringBuilder()
            .append("database name: ").append(viewName.getDatabaseName()).append(", ")
            .append("view name: ").append(viewName.getViewName())
            .toString();
    }

    /**
     * create view
     *
     * @param databaseName
     * @param viewInput
     */
    @Override
    public void createView(DatabaseName databaseName, ViewInput viewInput) {
        if (viewInput.getViewStatements() == null) {
            throw new CatalogServerException(ErrorCode.ARGUMENT_ILLEGAL, "view statement");
        }
        ViewRecordObject view = convertToViewRecordObject(databaseName.getProjectId(), viewInput);
        //versionStore.createView(databaseName, view);
        String catalogCommitEventId = UuidUtil.generateId();
        try (TransactionContext context = storeTransaction.openTransaction()) {
            DatabaseIdent databaseIdent = DatabaseObjectHelper.getDatabaseIdent(context, databaseName);
            if (viewStore.viewExist(context, databaseIdent, view.getName())) {
                throw new CatalogServerException(ErrorCode.VIEW_ALREADY_EXISTS, view.getName());
            }

            //1、insert an object into ObjectName subspace
            String objectId = UuidUtil.generateId();
            viewStore.insertViewObjectName(context, databaseIdent, view.getName(), objectId);
            viewStore.insertView(context, databaseIdent, objectId, view);

            //3、 insert catalog commit
            String detail = new StringBuilder()
                .append("project id: ").append(databaseName.getProjectId()).append(", ")
                .append("catalog name: ").append(databaseName.getCatalogName()).append(", ")
                .append("database name: ").append(databaseName.getDatabaseName()).append(", ")
                .append("view name: ").append(view.getName())
                .toString();
            long commitTime = RecordStoreHelper.getCurrentTime();
            catalogStore.insertCatalogCommit(context, view.getProjectId(),
                view.getCatalogId(), catalogCommitEventId, commitTime, CREATE_VIEW, detail);
            context.commit();
        }
    }

    private ViewRecordObject convertToViewRecordObject(String projectId, ViewInput viewInput) {
        ViewRecordObject viewRecordObject = new ViewRecordObject();
        viewRecordObject.setName(viewInput.getViewName());

        List<ViewPolicyObject> viewPolicyList = new ArrayList<>();
        for (int i = 0; i < viewInput.getViewStatements().length; i++) {
            ViewStatement viewStatement = viewInput.getViewStatements()[i];

            CatalogInnerObject catalogInnerObject = ServiceImplHelper.getCatalogObject(projectId, ObjectType.TABLE.name(),
                viewStatement.getCatalogName() + "." + viewStatement.getDatabaseName()
                    + "." + viewStatement.getObjectName());

            ViewPolicyObject viewPolicyObject = new ViewPolicyObject();
            viewPolicyObject.setCatalogId(catalogInnerObject.getCatalogId());
            viewPolicyObject.setDatabaseId(catalogInnerObject.getDatabaseId());
            viewPolicyObject.setObjectType(ObjectType.TABLE.name());
            viewPolicyObject.setObjectId(catalogInnerObject.getObjectId());
            viewPolicyObject.setColumnFlag(viewStatement.isRenameColumn());
            viewPolicyObject.setFilterFlag(viewStatement.isFilterFlag());

            if (viewPolicyObject.isColumnFlag()) {
                viewPolicyObject.getColumnName().addAll(Arrays.stream(viewStatement.getColumnNames())
                    .collect(Collectors.toList()));
            }

            if (viewPolicyObject.isFilterFlag()) {
                viewPolicyObject.getViewFilter().addAll(Arrays.stream(viewStatement.getViewColumnFilters())
                    .map(this::convert).collect(Collectors.toList()));
            }
            viewPolicyList.add(viewPolicyObject);

            viewRecordObject.setCatalogId(catalogInnerObject.getCatalogId());
        }

        viewRecordObject.getViewPolicy().addAll(viewPolicyList);
        viewRecordObject.setName(viewInput.getViewName());
        viewRecordObject.setProjectId(projectId);

        return viewRecordObject;
    }

    private ViewFilterObject convert(ViewColumnFilter viewColumnFilter) {
        ViewFilterObject viewFilter = new ViewFilterObject();
        viewFilter.setOperator(viewColumnFilter.getOperator());
        viewFilter.setColumnName(viewColumnFilter.getColumnName());
        viewFilter.setValue(viewColumnFilter.getValue());
        return viewFilter;
    }

    public ViewIdent getViewIdentByViewName(TransactionContext context, ViewName viewName) {
        DatabaseName databaseName = StoreConvertor.databaseName(viewName.getProjectId(),
            viewName.getCatalogName(), viewName.getDatabaseName());
        DatabaseIdent databaseIdent = DatabaseObjectHelper.getDatabaseIdent(context, databaseName);
        if (null == databaseIdent) {
            return null;
        }

        ViewNameObject objectName = viewStore.getViewObjectName(context, databaseIdent, viewName.getViewName());
        ViewIdent viewIdent = new ViewIdent(databaseIdent.getProjectId(), databaseIdent.getCatalogId(),
            databaseIdent.getDatabaseId(), objectName.getObjectId());
        return viewIdent;
    }

    /**
     * drop view
     *
     * @param viewName
     */
    @Override
    public void dropView(ViewName viewName) {
        //versionStore.dropView(viewName);
        String catalogCommitEventId = UuidUtil.generateId();
        try (TransactionContext context = storeTransaction.openTransaction()) {

            ViewIdent viewIdent = getViewIdentByViewName(context, viewName);
            if (viewIdent == null) {
                throw new CatalogServerException(ErrorCode.VIEW_NOT_FOUND);
            }

            DatabaseIdent databaseIdent = StoreConvertor.databaseIdent(viewIdent.getProjectId(), viewIdent.getCatalogId(),
                viewIdent.getDatabaseId(), viewIdent.getRooCatalogId());

            viewStore.dropViewObjectName(context, databaseIdent, viewName.getViewName());

            //2. drop view from View Subspace
            viewStore.dropView(context, viewIdent);

            //3. insert CatalogCommit subspace
            long commitTime = RecordStoreHelper.getCurrentTime();
            catalogStore.insertCatalogCommit(context, viewIdent.getProjectId(),
                viewIdent.getCatalogId(), catalogCommitEventId, commitTime, DROP_VIEW, createCommitDetail(viewName));
            context.commit();
        }
    }

    /**
     * get view by viewName
     *
     * @param viewName
     * @return view
     */
    @Override
    public ViewRecordObject getViewByName(ViewName viewName) {
        CheckUtil.checkStringParameter(viewName.getViewName());
        //return versionStore.getViewByName(viewName);
        try (TransactionContext context = storeTransaction.openTransaction()) {
            ViewIdent viewIdent = getViewIdentByViewName(context, viewName);
            if (viewIdent == null) {
                throw new MetaStoreException(ErrorCode.VIEW_NOT_FOUND);
            }

            ViewRecordObject viewRecord = viewStore.getView(context, viewIdent);
            context.commit();
            return viewRecord;
        }
    }

    /**
     * get view by Id
     *
     * @param viewIdent
     * @return view
     */

    @Override
    public ViewRecordObject getViewById(ViewIdent viewIdent) {
        CheckUtil.checkStringParameter(viewIdent.getViewId());
        //return versionStore.getViewById(viewIdent);
        try (TransactionContext context = storeTransaction.openTransaction()) {
            ViewRecordObject viewRecord = viewStore.getView(context, viewIdent);
            context.commit();
            return viewRecord;
        }
    }

    /**
     * alter view
     *
     * @param viewName
     * @param viewInput
     */
    @Override
    public void alterView(ViewName viewName, ViewInput viewInput) {
        //versionStore.alterView(viewName, viewInput.getViewName());
        try (TransactionContext context = storeTransaction.openTransaction()) {
            // check whether the view exists.
            ViewIdent viewIdent = getViewIdentByViewName(context, viewName);
            ViewRecordObject viewRecord = viewStore.getView(context, viewIdent);

            // alter the view name
            viewStore.updateView(context, viewIdent, viewRecord, viewInput.getViewName());

            // delete the old object name and add the new object name into ObjectName subspace
            DatabaseIdent databaseIdent = StoreConvertor.databaseIdent(viewIdent.getProjectId(), viewIdent.getCatalogId(),
                viewIdent.getDatabaseId(), viewIdent.getRooCatalogId());
            viewStore.dropViewObjectName(context, databaseIdent, viewName.getViewName());

            viewStore.insertViewObjectName(context, databaseIdent, viewInput.getViewName(), viewIdent.getViewId());
            context.commit();
        }
    }

}
