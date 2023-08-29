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

import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.*;
import io.polycat.catalog.store.api.ViewStore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "gauss")
public class ViewStoreImpl implements ViewStore {
    @Override
    public ViewRecordObject getViewById(ViewIdent viewIdent) throws MetaStoreException {
        return null;
    }

    @Override
    public void insertViewObjectName(TransactionContext context, DatabaseIdent databaseIdent, String viewName, String objectId) {

    }

    @Override
    public Boolean viewExist(TransactionContext context, DatabaseIdent databaseIdent, String name) {
        return null;
    }

    @Override
    public void insertView(TransactionContext context, DatabaseIdent databaseIdent, String viewId, ViewRecordObject view) {

    }

    @Override
    public void dropViewObjectName(TransactionContext context, DatabaseIdent databaseIdent, String viewName) {

    }

    @Override
    public void dropView(TransactionContext context, ViewIdent viewIdent) {

    }

    @Override
    public ViewRecordObject getView(TransactionContext context, ViewIdent viewIdent) {
        return null;
    }

    @Override
    public void updateView(TransactionContext context, ViewIdent viewIdent, ViewRecordObject view, String newName) {

    }

    @Override
    public ViewNameObject getViewObjectName(TransactionContext context, DatabaseIdent databaseIdent, String viewName) {
        return null;
    }
}
