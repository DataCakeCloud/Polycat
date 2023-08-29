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

import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.ViewIdent;
import io.polycat.catalog.common.model.ViewNameObject;
import io.polycat.catalog.common.model.ViewRecordObject;
import io.polycat.catalog.store.protos.ViewObjectName;


public interface ViewStore {

    ViewRecordObject getViewById(ViewIdent viewIdent) throws MetaStoreException;

    void insertViewObjectName(TransactionContext context, DatabaseIdent databaseIdent, String viewName, String objectId);

    Boolean viewExist(TransactionContext context, DatabaseIdent databaseIdent, String name);

    void insertView(TransactionContext context, DatabaseIdent databaseIdent, String viewId, ViewRecordObject view);

    void dropViewObjectName(TransactionContext context, DatabaseIdent databaseIdent, String viewName);

    void dropView(TransactionContext context, ViewIdent viewIdent);

    ViewRecordObject getView(TransactionContext context, ViewIdent viewIdent);

    void updateView(TransactionContext context, ViewIdent viewIdent, ViewRecordObject view, String newName);

    ViewNameObject getViewObjectName(TransactionContext context, DatabaseIdent databaseIdent, String viewName);
}