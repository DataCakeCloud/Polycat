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

import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.AcceleratorPropertiesObject;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.SqlTemplate;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.plugin.request.input.AcceleratorInput;
import io.polycat.catalog.common.plugin.request.input.SqlTemplateInput;


public interface AcceleratorStore {
    Boolean acceleratorObjectNameExist(TransactionContext context, DatabaseIdent databaseIdent,
        String acceleratorName) throws MetaStoreException;

    String getAcceleratorId(TransactionContext context, DatabaseIdent databaseIdent,
        String acceleratorName) throws MetaStoreException;

    void insertAcceleratorObjectName(TransactionContext context, DatabaseIdent databaseIdent,
        String acceleratorName, String acceleratorId) throws MetaStoreException;

    void deleteAcceleratorObjectName(TransactionContext context, DatabaseIdent databaseIdent,
        String acceleratorName) throws MetaStoreException;

    void insertAcceleratorProperties(TransactionContext context, String projectId, String acceleratorId,
        AcceleratorInput acceleratorInput, String location) throws MetaStoreException;

    void deleteAcceleratorProperties(TransactionContext context, String projectId, String acceleratorId)
        throws MetaStoreException;

    void updateAcceleratorPropertiesCompiled(TransactionContext context, String projectId, String acceleratorId,
        boolean compiled)
        throws MetaStoreException;

    List<AcceleratorPropertiesObject> listAcceleratorProperties(TransactionContext context,
        DatabaseIdent databaseIdent) throws MetaStoreException;

    void insertTemplates(TransactionContext context, String projectId, String acceleratorId,
        List<SqlTemplateInput> sqlTemplateInputList) throws MetaStoreException;

    List<SqlTemplate> listAcceleratorTemplate(TransactionContext context, String projectId,
        String acceleratorId)
        throws MetaStoreException;

    void updateTemplateStatusCompiled(TransactionContext context, String projectId, String acceleratorId,
        SqlTemplateInput templateInput) throws MetaStoreException;

    void deleteAcceleratorTemplateAll(TransactionContext context, String projectId, String acceleratorId) throws MetaStoreException;
}
