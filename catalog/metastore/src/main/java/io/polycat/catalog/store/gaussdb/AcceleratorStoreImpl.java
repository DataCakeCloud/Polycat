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
import io.polycat.catalog.common.model.AcceleratorPropertiesObject;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.SqlTemplate;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.plugin.request.input.AcceleratorInput;
import io.polycat.catalog.common.plugin.request.input.SqlTemplateInput;
import io.polycat.catalog.store.api.AcceleratorStore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import java.util.List;
@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "gauss")
public class AcceleratorStoreImpl implements AcceleratorStore {
    @Override
    public Boolean acceleratorObjectNameExist(TransactionContext context, DatabaseIdent databaseIdent, String acceleratorName) throws MetaStoreException {
        return null;
    }

    @Override
    public String getAcceleratorId(TransactionContext context, DatabaseIdent databaseIdent, String acceleratorName) throws MetaStoreException {
        return null;
    }

    @Override
    public void insertAcceleratorObjectName(TransactionContext context, DatabaseIdent databaseIdent, String acceleratorName, String acceleratorId) throws MetaStoreException {

    }

    @Override
    public void deleteAcceleratorObjectName(TransactionContext context, DatabaseIdent databaseIdent, String acceleratorName) throws MetaStoreException {

    }

    @Override
    public void insertAcceleratorProperties(TransactionContext context, String projectId, String acceleratorId, AcceleratorInput acceleratorInput, String location) throws MetaStoreException {

    }

    @Override
    public void deleteAcceleratorProperties(TransactionContext context, String projectId, String acceleratorId) throws MetaStoreException {

    }

    @Override
    public void updateAcceleratorPropertiesCompiled(TransactionContext context, String projectId, String acceleratorId, boolean compiled) throws MetaStoreException {

    }

    @Override
    public List<AcceleratorPropertiesObject> listAcceleratorProperties(TransactionContext context, DatabaseIdent databaseIdent) throws MetaStoreException {
        return null;
    }

    @Override
    public void insertTemplates(TransactionContext context, String projectId, String acceleratorId, List<SqlTemplateInput> sqlTemplateInputList) throws MetaStoreException {

    }

    @Override
    public List<SqlTemplate> listAcceleratorTemplate(TransactionContext context, String projectId, String acceleratorId) throws MetaStoreException {
        return null;
    }

    @Override
    public void updateTemplateStatusCompiled(TransactionContext context, String projectId, String acceleratorId, SqlTemplateInput templateInput) throws MetaStoreException {

    }

    @Override
    public void deleteAcceleratorTemplateAll(TransactionContext context, String projectId, String acceleratorId) throws MetaStoreException {

    }
}
