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
import io.polycat.catalog.common.model.CatalogIdent;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.FunctionObject;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.plugin.request.input.FunctionInput;


public interface FunctionStore {

    void createFunctionSubspace(TransactionContext context, String projectId);

    void dropFunctionSubspace(TransactionContext context, String projectId);

    Boolean functionExist(TransactionContext context, DatabaseIdent databaseIdent,String functionName)
            throws MetaStoreException;

    FunctionObject getFunction(TransactionContext context, DatabaseIdent databaseIdent,String functionName)
            throws MetaStoreException;

    void insertFunction(TransactionContext context, DatabaseIdent databaseIdent, String funcName,
                        FunctionInput funcInput, long createTime) throws MetaStoreException;

    /**
     *    // pattern: todo gonna to support sql wildcard character '_' and '%';
     * @param context
     * @param databaseIdent
     * @param pattern
     * @return
     */
    List <String> listFunctions(TransactionContext context, DatabaseIdent databaseIdent, String pattern);

    List<FunctionObject> listAllFunctions(TransactionContext context, CatalogIdent catalogIdent);

    void dropFunction(TransactionContext context, DatabaseIdent databaseIdent, String functionName);

}
