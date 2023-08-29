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

import java.util.Map;

import io.polycat.catalog.common.model.*;
import io.polycat.catalog.common.plugin.request.input.IndexInput;
import io.polycat.catalog.common.plugin.request.input.IndexRefreshInput;

// Interface to insert/get the index related metadata from the underlying db
public interface IndexStore {

  void insertIndexObjectName(TransactionContext context, IndexIdent indexIdent, IndexName indexName,
      DatabaseIdent databaseIdent);

  void insertIndexRecord(TransactionContext context, IndexIdent indexIdent, IndexName indexName,
      IndexInput indexInput);

  void insertIndexSchema(TransactionContext context, IndexIdent indexIdent,
      IndexInput indexInput);

  void deleteIndexReference(TransactionContext context, IndexIdent indexIdent);

  void deleteIndexSchema(TransactionContext context, IndexIdent indexIdent);

  void deleteIndexObjectName(TransactionContext context, DatabaseIdent databaseIdent,
      IndexName indexName, IndexIdent indexIdent);

  Map<String, String> getIndexToObjectIdMap(TransactionContext context,
      DatabaseIdent databaseIdent, boolean includeDropped);

  void updateIndexRecord(TransactionContext context, DatabaseIdent databaseIdent,
      IndexIdent indexIdent, IndexName indexName, IndexRefreshInput indexRefreshInput);

  IndexIdent getIndexIdentByIndexName(TransactionContext context, DatabaseIdent databaseIdent, IndexName indexName);

  IndexInfo getIndexRecord(TransactionContext context, IndexIdent indexIdent);
}
