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

import java.util.Optional;

import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.TableCommitObject;
import io.polycat.catalog.common.model.TableIdent;
import io.polycat.catalog.common.model.TableReferenceObject;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.store.api.TableMetaStore;
import io.polycat.catalog.store.common.StoreConvertor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class TableReferenceHelper {
    private static TableMetaStore tableMetaStore;

    @Autowired
    public void setTableMetaStore(TableMetaStore tableMetaStore) {
        this.tableMetaStore = tableMetaStore;
    }

    /*public static TableReferenceObject getTableReference(TransactionContext context, TableIdent tableIdent)
        throws MetaStoreException {
        TableReferenceObject tableReference = tableMetaStore.getTableReference(context, tableIdent);
        if (tableReference == null) {
            if (!TableCommitHelper.checkTableDropped(context, tableIdent)) {
                tableReference = getSubBranchFakeTableReference(context, tableIdent);
            }
        }
        return tableReference;
    }*/

    /*public static TableReferenceObject getTableReferenceOrElseThrow(TransactionContext context, TableIdent tableIdent) {
        TableReferenceObject tableReference = getTableReference(context, tableIdent);
        if (tableReference == null) {
            throw new CatalogServerException(ErrorCode.TABLE_REFERENCE_NOT_FOUND, tableIdent.getTableId());
        }

        return tableReference;
    }*/

    private static TableReferenceObject getSubBranchFakeTableReference(TransactionContext context, TableIdent subBranchTableIdent)
        throws MetaStoreException {
        ParentBranchDatabaseIterator parentBranchDatabaseIterator = new ParentBranchDatabaseIterator(context,
            subBranchTableIdent);

        while (parentBranchDatabaseIterator.hasNext()) {
            DatabaseIdent parentDatabaseIdent = parentBranchDatabaseIterator.nextDatabase();
            String subBranchVersion = parentBranchDatabaseIterator.nextBranchVersion(parentDatabaseIdent);

            TableIdent parentBranchTableIdent = StoreConvertor.tableIdent(parentDatabaseIdent,
                subBranchTableIdent.getTableId());
            Optional<TableCommitObject> tableCommit = tableMetaStore.getLatestTableCommit(context, parentBranchTableIdent,
                subBranchVersion);
            if ((tableCommit.isPresent()) && (!TableCommitHelper.isDropCommit(tableCommit.get()))) {
                return new TableReferenceObject(0, 0);
            }
        }
        // TODO: 从上游Branch中查询TableReference，都已经给到ID了说明都在，所以如果连上级Branch都查询不到就抛出Exception
        return null;
    }

}
