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

import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.store.api.Transaction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "gauss")
public class TransactionImpl implements Transaction {
    @Autowired
    private DataSourceTransactionManager transactionManager;

    @Override
    public TransactionContext openTransaction() {
        DefaultTransactionDefinition def = new DefaultTransactionDefinition();
        def.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        def.setTimeout(6000);
        TransactionStatus transaction = transactionManager.getTransaction(def);
        return new TransactionContext(transaction, this);
    }

    @Override
    public TransactionContext openReadTransaction() {
        return null;
    }

    @Override
    public void commitTransaction(TransactionContext context) {
        transactionManager.commit((TransactionStatus)context.getStoreContext());
    }

    @Override
    public void rollbackTransaction(TransactionContext context) {
        transactionManager.rollback((TransactionStatus)context.getStoreContext());
    }

    @Override
    public String getCurContextVersion(TransactionContext context) {
        return null;
    }

    @Override
    public void close(TransactionContext context) {}

}
