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

import java.lang.reflect.Field;
import java.util.Objects;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBTransactionContext;
import com.apple.foundationdb.subspace.Subspace;

import io.polycat.catalog.common.MetaStoreException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.model.StoreObject;
import io.polycat.catalog.store.api.StoreBase;

@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "fdb")
public class StoreImpl implements StoreBase {
    FDBDatabase fdb = FDBDatabaseFactory.instance().getDatabase();

    @Override
    public StoreObject openDB() {
        return new StoreObject(fdb);
    }

    @Override
    public void clearDB() {
        try (FDBRecordContext context = fdb.openContext()) {
            Transaction tx = getTransaction(context);
            final byte[] st = new Subspace(new byte[]{(byte) 0x00}).getKey();
            final byte[] en = new Subspace(new byte[]{(byte) 0xFF}).getKey();
            tx.clear(st, en);
            context.commit();
        } finally {
            fdb.close();
        }
    }

    private Transaction getTransaction(FDBTransactionContext context) throws MetaStoreException {
        try {
            Field field = FDBTransactionContext.class.getDeclaredField("transaction");
            field.setAccessible(true);
            return (Transaction) Objects.requireNonNull(field.get(context));
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }



}
