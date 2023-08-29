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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.tuple.Versionstamp;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.utils.CodecUtil;
import io.polycat.catalog.store.api.StoreBase;
import io.polycat.catalog.store.api.Transaction;

import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "fdb")
public class TransactionImpl implements Transaction {
    @Autowired
    private StoreBase storeBase;
    FDBDatabase db = (FDBDatabase) storeBase.openDB().getStoreObject();

    public TransactionImpl() {
    }

    private static class TransactionImplHandler {
        private static final TransactionImpl INSTANCE = new TransactionImpl();
    }

    public static TransactionImpl getInstance() {
        return TransactionImpl.TransactionImplHandler.INSTANCE;
    }

    @Override
    public TransactionContext openTransaction() {
        FDBRecordContext fdbContext = db.openContext();
        return new TransactionContext(fdbContext, this);
    }

    @Override
    public TransactionContext openReadTransaction() {
        return openTransaction();
    }

    @Override
    public void commitTransaction(TransactionContext context) {
        FDBRecordContext fdbContext = getStoreContext(context);
        fdbContext.commit();
    }

    @Override
    public void rollbackTransaction(TransactionContext context) {
    }

    @Override
    public void close(TransactionContext context) {
        FDBRecordContext fdbRecordContext = getStoreContext(context);
        fdbRecordContext.close();
    }

    @Override
    public String getCurContextVersion(TransactionContext context) {
        FDBRecordContext fdbRecordContext = getStoreContext(context);
        Versionstamp versionstamp = getCurContextVersion(fdbRecordContext);
        return CodecUtil.bytes2Hex(versionstamp.getBytes());
    }

    private FDBRecordContext getStoreContext(TransactionContext context) {
        if ((context != null) && (context.getStoreContext() instanceof FDBRecordContext)) {
            return (FDBRecordContext) context.getStoreContext();
        }
        return null;
    }

    private Versionstamp getCurContextVersion(FDBRecordContext context) {
        return Versionstamp.fromBytes(ByteBuffer.allocate(Versionstamp.LENGTH)
            .order(ByteOrder.BIG_ENDIAN)
            .putLong(context.getReadVersion())
            .putInt(0xffffffff)
            .array());
    }

}
