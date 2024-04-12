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

import javax.annotation.Nonnull;

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.record.RecordCoreRetriableTransactionException;
import io.polycat.catalog.store.api.RetriableException;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "fdb")
public class RetriableExceptionImpl implements RetriableException {

    private static class RetriableExceptionHandler {
        private static final RetriableException INSTANCE = new RetriableExceptionImpl();
    }

    public static RetriableException getInstance() {
        return RetriableExceptionImpl.RetriableExceptionHandler.INSTANCE;
    }


    @Override
    public boolean isRetriableException(@Nonnull Throwable t) {
        if (isRetriableExceptionCommon(t)) {
            return true;
        }

        boolean retry;
        for(retry = false; t != null; t = t.getCause()) {
            if (!(t instanceof FDBException)) {
                if (t instanceof RecordCoreRetriableTransactionException) {
                    retry = true;
                }
            } else {
                FDBException fdbE = (FDBException)t;
                retry = retry || fdbE.isRetryable();
            }
        }

        return retry;
    }
}
