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
package io.polycat.catalog.server.util;

import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.Resource;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.store.api.RetriableException;
import io.polycat.catalog.store.api.Transaction;
import io.polycat.catalog.store.fdb.record.impl.RetriableExceptionImpl;

import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;

public class TransactionRunner implements AutoCloseable {
    private int maxAttempts = 16;
    private long maxDelayMillis = 1000L;
    private long minDelayMillis = 2L;
    private long initialDelayMillis = 10L;

    public int getMaxAttempts() {
        return this.maxAttempts;
    }

    public long getMaxDelayMillis() { return this.maxDelayMillis; }
    public long getMinDelayMillis() {
        return this.minDelayMillis;
    }

    public long getInitialDelayMillis() {
        return this.initialDelayMillis;
    }

    public void setMaxAttempts(int maxAttempts) {
        if (maxAttempts <= 0) {
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        } else {
            this.maxAttempts = maxAttempts;
        }
    }


    public TransactionRunner() {
    }


    public <T> T run(@Nonnull Function<TransactionContext, ? extends T> retriable) {
        return (T) (new RunRetriable()).run(retriable);
    }

    private class RunRetriable<T> {
        private int currAttempt;
        private long currDelay;

        @Nullable
        private TransactionContext context;
        @Nullable
        T retVal;
        @Nullable
        RuntimeException exception;

        private RunRetriable() {
            this.currAttempt = 0;
            this.retVal = null;
            this.exception = null;
            this.currDelay = TransactionRunner.this.getInitialDelayMillis();
        }

        public T run(@Nonnull Function<TransactionContext, ? extends T> retriable) {
            boolean again = true;
            Transaction transaction = TransactionRunnerHelper.getTransaction();
            RetriableException retriableException = TransactionRunnerHelper.getRetriableException();
            while(again) {
                try {
                    this.context = transaction.openTransaction();
                    this.retVal = retriable.apply(this.context);
                    this.context.commit();
                    return this.retVal;
                } catch (RuntimeException e) {
                    if (this.context != null)  {
                        this.context.rollback();
                        if (retriableException.isRetriableException(e)) {
                            if (this.currAttempt + 1 < TransactionRunner.this.maxAttempts) {
                                long delay = (long)(Math.random() * (double)this.currDelay);
                                this.currAttempt += 1;
                                this.currDelay = Math.max(Math.min(delay * 2L, TransactionRunner.this.getMaxDelayMillis()),
                                    TransactionRunner.this.getMinDelayMillis());

                                try {
                                    Thread.sleep(this.currDelay);
                                } catch (InterruptedException ex) {
                                    ex.printStackTrace();
                                }

                            } else {
                                again = false;
                                this.exception = e;
                            }
                        } else {
                            again = false;
                            this.exception = e;
                        }
                    } else {
                        if (this.context != null) {
                            this.context.rollback();
                        }
                        again = false;
                        this.exception = e;
                    }
                } catch (Exception e) {
                    if (this.context != null) {
                        this.context.rollback();
                    }

                    again = false;
                    this.exception = new CatalogServerException(e.getMessage(), ErrorCode.INNER_SERVER_ERROR);
                } finally {
                    if (this.context != null) {
                        this.context.close();
                        this.context = null;
                    }
                }
            }

            if (this.exception == null) {
                return this.retVal;
            } else {
                throw this.exception;
            }
        }
    }

    public synchronized void close() {

    }

}
