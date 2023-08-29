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

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.store.api.RetriableException;
import io.polycat.catalog.store.api.Transaction;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TransactionFrameRunner implements AutoCloseable {
    private int maxAttempts = 16;
    private long maxDelayMillis = 1000L;
    private long minDelayMillis = 2L;
    private long initialDelayMillis = 10L;
    private boolean exceptionThrow = false;

    private Transaction transaction = TransactionRunnerHelper.getTransaction();
    private RetriableException retriableException = TransactionRunnerHelper.getRetriableException();

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

    public void setExceptionThrow(boolean exceptionThrow) {
        this.exceptionThrow = exceptionThrow;
    }

    public TransactionFrameRunner() {
        log.debug("Create TransactionFrameRunner.");
    }

    public TransactionFrameRunner(String read) {
        log.debug("Create read TransactionFrameRunner.");
    }

    public <T> RunnerResult<T> run(@Nonnull Function<TransactionContext, T> retriable) {
        return (RunnerResult<T>) (new TransactionFrameRunner.RunRetriable()).run(retriable);
    }


    public <T> RunnerResult<T> runRead(@Nonnull Function<TransactionContext, T> retriable) {
        return (RunnerResult<T>) (new TransactionFrameRunner.RunRetriable()).runRead(retriable);
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
            this.currDelay = TransactionFrameRunner.this.getInitialDelayMillis();
        }

        public RunnerResult<T> run(@Nonnull Function<TransactionContext, T> retriable) {
            boolean again = true;
            while(again) {
                try {
                    this.context = TransactionFrameRunner.this.transaction.openTransaction();
                    this.retVal = retriable.apply(this.context);
                    this.context.commit();
                    again = false;
                } catch (RuntimeException e) {
                    if (this.context != null) {
                        this.context.rollback();
                        if ((TransactionFrameRunner.this.retriableException.isRetriableException(e))) {
                            if (this.currAttempt + 1 < TransactionFrameRunner.this.maxAttempts) {
                                long delay = (long) (Math.random() * (double) this.currDelay);
                                this.currAttempt += 1;
                                this.currDelay = Math
                                    .max(Math.min(delay * 2L, TransactionFrameRunner.this.getMaxDelayMillis()),
                                        TransactionFrameRunner.this.getMinDelayMillis());

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
            if (TransactionFrameRunner.this.exceptionThrow && this.exception != null) {
                throw this.exception;
            }
            return new RunnerResult<>(this.retVal, this.exception);
        }

        public RunnerResult<T> runRead(@Nonnull Function<TransactionContext, T> retriable) {
            boolean again = true;
            while(again) {
                try {
                    this.context = TransactionFrameRunner.this.transaction.openReadTransaction();
                    this.retVal = retriable.apply(this.context);
                    again = false;
                } catch (RuntimeException e) {
                    if (this.context != null) {
                        if ((TransactionFrameRunner.this.retriableException.isRetriableException(e))) {
                            if (this.currAttempt + 1 < TransactionFrameRunner.this.maxAttempts) {
                                long delay = (long) (Math.random() * (double) this.currDelay);
                                this.currAttempt += 1;
                                this.currDelay = Math
                                        .max(Math.min(delay * 2L, TransactionFrameRunner.this.getMaxDelayMillis()),
                                                TransactionFrameRunner.this.getMinDelayMillis());

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
                        again = false;
                        this.exception = e;
                    }
                } catch (Exception e) {
                    again = false;
                    this.exception = new CatalogServerException(e.getMessage(), ErrorCode.INNER_SERVER_ERROR);
                } finally {
                    if (this.context != null) {
                        this.context.close();
                        this.context = null;
                    }
                }
            }
            if (TransactionFrameRunner.this.exceptionThrow && this.exception != null) {
                throw this.exception;
            }
            return new RunnerResult<>(this.retVal, this.exception);
        }

    }

    public synchronized void close() {

    }

}
