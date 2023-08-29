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

import io.polycat.catalog.common.model.TransactionContext;

import javax.annotation.Nonnull;
import java.util.function.Function;

public class TransactionRunnerUtil {

    public static <T> RunnerResult<T> transactionRun(@Nonnull Function<TransactionContext, T> retriable) {
        return transactionRun(null, retriable);
    }

    private static <T> RunnerResult<T> transactionRun(TransactionFrameRunner runner, @Nonnull Function<TransactionContext, T> retriable) {
        if (runner == null) {
            runner = new TransactionFrameRunner();
        }
        return runner.run(retriable);
    }

    private static <T> RunnerResult<T> transactionReadRun(TransactionFrameRunner runner, @Nonnull Function<TransactionContext, T> retriable) {
        if (runner == null) {
            runner = new TransactionFrameRunner();
        }
        return runner.runRead(retriable);
    }

    public static <T> RunnerResult<T> transactionRun(int maxAttempts, boolean exceptionThrow, @Nonnull Function<TransactionContext, T> retriable) {
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(maxAttempts);
        runner.setExceptionThrow(exceptionThrow);
        return transactionRun(runner, retriable);
    }

    public static <T> RunnerResult<T> transactionRun(boolean exceptionThrow, @Nonnull Function<TransactionContext, T> retriable) {
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setExceptionThrow(exceptionThrow);
        return transactionRun(runner, retriable);
    }

    public static <T> RunnerResult<T> transactionRunThrow(@Nonnull Function<TransactionContext, T> retriable) {
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setExceptionThrow(true);
        return transactionRun(runner, retriable);
    }


    public static <T> RunnerResult<T> transactionReadRunThrow(int maxAttempts, boolean exceptionThrow, @Nonnull Function<TransactionContext, T> retriable) {
        TransactionFrameRunner runner = new TransactionFrameRunner("");
        runner.setMaxAttempts(maxAttempts);
        runner.setExceptionThrow(exceptionThrow);
        return transactionReadRun(runner, retriable);
    }

    public static <T> RunnerResult<T> transactionReadRunThrow(boolean exceptionThrow, @Nonnull Function<TransactionContext, T> retriable) {
        TransactionFrameRunner runner = new TransactionFrameRunner("");
        runner.setExceptionThrow(exceptionThrow);
        return transactionReadRun(runner, retriable);
    }

    public static <T> RunnerResult<T> transactionReadRunThrow(@Nonnull Function<TransactionContext, T> retriable) {
        TransactionFrameRunner runner = new TransactionFrameRunner("");
        runner.setExceptionThrow(true);
        return transactionReadRun(runner, retriable);
    }
}
