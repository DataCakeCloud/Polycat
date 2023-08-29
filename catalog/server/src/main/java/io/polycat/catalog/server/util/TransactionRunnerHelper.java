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

import io.polycat.catalog.store.api.RetriableException;
import io.polycat.catalog.store.api.Transaction;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class TransactionRunnerHelper {
    @Getter
    private static Transaction transaction;

    @Getter
    private static RetriableException retriableException;

    @Autowired
    public void setTransaction(Transaction transaction) {
        TransactionRunnerHelper.transaction = transaction;
    }

    @Autowired
    public void setRetriableException(RetriableException retriableException) {
        TransactionRunnerHelper.retriableException = retriableException;
    }
}
