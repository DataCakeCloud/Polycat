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
package io.polycat.catalog.common.model;

import java.util.Optional;

import io.polycat.catalog.common.utils.CatalogToken;


public class TraverseCursorResult<T> {

    private final T resultValue;

    private CatalogToken continuation;

    private CatalogToken previousToken;

    public TraverseCursorResult(final T result, final CatalogToken continuation) {
        this.resultValue = result;
        this.continuation = continuation;
    }

    public TraverseCursorResult(final T result, final CatalogToken continuation, CatalogToken previousToken) {
        this.resultValue = result;
        this.continuation = continuation;
        this.previousToken = previousToken;
    }

    public Optional<CatalogToken> getContinuation() {
        return Optional.ofNullable(continuation);
    }

    public T getResult() {
        return resultValue;
    }

    public String getPreviousTokenString() {
        if (previousToken != null) {
            return previousToken.toString();
        }
        return null;
    }
}

