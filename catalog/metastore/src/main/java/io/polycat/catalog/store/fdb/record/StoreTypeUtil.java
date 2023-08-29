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
package io.polycat.catalog.store.fdb.record;

import com.apple.foundationdb.record.IsolationLevel;
import io.polycat.catalog.common.model.TransactionIsolationLevel;


public class StoreTypeUtil {
    public static IsolationLevel trans2IsolationLevel(TransactionIsolationLevel isolationLevel) {
        switch (isolationLevel) {
            case SNAPSHOT:
                return IsolationLevel.SNAPSHOT;
            case SERIALIZABLE:
                return IsolationLevel.SERIALIZABLE;
            default:
                throw new UnsupportedOperationException("failed to convert " + isolationLevel.name());
        }
    }

    public static TransactionIsolationLevel trans2TransactionIsolationLevel(IsolationLevel isolationLevel) {
        switch (isolationLevel) {
            case SNAPSHOT:
                return TransactionIsolationLevel.SNAPSHOT;
            case SERIALIZABLE:
                return TransactionIsolationLevel.SERIALIZABLE;
            default:
                throw new UnsupportedOperationException("failed to convert " + isolationLevel.name());
        }
    }


}
