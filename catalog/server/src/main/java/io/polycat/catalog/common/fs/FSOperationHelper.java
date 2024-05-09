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
package io.polycat.catalog.common.fs;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.polycat.catalog.common.Operation;

/**
 * @author liangyouze
 * @date 2023/7/21
 */
public class FSOperationHelper {

    private static final Map<Operation, FSOperation> operationMap = new ConcurrentHashMap<>();

    static {
        operationMap.put(Operation.DROP_CATALOG, FSOperation.ALL);
        operationMap.put(Operation.ALTER_CATALOG, FSOperation.ALL);

        operationMap.put(Operation.CREATE_DATABASE, FSOperation.ALL);
        operationMap.put(Operation.DROP_DATABASE, FSOperation.ALL);
        operationMap.put(Operation.ALTER_DATABASE, FSOperation.ALL);

        operationMap.put(Operation.CREATE_TABLE, FSOperation.ALL);
        operationMap.put(Operation.SELECT_TABLE, FSOperation.READ);
        operationMap.put(Operation.INSERT_TABLE, FSOperation.ALL);
        operationMap.put(Operation.DROP_TABLE, FSOperation.ALL);
    }

    public static FSOperation getFsOperation(Operation operation) {
        return operationMap.get(operation);
    }

}
