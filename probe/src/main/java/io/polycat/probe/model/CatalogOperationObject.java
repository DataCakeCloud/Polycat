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
package io.polycat.probe.model;

import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.model.CatalogInnerObject;


import lombok.Data;

@Data
public class CatalogOperationObject {
    CatalogInnerObject catalogObject;
    Operation operation;

    public CatalogOperationObject() {}

    public CatalogOperationObject(CatalogInnerObject catalogObject, Operation operation) {
        this.catalogObject = catalogObject;
        this.operation = operation;
    }

    public CatalogInnerObject getCatalogObject() {
        return catalogObject;
    }

    public void setCatalogObject(CatalogInnerObject catalogObject) {
        this.catalogObject = catalogObject;
    }

    public Operation getOperation() {
        return operation;
    }

    public void setOperation(Operation operation) {
        this.operation = operation;
    }
}
