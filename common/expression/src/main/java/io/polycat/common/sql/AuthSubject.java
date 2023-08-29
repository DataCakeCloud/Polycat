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
package io.polycat.common.sql;

import java.util.Collections;
import java.util.List;

import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.model.AuthorizationType;
import io.polycat.catalog.common.model.CatalogInnerObject;
import io.polycat.catalog.common.model.GrantObject;
import io.polycat.catalog.common.plugin.CatalogContext;
import io.polycat.catalog.common.plugin.request.base.ProjectRequestBase;
import io.polycat.catalog.common.plugin.request.input.AuthorizationInput;

/**
 * 2 ways to implement Authorization
 *  option 1: basing on request, need to overwrite getRequest method
 *  option 2: implement initializePlan, getOperation, getCatalogObject, getGrantObject
 */
public interface AuthSubject {

    default ProjectRequestBase<?> getRequest() {
        return null;
    }

    default void initializePlan(CatalogContext context) {
        if (getRequest() != null) {
            getRequest().initContext(context);
        }
    }

    default Operation getOperation() {
        if (getRequest() != null) {
            return getRequest().getOperation();
        }
        return Operation.ILLEGAL_OPERATION;
    }

    default CatalogInnerObject getCatalogObject(CatalogContext context) {
        if (getRequest() != null) {
            return getRequest().getCatalogObject(context);
        }
        return null;
    }

    default GrantObject getGrantObject() {
        if (getRequest() != null) {
            return getRequest().getGrantObject();
        }
        return null;
    }

    default AuthorizationType getAuthorizationType() {
        return AuthorizationType.NORMAL_OPERATION;
    }

    default List<AuthorizationInput> getAuthorizationInputList(CatalogContext context) {
        AuthorizationInput authorizationInput = new AuthorizationInput();
        authorizationInput.setAuthorizationType(getAuthorizationType());
        fillAuthorization(context, authorizationInput);
        authorizationInput.setUser(context.getUser());
        authorizationInput.setToken(context.getToken());
        return Collections.singletonList(authorizationInput);
    }

    default void fillAuthorization(CatalogContext context, AuthorizationInput authorizationInput) {
        ProjectRequestBase<?> request = getRequest();
        if (request == null) {
            Operation operation = getOperation();
            if (operation != null) {
                authorizationInput.setOperation(operation);
            }
            initializePlan(context);
            CatalogInnerObject catalogInnerObject = getCatalogObject(context);
            if (catalogInnerObject != null) {
                authorizationInput.setCatalogInnerObject(catalogInnerObject);
            }
            GrantObject grantObject = getGrantObject();
            if (grantObject != null) {
                authorizationInput.setGrantObject(grantObject);
            }
        } else {
            Operation operation = request.getOperation();
            if (operation != null) {
                authorizationInput.setOperation(operation);
            }
            request.initContext(context);
            CatalogInnerObject catalogInnerObject = request.getCatalogObject(context);
            if (catalogInnerObject != null) {
                authorizationInput.setCatalogInnerObject(catalogInnerObject);
            }
            GrantObject grantObject = request.getGrantObject();
            if (grantObject != null) {
                authorizationInput.setGrantObject(grantObject);
            }
        }
    }
}
