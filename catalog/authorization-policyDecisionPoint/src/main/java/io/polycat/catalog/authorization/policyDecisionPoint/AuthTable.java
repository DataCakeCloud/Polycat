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
package io.polycat.catalog.authorization.policyDecisionPoint;

import io.polycat.catalog.common.model.MetaPrivilegePolicy;
import io.polycat.catalog.common.ObjectType;

/*
Authentication Table(catalog, database, table) class
 */
public class AuthTable extends AuthBase {

    /* for example catalog.database.table */
    private static final String objectRegex = "\\.";

    /**
     * isOwner : Override base class methods
     *
     * @param request request
     * @return AuthResult
     */
    @Override
    public AuthResult isOwner(AuthRequest request) {
        String catalogId = request.getObjectId();
        String databaseId = request.getObjectId();
        AuthResult.Builder result = new AuthResult.Builder();
        if (request.getObjectType() == ObjectType.DATABASE.getNum()) {
            String[] objectIds = request.getObjectId().split(objectRegex);
            catalogId = objectIds[0];
        }
        if (request.getObjectType() == ObjectType.TABLE.getNum()) {
            String[] objectIds = request.getObjectId().split(objectRegex);
            if(objectIds.length == 3) {
                catalogId = objectIds[0];
                databaseId = objectIds[0] + "." + objectIds[1];
            }
        }
        for (MetaPrivilegePolicy policy : ownerPolicies) {
            if (((policy.getObjectType() == request.getObjectType()) &&
                policy.getObjectId().equals(request.getObjectId())) ||
                (policy.getObjectId().equals(databaseId)) ||
            (policy.getObjectId().equals(catalogId))) {
                result.setIsAllowed(true);
                result.setPolicyId(policy.getPolicyId());
                result.setReason(request.getUser() + "is owner!");
                return result.build();
            }
        }
        result.setIsAllowed(false);
        return result.build();
    }
}
