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

import java.util.ArrayList;
import java.util.List;

import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.model.OperationPrivilege;
import io.polycat.catalog.common.model.PrincipalType;
import io.polycat.catalog.common.model.PrivilegeType;
import io.polycat.catalog.common.model.ShareConsumer;

/*
SharePrivilegeType: alter, drop, show, select;
 */
public class AuthShare extends AuthBase {

    private List<ShareConsumer> shareConsumers = new ArrayList<>();
    private String catalogId = "";
    private static final String objectRegex = "\\.";

    public AuthShare() {
        operationPrivilegeMap.put(Operation.ALTER_SHARE,
            new OperationPrivilege(ObjectType.SHARE,
                PrivilegeType.ALTER.getType()));
        operationPrivilegeMap.put(Operation.DROP_SHARE,
            new OperationPrivilege(ObjectType.SHARE,
                PrivilegeType.DROP.getType()));
        operationPrivilegeMap.put(Operation.DESC_SHARE,
            new OperationPrivilege(ObjectType.SHARE,
                PrivilegeType.DESC.getType()));
        operationPrivilegeMap.put(Operation.SELECT_SHARE,
            new OperationPrivilege(ObjectType.SHARE,
                PrivilegeType.SELECT.getType()));
    }

    public void getShareConsumers(List<ShareConsumer> shareConsumers) {
        this.shareConsumers.addAll(shareConsumers);
    }
    public void getCatalogId(String catalogId) {
        this.catalogId = catalogId;
    }

    private boolean isConsumerUser(AuthRequest request) {
        for (ShareConsumer shareConsumer : shareConsumers) {
            if(!shareConsumer.getAccountId().equals(request.getAccountId())) {
                return false;
            }
            String principalTypeAndSource = shareConsumer.getUsers().get(request.getUser());
            if (principalTypeAndSource.isEmpty()) {
                return false;
            }
            if(principalTypeAndSource.equals(PrincipalType.USER.toString() + ":" + request.getUserSource())) {
                return true;
            }
        }
        return false;
    }

    //Projectid.shareId.databaseId.tableId To catalogId.databaeId.ta
    private String convertObjectId(String shareObjectId) {
        if((shareObjectId.isEmpty()) || catalogId.isEmpty()) {
            return null;
        }
        String[] objectIds = shareObjectId.split(objectRegex);
        if(objectIds.length == 4) {
            return catalogId + "." + objectIds[2] + "." + objectIds[3];
        }
        return  null;
    }

    /**
     * checkPrivilege : Override base class methods
     *
     * @param request request
     * @return AuthResult
     */
    @Override
    public AuthResult checkPrivilege(AuthRequest request) {
        if (request.getAccessType() == AccessType.SHARE_ACCESS_SELECT.getNum()) {
            AuthResult.Builder result = new AuthResult.Builder();
            //Check permission for the accountId he userId of request.
            if (!isConsumerUser(request)) {
                result.setIsAllowed(false);
                result.setReason(request.getAccountId() + ":" + request.getUser() +
                    " is not privilege!");
                return result.build();
            }
            AuthRequest.Builder shareRequest = new AuthRequest.Builder();
            shareRequest.setOperation(request.getOperation());
            shareRequest.setObjectType(ObjectType.TABLE.getNum());
            //objectId =  catalogId.databaseId.tableId
            shareRequest.setObjectId(convertObjectId(request.getObjectId()));
            return privilegePolicyEva.isMatch(setRequestPrivilege(shareRequest.build()));
        } else {
            AuthResult result = isOwner(request);
            if (result.isAllowed()) {
                return result;
            }
            return privilegePolicyEva.isMatch(setRequestPrivilege(request));
        }
    }
}