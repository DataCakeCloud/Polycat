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

import java.util.Map;

import lombok.Data;

@Data
public class ShareObject {
    private String projectId;
    private String shareId;
    private String shareName;
    private String ownerAccount;
    private String ownerUser;
    private long createTime;
    private String catalogId;

    public ShareObject(String shareId, String shareName, String ownerAccount, String ownerUser, long createTime) {
        this.shareId = shareId;
        this.shareName = shareName;
        this.ownerAccount = ownerAccount;
        this.ownerUser = ownerUser;
        this.createTime = createTime;
    }

    public ShareObject(String shareId, String shareName, String ownerAccount, String ownerUser, long createTime,
        String catalogId) {
        this.shareId = shareId;
        this.shareName = shareName;
        this.ownerAccount = ownerAccount;
        this.ownerUser = ownerUser;
        this.createTime = createTime;
        this.catalogId = catalogId;
    }

    public ShareObject(String projectId, String shareId, String shareName, String ownerAccount, String ownerUser,
         long createTime, String catalogId) {
        this.projectId = projectId;
        this.shareId = shareId;
        this.shareName = shareName;
        this.ownerAccount = ownerAccount;
        this.ownerUser = ownerUser;
        this.createTime = createTime;
        this.catalogId = catalogId;
    }
}
