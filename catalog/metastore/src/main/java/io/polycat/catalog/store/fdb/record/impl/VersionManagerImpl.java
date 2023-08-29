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
package io.polycat.catalog.store.fdb.record.impl;

import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.utils.CodecUtil;
import io.polycat.catalog.store.api.VersionManager;
import io.polycat.catalog.store.fdb.record.RecordStoreHelper;
import io.polycat.catalog.store.fdb.record.TransactionContextUtil;

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.tuple.Versionstamp;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "fdb")
public class VersionManagerImpl implements VersionManager {

    @Override
    public void createVersionSubspace(TransactionContext context, String projectId, String catalogId) {

    }

    @Override
    public void dropVersionSubspace(TransactionContext context, String projectId, String catalogId) {

    }

    @Override
    public String getLatestVersion(TransactionContext context, String projectId, String catalogId) {
        FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
        Versionstamp versionstamp = RecordStoreHelper.getCurContextVersion(fdbRecordContext);
        String version = CodecUtil.bytes2Hex(versionstamp.getBytes());
        return version;
    }

    @Override
    public String getNextVersion(TransactionContext context, String projectId, String catalogId) {
        return "";
    }

}
