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
package io.polycat.catalog.store.gaussdb;

import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.utils.CodecUtil;
import io.polycat.catalog.store.api.VersionManager;
import io.polycat.catalog.store.mapper.SequenceMapper;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "gauss")
public class VersionManagerImpl implements VersionManager {

    @Autowired
    private SequenceMapper sequenceMapper;

    @Override
    public void createVersionSubspace(TransactionContext context, String projectId, String catalogId) {
        try {
            String name = SequenceHelper.getVersionSequenceName(catalogId);
            sequenceMapper.createSequenceSubspace(projectId, name);
        } catch (Exception e) {
            log.warn(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void dropVersionSubspace(TransactionContext context, String projectId, String catalogId) {
        try {
            String name = SequenceHelper.getVersionSequenceName(catalogId);
            sequenceMapper.dropSequenceSubspace(projectId, name);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public String getLatestVersion(TransactionContext context, String projectId, String catalogId) {
        return getNextVersion(context, projectId, catalogId);
    }

    @Override
    public String getNextVersion(TransactionContext context, String projectId, String catalogId) {
        try {
            String name = SequenceHelper.getVersionSequenceName(catalogId);
            Long seq = sequenceMapper.getNextSequence(projectId, name);
            byte[] seqBuf = CodecUtil.longToBytes(seq);
            return CodecUtil.bytes2Hex(seqBuf);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }
}
