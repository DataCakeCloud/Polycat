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
import io.polycat.catalog.store.mapper.SequenceMapper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "gauss")
public class SequenceHelper {

    private static final Logger log = Logger.getLogger(SequenceHelper.class);

    public static int cache = 200;

    private static SequenceMapper sequenceMapper;

    @Autowired
    public void setSequenceMapper(SequenceMapper sequenceMapper) {
        this.sequenceMapper = sequenceMapper;
    }

    public static String getVersionSequenceName(String catalogId) {
        return "version_" + catalogId;
    }

    public static String getObjectIdSequenceName(String projectId) {
        return "objectId_" + projectId;
    }

    public static String generateObjectId(TransactionContext context, String projectId) throws MetaStoreException {
        try {
            Long seq = sequenceMapper.getNextSequence(projectId,
                SequenceHelper.getObjectIdSequenceName(projectId));
            byte[] seqBuf = CodecUtil.longToBytes(seq);
            return CodecUtil.bytes2Hex(seqBuf);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

}
