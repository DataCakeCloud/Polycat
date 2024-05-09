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
package io.polycat.catalog.store.mapper;

import io.polycat.catalog.store.gaussdb.pojo.LockRecord;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * @author liangyouze
 * @date 2024/2/20
 */
public interface LockMapper {
    void createLockSubspace();

    void insertLockInfo(@Param("data") LockRecord lockRecord);

    List<LockRecord> getLocksByObjectName(@Param("projectId") String projectId, @Param("objectName") String objectName);

    LockRecord getLockById(@Param("projectId") String projectId, @Param("lockId") Long lockId);

    int updateLockState(@Param("projectId") String projectId, @Param("lockId") Long lockId, @Param("lockState") String lockState);

    int updateLastHeartbeat(@Param("projectId") String projectId, @Param("lockId") Long lockId, @Param("lastHeartbeat") Long lastHeartbeat);

    int deleteLock(@Param("projectId") String projectId, @Param("lockId") Long lockId);

    List<Long> getTimeoutLocks(@Param("timeout") Integer timeout);

    void deleteLocks(@Param("timeoutLocks") List<Long> timeoutLocks);

    List<LockRecord> getLocks(@Param("projectId") String projectId);
}
