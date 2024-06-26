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
package io.polycat.catalog.service.api;

import io.polycat.catalog.common.model.lock.LockInfo;
import io.polycat.catalog.common.plugin.request.input.LockInput;

import java.util.List;

/**
 * @author liangyouze
 * @date 2024/2/19
 */
public interface LockService {
    LockInfo createLock(String projectId, LockInput lockInput);

    LockInfo checklock(String projectId, Long lockId);

    void heartbeat(String projectId, Long lockId);

    void unlock(String projectId, Long lockId);

    List<LockInfo> showLocks(String projectId);

    void monitorLock();
}
