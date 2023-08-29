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

import org.apache.ibatis.annotations.*;
import org.springframework.stereotype.Component;
@Component
public interface SequenceMapper {
    // no cache : for version manager
    @Update("CREATE SEQUENCE schema_${projectId}.${name}")
    void createSequenceSubspace(@Param("projectId") String projectId, @Param("name") String name);

    // has cache : for id
    @Update("CREATE SEQUENCE schema_${projectId}.${name} CACHE ${cache}")
    void createSequenceSubspaceCache(@Param("projectId") String projectId, @Param("name") String name,
        @Param("cache") int cache);

    @Delete("DROP SEQUENCE IF EXISTS schema_${projectId}.${name}")
    void dropSequenceSubspace(@Param("projectId") String projectId, @Param("name") String name);

    @Select("SELECT nextval('schema_${projectId}.${name}')")
    Long getNextSequence(@Param("projectId") String projectId, @Param("name") String name);
}
