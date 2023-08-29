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


import io.polycat.catalog.store.gaussdb.pojo.ObjectNameMapRecord;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public interface ObjectNameMapMapper {
    void createObjectNameMapSubspace(@Param("projectId") String projectId);

    @Update("DROP TABLE IF EXISTS schema_${projectId}.object_name_map")
    void dropObjectNameMapSubspace(@Param("projectId") String projectId);

    void insertObjectNameMap(@Param("projectId") String projectId,
    @Param("objectType") String objectType, @Param("upperObjectName") String upperObjectName,
                             @Param("objectName") String objectName, @Param("topObjectId") String topObjectId,
                             @Param("upperObjectId") String upperObjectId, @Param("objectId") String objectId);

    @Delete("DELETE FROM schema_${projectId}.object_name_map WHERE object_type = #{objectType} AND upper_object_name = #{upperObjectName} AND object_name = #{objectName}")
    void deleteObjectNameMap(@Param("projectId") String projectId, @Param("objectType") String objectType, @Param("upperObjectName") String upperObjectName,
                             @Param("objectName") String objectName);

    List<ObjectNameMapRecord> listObjectNameMap(@Param("projectId") String projectId, @Param("objectType") String objectType, @Param("upperObjectName") String upperObjectName,
                                                @Param("topObjectId") String topObjectId);

    ObjectNameMapRecord getObjectNameMap(@Param("projectId") String projectId, @Param("objectType") String objectType, @Param("upperObjectName") String upperObjectName,
                                                @Param("objectName") String objectName);

}
