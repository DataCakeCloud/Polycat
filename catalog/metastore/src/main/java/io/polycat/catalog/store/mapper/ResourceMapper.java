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

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Component;

@Component
public interface ResourceMapper {

    @Select("SELECT EXISTS(SELECT true FROM pg_tables WHERE schemaname='${schemaName}' and tablename='${tableName}') ")
    Boolean doesExistTable(@Param("schemaName") String schemaName, @Param("tableName") String tableName);

    @Select("SELECT EXISTS(SELECT true FROM pg_proc WHERE proname=#{functionName}) ")
    Boolean doesExistsFunction(@Param("functionName") String functionName);

    @Select("SELECT EXISTS(SELECT true FROM pg_views WHERE schemaname='${schemaName}' and viewname=#{viewName}) ")
    Boolean doesExistsView(@Param("schemaName") String schemaName, @Param("viewName") String viewName);
}
