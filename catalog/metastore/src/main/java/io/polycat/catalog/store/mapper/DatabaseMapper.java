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

import java.util.List;

import io.polycat.catalog.store.gaussdb.pojo.DatabaseHistoryRecord;
import io.polycat.catalog.store.gaussdb.pojo.DatabaseNameRecord;
import io.polycat.catalog.store.gaussdb.pojo.DatabaseRecord;
import io.polycat.catalog.store.gaussdb.pojo.DroppedDatabaseNameRecord;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import org.springframework.stereotype.Component;

@Component
public interface DatabaseMapper {
    void createDatabaseSubspace(@Param("projectId") String projectId);

    @Update("DROP TABLE IF EXISTS schema_${projectId}.database")
    void dropDatabaseSubspace(@Param("projectId") String projectId);

    void insertDatabase(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("databaseId") String databaseId, @Param("databaseName") String databaseName,
        @Param("databaseInfo") byte[] databaseInfo);

    @Delete("DELETE FROM schema_${projectId}.database WHERE catalog_id = #{catalogId} AND database_id = #{databaseId}")
    void deleteDatabase(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("databaseId") String databaseId);

    DatabaseRecord getDatabaseById(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("databaseId") String databaseId);

    List<DatabaseRecord> getDatabasesByFilter(@Param("projectId") String projectId, @Param("filter") String filter);

    @Update("UPDATE schema_${projectId}.database SET database_name = #{databaseName}, database_info = #{databaseInfo} "
        + "WHERE catalog_id = #{catalogId} AND database_id = #{databaseId}")
    void updateDatabase(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("databaseId") String databaseId,
        @Param("databaseName") String databaseName,  @Param("databaseInfo") byte[] databaseInfo);

    void createDatabaseHistorySubspace(@Param("projectId") String projectId);

    @Update("DROP TABLE IF EXISTS schema_${projectId}.database_history")
    void dropDatabaseHistorySubspace(@Param("projectId") String projectId);

    void insertDatabaseHistory(@Param("projectId") String projectId,  @Param("catalogId") String catalogId,
        @Param("databaseId") String databaseId, @Param("dbhId") String dbhId, @Param("version") String version,
        @Param("databaseName") String databaseName, @Param("databaseInfo") byte[] databaseInfo);

    DatabaseHistoryRecord getLatestDatabaseHistory(@Param("projectId") String projectId,  @Param("catalogId") String catalogId,
        @Param("databaseId") String databaseId, @Param("version") String version);

    List<DatabaseHistoryRecord> getLatestDatabaseHistoryByFilter(@Param("projectId") String projectId,  @Param("filter") String filter);

    DatabaseHistoryRecord getDatabaseHistory(@Param("projectId") String projectId,  @Param("catalogId") String catalogId,
        @Param("databaseId") String databaseId, @Param("version") String version);

    List<DatabaseHistoryRecord> listDatabaseHistory(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("databaseId") String databaseId, @Param("version") String version, @Param("offset") long offset, @Param("count") long count);

    List<String> listDatabaseId(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("offset") long offset, @Param("count") long count);

    String getDatabaseId(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("databaseName") String databaseName);

    void createDroppedDatabaseNameSubspace(@Param("projectId") String projectId);

    @Update("DROP TABLE IF EXISTS schema_${projectId}.dropped_database")
    void dropDroppedDatabaseNameSubspace(@Param("projectId") String projectId);

    void insertDroppedDatabaseName(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("databaseId") String databaseId, @Param("databaseName") String databaseName,
        @Param("dropTime") long dropTime);

    @Delete("DELETE FROM schema_${projectId}.dropped_database WHERE catalog_id = #{catalogId} And database_id = #{databaseId} AND database_name = #{databaseName}")
    void deleteDroppedDatabaseName(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("databaseId") String databaseId, @Param("databaseName") String databaseName);

    DroppedDatabaseNameRecord getDroppedDatabaseName(@Param("projectId") String projectId,
        @Param("catalogId") String catalogId, @Param("databaseId") String databaseId,
        @Param("databaseName") String databaseName);

    List<DroppedDatabaseNameRecord> listDroppedDatabaseName(@Param("projectId") String projectId,
        @Param("catalogId") String catalogId, @Param("offset") long offset, @Param("count") long count);

    @Select("SELECT EXISTS(SELECT true from schema_${projectId}.database_history WHERE catalog_id = #{catalogId} AND database_id = #{databaseId})")
    Boolean existsDatabaseHistoryTable(@Param("projectId") String projectId,
                                      @Param("catalogId") String catalogId, @Param("databaseId") String databaseId);

}
