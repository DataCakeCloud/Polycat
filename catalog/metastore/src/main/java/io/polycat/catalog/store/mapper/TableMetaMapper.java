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

import io.polycat.catalog.common.model.TableNameObject;
import io.polycat.catalog.common.model.TableReferenceObject;
import io.polycat.catalog.store.gaussdb.pojo.DroppedTableRecord;
import io.polycat.catalog.store.gaussdb.pojo.TableBaseHistoryRecord;
import io.polycat.catalog.store.gaussdb.pojo.TableBaseRecord;
import io.polycat.catalog.store.gaussdb.pojo.TableCommitRecord;
import io.polycat.catalog.store.gaussdb.pojo.TableRecord;
import io.polycat.catalog.store.gaussdb.pojo.TableSchemaHistoryRecord;
import io.polycat.catalog.store.gaussdb.pojo.TableSchemaRecord;
import io.polycat.catalog.store.gaussdb.pojo.TableStorageHistoryRecord;
import io.polycat.catalog.store.gaussdb.pojo.TableStorageRecord;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

public interface TableMetaMapper {

    /*
    table subspace
     */
    void createTableSubspace(@Param("projectId") String projectId, @Param("catalogId") String catalogId);

    @Update("DROP TABLE IF EXISTS schema_${projectId}.table_${catalogId}")
    void dropTableSubspace(@Param("projectId") String projectId, @Param("catalogId") String catalogId);

    void insertTable(@Param("tableRecord") TableRecord tableRecord);

    void updateTable(@Param("tableRecord") TableRecord tableRecord);

    @Delete("DELETE FROM schema_${projectId}.table_${catalogId} WHERE table_id = #{tableId}")
    void deleteTable(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("tableId") String tableId);

    TableRecord getTable(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("tableId") String tableId);

    String getTableId(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("databaseId") String databaseId, @Param("tableName") String tableName);

    @Select("SELECT history_subspace_flag FROM schema_${projectId}.table_${catalogId} WHERE table_id = #{tableId}")
    Integer getTableHistorySubspaceFlag(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("tableId") String tableId);

    List<TableNameObject> listTableObjectName(@Param("projectId") String projectId,
        @Param("catalogId") String catalogId, @Param("databaseId") String databaseId, @Param("offset") long offset, @Param("count") long count);

    void createTableObjectNameTmpTable(@Param("projectId") String projectId,
                                              @Param("catalogId") String catalogId, @Param("databaseId") String databaseId, @Param("tmpTableName") String tmpTableName);

    TableBaseRecord getTableBase(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("tableId") String tableId);

    TableSchemaRecord getTableSchema(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("tableId") String tableId);

    TableStorageRecord getTableStorage(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("tableId") String tableId);

    /*
    table reference subspace
     */
    void createTableReferenceSubspace(@Param("projectId") String projectId, @Param("catalogId") String catalogId);

    @Update("DROP TABLE IF EXISTS schema_${projectId}.table_reference_${catalogId}")
    void dropTableReferenceSubspace(@Param("projectId") String projectId, @Param("catalogId") String catalogId);

    @Select("SELECT update_time FROM schema_${projectId}.table_reference_${catalogId} WHERE table_id = #{tableId}")
    long getTableReference(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("tableId") String tableId);

    @Insert("INSERT INTO schema_${projectId}.table_reference_${catalogId} VALUES (#{tableId}, #{updateTime})")
    void insertTableReference(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("tableId") String tableId, @Param("updateTime") long updateTime);

    @Update("UPDATE schema_${projectId}.table_reference_${catalogId} SET update_time = #{updateTime} "
        + " WHERE table_id = #{tableId}")
    void updateTableReference(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("tableId") String tableId, @Param("updateTime") long updateTime);

    @Delete("DELETE FROM schema_${projectId}.table_reference_${catalogId} WHERE table_id = #{tableId}")
    void deleteTableReference(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("tableId") String tableId);

    /*
    table base history subspace
     */
    void createTableBaseHistorySubspace(@Param("projectId") String projectId);

    @Update("DROP TABLE IF EXISTS schema_${projectId}.table_base_history")
    void dropTableBaseHistorySubspace(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("tableId") String tableId);

    @Select("SELECT EXISTS(SELECT true FROM schema_${projectId}.table_base_history WHERE catalog_id=#{catalogId} AND table_id=#{tableId})")
    boolean tableBaseHistorySubspaceExist(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("tableId") String tableId);

    void insertTableBaseHistory(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("tableId") String tableId, @Param("baseHisId") String baseHisId,
        @Param("version") String version, @Param("base") byte[] base);

    TableBaseHistoryRecord getLatestTableBaseHistory(@Param("projectId") String projectId,
        @Param("catalogId") String catalogId, @Param("tableId") String tableId, @Param("version") String version);

    List<TableCommitRecord> getLatestTableCommitByFilter(@Param("projectId") String projectId, @Param("filter") String filter);
    List<TableCommitRecord> getLatestTableCommitByTmpFilter(@Param("projectId") String projectId, @Param("filter") String filter, @Param("tmpTable") String tmpTable);

    List<TableSchemaHistoryRecord> getLatestTableSchemaByFilter(@Param("projectId") String projectId, @Param("filter") String filter);
    List<TableSchemaHistoryRecord> getLatestTableSchemaByTmpFilter(@Param("projectId") String projectId, @Param("filter") String filter, @Param("tmpTable") String tmpTable);

    List<TableStorageHistoryRecord> getLatestTableStorageByFilter(@Param("projectId") String projectId, @Param("filter") String filter);
    List<TableStorageHistoryRecord> getLatestTableStorageByTmpFilter(@Param("projectId") String projectId, @Param("filter") String filter, @Param("tmpTable") String tmpTable);

    List<TableBaseHistoryRecord> getLatestTableBaseHistoryByFilter(@Param("projectId") String projectId, @Param("filter") String filter);
    List<TableBaseHistoryRecord> getLatestTableBaseHistoryByTmpFilter(@Param("projectId") String projectId, @Param("filter") String filter, @Param("tmpTable") String tmpTable);

    List<TableBaseHistoryRecord> listTableBaseHistory(@Param("projectId") String projectId,
        @Param("catalogId") String catalogId, @Param("tableId") String tableId,
        @Param("version") String version, @Param("offset") long offset, @Param("count") long count);

    /*
    table schema history subspace
     */
    void createTableSchemaHistorySubspace(@Param("projectId") String projectId);

    @Update("DROP TABLE IF EXISTS schema_${projectId}.table_schema_history")
    void dropTableSchemaHistorySubspace(@Param("projectId") String projectId);

    @Select("SELECT EXISTS(SELECT true FROM schema_${projectId}.table_schema_history WHERE catalog_id=#{catalogId} AND table_id=#{tableId})")
    Boolean tableSchemaHistorySubspaceExist(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("tableId") String tableId);

    void insertTableSchemaHistory(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("tableId") String tableId, @Param("schemaHisId") String schemaHisId,
        @Param("version") String version, @Param("schema") byte[] schema);

    TableSchemaHistoryRecord getLatestTableSchemaHistory(@Param("projectId") String projectId,
        @Param("catalogId") String catalogId, @Param("tableId") String tableId, @Param("version") String version);

    List<TableSchemaHistoryRecord> listTableSchemaHistory(@Param("projectId") String projectId,
        @Param("catalogId") String catalogId, @Param("tableId") String tableId, @Param("version") String version,
        @Param("offset") long offset, @Param("count") long count);

    /*
    table storage history subspace
     */
    void createTableStorageHistorySubspace(@Param("projectId") String projectId);

    @Update("DROP TABLE IF EXISTS schema_${projectId}.table_storage_history")
    void dropTableStorageHistorySubspace(@Param("projectId") String projectId);

    @Select("SELECT EXISTS(SELECT true FROM schema_${projectId}.table_storage_history WHERE catalog_id=#{catalogId} AND table_id=#{tableId})")
    Boolean tableStorageHistorySubspaceExist(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("tableId") String tableId);

    void insertTableStorageHistory(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("tableId") String tableId, @Param("storageHisId") String storageHisId,
        @Param("version") String version, @Param("storage") byte[] storage);

    TableStorageHistoryRecord getLatestTableStorageHistory(@Param("projectId") String projectId,
        @Param("catalogId") String catalogId, @Param("tableId") String tableId, @Param("version") String version);

    List<TableStorageHistoryRecord> listTableStorageHistory(@Param("projectId") String projectId,
        @Param("catalogId") String catalogId, @Param("tableId") String tableId, @Param("version") String version,
        @Param("offset") long offset, @Param("count") long count);

    /*
    table commit subspace
     */
    void createTableCommitSubspace(@Param("projectId") String projectId);

    @Update("DROP TABLE IF EXISTS schema_${projectId}.table_commit")
    void dropTableCommitSubspace(@Param("projectId") String projectId);

    @Select("SELECT EXISTS(SELECT true FROM schema_${projectId}.table_commit WHERE catalog_id=#{catalogId} AND table_id=#{tableId})")
    Boolean tableCommitSubspaceExist(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("tableId") String tableId);

    void insertTableCommit(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("tableId") String tableId, @Param("tableCommitRecord") TableCommitRecord tableCommitRecord);

    TableCommitRecord getTableCommit(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("tableId") String tableId, @Param("version") String version);

    TableCommitRecord getLatestTableCommit(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("tableId") String tableId, @Param("version") String version);

    List<TableCommitRecord> listTableCommit(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("tableId") String tableId, @Param("version") String version,
        @Param("offset") long offset, @Param("count") long count);

    List<TableCommitRecord> traverseTableCommit(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("tableId") String tableId, @Param("startVersion") String startVersion,
        @Param("endVersion") String endVersion);

    /*
    dropped table
     */
    void createDroppedTableSubspace(@Param("projectId") String projectId, @Param("catalogId") String catalogId);

    @Update("DROP TABLE IF EXISTS schema_${projectId}.dropped_table_${catalogId}")
    void dropDroppedTableSubspace(@Param("projectId") String projectId, @Param("catalogId") String catalogId);

    void insertDroppedTable(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("tableId") String tableId, @Param("tableName") String tableName,
        @Param("createTime") long createTime, @Param("droppedTime") long droppedTime,
        @Param("isPurge") boolean isPurge);

    DroppedTableRecord getDroppedTable(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("tableId") String tableId, @Param("tableName") String tableName);

    List<DroppedTableRecord> listDroppedTable(@Param("projectId") String projectId,
        @Param("catalogId") String catalogId, @Param("offset") long offset, @Param("count") long count);

    List<DroppedTableRecord> getDroppedTablesByName(@Param("projectId") String projectId,
        @Param("catalogId") String catalogId, @Param("tableName") String tableName);

    @Delete("DELETE FROM schema_${projectId}.dropped_table_${catalogId} "
        + "WHERE table_id = #{tableId} AND table_name = #{tableName}")
    void deleteDroppedTable(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("tableId") String tableId, @Param("tableName") String tableName);

}
