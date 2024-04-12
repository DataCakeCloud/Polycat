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

import io.polycat.catalog.common.model.PartitionColumnInfo;
import io.polycat.catalog.common.model.PartitionInfo;
import io.polycat.catalog.store.gaussdb.pojo.TableDataHistoryRecord;
import io.polycat.catalog.store.gaussdb.pojo.TableDataPartitionSetRecord;
import io.polycat.catalog.store.gaussdb.pojo.TableIndexHistoryRecord;
import io.polycat.catalog.store.gaussdb.pojo.TableIndexPartitionSetRecord;
import io.polycat.catalog.store.gaussdb.pojo.TableIndexRecord;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

public interface TableDataMapper {
    /*
    table data history subspace
     */
    void createTableDataHistorySubspace(@Param("projectId") String projectId);

    @Update("DROP TABLE IF EXISTS schema_${projectId}.table_data_history WHERE catalog_id=#{catalogId} AND table_id=#{tableId}")
    void dropTableDataHistorySubspace(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("tableId") String tableId);

    void insertTableDataHistory(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("tableId") String tableId, @Param("dataHisId") String dataHisId,
        @Param("version") String version, @Param("data") byte[] data);

    TableDataHistoryRecord getLatestTableDataHistory(@Param("projectId") String projectId,
        @Param("catalogId") String catalogId, @Param("tableId") String tableId, @Param("version") String version);

    @Delete("DELETE FROM  schema_${projectId}.table_data_history WHERE catalog_id=#{catalogId} AND table_id=#{tableId} AND version >= #{startVersion} AND version &lt;= #{endVersion} ORDER BY version")
    void deleteTableDataHistory(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("tableId") String tableId, @Param("startVersion") String startVersion,
        @Param("endVersion") String endVersion);

    TableDataHistoryRecord getTableDataHistory(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("tableId") String tableId, @Param("version") String version);

    /**
     * table partition set subspace
     * */

    void createTableDataPartitionSetSubspace(@Param("projectId") String projectId, @Param("tableId") String tableId);

    void createTablePartitionInfo(@Param("projectId") String projectId, @Param("tableId") String tableId);

    void createTablePartitionColumnInfo(@Param("projectId") String projectId, @Param("tableId") String tableId);

    @Update("DROP TABLE IF EXISTS schema_${projectId}.table_data_partition_set_${tableId}")
    void dropTableDataPartitionSetSubspace(@Param("projectId") String projectId, @Param("tableId") String tableId);

    @Update("DROP TABLE IF EXISTS schema_${projectId}.table_partition_info_${tableId}")
    void dropTableDataPartitionInfo(@Param("projectId") String projectId, @Param("tableId") String tableId);

    @Update("DROP TABLE IF EXISTS schema_${projectId}.table_partition_column_info_${tableId}")
    void dropTableDataPartitionColumnInfo(@Param("projectId") String projectId, @Param("tableId") String tableId);

    void createTableIndexPartitionSetSubspace(@Param("projectId") String projectId, @Param("tableId") String tableId);

    @Update("DROP TABLE IF EXISTS schema_${projectId}.table_index_partition_set_${tableId}")
    void dropTableIndexPartitionSetSubspace(@Param("projectId") String projectId, @Param("tableId") String tableId);

    @Delete("DELETE FROM schema_${projectId}.table_data_partition_set_${tableId} WHERE catalogId = #{catalogId} AND databaseId = #{databaseId} AND tableId = #{tableId}")
    void deleteTableDataPartition(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("databaseId") String databaseId, @Param("tableId") String tableId);

    @Delete("DELETE FROM schema_${projectId}.table_index_partition_set_${tableId} WHERE catalogId = #{catalogId} AND databaseId = #{databaseId} AND tableId = #{tableId}")
    void deleteTableIndexPartition(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("databaseId") String databaseId, @Param("tableId") String tableId);

    void insertTableDataPartitionSet(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("databaseId") String databaseId, @Param("tableId") String tableId, @Param("setId") String setId);

    void insertTablePartitionInfo(@Param("projectId") String projectId, @Param("tableId") String tableId, @Param("partitionInfoList") List<PartitionInfo> partitionInfoList);

    List<PartitionInfo> listTablePartitionInfos(@Param("projectId") String projectId, @Param("tableId") String tableId,
        @Param("setId") String setId, @Param("maxParts") Integer maxParts);

    @Select("SELECT partition_name FROM schema_${projectId}.table_partition_info_${tableId} WHERE ${filter} LIMIT ${maxParts}")
    List<String> listTablePartitionNames(@Param("projectId") String projectId, @Param("tableId") String tableId, @Param("filter") String filter, @Param("maxParts") Integer maxParts);

    List<PartitionInfo> getTablePartitionInfoByName(@Param("projectId") String projectId, @Param("tableId") String tableId,
        @Param("setId") String setId, @Param("partitionNames") List<String> partitionNames, @Param("maxParts") int maxParts);

    List<PartitionInfo> getTablePartitionInfoByNameWithColumnInfo(@Param("projectId") String projectId, @Param("tableId") String tableId,
        @Param("setId") String setId, @Param("partitionNames") List<String> partitionNames, @Param("maxParts") int maxParts);

    List<PartitionInfo> getTablePartitionInfoByFilter(@Param("projectId") String projectId, @Param("tableId") String tableId,
                                                      @Param("setId") String setId, @Param("filter") String filter, @Param("maxParts")int maxParts);

    // List<PartitionInfo> getTablePartitionInfoBySqlFilter(@Param("projectId") String projectId, @Param("tableId") String tableId, @Param("filter") String filter, @Param("maxParts")int maxParts);

    void deletePartitionInfoByName(@Param("projectId") String projectId, @Param("tableId") String tableId,
        @Param("setId") String setId, @Param("partitionNames") List<String> partitionNames);

    void insertTablePartitionColumnInfo(@Param("projectId") String projectId, @Param("tableId") String tableId, @Param("partitionColumnInfos") List<PartitionColumnInfo> partitionColumnInfoList);

    void insertTableIndexPartitionSet(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("databaseId") String databaseId, @Param("tableId") String tableId, @Param("setId") String setId,
        @Param("setInfo") byte[] setInfo);

    void updateTableDataPartitionSet(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("databaseId") String databaseId, @Param("tableId") String tableId, @Param("setId") String setId,
        @Param("setInfo") byte[] setInfo);

    void updateTableIndexPartitionSet(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("databaseId") String databaseId, @Param("tableId") String tableId, @Param("setId") String setId,
        @Param("setInfo") byte[] setInfo);

    TableDataPartitionSetRecord getTableDataPartitionSet(@Param("projectId") String projectId,
        @Param("tableId") String tableId, @Param("setId") String setId);

    TableIndexPartitionSetRecord getTableIndexPartitionSet(@Param("projectId") String projectId,
        @Param("tableId") String tableId, @Param("setId") String setId);

    Integer getPartitionCountByFilter(@Param("projectId") String projectId, @Param("tableId") String tableId, @Param("filter") String filter);

    String getLatestPartitionName(@Param("projectId") String projectId, @Param("tableId") String tableId);


    /**
     * table index subspace
     * */

    void createTableIndexHistorySubspace(@Param("projectId") String projectId);

    @Update("DELETE FROM schema_${projectId}.table_index_history WHERE catalog_id=#{catalogId} AND table_id=#{tableId}")
    void dropTableIndexHistorySubspace(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("tableId") String tableId);

    void createTableIndexSubspace(@Param("projectId") String projectId);

    @Update("DELETE FROM schema_${projectId}.table_index WHERE catalog_id=#{catalogId} AND table_id=#{tableId}")
    void dropTableIndexSubspace(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("tableId") String tableId);

    void insertTableIndexHistory(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("tableId") String tableId, @Param("dataHisId") String dataHisId,
        @Param("version") String version, @Param("indexInfo") byte[] indexInfo);

    TableIndexHistoryRecord getLatestTableIndexHistory(@Param("projectId") String projectId,
        @Param("catalogId") String catalogId, @Param("tableId") String tableId, @Param("version") String version);

    void insertTableIndex(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("tableId") String tableId, @Param("indexInfo") byte[] indexInfo);

    TableIndexRecord getTableIndex(@Param("projectId") String projectId,
        @Param("catalogId") String catalogId, @Param("tableId") String tableId);

    @Select("SELECT EXISTS(SELECT true FROM schema_${projectId}.table_index WHERE catalog_id=#{catalogId} AND table_id=#{tableId})")
    Boolean tableIndexSubspaceExist(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
                                     @Param("tableId") String tableId);

    @Select("SELECT EXISTS(SELECT true FROM schema_${projectId}.table_index_history WHERE catalog_id=#{catalogId} AND table_id=#{tableId})")
    Boolean tableIndexHistorySubspaceExist(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
                                    @Param("tableId") String tableId);

    @Select("SELECT EXISTS(SELECT true FROM schema_${projectId}.table_partition_info_${tableId} WHERE partition_name=#{partitionName})")
    Boolean tablePartitionNameExist(@Param("projectId") String projectId, @Param("tableId") String tableId, @Param("partitionName") String partitionName);

}
