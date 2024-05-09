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

import io.polycat.catalog.store.gaussdb.pojo.ColumnStatisticsAggrRecord;
import io.polycat.catalog.store.gaussdb.pojo.ColumnStatisticsRecord;
import io.polycat.catalog.store.gaussdb.pojo.PartitionColumnStatisticsTableMetaRecord;

import java.util.List;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

public interface ColumnStatisticsMapper {

    @Select("SELECT EXISTS(SELECT true FROM pg_tables WHERE schemaname='schema_${projectId}' and tablename='table_column_statistics') ")
    Boolean doesExistColumnStatisticsSubspace(@Param("projectId") String projectId);

    @Select("SELECT EXISTS(SELECT true FROM schema_${projectId}.pcs_table_meta WHERE ${filter}) ")
    Boolean doesExistPartitionStatisticsTableMeta(@Param("projectId") String projectId, @Param("filter") String filter);

    PartitionColumnStatisticsTableMetaRecord getPartitionStatisticsTableMeta(@Param("projectId") String projectId, @Param("filter") String filter);

    void updatePartitionStatisticsTableMeta(@Param("projectId") String projectId, @Param("pcs") PartitionColumnStatisticsTableMetaRecord pcs);

    void createColumnStatisticsSubspace(@Param("projectId") String projectId);

    void insertTableColumnStatistics(@Param("projectId") String projectId, @Param("tcsList") List<ColumnStatisticsRecord> tcsList);

    List<ColumnStatisticsRecord> getTableColumnStatistics(@Param("projectId") String projectId, @Param("filter") String filter);

    void updateTableColumnStatistics(@Param("projectId") String projectId, @Param("tcsList") List<ColumnStatisticsRecord> tcsList);

    void deleteTableColumnStatistics(@Param("projectId") String projectId, @Param("filter") String filter);

    void createPartitionStatisticsTable(@Param("projectId") String projectId, @Param("pcsTableName") String pcsTableName);

    void updatePartitionColumnStatistics(@Param("projectId") String projectId, @Param("pcsTableName") String pcsTableName, @Param("list") List<ColumnStatisticsRecord> list);

    List<ColumnStatisticsRecord> getPartitionColumnStatistics(@Param("projectId") String projectId, @Param("pcsTableName") String pcsTableName, @Param("filter") String filter);

    void deletePartitionColumnStatistics(@Param("projectId") String projectId, @Param("pcsTableName") String pcsTableName, @Param("filter") String filter);

    List<ColumnStatisticsAggrRecord> getAggrColStatsFor(@Param("projectId") String projectId, @Param("pcsTableName") String tableMetaName, @Param("filter") String filter);

    @Select("SELECT COUNT(column_name) from schema_${projectId}.${pcsTableName} "
            + "    WHERE ${filter} group by partition_name")
    List<Long> getFoundPartNums(@Param("projectId") String projectId, @Param("pcsTableName") String tableMetaName, @Param("filter") String filter);
}
