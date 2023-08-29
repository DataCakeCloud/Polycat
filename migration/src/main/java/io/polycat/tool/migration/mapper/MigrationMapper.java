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
package io.polycat.tool.migration.mapper;

import io.polycat.tool.migration.entity.Migration;
import io.polycat.tool.migration.entity.MigrationExample;
import java.util.List;
import org.apache.ibatis.annotations.Param;

public interface MigrationMapper {
    /**
     * This method was generated by MyBatis Generator.
     * This method corresponds to the database table migration
     *
     * @mbg.generated Mon Feb 14 14:33:10 CST 2022
     */
    long countByExample(MigrationExample example);

    /**
     * This method was generated by MyBatis Generator.
     * This method corresponds to the database table migration
     *
     * @mbg.generated Mon Feb 14 14:33:10 CST 2022
     */
    int deleteByExample(MigrationExample example);

    /**
     * This method was generated by MyBatis Generator.
     * This method corresponds to the database table migration
     *
     * @mbg.generated Mon Feb 14 14:33:10 CST 2022
     */
    int deleteByPrimaryKey(Integer id);

    /**
     * This method was generated by MyBatis Generator.
     * This method corresponds to the database table migration
     *
     * @mbg.generated Mon Feb 14 14:33:10 CST 2022
     */
    int insert(Migration record);

    /**
     * This method was generated by MyBatis Generator.
     * This method corresponds to the database table migration
     *
     * @mbg.generated Mon Feb 14 14:33:10 CST 2022
     */
    int insertSelective(Migration record);

    /**
     * This method was generated by MyBatis Generator.
     * This method corresponds to the database table migration
     *
     * @mbg.generated Mon Feb 14 14:33:10 CST 2022
     */
    List<Migration> selectByExample(MigrationExample example);

    /**
     * This method was generated by MyBatis Generator.
     * This method corresponds to the database table migration
     *
     * @mbg.generated Mon Feb 14 14:33:10 CST 2022
     */
    Migration selectByPrimaryKey(Integer id);

    /**
     * This method was generated by MyBatis Generator.
     * This method corresponds to the database table migration
     *
     * @mbg.generated Mon Feb 14 14:33:10 CST 2022
     */
    int updateByExampleSelective(@Param("record") Migration record, @Param("example") MigrationExample example);

    /**
     * This method was generated by MyBatis Generator.
     * This method corresponds to the database table migration
     *
     * @mbg.generated Mon Feb 14 14:33:10 CST 2022
     */
    int updateByExample(@Param("record") Migration record, @Param("example") MigrationExample example);

    /**
     * This method was generated by MyBatis Generator.
     * This method corresponds to the database table migration
     *
     * @mbg.generated Mon Feb 14 14:33:10 CST 2022
     */
    int updateByPrimaryKeySelective(Migration record);

    /**
     * This method was generated by MyBatis Generator.
     * This method corresponds to the database table migration
     *
     * @mbg.generated Mon Feb 14 14:33:10 CST 2022
     */
    int updateByPrimaryKey(Migration record);
}