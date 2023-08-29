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
package io.polycat.tool.migration.service.impl;

import io.polycat.tool.migration.common.MigrationStatus;
import io.polycat.tool.migration.entity.Migration;
import io.polycat.tool.migration.entity.MigrationExample;
import io.polycat.tool.migration.mapper.MigrationMapper;
import io.polycat.tool.migration.service.MigrationSerivce;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Date;
import java.util.List;

public class MigrationServiceImpl implements MigrationSerivce {
    @Autowired
    private MigrationMapper migrationMapper;


    @Override
    public List<Migration> listMigration() {
        return migrationMapper.selectByExample(new MigrationExample());
    }

    @Override
    public Migration getMigrationByName(String name) {
        MigrationExample example = new MigrationExample();
        MigrationExample.Criteria criteria = example.createCriteria();
        criteria.andNameEqualTo(name);
        List<Migration> migrations = migrationMapper.selectByExample(example);

        return migrations.size() == 0 ? null : migrations.get(0);
    }

    @Override
    public int insertMigration(Migration migration) {
        migration.setCreateTime(new Date());
        migration.setStatus(MigrationStatus.INITIALIZATION.ordinal());
        return migrationMapper.insert(migration);
    }

    @Override
    public int deleteMigrationById(Integer migrationId) {
        Migration migration = migrationMapper.selectByPrimaryKey(migrationId);
        // 软删除，将状态标记成deleted
        migration.setStatus(MigrationStatus.DELETED.ordinal());
        return migrationMapper.updateByPrimaryKey(migration);
    }

    @Override
    public int updateMigrationById(Migration migration) {
        return migrationMapper.updateByPrimaryKey(migration);
    }
}
