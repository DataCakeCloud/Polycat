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
import io.polycat.tool.migration.common.MigrationType;
import io.polycat.tool.migration.entity.MigrationItem;
import io.polycat.tool.migration.entity.MigrationItemExample;
import io.polycat.tool.migration.mapper.MigrationItemMapper;
import io.polycat.tool.migration.service.MigrationItemService;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;

public class MigrationItemServiceImpl implements MigrationItemService {

    private static final String[] SKIP_DB = new String[]{
            "default",
    };

    @Autowired
    private MigrationItemMapper itemMapper;

    @Override
    public List<MigrationItem> getMigrationItemByType(Integer migration, MigrationType type) {
        MigrationItemExample example = new MigrationItemExample();
        MigrationItemExample.Criteria criteria = example.createCriteria();
        criteria.andMigrationIdEqualTo(migration);
        criteria.andTypeEqualTo(type.ordinal());
        return itemMapper.selectByExample(example);
    }

    @Override
    public List<MigrationItem> getUnFinishedMigrationItemByType(Integer migration, MigrationType type) {
        MigrationItemExample example = new MigrationItemExample();
        MigrationItemExample.Criteria criteria = example.createCriteria();
        criteria.andMigrationIdEqualTo(migration);
        criteria.andTypeEqualTo(type.ordinal());
        List<Integer> values = new ArrayList<>();
        values.add(MigrationStatus.INITIALIZATION.ordinal());
        values.add(MigrationStatus.FAILED.ordinal());
        criteria.andStatusIn(values);
        return itemMapper.selectByExample(example);
    }

    @Override
    public int insertMigrationItem(MigrationItem migrationItem) {
        migrationItem.setStatus(MigrationStatus.INITIALIZATION.ordinal());
        return itemMapper.insert(migrationItem);
    }


    @Override
    public int updateMigrationItem(MigrationItem item) {
        return itemMapper.updateByPrimaryKey(item);
    }
}
