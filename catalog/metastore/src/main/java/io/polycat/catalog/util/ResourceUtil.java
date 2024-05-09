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
package io.polycat.catalog.util;

import io.polycat.catalog.store.gaussdb.common.MetaTableConsts;

import static io.polycat.catalog.store.gaussdb.common.MetaTableConsts.PG_SCHEMA_PREFIX;

/**
 * @author liangyouze
 * @date 2023/12/5
 */
public class ResourceUtil {

    public static String getSchema(String projectId) {
        if (projectId == null || projectId.length() == 0) {
            return projectId;
        }
        return MetaTableConsts.PG_SCHEMA_PREFIX + projectId;
    }

    public static String getProjectId(String schema) {
        if (schema == null || schema.length() == 0) {
            return schema;
        }
        return schema.replace(PG_SCHEMA_PREFIX, "");
    }

    public static String getTableQName(String projectId, String tableName) {
        return String.format("%s.%s", getSchema(projectId), tableName);
    }
}
