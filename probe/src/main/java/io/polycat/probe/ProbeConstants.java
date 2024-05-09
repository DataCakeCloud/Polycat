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
package io.polycat.probe;

/**
 * @author renjianxu
 * @date 2023/12/11
 */
public class ProbeConstants {
    public static final String POLYCAT_CLIENT_HOST = "polycat.client.host";
    public static final String POLYCAT_CLIENT_PORT = "polycat.client.port";
    public static final String POLYCAT_CLIENT_USERNAME = "polycat.client.userName";
    public static final String POLYCAT_CLIENT_PASSWORD = "polycat.client.password";
    public static final String POLYCAT_CLIENT_PROJECT_ID = "polycat.client.projectId";
    public static final String POLYCAT_CLIENT_TOKEN = "polycat.client.token";
    public static final String POLYCAT_CLIENT_TENANT_NAME = "polycat.client.tenantName";
    public static final String POLYCAT_CATALOG = "polycat.catalog.name";

    public static final String CATALOG_PROBE_USER_ID = "catalog.probe.userId";
    public static final String CATALOG_PROBE_USER_GROUP = "catalog.probe.userGroup";
    public static final String CATALOG_PROBE_TASK_ID = "catalog.probe.taskId";
    public static final String CATALOG_PROBE_TASK_NAME = "catalog.probe.taskName";
    public static final String CATALOG_PROBE_CLUSTER_NAME = "catalog.probe.cluster";
    public static final String CATALOG_PROBE_SOURCE = "catalog.probe.from";

    // hive config
    public static final String HIVE_DEFAULT_DB_NAME_CONFIG = "hive.sql.default.dbName";
    public static final String HIVE_AUTHORIZATION_ENABLE_CONFIG = "hive.sql.authorization.enable";
    public static final String HIVE_PUSH_USAGE_PROFILE_ENABLE_CONFIG =
            "hive.sql.push.usageProfile.enable";
    public static final String HIVE_HMS_BRIDGE_CATALOG_NAME = "hive.hmsbridge.defaultCatalogName";
}
