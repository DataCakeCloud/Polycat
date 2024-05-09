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
package io.polycat.catalog.common.constants;

public interface  CompatibleHiveConstants {

    String COMMON_PARAM_TRANSIENT_LAST_DDL_TIME = "transient_lastDdlTime";

    String V3_PARAM = "catalog.hive.version3";

    String V3_TABLE_CREATION_METADATA = "creationMetadata";

    String V3_CONF_CATALOG_NAME_MAPPING = "catalog.hive.catalog.names.mapping";

}
