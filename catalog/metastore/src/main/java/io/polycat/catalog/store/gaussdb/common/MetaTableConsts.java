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
package io.polycat.catalog.store.gaussdb.common;

public class MetaTableConsts {
    /**
     * schema define prefix
     */
    public static final String PG_SCHEMA_PREFIX = "schema_";

    public static final String TABLE_COLUMN_STAT_TBL = "table_column_statistics";
    public static final String PARTITION_COLUMN_STAT_TBL_META = "pcs_table_meta";

    public static final String DISCOVERY_INFO = "discovery_info";
    public static final String DISCOVERY_CATEGORY = "discovery_category";

    public static final String TABLE_USAGE_PROFILE_DETAIL = "table_usage_profile_detail";
    public static final String TABLE_USAGE_PROFILE_PRESTAT = "table_usage_profile_prestat";
    public static final String TABLE_USAGE_PROFILE_ACCESS_STAT = "table_usage_profile_access_stat";
    public static final String MV_TABLE_PROFILE_HOTSTAT = "mv_table_profile_hotstat";

    public static final String GLOSSARY = "glossary";
    public static final String CATEGORY = "category";

    public static final String LINEAGE_VERTEX = "t_vertex";
    public static final String LINEAGE_EDGE = "t_edge";
    public static final String LINEAGE_EDGE_FACT = "t_edge_fact";



    /**
     *  function definition
     */
    public static final String FUNC_LINEAGE_GRAPH_SEARCH = "f_lineage_search";
}
