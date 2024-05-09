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
package io.polycat.catalog.common.lineage;

public class LineageConstants {

    /**
     * calculating dependency way
     */
    public static String LINEAGE_KEY_COLUMN_DW = "dw";
    /**
     * calculating dependency code segment.
     */
    public static String LINEAGE_KEY_COLUMN_DCS = "dcs";


    /**
     * lineage job fact properties map store prefix is the configuration of these.
     */
    public static String LINEAGE_JOB_FACT_PREFIX = "polycat.lineage.conf.";
    /**
     * lineage job fact properties map stored config these key's configuration.
     */
    public static String LINEAGE_JOB_FACT_CONTAINS_KEY = "polycat.lineage.conf.contain.keys";


}
