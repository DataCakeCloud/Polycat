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
package io.polycat.catalog.common;

public class Constants {

    /**
     * ERROR_CODE
     */
    public static final String KEY_ERROR_CODE = "errorCode";

    /**
     * MESSAGE
     */
    public static final String KEY_MESSAGE = "message";

    /**
     * ERRORS
     */
    public static final String KEY_ERRORS = "errors";

    public static final String FILTER = "filter";

    public static final String MAX_RESULTS = "maxResults";

    public static final String TABLE_TYPE = "tableType";

    public static final String TABLE_TYPE_PROP = "table_type";

    public static final String ICEBERG_TABLE_TYPE_VALUE = "iceberg";

    public static final String PAGE_TOKEN = "pageToken";

    public static final String MAX_LIMIT_NUM = "1000";
    public static final String EMPTY_MARKER = "";
    public static final int RESOURCE_MAX_LENGTH = 60;
    public static final int RESOURCE_MIN_LENGTH = 1;

    // common
    public static final String INCLUDE_DROP = "include_drop";
    public static final String MARKER = "marker";
    public static final String LIMIT = "limit";
    public static final String PATTERN = "pattern";
    public static final String COMMENT = "comment";

    public static final String LOCATION = "location";
    public static final String PARTITION_OVERWRITE = "partition-overwrite";

    // common end

    // catalog

    // catalog end

    // database
    public static final String DB_PATTERN = "dbPattern";
    // database end

    // partition
    public static final String PARTITION_NAME = "partName";
    public static final String COLUMN_NAME = "colName";


    //    mrs token beg
    public static final String MASTER_KEY = "masterKey";
    public static final String MRS_TOKEN = "mrsToken";
    public static final String RENEW_TOKEN = "renewToken";
    public static final String UPDATE_MASTER_KEY = "updateMasterKey";
    public static final String ADD_MASTER_KEY = "addMasterKey";
    public static final String OWNER_PARAM = "owner";
    public static final String RENEWER_PARAM = "renewer";
    public static final String OPERATE_TYPE = "operate";
    public static final String CANCEL_TOKEN = "cancelToken";
    //    mrs token end

    // table
    public static final String TRUNCATE = "truncate";
    public static final String ALTER = "alter";
    public static final String TBL_NAMES = "tblNames";
    public static final String COLUMNS = "columns";
    // table end

    // constraint beg
    public static final String PARENT_DB = "parentDbName";
    public static final String PARENT_TBL = "parentTblName";
    public static final String TYPE = "type";


    // constraint end
}
