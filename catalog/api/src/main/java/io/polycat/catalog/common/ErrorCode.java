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

import java.io.Serializable;

import org.slf4j.helpers.MessageFormatter;
import org.springframework.http.HttpStatus;

public enum ErrorCode implements Serializable {
    INNER_SERVER_ERROR("Dash.00001", "Server internal error", HttpStatus.INTERNAL_SERVER_ERROR, 0),
    ARGUMENT_ILLEGAL("Dash.00002", "Invalid argument: {}", HttpStatus.BAD_REQUEST, 1),
    ARGUMENT_ILLEGAL_2("Dash.000021", "Invalid argument: {} contains [{}]", HttpStatus.BAD_REQUEST, 2),
    OBJECT_NAME_IS_ILLEGAL("Dash.00003", "Invalid object id: {}", HttpStatus.BAD_REQUEST, 1),
    OBJECT_ID_NOT_FOUND("Dash.00004", "Invalid object name: {}", HttpStatus.BAD_REQUEST, 1),
    CATALOG_NOT_FOUND("Dash.00005", "Catalog {} not found", HttpStatus.NOT_FOUND, 1),
    CATALOG_ID_NOT_FOUND("Dash.00006", "Catalog id {} not found", HttpStatus.NOT_FOUND, 1),
    CATALOG_ALREADY_EXIST("Dash.00007", "Catalog {} already exists", HttpStatus.FORBIDDEN, 1),
    CATALOG_BRANCH_SAME("Dash.00008", "Src branch name is same as dest branch: {}", HttpStatus.FORBIDDEN, 1),
    DATABASE_NOT_FOUND("Dash.00011", "Database {} not found", HttpStatus.NOT_FOUND, 1),
    DATABASE_ID_NOT_FOUND("Dash.00012", "Database id {} not found", HttpStatus.NOT_FOUND, 1),
    DATABASE_ALREADY_EXIST("Dash.00013", "Database {} already exists", HttpStatus.FORBIDDEN, 1),
    DATABASE_REQ_PARAM_ERROR("Dash.00014", "Wrong database request parameter: {}", HttpStatus.FORBIDDEN, 1),
    DATABASE_TABLE_EXISTS("Dash.00015", "Database {} has existing table, can not drop database", HttpStatus.FORBIDDEN, 1),
    DATABASE_MULTIPLE_EXISTS("Dash.00016", "The database {} multiple exists", HttpStatus.FORBIDDEN, 1),
    DATABASE_HISTORY_NOT_FOUND("Dash.00017", "The database id {} history not found",  HttpStatus.NOT_FOUND, 1),
    TABLE_REFERENCE_NOT_FOUND("Dash.00018", "Table id {} reference not found",  HttpStatus.NOT_FOUND, 1),
    TABLE_NOT_FOUND("Dash.000019", "Table {} not found", HttpStatus.NOT_FOUND, 1),
    TABLE_ID_NOT_FOUND("Dash.000020", "Table id {} not found", HttpStatus.NOT_FOUND, 1),
    TABLE_ALREADY_EXIST("Dash.00021", "Table {} already exists", HttpStatus.FORBIDDEN, 1),
    TABLE_COLUMN_TYPE_INVALID("Dash.00022", "Column datatype {} is not valid", HttpStatus.FORBIDDEN, 1),
    TABLE_DATA_HISTORY_NOT_FOUND("Dash.00023", "Table id {} data history not found", HttpStatus.NOT_FOUND, 1),
    TABLE_COMMIT_NOT_FOUND("Dash.00024", "Table id {} commit not found", HttpStatus.NOT_FOUND, 1),
    TABLE_BASE_NOT_FOUND("Dash.00025", "Table id {} base not found", HttpStatus.NOT_FOUND, 1),
    TABLE_SCHEMA_NOT_FOUND("Dash.00026", "Table id {} schema not found", HttpStatus.NOT_FOUND, 1),
    TABLE_STORAGE_NOT_FOUND("Dash.00027", "Table id {} storage not found", HttpStatus.NOT_FOUND, 1),
    TABLE_PK_NOT_FOUND("Dash.00029", "Table primary key not found", HttpStatus.NOT_FOUND, 0),
    TABLE_MULTIPLE_EXISTS("Dash.000030", "Table multiple exists", HttpStatus.FORBIDDEN, 0),
    TABLE_CONFLICT("Dash.00032", "Merge failed since table {} has conflict commits", HttpStatus.FORBIDDEN, 1),
    TABLE_PURGE_NOT_FOUND("Dash.00033", "Purge is illegel. Table {} is not dropped or Name [tableId] invalid",
        HttpStatus.NOT_FOUND,
        1),
    COLUMN_ALREADY_EXISTS("Dash.00034", "Column {} is duplicated", HttpStatus.FORBIDDEN, 1),
    COLUMN_NOT_FOUND("Dash.00035", "Column {} not found", HttpStatus.NOT_FOUND, 1),
    COLUMN_STATISTICS_INVALID("Dash.000351", "Invalid column stats object.", HttpStatus.FORBIDDEN, 0),
    OBJECT_NAME_MAP_NOT_FOUND("Dash.00036", "Object name map {} not found", HttpStatus.NOT_FOUND, 1),
    OBJECT_NAME_MAP_TYPE_ERROR("Dash.00037", "Object name map {} type error", HttpStatus.FORBIDDEN, 1),
    SHARE_NOT_FOUND("Dash.00040", "Share {} not found", HttpStatus.NOT_FOUND, 1),
    SHARE_ALREADY_EXISTS("Dash.00041", "Share {} already exist", HttpStatus.NOT_FOUND, 1),
    SHARE_PRIVILEGE_INVALID("Dash.00042", "Share privilege is invalid", HttpStatus.FORBIDDEN, 0),
    SHARE_CATALOG_CONFLICT("Dash.00043", "Share catalog conflict", HttpStatus.FORBIDDEN, 0),
    SEGMENT_INVALID("Dash.00050", "Segment is invalid", HttpStatus.FORBIDDEN, 0),
    SEGMENT_FILE_INVALID("Dash.00051", "Invalid data file list", HttpStatus.FORBIDDEN, 0),
    SEGMENT_SCHEMA_NOT_FOUND("Dash.00052", "Segment schema not found", HttpStatus.NOT_FOUND, 0),
    TABLE_COLUMN_TYPE_NOT_SUPPORTED("Dash.00053", "Column datatype {} is not supported", HttpStatus.FORBIDDEN, 1),
    SHARE_ID_NOT_FOUND("Dash.00054", "Share id {} not found", HttpStatus.NOT_FOUND, 1),
    SHARE_CONSUMER_NOT_FOUND("Dash.00055", "Share id {}, consumer account {} not found", HttpStatus.NOT_FOUND, 2),
    SHARE_PRIVILEGE_NOT_FOUND("Dash.00056", "Share id {}, object id {} not found", HttpStatus.NOT_FOUND, 2),
    VIEW_NOT_FOUND("Dash.00060", "View not found", HttpStatus.NOT_FOUND, 0),
    VIEW_ALREADY_EXISTS("Dash.00061", "View already exists", HttpStatus.NOT_FOUND, 0),
    ROLE_NOT_FOUND("Dash.000070", "Role {} not found", HttpStatus.NOT_FOUND, 1),
    ROLE_PRIVILEGE_INVALID("Dash.00074", "Role privilege is invalid", HttpStatus.FORBIDDEN, 0),
    ROLE_PRIVILEGE_OPERATION_INCONSISTENT("Dash.000741", "Role add privilege: {}, objectType: {} and operation: {} are inconsistent", HttpStatus.FORBIDDEN, 3),
    ROLE_ALREADY_EXIST("Dash.00075", "Role {} already exists", HttpStatus.FORBIDDEN, 1),
    ROLE_ID_NOT_FOUND("Dash.000076", "Role id {} not found", HttpStatus.NOT_FOUND, 1),
    ROLE_USER_RELATIONSHIP_NOT_FOUND("Dash.000077", "Role:{} and user: {} relationship not found", HttpStatus.NOT_FOUND, 2),
    AUTHORIZATION_TYPE_ERROR("Dash.00080", "Authorization type is error", HttpStatus.FORBIDDEN, 0),
    AUTHORIZATION_TOKEN_ERROR("Dash.00081", "Authorization token invalid", HttpStatus.UNAUTHORIZED, 0),
    DELEGATE_ALREADY_EXIST("Dash.00090", "Storage delegate {} already exists", HttpStatus.FORBIDDEN, 1),
    DELEGATE_PROVIDER_ILLEGAL("Dash.00091", "Storage delegate {} provider is illegal", HttpStatus.FORBIDDEN, 1),
    DELEGATE_NOT_FOUND("Dash.00092", "Storage delegate {} does not exist", HttpStatus.NOT_FOUND, 1),
    SHARED_OBJECT_PURGE_ILLEGAL("Dash.00093", "The shared object {} named {} cannot purge", HttpStatus.FORBIDDEN, 2),
    ACCELERATOR_ALREADY_EXISTS("Dash.00100", "Accelerator {} already exists", HttpStatus.FORBIDDEN, 1),
    ACCELERATOR_NOT_FOUND("Dash.00101", "Accelerator {} not found", HttpStatus.NOT_FOUND, 1),
    TASK_EXECUTE_TIMEOUT("Dash.00110", "The backendTask {} excute time out", HttpStatus.REQUEST_TIMEOUT, 1),
    PARTITION_NOT_DEFINED("Dash.00120", "Partition {} does not defined in table", HttpStatus.FORBIDDEN, 1),
    PARTITION_VALUES_NOT_MATCH("Dash.00121", "Partition values {} exceed the number in the actual partition", HttpStatus.BAD_REQUEST, 1),
    PARTITION_NAME_NOT_FOUND("Dash.00122", "Partition name {} not found", HttpStatus.BAD_REQUEST, 1),
    PARTITION_KEYS_VALUES_NOT_MATCH("Dash.00123", "Incorrect number of partition values. numPartKeys size={}, part_val size={}", HttpStatus.BAD_REQUEST, 2),
    PARTITION_FILTER_ILLEGAL("Dash.00124", "Partition filter illegal, {}", HttpStatus.BAD_REQUEST, 1),

    DATA_LINEAGE_OUTPUT_ERROR("Dash.00130", "Data lineage record output error.", HttpStatus.FORBIDDEN, 0),
    DATA_LINEAGE_SOURCE_ERROR("Dash.00131", "Data lineage record source error {}.", HttpStatus.FORBIDDEN, 1),
    DATA_LINEAGE_SOURCE_TYPE_ILLEGAL("Dash.00132", "Data lineage record source type illegal {}.", HttpStatus.FORBIDDEN, 1),
    META_STORE_RESERVED_WORD_ERROR("Dash.00140", "Reserved word error of {}.", HttpStatus.FORBIDDEN, 1),
    COLUMN_CAN_NOT_BE_DROPPED("Dash.00150", "Column {} can not be dropped", HttpStatus.FORBIDDEN, 1),
    FUNCTION_ALREADY_EXIST("Dash.00151", "Function {} already exists", HttpStatus.FORBIDDEN, 1),
    FUNCTION_NOT_FOUND("Dash.00152", "Function {} not found", HttpStatus.NOT_FOUND, 1),
    TOKEN_GET_FAILED("Dash.00190", "TokenId {} token get failed.", HttpStatus.FORBIDDEN, 1),
    CONFIG_SECURITY_INVALID("Dash.00191", "Get config failed.", HttpStatus.FORBIDDEN, 0),
    INVALID_OBJECT("Dash.00192", "Object {} is invalid or doesn't exists.", HttpStatus.BAD_REQUEST, 1),
    INVALID_PARAMS_EXCEED_LIMIT("Dash.001921", "Param {} is exceed the limit: {}.", HttpStatus.BAD_REQUEST, 2),
    INVALID_OPERATION("Dash.00193", "Operation {} is invalid.", HttpStatus.BAD_REQUEST, 1),
    INVALID_PARTITION("Dash.00194", "Partition {} is invalid.", HttpStatus.BAD_REQUEST, 1),
    META_OPERATE_ERROR("Dash.00195", "RDBMS has internal error.", HttpStatus.INTERNAL_SERVER_ERROR, 0),
    NO_SUCH_LOCK("Dash.00196", "Lock {} not found.", HttpStatus.BAD_REQUEST, 1),
    NO_SUCH_TXN("Dash.00197", "Txn {} not found.", HttpStatus.BAD_REQUEST, 1),
    TXN_ABORTED("Dash.00198", "Txn {} is aborted.", HttpStatus.FORBIDDEN, 1),
    TXN_OPEN_FAILED("Dash.00199", "Txn {} can not be created.", HttpStatus.FORBIDDEN, 1),
    TRANSLATE_ERROR("Dash.00200", "Txn {} occur error.", HttpStatus.FORBIDDEN, 1),
    THRIFT_APPLICATION_ERROR("Dash.00201", "application {} occur error.", HttpStatus.FORBIDDEN, 1),
    FEATURE_NOT_SUPPORT("Dash.00202", "feature {} not support.", HttpStatus.NOT_IMPLEMENTED, 1),
    MV_NOT_FOUND("Dash.00220", "Materialized view {} not found", HttpStatus.NOT_FOUND, 1),
    POLICY_ID_NOT_FOUND("Dash.000301", "Policy id is invalid", HttpStatus.NOT_FOUND, 0),
    TENANT_PROJECT_ALREADY_EXIST("Dash.000302", "Tenant projectId {} already exists", HttpStatus.FORBIDDEN, 1),
    TENANT_PROJECT_DOES_NOT_EXIST("Dash.000303", "Tenant projectId:{} doesn't exists", HttpStatus.FORBIDDEN, 1),
    LINEAGE_REQ_PARAM_ERROR("Dash.000401", "Lineage info request: nodeMap or jobFact parameter not empty.", HttpStatus.FORBIDDEN, 0),
    ;

    private final String errorCode;

    private final String messageFormat;

    private final HttpStatus statusCode;

    // number of arguments in message, for validation
    private final int argsCount;

    ErrorCode(String errorCode, String messageFormat, HttpStatus statusCode, int argsCount) {
        this.errorCode = errorCode;
        this.messageFormat = messageFormat;
        this.statusCode = statusCode;
        this.argsCount = argsCount;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public String getMessageFormat() {
        return messageFormat;
    }

    public HttpStatus getStatusCode() {
        return statusCode;
    }

    public String buildErrorMessage() {
        if (argsCount == 0) {
            return messageFormat;
        } else {
            return ErrorCode.INNER_SERVER_ERROR.messageFormat;
        }
    }

    public String buildErrorMessage(Object[] args) {
        if (args.length != argsCount) {
            // 参数个数不一致，则不做格式化，只返回描述信息
            return messageFormat;
        }
        return MessageFormatter.arrayFormat(messageFormat, args).getMessage();
    }

}
