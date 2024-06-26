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

import lombok.Getter;

import java.util.Arrays;
import java.util.List;

public enum Operation {
    // DDL Operation
    CREATE_CATALOG("CREATE CATALOG", JobType.DDL),
    DROP_CATALOG("DROP CATALOG", JobType.DDL),
    ALTER_CATALOG("ALTER CATALOG", JobType.DDL),
    DESC_CATALOG("DESC CATALOG", JobType.DDL),
    SHOW_CATALOG("SHOW CATALOG", JobType.DDL),
    USE_CATALOG("USE CATALOG", JobType.DDL),
    CREATE_BRANCH("CREATE BRANCH", JobType.DDL),
    USE_BRANCH("USE BRANCH", JobType.DDL),
    SHOW_BRANCH("SHOW BRANCH", JobType.DDL),
    MERGE_BRANCH("MERGE BRANCH", JobType.DDL),
    CREATE_DATABASE("CREATE DATABASE", JobType.DDL),
    DROP_DATABASE("DROP DATABASE", JobType.DDL),
    UNDROP_DATABASE("UNDROP DATABASE", JobType.DDL),
    ALTER_DATABASE("ALTER DATABASE", JobType.DDL),
    SHOW_DATABASE("SHOW DATABASE", JobType.DDL),
    DESC_DATABASE("DESC DATABASE", JobType.DDL),
    USE_DATABASE("USE DATABASE", JobType.DDL),
    CREATE_TABLE("CREATE TABLE", JobType.DDL),
    GET_TABLE_META("GET TABLE META", JobType.DDL),
    TRUNCATE_TABLE("TRUNCATE TABLE", JobType.DDL),
    DROP_TABLE("DROP TABLE", JobType.DDL),
    PURGE_TABLE("PURGE TABLE", JobType.DDL),
    UNDROP_TABLE("UNDROP TABLE", JobType.DDL),
    RESTORE_TABLE("RESTORE TABLE", JobType.DDL),
    ALTER_TABLE("ALTER TABLE", JobType.DDL),
    GET_TABLE_NAMES("GET TABLE NAMES", JobType.DDL),
    SHOW_TABLE("SHOW TABLE", JobType.DDL),
    DESC_TABLE("DESC TABLE", JobType.DDL),
    SHOW_CREATE_TABLE("SHOW CREATE TABLE", JobType.DDL),
    SHOW_COLUMN("SHOW COLUMN", JobType.DDL),
    ALTER_COLUMN("ALTER COLUMN", JobType.DDL),
    ADD_COLUMN("ADD COLUMN", JobType.DDL),
    DROP_COLUMN("DROP COLUMN", JobType.DDL),
    RENAME_COLUMN("RENAME COLUMN", JobType.DDL),
    REPLACE_COLUMN("REPLACE COLUMN", JobType.DDL),
    CHANGE_COLUMN("CHANGE COLUMN", JobType.DDL),
    SET_PROPERTIES("SET PROPERTIES", JobType.DDL),
    SET_LOCATION("SET LOCATION", JobType.DDL),
    UNSET_PROPERTIES("UNSET PROPERTIES", JobType.DDL),
    UPDATE("UPDATE ROW", JobType.DDL),
    DELETE("DELETE ROW", JobType.DDL),
    CREATE_VIEW("CREATE VIEW", JobType.DDL),
    ALTER_VIEW("ALTER VIEW", JobType.DDL),
    DROP_VIEW("DROP VIEW", JobType.DDL),
    SHOW_VIEW("SHOW VIEW", JobType.DDL),
    DESC_VIEW("DESC VIEW", JobType.DDL),
    CREATE_SHARE("CREATE SHARE", JobType.DDL),
    ALTER_SHARE("ALTER SHARE", JobType.DDL),
    DROP_SHARE("DROP SHARE", JobType.DDL),
    SHOW_SHARE("SHOW SHARE", JobType.DDL),
    DESC_SHARE("DESC SHARE", JobType.DDL),
    CREATE_ROLE("CREATE ROLE", JobType.DDL),
    ALTER_ROLE("ALTER ROLE", JobType.DDL),
    DROP_ROLE("DROP ROLE", JobType.DDL),
    DESC_ROLE("DESC ROLE", JobType.DDL),
    SHOW_ROLE("SHOW ROLE", JobType.DDL),
    CREATE_STREAM("CREATE STREAM", JobType.DDL),
    SHOW_STREAMS("SHOW STREAMS", JobType.DDL),
    DESC_STREAM("DESC STREAM", JobType.DDL),
    START_STREAM("START STREAM", JobType.DDL),
    STOP_STREAM("STOP STREAM", JobType.DDL),
    DROP_STREAM("DROP STREAM", JobType.DDL),
    CREATE_ACCELERATOR("CREATE ACCELERATOR", JobType.DDL),
    SHOW_ACCELERATORS("SHOW ACCELERATORS", JobType.DDL),
    DROP_ACCELERATOR("DROP ACCELERATOR", JobType.DDL),
    ALTER_ACCELERATOR("ALTER ACCELERATOR", JobType.DDL),
    CREATE_DELEGATE("CREATE DELEGATE", JobType.DDL),
    DESC_DELEGATE("DESC DELEGATE", JobType.DDL),
    DROP_DELEGATE("DROP DELEGATE", JobType.DDL),
    SHOW_DELEGATES("SHOW DELEGATES", JobType.DDL),
    ADD_ALL_OPERATION("ADD ALL OPERATION", JobType.DDL),
    REVOKE_ALL_OPERATION("REVOKE ALL OPERATION", JobType.DDL),
    REVOKE_ALL_OPERATION_FROM_ROLE("REVOKE ALL OPERATION FROM ROLE", JobType.DDL),
    SHOW_JOB_HISTORY("SHOW JOB HISTORY", JobType.DDL),
    SHOW_ACCESS_STATS_FOR_CATALOG("SHOW ACCESS STATS FOR CATALOG", JobType.DDL),
    DESC_ACCESS_STATS_FOR_TABLE("DESC ACCESS STATS FOR TABLE", JobType.DDL),
    SHOW_DATA_LINEAGE_FOR_TABLE("SHOW DATA LINEAGE FOR TABLE", JobType.DDL),
    REVOKE_SHARE_FROM_USER("REVOKE_SHARE_FROM_USER", JobType.DDL),
    GRANT_SHARE_TO_USER("GRANT_SHARE_TO_USER", JobType.DDL),
    SHOW_DATABASE_HISTORY("SHOW_DATABASE_HISTORY", JobType.DDL),
    ATTACH_ENGINE("ATTACH ENGINE", JobType.DDL),
    DETACH_ENGINE("DETACH_ENGINE", JobType.DDL),
    USE_ENGINE("USE_ENGINE", JobType.DDL),
    SHOW_ENGINES("SHOW_ENGINES", JobType.DDL),
    SET_CONFIGURATION("SET_CONFIGURATION", JobType.DDL),
    UNSET_CONFIGURATION("UNSET_CONFIGURATION", JobType.DDL),
    CREATE_FUNCTION("CREATE_FUNCTION", JobType.DDL),
    DROP_FUNCTION("DROP_FUNCTION", JobType.DDL),
    ALTER_FUNCTION("ALTER_FUNCTION", JobType.DDL),
    GET_FUNCTION("GET_FUNCTION", JobType.DDL),
    ALTER_PRIVILEGE("ALTER_PRIVILEGE", JobType.DDL),
    SHOW_PRIVILEGES("SHOW_PRIVILEGES", JobType.DDL),
    GET_PRIVILEGES_CHANGE_RECORD("GET_PRIVILEGES_CHANGE_RECORD", JobType.DDL),
    GET_PRIVILEGES_BY_ID("GET_PRIVILEGES_BY_ID", JobType.DDL),
    // DDL Operations for Materialized view
    CREATE_MATERIALIZED_VIEW("CREATE_MATERIALIZED_VIEW", JobType.DDL),
    DROP_MATERIALIZED_VIEW("DROP_MATERIALIZED_VIEW", JobType.DDL),
    REFRESH_MATERIALIZED_VIEW("REFRESH_MATERIALIZED_VIEW", JobType.DDL),
    SHOW_MATERIALIZED_VIEWS("SHOW_MATERIALIZED_VIEWS", JobType.DDL),
    DESC_MATERIALIZED_VIEW("DESC MATERIALIZED_VIEW", JobType.DDL),
    ALTER_MATERIALIZED_VIEW("ALTER TABLE", JobType.DDL),

    // DML Operation
    INSERT_TABLE("INSERT TABLE", JobType.DML),
    INSERT_OVERWRITE("OVERWRITE TABLE", JobType.DML),
    CREATE_TABLE_AS_SELECT("CREATE TABLE AS SELECT", JobType.DML),
    DATA_GEN("DATAGEN", JobType.DML),
    ADD_PARTITION("ADD PARTITION", JobType.DML),
    DROP_PARTITION("DROP PARTITION", JobType.DML),
    DROP_PARTITIONS_BY_EXPR("DROP PARTITIONS BY EXPR", JobType.DML),
    TRUNCATE_PARTITION("TRUNCATE PARTITION", JobType.DML),
    ALTER_PARTITION("ALTER PARTITION", JobType.DDL),
    RENAME_PARTITION("RENAME PARTITION", JobType.DDL),
    RENAME_TABLE("RENAME TABLE", JobType.DDL),
    COPY_INTO("COPY INTO", JobType.DDL),

    // Partition Operation
    APPEND_PARTITION("APPEND PARTITION", JobType.DML),
    ADD_PARTITIONS("ADD PARTITIONS", JobType.DML),
    DELETE_PARTITION_COLUMN_STATISTICS("DELETE PARTITION COLUMN STATISTICS", JobType.DML),
    GET_PARTITION_COLUMN_STATISTICS("GET PARTITION COLUMN STATISTICS", JobType.DML),
    SET_PARTITION_COLUMN_STATISTICS("SET PARTITION COLUMN STATISTICS", JobType.DML),
    GET_AGGREGATE_PARTITIONS_COLUMN_STATISTICS("GET AGGREGATE PARTITIONS COLUMN STATISTICS", JobType.DML),
    GET_PARTITION("GET PARTITION", JobType.DML),
    GET_PARTITION_WITH_AUTH("GET PARTITION WITH AUTH", JobType.DML),
    GET_PARTITIONS_BY_NAMES("GET PARTITIONS BY NAMES", JobType.DML),
    LIST_PARTITIONS_BY_EXPR("LIST PARTITIONS BY EXPR", JobType.DML),
    LIST_PARTITIONS_WITH_AUTH("LIST PARTITIONS WITH AUTH", JobType.DML),

    // statistics Operation
    UPDATE_PARTITION_COLUMN_STATISTICS("UPDATE PARTITION COLUMN STATISTICS", JobType.DML),
    UPDATE_TABLE_COLUMN_STATISTICS("UPDATE TABLE COLUMN STATISTICS", JobType.DML),
    DELETE_TABLE_COLUMN_STATISTICS("DELETE TABLE COLUMN STATISTICS", JobType.DML),

    // Retrieval Operation
    SELECT_TABLE("SELECT TABLE", JobType.RETRIEVAL),
    SELECT_VIEW("SELECT VIEW", JobType.RETRIEVAL),
    SELECT_SHARE("SELECT SHARE", JobType.RETRIEVAL),
    EXPLAIN("EXPLAIN", JobType.RETRIEVAL),

    // MRS Token
    ADD_TOKEN("ADD TOKEN", JobType.OTHERS),
    ALTER_TOKEN("ALTER TOKEN", JobType.OTHERS),
    DELETE_TOKEN("DELETE TOKEN", JobType.OTHERS),
    GET_TOKEN("GET TOKEN", JobType.OTHERS),
    LIST_TOKEN("LIST TOKEN", JobType.OTHERS),

    // Constraints
    GET_PRIMARY_KEYS("GET PRIMARY KEYS", JobType.DDL),
    GET_FOREIGN_KEYS("GET FOREIGN KEYS", JobType.DDL),
    GET_CONSTRAINTS("GET CONSTRAINTS", JobType.DDL),
    ADD_PRIMARY_KEYS("ADD PRIMARY KEYS", JobType.DDL),
    ADD_FOREIGN_KEYS("ADD FOREIGN KEYS", JobType.DDL),
    ADD_CONSTRAINTS("ADD CONSTRAINTS", JobType.DDL),
    ALTER_CONSTRAINT("ALTER CONSTRAINT", JobType.DDL),
    DROP_CONSTRAINT("DROP CONSTRAINT", JobType.DDL),

    // Others Operation
    ILLEGAL_OPERATION("ILLEGAL OPERATION", JobType.OTHERS),
    NULL_OPERATION_AUTHORIZED("NULL OPERATION AUTHORIZED", JobType.OTHERS),

    COMPACT_TABLE("COMPACT TABLE", JobType.DML),

    // MV SQL Rewrite operation
    REWRITE_SQL("REWRITE SQL", JobType.RETRIEVAL);

    @Getter
    private final String printName;

    @Getter
    private final JobType type;

    public static List<Operation> ALTER_TABLE_CHILD_OPERATIONS = Arrays.asList(
            ADD_COLUMN, DROP_COLUMN, RENAME_COLUMN, REPLACE_COLUMN, CHANGE_COLUMN,
            SET_PROPERTIES, UNSET_PROPERTIES, SET_LOCATION, RENAME_TABLE,
            ADD_PARTITION, DROP_PARTITION, RENAME_PARTITION, ADD_PARTITIONS);

    public static Operation convertToParentOperation(Operation operation) {
        if (Operation.ALTER_TABLE_CHILD_OPERATIONS.contains(operation)) {
            return Operation.ALTER_TABLE;
        }
        return operation;
    }

    Operation(String printName, JobType type) {
        this.printName = printName;
        this.type = type;
    }
}
