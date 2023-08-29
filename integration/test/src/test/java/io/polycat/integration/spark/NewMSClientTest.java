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
package io.polycat.integration.spark;

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NewMSClientTest extends SparkEnv {

    @ParameterizedTest
    @ValueSource(strings = {"parquet", "orc", "csv"})
    public void non_part_table_delegate_test(String datasource) {
        String tblName = "delegate_non_part_tbl_" + datasource;
        sparkSession.sql("create database if not exists db1").collect();
        sparkSession.sql("use db1;").collect();
        sparkSession.sql(String.format(
            "create table if not exists %s (id int, name string) using %s", tblName, datasource)).collect();
        sparkSession.sql(String.format(
            "insert into %s values(1, 'alice');", tblName)).collect();
        Row[] rows = (Row[]) sparkSession.sql(String.format(
            "select * from %s;", tblName)).collect();
        assertEquals(1, rows.length);
        assertEquals(1, rows[0].getInt(0));
        assertEquals("alice", rows[0].getString(1));
    }

    @ParameterizedTest
    @ValueSource(strings = {"parquet", "orc", "csv"})
    public void part_table_delegate_test(String datasource) {
        String tblName = "delegate_part_tbl_" + datasource;
        sparkSession.sql("create database if not exists db1").collect();
        sparkSession.sql("use db1;").collect();
        sparkSession.sql(String.format(
            "create table if not exists %s (name string, age int) using %s partitioned by (age)",
            tblName, datasource)).collect();
        sparkSession.sql(String.format(
            "insert into %s partition(age=24) values('alice');", tblName)).collect();
        Row[] rows = (Row[]) sparkSession.sql(String.format(
            "select * from %s;", tblName)).collect();
        assertEquals(1, rows.length);
        assertEquals("alice", rows[0].getString(0));
        assertEquals(24, rows[0].getInt(1));
    }

    // hive table not support until polycat column type modification.
    // hive table can be used on the polycat-hms-stored deploy situation.
    @Disabled
    @Test
    public void hive_format_table_delegate_test() {
        sparkSession.sql("create database if not exists db1").collect();
        sparkSession.sql("use db1;").collect();
        sparkSession.sql("create table delegate_hive_table (name string, age int);").collect();
        //sparkSession.sql("desc table extended delegate_hive_table;").show();
        sparkSession.sql("insert into delegate_hive_table values ('alice', 24);").collect();
        sparkSession.sql("select * from delegate_hive_table;").show();
    }
}