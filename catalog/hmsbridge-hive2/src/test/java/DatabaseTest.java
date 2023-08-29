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
import java.util.HashMap;
import java.util.Map;

import io.polycat.catalog.common.model.Database;
import io.polycat.catalog.common.plugin.request.AlterDatabaseRequest;
import io.polycat.catalog.common.plugin.request.DeleteDatabaseRequest;
import io.polycat.catalog.common.plugin.request.GetDatabaseRequest;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DatabaseTest extends HMSBridgeTestEnv {

    @Test
    public void should_success_create_db_with_all_params() {
        /*
         * Spark-3.0 Syntax: CREATE {DATABASE | SCHEMA} [ IF NOT EXISTS ] database_name
         * [ COMMENT database_comment ]
         * [ LOCATION database_directory ]
         * [ WITH DBPROPERTIES (property_name=property_value [ , ...]) ]
         * */
        assertDoesNotThrow(() -> sparkSession.sql("create database DBTest_CREATE_DB"
            + " comment 'create database with all params'"
            + " location '" + warehouse
            + "' with dbproperties (lms_name='lms', ID=001)").collect());

        Row[] rows = (Row[]) sparkSession.sql("desc database extended DBTest_CREATE_DB").collect();
        assertEquals(5, rows.length);
        assertEquals("dbtest_create_db", rows[0].getString(1));
        assertEquals("create database with all params", rows[1].getString(1));
        assertTrue(rows[2].getString(1).endsWith(warehouse));
        assertEquals("((lms_name,lms), (ID,001))", rows[4].getString(1));

        // hms and polyCat create database synchronously. so polyCat client can also get this database
        GetDatabaseRequest getDatabaseRequest = makeGetDatabaseRequest("DBTest_CREATE_DB");
        Database database = catalogClient.getDatabase(getDatabaseRequest);
        assertNotNull(database);
        assertEquals("dbtest_create_db", database.getDatabaseName());

        assertDoesNotThrow(() -> sparkSession.sql("create database if not exists DBTest_CREATE_DB"
            + " comment 'create database with all params'"
            + " location '" + warehouse
            + "' with dbproperties (ID=001)").collect());

        rows = (Row[]) sparkSession.sql("show databases").collect();
        assertEquals(2, rows.length);
        assertEquals("dbtest_create_db", rows[0].getString(0));
        assertEquals("default", rows[1].getString(0));
    }

    private GetDatabaseRequest makeGetDatabaseRequest(String dbName) {
        GetDatabaseRequest request = new GetDatabaseRequest();
        request.setProjectId(catalogClient.getProjectId());
        request.setCatalogName(defaultCatalogName);
        request.setDatabaseName(dbName);
        return request;
    }

    @Test
    public void could_fail_drop_non_empty_database() {
        // Spark-3.0 Syntax: DROP DATABASE [IF EXISTS] db_name [RESTRICT|CASCADE]
        assertDoesNotThrow(() -> sparkSession.sql("use default").collect());
        assertDoesNotThrow(() -> sparkSession.sql("create table DBTest_DROP_DB_Tbl_1 (c1 string)"
            + " tblproperties('lms_name'='lms')").collect());

        AnalysisException cannotDropEx = assertThrows(AnalysisException.class,
            () -> sparkSession.sql("drop database default").collect());
        assertTrue(cannotDropEx.getMessage().startsWith("Can not drop default database"));
    }

    @Test
    public void drop_non_exist_db_does_not_throw() {
        String nonExistDB = "non_exist_db";
        assertDoesNotThrow(() -> sparkSession.sql("drop database if exists " + nonExistDB).collect());
    }

    @Test
    public void could_success_drop_empty_database() {
        String dbName1 = "Drop_Db_Test_bridge_side";
        sparkSession.sql("create database " + dbName1 + " with dbproperties('lms_name'='lms')").collect();
        String dbName2 = "Drop_Db_Test_rest_side";
        Map<String, String> dbproperties = new HashMap<String, String>() {
            {
                put("lms_name", dbName2);
            }
        };
        createDatabase(dbName2, dbproperties);
        Row[] rows = (Row[]) sparkSession.sql("show databases").collect();
        assertEquals(3, rows.length);
        assertEquals("default", rows[0].getString(0));
        assertEquals("drop_db_test_bridge_side", rows[1].getString(0));
        assertEquals("drop_db_test_rest_side", rows[2].getString(0));

        // spark can observe drop database from rest api side, vice versa
        deleteDatabase(dbName1);
        rows = (Row[]) sparkSession.sql("show databases").collect();
        assertEquals(2, rows.length);
        assertEquals("default", rows[0].getString(0));
        assertEquals("drop_db_test_rest_side", rows[1].getString(0));

        sparkSession.sql("drop database " + dbName2).collect();
        rows = (Row[]) sparkSession.sql("show databases").collect();
        assertEquals(1, rows.length);
        assertEquals("default", rows[0].getString(0));
    }

    private void deleteDatabase(String dbName) {
        DeleteDatabaseRequest request = new DeleteDatabaseRequest();
        request.setProjectId(catalogClient.getProjectId());
        request.setCatalogName(defaultCatalogName);
        request.setDatabaseName(dbName);
        catalogClient.deleteDatabase(request);
    }

    @Test
    public void should_success_alter_dbproperties() {
        // Spark-3.0 Syntax: ALTER DATABASE db_name SET DBPROPERTIES ('property_name'='property_value', ...)
        String dbName = "DBTest_ALTER_DB";
        sparkSession.sql("create database " + dbName
            + " comment 'test for alter databases'"
            + " with dbproperties (lms_name='lms', ID=001)").collect();

        // alter database from bridge side
        sparkSession.sql("alter database " + dbName
            + " set dbproperties ('remark1'='alter db from bridge', ID=100)").collect();

        // alter database from ddl server side
        AlterDatabaseRequest alterDatabaseRequest = makeAlterDatabaseRequest(dbName); // alter location and properties
        catalogClient.alterDatabase(alterDatabaseRequest);

        // we should observe all changes of databases
        Row[] rows = (Row[]) sparkSession.sql("desc database extended " + dbName).collect();
        assertEquals(5, rows.length);
        assertEquals("[Database Name,dbtest_alter_db]", rows[0].toString());
        assertEquals("[Comment,test for alter databases]", rows[1].toString());
        assertEquals("[Location," + warehouse + "]", rows[2].toString());
        assertEquals("[Owner,test]", rows[3].toString());
        assertEquals(
            "[Properties,((ID,100), (lms_name,lms), (remark2,alter from REST API), (remark1,alter db from bridge))]",
            rows[4].toString());

        GetDatabaseRequest getDatabaseRequest = makeGetDatabaseRequest(dbName);
        Database database = catalogClient.getDatabase(getDatabaseRequest);
        assertNotNull(database);
        Map<String, String> parameters = database.getParameters();
        assertEquals(4, parameters.size());
        assertEquals("lms", parameters.get("lms_name"));
        assertEquals("100", parameters.get("ID"));
        assertEquals("alter db from bridge", parameters.get("remark1"));
        assertEquals("alter from REST API", parameters.get("remark2"));

        sparkSession.sql("drop database if exists " + dbName).collect();
    }

    private AlterDatabaseRequest makeAlterDatabaseRequest(String dbName) {
        String newUri = warehouse;
        Map<String, String> params = new HashMap<String, String>() {
            {
                put("remark2", "alter from REST API");
            }
        };

        DatabaseInput input = getDatabaseInput(defaultCatalogName, dbName, newUri, userName);
        input.setParameters(params);
        return new AlterDatabaseRequest(catalogClient.getProjectId(), defaultCatalogName, dbName, input);
    }

    @Test
    public void show_dbs_wildcard_matching_should_success() {
        // Spark-3.0 Syntax: show databases like '*db*'
        Row[] rows = (Row[]) sparkSession.sql("show databases like '*efa*'").collect(); // part word of 'default'
        assertEquals(1, rows.length);
        assertEquals("default", rows[0].getString(0));
    }

    @Test
    public void show_dbs_both_in_lms_and_hms_should_success() {
        String lmsDb = "show_both_dbs_lms";
        String hmsDb = "show_both_dbs_hms";
        sparkSession.sql("create database " + lmsDb + " with dbproperties (lms_name='lms', ID=001)").collect();
        sparkSession.sql("create database " + hmsDb + " with dbproperties (ID=002)").collect();
        Row[] rows = (Row[]) sparkSession.sql("show databases like 'show_both*'").collect();
        assertEquals(2, rows.length);
        assertEquals(hmsDb, rows[0].getString(0));
        assertEquals(lmsDb, rows[1].getString(0));

        sparkSession.sql("drop database if exists " + lmsDb).collect();
        sparkSession.sql("drop database if exists " + hmsDb).collect();
    }
}
