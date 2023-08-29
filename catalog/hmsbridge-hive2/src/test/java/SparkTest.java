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
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import io.polycat.catalog.authentication.Authentication;
import io.polycat.catalog.authentication.model.AuthenticationResult;
import io.polycat.catalog.authentication.model.LocalIdentity;
import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.model.PagedList;
import io.polycat.catalog.common.model.Table;
import io.polycat.catalog.common.model.TableCommit;
import io.polycat.catalog.common.model.record.Record;
import io.polycat.catalog.common.plugin.request.CreateBranchRequest;
import io.polycat.catalog.common.plugin.request.CreateCatalogRequest;
import io.polycat.catalog.common.plugin.request.CreateDatabaseRequest;
import io.polycat.catalog.common.plugin.request.GetTableRequest;
import io.polycat.catalog.common.plugin.request.ListFileRequest;
import io.polycat.catalog.common.plugin.request.ListTableCommitsRequest;
import io.polycat.catalog.common.plugin.request.RestoreTableRequest;
import io.polycat.catalog.common.plugin.request.input.CatalogInput;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;
import io.polycat.common.RowBatch;
import io.polycat.common.util.SparkVersionUtil;

import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SparkTest extends HMSBridgeTestEnv {

    @Test
    void createTableUseSameNameTest() {
        // create table by using 'lms_name' in spark
        sparkSession.sql("create table tpp (c1 string, c2 int,commit1 string,commit string) using parquet"
            + " partitioned by (commit1,commit) tblproperties('lms_name'='lms') ").collect();

        CreateCatalogRequest createCatalogRequest = new CreateCatalogRequest();
        CatalogInput catalogInput = new CatalogInput();
        catalogInput.setCatalogName("lms1");
        createCatalogRequest.setInput(catalogInput);
        createCatalogRequest.setProjectId(catalogClient.getProjectId());
        createCatalogRequest.getInput().setOwner(userName);
        catalogClient.createCatalog(createCatalogRequest);

        CreateDatabaseRequest createDatabaseRequest = new CreateDatabaseRequest();
        DatabaseInput databaseInput = getDatabaseInput("lms1", "default", "target/default", userName);
        createDatabaseRequest.setInput(databaseInput);
        createDatabaseRequest.setProjectId(catalogClient.getProjectId());
        createDatabaseRequest.setCatalogName("lms1");
        catalogClient.createDatabase(createDatabaseRequest);

        AnalysisException e = assertThrows(AnalysisException.class,
            () -> sparkSession.sql("create table tpp (c1 string, c2 int,commit1 string,commit string) using parquet"
                + " partitioned by (commit1,commit) tblproperties('lms_name'='lms1') ").collect());
        if (SparkVersionUtil.isSpark30()) {
            assertEquals(String.format("Table %s.%s already exists.;", "default", "tpp"), e.getMessage());
        } else {
            assertEquals(String.format("Table %s.%s already exists.", "default", "tpp"), e.getMessage());
        }
    }


    @Test
    void createTableAndInsertDataDirectlyUsingNothingTest() {

        // create table by using 'lms_name' in spark
        sparkSession.sql("create table t1 (c1 string, c2 int)"
            + " partitioned by (commit1 string,lms_commit string) tblproperties('lms_name'='lms') ").collect();

        String part = UUID.randomUUID().toString();
        sparkSession.sql("insert into t1 partition(commit1='" + part + "',lms_commit='" + part + "')"
            + " values ('a', 1),('b',2)").collect();

        // check metadata in lms
        GetTableRequest getTableRequest = new GetTableRequest(catalogClient.getProjectId(), "lms", "default", "t1");
        Table table = catalogClient.getTable(getTableRequest);
        Assertions.assertNotNull(table);

        ListFileRequest listFileRequest = new ListFileRequest(catalogClient.getProjectId(), "lms", "default", "t1");
        List<String> dataFiles = catalogClient.listFiles(listFileRequest);
        Assertions.assertNotNull(dataFiles);

        //check data from spark
        Row[] rows = (Row[]) sparkSession.sql("select c1,c2 from t1 order by c1").collect();
        Assertions.assertEquals(2, rows.length);
        Assertions.assertEquals("a", rows[0].getString(0));
        Assertions.assertEquals(1, rows[0].getInt(1));
        Assertions.assertEquals("b", rows[1].getString(0));
        Assertions.assertEquals(2, rows[1].getInt(1));

        sparkSession.sql("drop table t1").collect();
    }

    @Test
    void createTableAndInsertDataDirectlyUsingCSVTest() {

        // create table by using 'lms_name' in spark
        sparkSession.sql("create table t1csv (c1 string, c2 int,commit1 string,lms_commit string) using csv"
            + " partitioned by (commit1,lms_commit) tblproperties('lms_name'='lms') ").collect();

        String part = UUID.randomUUID().toString();
        sparkSession.sql("insert into t1csv partition(commit1='" + part + "',lms_commit='" + part + "')"
            + " values ('a', 1),('b',2)").collect();

        // check metadata in lms
        GetTableRequest getTableRequest = new GetTableRequest(catalogClient.getProjectId(), "lms", "default", "t1csv");
        Table table = catalogClient.getTable(getTableRequest);
        Assertions.assertNotNull(table);

        ListFileRequest listFileRequest = new ListFileRequest(catalogClient.getProjectId(), "lms", "default", "t1csv");
        List<String> dataFiles = catalogClient.listFiles(listFileRequest);
        Assertions.assertNotNull(dataFiles);

        //check data from spark

        Row[] rows = (Row[]) sparkSession.sql("select c1,c2 from t1csv order by c1").collect();
        Assertions.assertEquals(2, rows.length);
        Assertions.assertEquals("a", rows[0].getString(0));
        Assertions.assertEquals(1, rows[0].getInt(1));
        Assertions.assertEquals("b", rows[1].getString(0));
        Assertions.assertEquals(2, rows[1].getInt(1));

        sparkSession.sql("drop table t1csv").collect();

        String tableName = "csvTbl";

        sparkSession.sql("create table " + tableName + " (c1 string, c2 int) using csv "
            + "tblproperties('lms_name'='lms')").collect();

        sparkSession.sql("drop table " + tableName).collect();
    }

    @Test
    void createTableAndInsertDataDirectlyStoredAsTextfileTest() {

        // create table by using 'lms_name' in spark
        sparkSession.sql("create table t1txt (c1 string, c2 int) stored as textfile"
            + " partitioned by (commit1 string,lms_commit string) tblproperties('lms_name'='lms') ").collect();

        String part = UUID.randomUUID().toString();
        sparkSession.sql("insert into t1txt partition(commit1='" + part + "',lms_commit='" + part + "')"
            + " values ('a', 1),('b',2)").collect();

        // check metadata in lms
        GetTableRequest getTableRequest = new GetTableRequest(catalogClient.getProjectId(), "lms", "default", "t1txt");
        Table table = catalogClient.getTable(getTableRequest);
        Assertions.assertNotNull(table);

        ListFileRequest listFileRequest = new ListFileRequest(catalogClient.getProjectId(), "lms", "default", "t1txt");
        List<String> dataFiles = catalogClient.listFiles(listFileRequest);
        Assertions.assertNotNull(dataFiles);

        //check data from spark

        Row[] rows = (Row[]) sparkSession.sql("select c1,c2 from t1txt order by c1").collect();
        Assertions.assertEquals(2, rows.length);
        Assertions.assertEquals("a", rows[0].getString(0));
        Assertions.assertEquals(1, rows[0].getInt(1));
        Assertions.assertEquals("b", rows[1].getString(0));
        Assertions.assertEquals(2, rows[1].getInt(1));

        sparkSession.sql("drop table t1txt").collect();
    }

    @Test
    void createTableAndInsertDataDirectlyUsingORCTest() {

        // create table by using 'lms_name' in spark
        sparkSession.sql("create table t1orc (c1 string, c2 int,commit1 string,lms_commit string) using orc"
            + " partitioned by (commit1,lms_commit) tblproperties('lms_name'='lms') ").collect();

        String part = UUID.randomUUID().toString();
        sparkSession.sql("insert into t1orc partition(commit1='" + part + "',lms_commit='" + part + "')"
            + " values ('a', 1),('b',2)").collect();

        // check metadata in lms
        GetTableRequest getTableRequest = new GetTableRequest(catalogClient.getProjectId(), "lms", "default", "t1orc");
        Table table = catalogClient.getTable(getTableRequest);
        Assertions.assertNotNull(table);

        ListFileRequest listFileRequest = new ListFileRequest(catalogClient.getProjectId(), "lms", "default", "t1orc");
        List<String> dataFiles = catalogClient.listFiles(listFileRequest);
        Assertions.assertNotNull(dataFiles);

        //check data from spark
        Row[] rows = (Row[]) sparkSession.sql("select c1,c2 from t1orc order by c1").collect();
        Assertions.assertEquals(2, rows.length);
        Assertions.assertEquals("a", rows[0].getString(0));
        Assertions.assertEquals(1, rows[0].getInt(1));
        Assertions.assertEquals("b", rows[1].getString(0));
        Assertions.assertEquals(2, rows[1].getInt(1));

        sparkSession.sql("drop table t1orc").collect();
    }

    @Test
    void insertDataAsSelectTest() {
        // create table by using 'lms_name' in spark
        sparkSession.sql("create table t6 (c1 string, c2 int, lms_commit string) using parquet"
            + " partitioned by (lms_commit) tblproperties('lms_name'='lms')").collect();

        String part = UUID.randomUUID().toString();
        sparkSession.sql("insert into t6 partition(lms_commit='" + part + "')"
            + " values ('a', 1),('b',2)").collect();

        sparkSession.sql("create table t7 (d1 string, d2 int, lms_commit string) using parquet"
            + " partitioned by (lms_commit) tblproperties('lms_name'='lms')").collect();

        String partt7 = UUID.randomUUID().toString();
        sparkSession.sql("insert into t7 partition(lms_commit='" + partt7 + "')"
            + " select c1,c2 from t6").collect();

        ListFileRequest listFileRequest = new ListFileRequest(catalogClient.getProjectId(), "lms", "default", "t7");
        List<String> dataFiles = catalogClient.listFiles(listFileRequest);
        Assertions.assertNotNull(dataFiles);

        // check data from spark
        Row[] rows = (Row[]) sparkSession.sql("select d1,d2 from t7 order by d1").collect();
        Assertions.assertEquals(2, rows.length);
        Assertions.assertEquals("a", rows[0].getString(0));
        Assertions.assertEquals(1, rows[0].getInt(1));
        Assertions.assertEquals("b", rows[1].getString(0));
        Assertions.assertEquals(2, rows[1].getInt(1));

        sparkSession.sql("drop table t6").collect();
        sparkSession.sql("drop table t7").collect();
    }

    @Test
    void alterPropertyTest() {
        sparkSession.sql("create table t21 (c1 string, c2 int, lms_commit string) using parquet"
            + " partitioned by (lms_commit) tblproperties('lms_name'='lms')").collect();

        String part1 = UUID.randomUUID().toString();
        sparkSession.sql("insert into t21 partition(lms_commit='" + part1 + "')"
            + " values ('a', 1),('b',2),('c',3),('d',4),('e',4),('f',4)").collect();

        sparkSession.sql("alter table t21 set tblproperties('lms_name'='b1')").collect();

        // check metadata in b1
        GetTableRequest getTableRequestOld = new GetTableRequest(catalogClient.getProjectId(), "b1", "default", "t21");
        CatalogException exception = assertThrows(CatalogException.class,
            () -> catalogClient.getTable(getTableRequestOld));
        assertEquals(exception.getMessage(), String.format("Table [%s] not found", "b1.default.t21"));

        sparkSession.sql("drop table t21").collect();
    }

    @Test
    void sqlForIMCTest() {
        // left join、inner join
        // sum() count() limit avg
        // string_agg()
        // group by、order by、desc、 having
        // case when then end
        // sub query

        // create table by using 'lms_name' in spark
        sparkSession.sql("create table t9 (c1 string, c2 int, lms_commit string) using parquet"
            + " partitioned by (lms_commit) tblproperties('lms_name'='lms')").collect();

        String part1 = UUID.randomUUID().toString();
        sparkSession.sql("insert into t9 partition(lms_commit='" + part1 + "')"
            + " values ('a', 1),('b',2),('c',3),('d',4),('e',4),('f',4)").collect();

        // check metadata in lms
        GetTableRequest getTableRequest = new GetTableRequest(catalogClient.getProjectId(), "lms", "default", "t9");
        Table lmsTable = catalogClient.getTable(getTableRequest);
        Assertions.assertNotNull(lmsTable);

        ListFileRequest listFileRequest = new ListFileRequest(catalogClient.getProjectId(), "lms", "default", "t9");
        List<String> dataFiles = catalogClient.listFiles(listFileRequest);
        Assertions.assertNotNull(dataFiles);

        // check data from spark
        // support order by&limit&desc
        Row[] rows = (Row[]) sparkSession.sql("select c1,c2 from t9 order by c2 desc limit 2 ").collect();
        Assertions.assertEquals(2, rows.length);
        Assertions.assertEquals("d", rows[0].getString(0));
        Assertions.assertEquals(4, rows[0].getInt(1));

        // support count()
        Row[] rows1 = (Row[]) sparkSession.sql("select count(c2) from t9 where c2 > 2").collect();
        Assertions.assertEquals(1, rows1.length);
        Assertions.assertEquals(4, rows1[0].getLong(0));

        // support avg()
        Row[] rows2 = (Row[]) sparkSession.sql("select avg(c2) from t9").collect();
        Assertions.assertEquals(1, rows2.length);
        Assertions.assertEquals(3, rows2[0].getDouble(0));

        // support group by&&having
        Row[] rows3 = (Row[]) sparkSession.sql("select c2,first(c1) from t9 group by c2 having c2 = 2 ").collect();
        Assertions.assertEquals(1, rows3.length);
        Assertions.assertEquals(2, rows3[0].getInt(0));

        // support case when then end
        Row[] rows4 = (Row[]) sparkSession
            .sql("select c1, case  when c2 == 4 then 'yes' else 'false' end as newc2 from t9 ").collect();
        Assertions.assertEquals(6, rows4.length);
        Assertions.assertEquals("yes", rows4[0].getString(1));

        //support inner join&&left join
        sparkSession.sql("create table t10 (c1 string, c2 int, lms_commit string) using parquet"
            + " partitioned by (lms_commit) tblproperties('lms_name'='lms')").collect();

        String part2 = UUID.randomUUID().toString();
        sparkSession.sql("insert into t10 partition(lms_commit='" + part2 + "')"
            + " values ('a', 5),('a',4),('c',4),('d',7),('e',8),('f',9)").collect();

        Row[] rows5 = (Row[]) sparkSession.sql("select * from t10 left join t9 on t10.c2=t9.c2 ").collect();
        Assertions.assertEquals(10, rows5.length);
        Assertions.assertEquals("a", rows5[0].getString(0));

        Row[] rows6 = (Row[]) sparkSession.sql("select * from t10 inner join t9 on t10.c2=t9.c2 ").collect();
        Assertions.assertEquals(6, rows6.length);
        Assertions.assertEquals("a", rows6[0].getString(0));

        //support sub query
        Row[] rows7 = (Row[]) sparkSession.sql("select * from (select c1 from t10 where c2=4)").collect();
        Assertions.assertEquals(2, rows7.length);
        Assertions.assertEquals("a", rows7[0].getString(0));

        sparkSession.sql("drop table t9").collect();
        sparkSession.sql("drop table t10").collect();
    }

    @Test
    void sqlForIMCTestWithoutPartition() {
        // left join、inner join
        // sum() count() limit avg
        // string_agg()
        // group by、order by、desc、 having
        // case when then end
        // sub query

        // create table by using 'lms_name' in spark
        sparkSession.sql("create table tbl1 (c1 string, c2 int) using parquet"
            + "  tblproperties('lms_name'='lms')").collect();

        // check metadata in lms
        GetTableRequest getTableRequest = new GetTableRequest(catalogClient.getProjectId(), "lms", "default", "tbl1");
        Table lmsTable = catalogClient.getTable(getTableRequest);
        Assertions.assertNotNull(lmsTable);

        sparkSession.sql("insert into tbl1 "
            + " values ('a', 1),('b',2),('c',3),('d',4),('e',4),('f',4)").collect();

        // check data from spark
        // support order by&limit&desc
        Row[] rows = (Row[]) sparkSession.sql("select c1,c2 from tbl1 order by c2 desc limit 2 ").collect();
        Assertions.assertEquals(2, rows.length);
        Assertions.assertEquals("d", rows[0].getString(0));
        Assertions.assertEquals(4, rows[0].getInt(1));

        // support count()
        Row[] rows1 = (Row[]) sparkSession.sql("select count(c2) from tbl1 where c2 > 2").collect();
        Assertions.assertEquals(1, rows1.length);
        Assertions.assertEquals(4, rows1[0].getLong(0));

        // support avg()
        Row[] rows2 = (Row[]) sparkSession.sql("select avg(c2) from tbl1").collect();
        Assertions.assertEquals(1, rows2.length);
        Assertions.assertEquals(3, rows2[0].getDouble(0));

        // support group by&&having
        Row[] rows3 = (Row[]) sparkSession.sql("select c2,first(c1) from tbl1 group by c2 having c2 = 2 ").collect();
        Assertions.assertEquals(1, rows3.length);
        Assertions.assertEquals(2, rows3[0].getInt(0));

        // support case when then end
        Row[] rows4 = (Row[]) sparkSession
            .sql("select c1, case  when c2 == 4 then 'yes' else 'false' end as newc2 from tbl1 ").collect();
        Assertions.assertEquals(6, rows4.length);
        Assertions.assertEquals("yes", rows4[0].getString(1));

        //support inner join&&left join
        sparkSession.sql("create table tbl2 (c1 string, c2 int) using parquet"
            + " tblproperties('lms_name'='lms')").collect();

        sparkSession.sql("insert into tbl2 "
            + " values ('a', 5),('a',4),('c',4),('d',7),('e',8),('f',9)").collect();

        Row[] rows5 = (Row[]) sparkSession.sql("select * from tbl2 left join tbl1 on tbl2.c2=tbl1.c2 ").collect();
        Assertions.assertEquals(10, rows5.length);
        Assertions.assertEquals("a", rows5[0].getString(0));

        Row[] rows6 = (Row[]) sparkSession.sql("select * from tbl2 inner join tbl1 on tbl2.c2=tbl1.c2 ").collect();
        Assertions.assertEquals(6, rows6.length);
        Assertions.assertEquals("a", rows6[0].getString(0));

        //support sub query
        Row[] rows7 = (Row[]) sparkSession.sql("select * from (select c1 from tbl2 where c2=4)").collect();
        Assertions.assertEquals(2, rows7.length);
        Assertions.assertEquals("a", rows7[0].getString(0));

        sparkSession.sql("drop table tbl1").collect();
        sparkSession.sql("drop table tbl2").collect();
    }

    @Test
    void createTableAndInsertDataDirectlyMultiplePartitionTransactionTest() {

        // create table by using 'lms_name' in spark
        sparkSession.sql("create table tpt (c1 string, c2 int,commit1 string,lms_commit string) using parquet"
            + " partitioned by (commit1,lms_commit) tblproperties('lms_name'='lms') ").collect();

        String part = UUID.randomUUID().toString();
        sparkSession.sql("insert into tpt partition(commit1='" + part + "',lms_commit='" + part + "')"
            + " values ('a', 1),('b',2)").collect();

        // check metadata in lms
        GetTableRequest getTableRequest = new GetTableRequest(catalogClient.getProjectId(), "lms", "default", "tpt");
        Table lmsTable = catalogClient.getTable(getTableRequest);
        Assertions.assertNotNull(lmsTable);

        ListFileRequest listFileRequest = new ListFileRequest(catalogClient.getProjectId(), "lms", "default", "tpt");
        List<String> dataFiles = catalogClient.listFiles(listFileRequest);
        Assertions.assertNotNull(dataFiles);

        //check data from spark

        Row[] rows = (Row[]) sparkSession.sql("select c1,c2 from tpt order by c1").collect();
        Assertions.assertEquals(2, rows.length);
        Assertions.assertEquals("a", rows[0].getString(0));
        Assertions.assertEquals(1, rows[0].getInt(1));
        Assertions.assertEquals("b", rows[1].getString(0));
        Assertions.assertEquals(2, rows[1].getInt(1));

        sparkSession.sql("drop table tpt").collect();
    }

    public static AuthenticationResult doAuthentication(String userName, String password) {
        LocalIdentity localIdentity = new LocalIdentity();
        localIdentity.setUserId(userName);
        localIdentity.setPasswd(password);
        localIdentity.setIdentityOwner("LocalAuthenticator");
        return Authentication.authAndCreateToken(localIdentity);
    }

    @Test
    void createTableAndInsertDataDirectlyMultiplePartitionTest() {

        // create table by using 'lms_name' in spark
        sparkSession.sql("create table tp (c1 string, c2 int,commit1 string,commit string) using parquet"
            + " partitioned by (commit1,commit) tblproperties('lms_name'='lms') ").collect();

        String part = UUID.randomUUID().toString();
        sparkSession.sql("insert into tp partition(commit1='" + part + "',commit='" + part + "')"
            + " values ('a', 1),('b',2)").collect();

        // check metadata in lms
        GetTableRequest getTableRequest = new GetTableRequest(catalogClient.getProjectId(), "lms", "default", "tp");
        Table lmsTable = catalogClient.getTable(getTableRequest);
        Assertions.assertNotNull(lmsTable);

        ListFileRequest listFileRequest = new ListFileRequest(catalogClient.getProjectId(), "lms", "default", "tp");
        List<String> dataFiles = catalogClient.listFiles(listFileRequest);
        Assertions.assertNotNull(dataFiles);

        //check data from spark

        Row[] rows = (Row[]) sparkSession.sql("select c1,c2 from tp order by c1").collect();
        Assertions.assertEquals(2, rows.length);
        Assertions.assertEquals("a", rows[0].getString(0));
        Assertions.assertEquals(1, rows[0].getInt(1));
        Assertions.assertEquals("b", rows[1].getString(0));
        Assertions.assertEquals(2, rows[1].getInt(1));

        sparkSession.sql("drop table tp").collect();
    }

    @Test
    void createTableAndInsertDataDirectlyUsingCSVSinglePartitionTransactionTest() {

        // create table by using 'lms_name' in spark
        sparkSession.sql("create table t1csvp (c1 string, c2 int,lms_commit string) using csv"
            + " partitioned by (lms_commit) tblproperties('lms_name'='lms') ").collect();

        String part = UUID.randomUUID().toString();
        sparkSession.sql("insert into t1csvp partition(lms_commit='" + part + "')"
            + " values ('a', 1),('b',4)").collect();

        sparkSession.sql("insert into t1csvp partition(lms_commit='" + part + "')"
            + " values ('a', 2),('b',3)").collect();

        // check metadata in lms
        GetTableRequest getTableRequest = new GetTableRequest(catalogClient.getProjectId(), "lms", "default", "t1csvp");
        Table lmsTable = catalogClient.getTable(getTableRequest);
        Assertions.assertNotNull(lmsTable);

        ListFileRequest listFileRequest = new ListFileRequest(catalogClient.getProjectId(), "lms", "default", "t1csvp");
        List<String> dataFiles = catalogClient.listFiles(listFileRequest);
        Assertions.assertNotNull(dataFiles);

        //check data from spark

        Row[] rows = (Row[]) sparkSession.sql("select c1,c2 from t1csvp order by c2").collect();
        Assertions.assertEquals(4, rows.length);
        Assertions.assertEquals("a", rows[0].getString(0));
        Assertions.assertEquals(1, rows[0].getInt(1));
        Assertions.assertEquals("a", rows[1].getString(0));
        Assertions.assertEquals(2, rows[1].getInt(1));

        sparkSession.sql("drop table t1csvp").collect();
    }

    @Test
    void checkoutBranchTest() {
        // create table by using 'lms_name' in spark
        sparkSession.sql("create table checkbrancht1 (c1 string, c2 int,lms_commit string) using parquet"
            + " partitioned by (lms_commit) tblproperties('lms_name'='lms') ").collect();

        String part = UUID.randomUUID().toString();
        sparkSession.sql("insert into checkbrancht1 partition(lms_commit='" + part + "')"
            + " values ('a', 1),('b',4)").collect();

        // create branch from lms catalog
        CreateBranchRequest createBranchRequest = new CreateBranchRequest();
        createBranchRequest.setCatalogName("lms");
        createBranchRequest.setProjectId(catalogClient.getProjectId());
        createBranchRequest.setUserId(userName);
        createBranchRequest.setBranchName("branch1");
        catalogClient.createBranch(createBranchRequest);

        // check metadata in lms
        GetTableRequest branch1TableRequest = new GetTableRequest(catalogClient.getProjectId(),
            "branch1", "default", "checkbrancht1");
        Table branch1Table = catalogClient.getTable(branch1TableRequest);
        Assertions.assertNotNull(branch1Table);

        // checkout to branch1
        sparkSession.sql("alter table checkbrancht1 " +
            " set tblproperties('lms_name'='branch1')").collect();

        // check data
        Row[] rows = (Row[]) sparkSession.sql("select * from checkbrancht1").collect();
        Assertions.assertEquals(2, rows.length);

        Row[] partitions = (Row[]) sparkSession.sql("show partitions checkbrancht1").collect();
        Assertions.assertEquals(1, partitions.length);

        // insert
        part = UUID.randomUUID().toString();
        sparkSession.sql("insert into checkbrancht1 partition(lms_commit='" + part + "')"
            + " values ('c', 1),('d',4)").collect();

        part = UUID.randomUUID().toString();
        sparkSession.sql("insert into checkbrancht1 partition(lms_commit='" + part + "')"
            + " values ('e', 5),('f',6)").collect();

        // check data
        rows = (Row[]) sparkSession.sql("select * from checkbrancht1").collect();
        Assertions.assertEquals(6, rows.length);

        partitions = (Row[]) sparkSession.sql("show partitions checkbrancht1").collect();
        Assertions.assertEquals(3, partitions.length);

        // checkout to lms
        sparkSession.sql("alter table checkbrancht1 " +
            " set tblproperties('lms_name'='lms')").collect();

        // check data
        rows = (Row[]) sparkSession.sql("select * from checkbrancht1").collect();
        Assertions.assertEquals(2, rows.length);

        partitions = (Row[]) sparkSession.sql("show partitions checkbrancht1").collect();
        Assertions.assertEquals(1, partitions.length);

        sparkSession.sql("drop table checkbrancht1").collect();
    }

    private static void writeBatch(FileWriter fw, RowBatch batchRecords) throws IOException {
        for (Record record : batchRecords.getRecords()) {
            String line = record.fields.stream().map(Object::toString)
                .collect(Collectors.joining(","));
            fw.write(line + "\n");
        }
    }

    private static Record row(Object... values) {
        return new Record(values);
    }

    private static RowBatch batch(Record... records) {
        return new RowBatch(Arrays.asList(records));
    }

    private static void makeCSVDataFiles() throws IOException {
        String dataRoot = "target/data_dir";
        csvFolder = new File(dataRoot);
        csvFolder.mkdir();

        FileWriter fw = new FileWriter(dataRoot + "/user.csv");
        RowBatch dataRowBach = batch(row(1, "obs"), row(2, "s3"), row(3, "azure"));
        writeBatch(fw, dataRowBach);
        fw.close();
    }

    @Test
    void insertNonTransactionalTableFromCSVFolder() {

        // create table by using 'lms_name' in spark
        sparkSession.sql("create table tt1 (c1 int, c2 string) using parquet"
            + " tblproperties('lms_name'='lms')").show();

        //sparkSession.sql("drop table if exists source").collect();
        sparkSession.sql(String.format("create table st1 (c1 int, c2 string) "
            + " using csv location '%s'", csvFolder.getAbsolutePath())).show();

        Row[] rows = (Row[]) sparkSession.sql("select c1,c2 from st1").collect();
        Assertions.assertEquals(3, rows.length);

        sparkSession.sql("insert into tt1 SELECT c1,c2 FROM st1").show();

        rows = (Row[]) sparkSession.sql("select c1,c2 from tt1").collect();
        Assertions.assertEquals(3, rows.length);

        sparkSession.sql("drop table tt1").collect();
        sparkSession.sql("drop table st1").collect();
    }

    @Test
    void insertTransactionalTableFromCSVFolder() {

        // create table by using 'lms_name' in spark
        sparkSession.sql("create table tt2 (c1 int, c2 string, lms_commit string) using parquet "
            + " partitioned by (lms_commit) tblproperties('lms_name'='lms') ").show();

        //sparkSession.sql("drop table if exists st2").collect();
        sparkSession.sql(String.format("create table st2 (c1 int, c2 string) "
            + " using csv location '%s'", csvFolder.getAbsolutePath())).show();

        Row[] rows = (Row[]) sparkSession.sql("select c1,c2 from st2").collect();
        Assertions.assertEquals(3, rows.length);

        sparkSession.sql("insert into tt2 partition (lms_commit='123') SELECT c1,c2 FROM st2").show();

        rows = (Row[]) sparkSession.sql("select c1,c2 from tt2").collect();
        Assertions.assertEquals(3, rows.length);

        sparkSession.sql("drop table st2").collect();
        sparkSession.sql("drop table tt2").collect();
    }

    @Test
    void migrateNoPartitionTableFromHiveMetaStoreToPolyCatMetaStore() {
        String tableName = "Migrate";

        sparkSession.sql("create table " + tableName + " (c1 string, c2 int ) using orc").collect();

        sparkSession.sql("insert into " + tableName + " values ('a', 1), ('b',4)").collect();

        sparkSession.sql("insert into " + tableName + " values ('a', 2), ('b',5)").collect();

        //migrate
        sparkSession.sql("alter table " + tableName + " set tblproperties('lms_name'='lms')").collect();

        sparkSession.sql("desc table " + tableName).collect();

        // check metadata in lms
        GetTableRequest getTableRequest = new GetTableRequest(catalogClient.getProjectId(), "lms",
            "default", tableName.toLowerCase());
        Table lmsTable = catalogClient.getTable(getTableRequest);
        Assertions.assertNotNull(lmsTable);

        ListFileRequest listFileRequest = new ListFileRequest(catalogClient.getProjectId(), "lms",
            "default", tableName.toLowerCase());
        List<String> dataFiles = catalogClient.listFiles(listFileRequest);
        Assertions.assertNotNull(dataFiles);
        Assertions.assertEquals(0, dataFiles.size());

        Row[] rows = (Row[]) sparkSession.sql("select c1,c2 from " + tableName + " order by c2").collect();
        Assertions.assertEquals(4, rows.length);
        Assertions.assertEquals("a", rows[0].getString(0));
        Assertions.assertEquals(1, rows[0].getInt(1));
        Assertions.assertEquals("a", rows[1].getString(0));
        Assertions.assertEquals(2, rows[1].getInt(1));

        sparkSession.sql("insert into " + tableName + " values ('a', 3), ('b', 6)").collect();
        Row[] rows1 = (Row[]) sparkSession.sql("select c1,c2 from " + tableName + " order by c2").collect();
        Assertions.assertEquals(6, rows1.length);
        Assertions.assertEquals("a", rows1[0].getString(0));
        Assertions.assertEquals(1, rows1[0].getInt(1));
        Assertions.assertEquals("a", rows1[1].getString(0));
        Assertions.assertEquals(2, rows1[1].getInt(1));
        Assertions.assertEquals("a", rows1[2].getString(0));
        Assertions.assertEquals(3, rows1[2].getInt(1));

        sparkSession.sql("drop table " + tableName).collect();
    }

    @Test
    void migratePartitionTableFromHiveMetaStoreToPolyCatMetaStore() {
        String tableName = "testmigrate2";

        sparkSession.sql("create table " + tableName + " (c1 string, c2 int , c3 string) using parquet " +
            "partitioned by (c3)").collect();

        sparkSession.sql("insert into " + tableName + " partition(c3='chengdu') values ('a', 1), ('b',4)").collect();

        sparkSession.sql("insert into " + tableName + " partition(c3='shenzhen') values ('a', 2), ('b',5)").collect();

        //migrate
        sparkSession.sql("alter table " + tableName + " set tblproperties('lms_name'='lms')").collect();

        // check metadata in lms
        GetTableRequest getTableRequest = new GetTableRequest(catalogClient.getProjectId(), "lms",
            "default", tableName);
        Table lmsTable = catalogClient.getTable(getTableRequest);
        Assertions.assertNotNull(lmsTable);

        ListFileRequest listFileRequest = new ListFileRequest(catalogClient.getProjectId(), "lms",
            "default", tableName);
        List<String> dataFiles = catalogClient.listFiles(listFileRequest);
        Assertions.assertNotNull(dataFiles);

        Row[] rows = (Row[]) sparkSession.sql("select c1,c2 from " + tableName + " order by c2").collect();
        Assertions.assertEquals(4, rows.length);
        Assertions.assertEquals("a", rows[0].getString(0));
        Assertions.assertEquals(1, rows[0].getInt(1));
        Assertions.assertEquals("a", rows[1].getString(0));
        Assertions.assertEquals(2, rows[1].getInt(1));

        sparkSession.sql("insert into " + tableName + " partition(c3='shenzhen') values ('a', 3), ('b',6)").collect();
        Row[] rows1 = (Row[]) sparkSession.sql("select c1,c2 from " + tableName + " order by c2").collect();
        Assertions.assertEquals(6, rows1.length);
        Assertions.assertEquals("a", rows1[0].getString(0));
        Assertions.assertEquals(1, rows1[0].getInt(1));
        Assertions.assertEquals("a", rows1[1].getString(0));
        Assertions.assertEquals(2, rows1[1].getInt(1));
        Assertions.assertEquals("a", rows1[2].getString(0));
        Assertions.assertEquals(3, rows1[2].getInt(1));

        sparkSession.sql("drop table " + tableName).collect();
    }

    @Test
    void insertIntoAfterMigrateNoPartitionTableWithEmpty() {
        String tableName = "testmigrate3";

        sparkSession.sql("create table " + tableName + " (c1 string, c2 int ) using csv").collect();

        //migrate
        sparkSession.sql("alter table " + tableName +
            " set tblproperties('lms_name'='lms')").collect();

        sparkSession.sql("insert into " + tableName + " values ('a', 1), ('b',4)").collect();

        sparkSession.sql("insert into " + tableName + " values ('a', 2), ('b',3)").collect();

        Row[] rows = (Row[]) sparkSession.sql("select c1,c2 from " + tableName + " order by c2").collect();
        Assertions.assertEquals(4, rows.length);
        Assertions.assertEquals("a", rows[0].getString(0));
        Assertions.assertEquals(1, rows[0].getInt(1));
        Assertions.assertEquals("a", rows[1].getString(0));
        Assertions.assertEquals(2, rows[1].getInt(1));

        sparkSession.sql("drop table " + tableName).collect();
    }

    @Test
    void insertIntoAfterMigratePartitionTableWithEmpty() {
        String tableName = "testmigrate4";

        sparkSession.sql("create table " + tableName + " (c1 string, c2 int , c3 string) using parquet " +
            "partitioned by (c3)").collect();

        //migrate
        sparkSession.sql("alter table " + tableName + " set tblproperties('lms_name'='lms')").collect();

        sparkSession.sql("insert into " + tableName + " partition(c3='chengdu') values ('a', 1), ('b',4)").collect();

        sparkSession.sql("insert into " + tableName + " partition(c3='shenzhen') values ('a', 2), ('b',3)").collect();

        Row[] rows = (Row[]) sparkSession.sql("select c1,c2 from " + tableName + " order by c2").collect();
        Assertions.assertEquals(4, rows.length);
        Assertions.assertEquals("a", rows[0].getString(0));
        Assertions.assertEquals(1, rows[0].getInt(1));
        Assertions.assertEquals("a", rows[1].getString(0));
        Assertions.assertEquals(2, rows[1].getInt(1));

        sparkSession.sql("drop table " + tableName).collect();
    }

    @Test
    void alterTableAfterMigratePartitionTableWithEmpty() {
        String tableName = "testmigrate5";
        String newTableName = "testmigrate6";

        sparkSession.sql("create table " + tableName + " (c1 string, c2 int , c3 string) using parquet " +
            "partitioned by (c3)").collect();

        //migrate
        sparkSession.sql("alter table " + tableName + " set tblproperties('lms_name'='lms')").collect();

        sparkSession.sql("ALTER TABLE " + tableName + " RENAME TO " + newTableName);

        sparkSession.sql("insert into " + newTableName + " partition(c3='chengdu') values ('a', 1), ('b',4)").collect();

        sparkSession.sql("insert into " + newTableName + " partition(c3='shenzhen') values ('a', 2), ('b',3)")
            .collect();

        Row[] rows = (Row[]) sparkSession.sql("select c1,c2 from " + newTableName + " order by c2").collect();
        Assertions.assertEquals(4, rows.length);
        Assertions.assertEquals("a", rows[0].getString(0));
        Assertions.assertEquals(1, rows[0].getInt(1));
        Assertions.assertEquals("a", rows[1].getString(0));
        Assertions.assertEquals(2, rows[1].getInt(1));

        sparkSession.sql("drop table " + newTableName).collect();
    }

    @Test
    void insertIntoPartitionTableWithNoTransaction() {
        String tableName = "tablept1";

        sparkSession.sql("create table " + tableName + " (c1 string, c2 int , c3 string) using csv " +
            "partitioned by (c3) tblproperties('lms_name'='lms')").collect();

        sparkSession.sql("insert into " + tableName + " partition(c3='chengdu') values ('a', 1), ('b',4)").collect();

        ListFileRequest listFileRequest = new ListFileRequest(catalogClient.getProjectId(), "lms",
            "default", tableName);
        List<String> dataFiles = catalogClient.listFiles(listFileRequest);
        Assertions.assertNotNull(dataFiles);

        sparkSession.sql("insert into " + tableName + " partition(c3='chengdu') values ('a', 2), ('b',3)").collect();

        // check metadata in lms
        GetTableRequest getTableRequest = new GetTableRequest(catalogClient.getProjectId(), "lms",
            "default", tableName);
        Table lmsTable = catalogClient.getTable(getTableRequest);
        Assertions.assertNotNull(lmsTable);

        ListFileRequest listFileRequest1 = new ListFileRequest(catalogClient.getProjectId(), "lms",
            "default", tableName);
        List<String> dataFiles1 = catalogClient.listFiles(listFileRequest1);
        Assertions.assertNotNull(dataFiles1);

        Row[] rows = (Row[]) sparkSession.sql("select c1,c2 from " + tableName + " order by c2").collect();
        Assertions.assertEquals(4, rows.length);
        Assertions.assertEquals("a", rows[0].getString(0));
        Assertions.assertEquals(1, rows[0].getInt(1));
        Assertions.assertEquals("a", rows[1].getString(0));
        Assertions.assertEquals(2, rows[1].getInt(1));

        sparkSession.sql("drop table " + tableName).collect();
    }

    @Test
    void DataTypeTest() {
        // test for data type: int/string/date/float/double/date
        // char/varchar are not supported in the table schema since Spark 3.1
        String tableName = "testdatatype";
        // create table by using 'lms_name' in spark
        sparkSession.sql("create table " + tableName + " (c1 int, c2 string, c3 float, c4 double, " +
            "c5 date, c6 TIMESTAMP, c7 DECIMAL(10,2)) " +
            "using parquet tblproperties('lms_name'='lms')").collect();
        // desc table
        Row[] rowsTable = (Row[]) sparkSession.sql("DESC TABLE " + tableName).collect();
        Assertions.assertEquals(7, rowsTable.length);
        Assertions.assertEquals("int", rowsTable[0].getString(1));
        Assertions.assertEquals("string", rowsTable[1].getString(1));
        Assertions.assertEquals("float", rowsTable[2].getString(1));
        Assertions.assertEquals("double", rowsTable[3].getString(1));
        Assertions.assertEquals("date", rowsTable[4].getString(1));
        Assertions.assertEquals("timestamp", rowsTable[5].getString(1));
        Assertions.assertEquals("decimal(10,2)", rowsTable[6].getString(1));
        // insert table
        String part = UUID.randomUUID().toString();
        sparkSession.sql("insert into " + tableName
            + "  select 1, 'a', 3.1415, 3.141592654, to_date('2017-08-09 10:11:12','yyyy-MM-dd HH:mm:ss'),"
            + " to_timestamp('2017-08-09 10:11:12','yyyy-MM-dd HH:mm:ss'), 12.1 ").collect();
        double EPS1 = 1.0E-7;
        double EPS2 = 1.0E-16;
        Row[] rows = (Row[]) sparkSession.sql("select * from " + tableName).collect();
        Assertions.assertEquals(1, rows.length);
        Assertions.assertEquals(1, rows[0].getInt(0));
        Assertions.assertEquals("a", rows[0].getString(1));
        Assertions.assertTrue(Math.abs(rows[0].getFloat(2) - 3.1415) < EPS1);
        Assertions.assertTrue(Math.abs(rows[0].getDouble(3) - 3.141592654) < EPS2);
        SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd");
        Assertions.assertEquals("2017-08-09", dateformat.format(rows[0].getDate(4)));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Assertions.assertEquals("2017-08-09 10:11:12", sdf.format(rows[0].getTimestamp(5)));
        Assertions.assertEquals(12.1, rows[0].getDecimal(6).doubleValue());

        sparkSession.sql("drop table " + tableName).collect();
    }

    @Test
    void decimalDataTypeTest() {
        String tableName = "decimal";

        AnalysisException e = assertThrows(AnalysisException.class,
            () -> sparkSession.sql(
                "create table " + tableName + " (c1 DECIMAL(15,20), c2 DECIMAL(20,15), c3 DECIMAL(10,8)) using csv " +
                    "partitioned by (c3) tblproperties('lms_name'='lms')").collect());
        Assertions.assertEquals(e.getMessage().contains("Decimal scale (20) cannot be greater than precision (15)."),
            true);

        sparkSession.sql(
            "create table " + tableName + " (c1 DECIMAL(10,2), c2 DECIMAL(6,5), c3 DECIMAL(4,1)) using csv " +
                "partitioned by (c3) tblproperties('lms_name'='lms')").collect();

        ArithmeticException e1 = assertThrows(ArithmeticException.class,
            () -> sparkSession.sql("insert into " + tableName + " partition(c3=314.1) values (123456789.12, 1.12)")
                .collect());
        Assertions.assertEquals(
            e1.getMessage().contains("Decimal(expanded,123456789.12,11,2}) cannot be represented as Decimal(10, 2)."),
            true);

        sparkSession.sql("insert into " + tableName +
            " partition(c3=13.14) values (12345678.12, 1.123)").collect();
        sparkSession.sql("insert into " + tableName +
            " partition(c3=14.14) values (12345678.1234, 1.12345)").collect();
        Row[] rows = (Row[]) sparkSession.sql("select * from " + tableName).collect();
        Assertions.assertEquals(2, rows.length);
        Assertions.assertEquals(12345678.12, rows[0].getDecimal(0).doubleValue());
        Assertions.assertEquals(1.123, rows[0].getDecimal(1).doubleValue());
        Assertions.assertEquals(13.1, rows[0].getDecimal(2).doubleValue());
        Assertions.assertEquals(12345678.12, rows[1].getDecimal(0).doubleValue());
        Assertions.assertEquals(1.12345, rows[1].getDecimal(1).doubleValue());
        Assertions.assertEquals(14.1, rows[1].getDecimal(2).doubleValue());
    }

    @Test
    void selectFromPartitionTableByStringPartitionColumnTest() {
        String tableName = "prtStringTable";

        sparkSession.sql("create table " + tableName + " (c1 string, c2 int , c3 string) using csv " +
            "partitioned by (c3) tblproperties('lms_name'='lms')").collect();

        sparkSession.sql("insert into " + tableName +
            " partition(c3='chengdu') values ('a', 1), ('b',4)").collect();

        sparkSession.sql("insert into " + tableName +
            " partition(c3='shenzhen') values ('a', 1), ('b',4)").collect();

        sparkSession.sql("insert into " + tableName +
            " partition(c3='beijing') values ('a', 1), ('b',4)").collect();

        Row[] rows = (Row[]) sparkSession.sql("select * from " + tableName + " where c3='chengdu' order by c2")
            .collect();
        Assertions.assertEquals(2, rows.length);
        Assertions.assertEquals("[a,1,chengdu]", rows[0].toString());
        Assertions.assertEquals("[b,4,chengdu]", rows[1].toString());

        sparkSession.sql("drop table " + tableName).collect();
    }

    @Test
    void selectFromPartitionTableByIntPartitionColumnTest() {
        String tableName = "prtIntTable";

        sparkSession.sql("create table " + tableName + " (c1 string, c2 int , c3 int) using csv " +
            "partitioned by (c3) tblproperties('lms_name'='lms')").collect();

        sparkSession.sql("insert into " + tableName +
            " partition(c3=1) values ('a', 1), ('b',4)").collect();

        sparkSession.sql("insert into " + tableName +
            " partition(c3=2) values ('a', 1), ('b',4)").collect();

        sparkSession.sql("insert into " + tableName +
            " partition(c3=3) values ('a', 1), ('b',4)").collect();

        Row[] rows = (Row[]) sparkSession.sql("select * from " + tableName + " where c3=1 order by c2").collect();
        Assertions.assertEquals(2, rows.length);
        Assertions.assertEquals("[a,1,1]", rows[0].toString());
        Assertions.assertEquals("[b,4,1]", rows[1].toString());
        sparkSession.sql("drop table " + tableName).collect();
    }

    @Test
    void selectFromPartitionTableByMixPartitionColumnTest() {
        String tableName = "prtIntTable";

        sparkSession.sql("create table " + tableName + " (c1 string, c2 int , c3 int, c4 string) using csv " +
            "partitioned by (c3,c4) tblproperties('lms_name'='lms')").collect();

        sparkSession.sql("insert into " + tableName +
            " partition(c3=1,c4='chengdu') values ('a', 1), ('b',2)").collect();

        sparkSession.sql("insert into " + tableName +
            " partition(c3=1,c4='shenzhen') values ('a', 3), ('b',4)").collect();

        sparkSession.sql("insert into " + tableName +
            " partition(c3=2,c4='beijing') values ('a', 5), ('b',6)").collect();

        Row[] rows = (Row[]) sparkSession.sql("select * from " + tableName +
            " where c3=1 and c4='chengdu' order by c2").collect();
        Assertions.assertEquals(2, rows.length);
        Assertions.assertEquals("[a,1,1,chengdu]", rows[0].toString());
        Assertions.assertEquals("[b,2,1,chengdu]", rows[1].toString());

        Row[] rows1 = (Row[]) sparkSession.sql("select * from " + tableName +
            " where c3>1 order by c2").collect();
        Assertions.assertEquals(2, rows1.length);
        Assertions.assertEquals("[a,5,2,beijing]", rows1[0].toString());
        Assertions.assertEquals("[b,6,2,beijing]", rows1[1].toString());

        Row[] rows2 = (Row[]) sparkSession.sql("select * from " + tableName +
            " where c3<2 and c4='shenzhen' order by c2").collect();
        Assertions.assertEquals(2, rows2.length);
        Assertions.assertEquals("[a,3,1,shenzhen]", rows2[0].toString());
        Assertions.assertEquals("[b,4,1,shenzhen]", rows2[1].toString());

        Row[] rows3 = (Row[]) sparkSession.sql("select * from " + tableName +
            " where c4='chengdu' or c4='shenzhen' order by c2").collect();
        Assertions.assertEquals(4, rows3.length);
        Assertions.assertEquals("[a,1,1,chengdu]", rows3[0].toString());
        Assertions.assertEquals("[b,2,1,chengdu]", rows3[1].toString());

        sparkSession.sql("drop table " + tableName).collect();
    }

    private void preparePartitionTableAndData(String tableName) {
        sparkSession.sql("create table " + tableName + " (c1 string, c2 int , c3 int, c4 string) using csv " +
            "partitioned by (c3,c4) tblproperties('lms_name'='lms')").collect();

        sparkSession.sql("insert into " + tableName +
            " partition(c3=1,c4='chengdu') values ('a', 1), ('b',2)").collect();

        sparkSession.sql("insert into " + tableName +
            " partition(c3=2,c4='shenzhen') values ('a', 3), ('b',4)").collect();

        sparkSession.sql("insert into " + tableName +
            " partition(c3=3,c4='beijing') values ('a', 5), ('b',6)").collect();
    }

    @Test
    void getPartitionByFilterApiTest() throws MetaException, NoSuchObjectException {
        String tableName = "apitable";

        preparePartitionTableAndData(tableName);

        List<Partition> partitions = bridgeStore.getPartitionsByFilter("default", tableName, "c3=1", (short) 0);
        Assertions.assertEquals(1, partitions.size());
        assertTrue(partitions.get(0).getSd().getLocation().contains("c3=1/c4=chengdu"));

        List<Partition> partitions1 = bridgeStore.getPartitionsByFilter("default", tableName, "c3>2", (short) 0);
        Assertions.assertEquals(1, partitions1.size());
        assertTrue(partitions1.get(0).getSd().getLocation().contains("c3=3/c4=beijing"));

        List<Partition> partitions2 = bridgeStore.getPartitionsByFilter("default", tableName, "c4='shenzhen'",
            (short) 0);
        Assertions.assertEquals(1, partitions2.size());
        assertTrue(partitions2.get(0).getSd().getLocation().contains("c3=2/c4=shenzhen"));

        sparkSession.sql("drop table " + tableName).collect();
    }

    @Test
    void getPartitionsByNamesApiTest() throws NoSuchObjectException, MetaException {
        String tableName = "apitable1";

        preparePartitionTableAndData(tableName);

        List<String> partNames = new ArrayList<>();
        partNames.add("c3=1/c4=chengdu");
        partNames.add("c3=3/c4=beijing");
        List<Partition> partitions = bridgeStore.getPartitionsByNames("default", tableName, partNames);
        Assertions.assertEquals(2, partitions.size());
        assertTrue(partitions.get(0).getSd().getLocation().contains("c3=1/c4=chengdu"));
        assertTrue(partitions.get(1).getSd().getLocation().contains("c3=3/c4=beijing"));

        sparkSession.sql("drop table " + tableName).collect();
    }

    @Test
    void getPartitionWithAuthApiTest() throws NoSuchObjectException, MetaException, InvalidObjectException {
        String tableName = "apitable2";

        preparePartitionTableAndData(tableName);

        List<String> partValues = new ArrayList<>();
        partValues.add("1");
        partValues.add("chengdu");
        Partition partition = bridgeStore.getPartitionWithAuth("default", tableName, partValues,
            null, null);
        Assertions.assertNotNull(partition);
        assertTrue(partition.getSd().getLocation().contains("c3=1/c4=chengdu"));

        List<String> partValues1 = new ArrayList<>();
        partValues1.add("chengdu");
        partValues1.add("1");
        Partition partition1 = bridgeStore.getPartitionWithAuth("default", tableName, partValues1,
            null, null);
        Assertions.assertNull(partition1);

        sparkSession.sql("drop table " + tableName).collect();
    }

    @Test
    void listPartitionNamesApiTest() throws MetaException {
        String tableName = "apitable3";

        preparePartitionTableAndData(tableName);

        List<String> partitionNames = bridgeStore.listPartitionNames("default", tableName, (short) 0);
        Assertions.assertEquals(3, partitionNames.size());
        assertEquals(partitionNames.get(0), "c3=1/c4=chengdu");
        assertEquals(partitionNames.get(1), "c3=2/c4=shenzhen");
        assertEquals(partitionNames.get(2), "c3=3/c4=beijing");

        sparkSession.sql("drop table " + tableName).collect();
    }

    @Test
    void listPartitionNamesByFilterApiTest() throws MetaException {
        String tableName = "apitable4";

        preparePartitionTableAndData(tableName);

        List<String> partitionNames = bridgeStore.listPartitionNamesByFilter("default", tableName,
            "c3=1", (short) 0);
        Assertions.assertEquals(1, partitionNames.size());
        assertEquals(partitionNames.get(0), "c3=1/c4=chengdu");

        List<String> partitionNames1 = bridgeStore.listPartitionNamesByFilter("default", tableName,
            "c3>2", (short) 0);
        Assertions.assertEquals(1, partitionNames1.size());
        assertEquals(partitionNames1.get(0), "c3=3/c4=beijing");

        sparkSession.sql("drop table " + tableName).collect();
    }

    @Test
    void listPartitionNamesPsApiTest() throws NoSuchObjectException, MetaException {
        String tableName = "apitable5";

        preparePartitionTableAndData(tableName);

        List<String> partValues = new ArrayList<>();
        partValues.add("1");
        partValues.add("chengdu");
        List<String> partitionNames = bridgeStore.listPartitionNamesPs("default", tableName,
            partValues, (short) 0);
        Assertions.assertEquals(1, partitionNames.size());
        assertEquals(partitionNames.get(0), "c3=1/c4=chengdu");

        sparkSession.sql("drop table " + tableName).collect();
    }

    @Test
    void listPartitionsPsWithAuthApiTest() throws InvalidObjectException, NoSuchObjectException, MetaException {
        String tableName = "apitable6";

        preparePartitionTableAndData(tableName);

        List<String> partValues = new ArrayList<>();
        partValues.add("");
        partValues.add("chengdu");
        List<Partition> partitions = bridgeStore.listPartitionsPsWithAuth("default", tableName,
            partValues, (short) 0, null, null);
        Assertions.assertEquals(1, partitions.size());
        assertTrue(partitions.get(0).getSd().getLocation().contains("c3=1/c4=chengdu"));

        List<String> partValues1 = new ArrayList<>();
        partValues1.add("3");
        List<Partition> partitions1 = bridgeStore.listPartitionsPsWithAuth("default", tableName,
            partValues1, (short) 0, null, null);
        Assertions.assertEquals(1, partitions1.size());
        assertTrue(partitions1.get(0).getSd().getLocation().contains("c3=3/c4=beijing"));

        sparkSession.sql("drop table " + tableName).collect();
    }

    @Test
    void dropThePartitionOfPartitionTableTest() {
        String tableName = "testTbl";

        sparkSession.sql("create table " + tableName + " (c1 string, c2 int , c3 string) using csv " +
            "partitioned by (c3) tblproperties('lms_name'='lms')").collect();

        sparkSession.sql("insert into " + tableName +
            " partition(c3='chengdu') values ('a', 1), ('b',4)").collect();

        sparkSession.sql("insert into " + tableName +
            " partition(c3='shenzhen') values ('a', 1), ('b',4)").collect();

        sparkSession.sql("insert into " + tableName +
            " partition(c3='beijing') values ('a', 1), ('b',4)").collect();

        sparkSession.sql("alter table " + tableName + " drop partition(c3='chengdu')");

        Row[] rows = (Row[]) sparkSession.sql("show partitions " + tableName).collect();
        Assertions.assertEquals(2, rows.length);
        Assertions.assertEquals("[c3=beijing]", rows[0].toString());
        Assertions.assertEquals("[c3=shenzhen]", rows[1].toString());

        Row[] rows3 = (Row[]) sparkSession.sql("select * from " + tableName + " order by c2").collect();
        Assertions.assertEquals(4, rows3.length);
        Assertions.assertEquals("a", rows3[0].getString(0));
        Assertions.assertEquals(1, rows3[0].getInt(1));

        sparkSession.sql("insert into " + tableName +
            " partition(c3='chengdu') values ('a', 1), ('b',4)").collect();

        Row[] rows1 = (Row[]) sparkSession.sql("show partitions " + tableName).collect();
        Assertions.assertEquals(3, rows1.length);
        Assertions.assertEquals("[c3=beijing]", rows1[0].toString());
        Assertions.assertEquals("[c3=chengdu]", rows1[1].toString());
        Assertions.assertEquals("[c3=shenzhen]", rows1[2].toString());

        Row[] rows2 = (Row[]) sparkSession.sql("select * from " + tableName + " order by c2").collect();
        Assertions.assertEquals(6, rows2.length);
        Assertions.assertEquals("a", rows2[0].getString(0));
        Assertions.assertEquals(1, rows2[0].getInt(1));

        sparkSession.sql("alter table " + tableName + " drop partition(c3='chengdu')");
        sparkSession.sql("alter table " + tableName + " drop partition(c3='shenzhen')");
        sparkSession.sql("alter table " + tableName + " drop partition(c3='beijing')");
        Row[] rows4 = (Row[]) sparkSession.sql("show partitions " + tableName).collect();
        Assertions.assertEquals(0, rows4.length);

        sparkSession.sql("drop table " + tableName).collect();
    }

    @Test
    void dropPartitionTableTest() {
        String tableName = "prtTbl1";

        sparkSession.sql("create table " + tableName + " (c1 string, c2 int , c3 string) using csv " +
            "partitioned by (c3) tblproperties('lms_name'='lms')").collect();

        sparkSession.sql("insert into " + tableName +
            " partition(c3='chengdu') values ('a', 1), ('b',4)").collect();

        Row[] rows = (Row[]) sparkSession.sql("select c1,c2 from " + tableName + " order by c2").collect();
        Assertions.assertEquals(2, rows.length);
        Assertions.assertEquals("a", rows[0].getString(0));
        Assertions.assertEquals(1, rows[0].getInt(1));

        sparkSession.sql("drop table " + tableName).collect();
        AnalysisException e = assertThrows(AnalysisException.class,
            () -> sparkSession.sql("desc table " + tableName).collect());
        if (SparkVersionUtil.isSpark30()) {
            assertEquals(String.format("Table or view not found: %s", tableName),
                e.getMessage().split(";")[0]);
        } else {
            assertEquals(String.format("Table or view not found for 'DESCRIBE TABLE': %s", tableName),
                e.getMessage().split(";")[0]);
        }
    }

    @Test
    void insertIntoPartitionTable() {
        String tableName = "prtTbl2";

        sparkSession.sql("create table " + tableName + " (c1 string, c2 int , c3 string) using csv " +
            "partitioned by (c3) tblproperties('lms_name'='lms')").collect();

        sparkSession.sql("insert into " + tableName + " values ('a', 1, 'chengdu'), ('b', 4, 'shenzhen')").collect();

        Row[] rows = (Row[]) sparkSession.sql("select c1,c2,c3 from " + tableName + " order by c2").collect();
        Assertions.assertEquals(2, rows.length);
        Assertions.assertEquals("a", rows[0].getString(0));
        Assertions.assertEquals(1, rows[0].getInt(1));
        Assertions.assertEquals("chengdu", rows[0].getString(2));
        Assertions.assertEquals("b", rows[1].getString(0));
        Assertions.assertEquals(4, rows[1].getInt(1));
        Assertions.assertEquals("shenzhen", rows[1].getString(2));

        sparkSession.sql("drop table " + tableName).collect();
    }

    @Test
    void createBranchAfterMigrateTable() {
        String tableName = "testmigrate";

        sparkSession.sql("create table " + tableName + " (c1 string, c2 int , c3 string) using parquet " +
            "partitioned by (c3)").collect();

        sparkSession.sql("insert into " + tableName + " partition(c3='chengdu') values ('a', 1), ('b',4)").collect();

        sparkSession.sql("insert into " + tableName + " partition(c3='shenzhen') values ('a', 2), ('b',5)").collect();

        //migrate
        sparkSession.sql("alter table " + tableName + " set tblproperties('lms_name'='lms')").collect();

        // create branch from lms catalog
        CreateBranchRequest createBranchRequest = new CreateBranchRequest();
        createBranchRequest.setCatalogName("lms");
        createBranchRequest.setProjectId(catalogClient.getProjectId());
        createBranchRequest.setUserId(userName);
        createBranchRequest.setBranchName("branch2");
        catalogClient.createBranch(createBranchRequest);

        // check metadata in lms
        GetTableRequest branch1TableRequest = new GetTableRequest(catalogClient.getProjectId(),
            "branch2", "default", tableName);
        Table branch1Table = catalogClient.getTable(branch1TableRequest);
        Assertions.assertNotNull(branch1Table);
        Assertions.assertEquals(tableName, branch1Table.getTableName());
        Assertions.assertEquals("default", branch1Table.getDatabaseName());

        // checkout to branch1
        sparkSession.sql("alter table " + tableName + " set tblproperties('lms_name'='branch2')").collect();

        Row[] rows = (Row[]) sparkSession.sql("select c1,c2 from " + tableName + " order by c2").collect();
        Assertions.assertEquals(4, rows.length);
        Assertions.assertEquals("a", rows[0].getString(0));
        Assertions.assertEquals(1, rows[0].getInt(1));
        Assertions.assertEquals("a", rows[1].getString(0));
        Assertions.assertEquals(2, rows[1].getInt(1));
    }

    @Test
    void externalTableTest() {
        String tableName = "externalTable";
        String externalRoot = "target/external_dir";
        File csvExternalFolder = new File(externalRoot);
        deleteFolder(csvExternalFolder);
        csvExternalFolder.mkdir();

        sparkSession.sql("create table " + tableName + " (c1 string, c2 int , c3 string) using csv " +
                String.format("tblproperties('lms_name'='lms') location '%s'", csvExternalFolder.getAbsolutePath()))
            .collect();

        sparkSession.sql("insert into " + tableName + " values ('a', 1, 'chengdu'), ('b',4, 'chengdu')").collect();

        Row[] rows = (Row[]) sparkSession.sql("select c1,c2,c3 from " + tableName + " order by c2").collect();
        Assertions.assertEquals(2, rows.length);
        Assertions.assertEquals("[a,1,chengdu]", rows[0].toString());
        Assertions.assertEquals("[b,4,chengdu]", rows[1].toString());

        sparkSession.sql("drop table " + tableName).collect();

        String tableName1 = "externalTable1";
        sparkSession.sql("create table " + tableName1 + " (c1 string, c2 int , c3 string) using csv " +
                String.format("tblproperties('lms_name'='lms') location '%s'", csvExternalFolder.getAbsolutePath()))
            .collect();

        Row[] rows1 = (Row[]) sparkSession.sql("select c1,c2,c3 from " + tableName1 + " order by c2").collect();
        Assertions.assertEquals(2, rows1.length);
        Assertions.assertEquals("[a,1,chengdu]", rows1[0].toString());
        Assertions.assertEquals("[b,4,chengdu]", rows1[1].toString());

        sparkSession.sql("insert into " + tableName1 + " values ('a', 2, 'shenzhen'), ('b',3, 'shenzhen')").collect();
        Row[] rows2 = (Row[]) sparkSession.sql("select c1,c2,c3 from " + tableName1 + " order by c2").collect();
        Assertions.assertEquals(4, rows2.length);
        Assertions.assertEquals("[a,1,chengdu]", rows2[0].toString());
        Assertions.assertEquals("[a,2,shenzhen]", rows2[1].toString());
        Assertions.assertEquals("[b,3,shenzhen]", rows2[2].toString());
        Assertions.assertEquals("[b,4,chengdu]", rows2[3].toString());

        sparkSession.sql("drop table " + tableName1).collect();
        deleteFolder(csvExternalFolder);
    }

    @Test
    void externalTableWithExternalTest() {
        String tableName = "externalTable1";
        String externalRoot = "target/external_dir";
        File csvExternalFolder = new File(externalRoot);
        deleteFolder(csvExternalFolder);
        csvExternalFolder.mkdir();

        sparkSession.sql("create external table " + tableName + " (c1 string, c2 int , c3 string) " +
                String.format("tblproperties('lms_name'='lms') location '%s'", csvExternalFolder.getAbsolutePath()))
            .collect();

        sparkSession.sql("insert into " + tableName + " values ('a', 1, 'chengdu'), ('b',4, 'chengdu')").collect();

        Row[] rows = (Row[]) sparkSession.sql("select c1,c2,c3 from " + tableName + " order by c2").collect();
        Assertions.assertEquals(2, rows.length);
        Assertions.assertEquals("[a,1,chengdu]", rows[0].toString());
        Assertions.assertEquals("[b,4,chengdu]", rows[1].toString());

        sparkSession.sql("drop table " + tableName).collect();
        deleteFolder(csvExternalFolder);
    }

    private void deleteFolder(File folder) {
        if (!folder.exists()) {
            return;
        }
        File[] files = folder.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteFolder(file);
                } else {
                    file.delete();
                }
            }
        }
        folder.delete();
    }

    @Test
    void csvTableSortTest() {
        String tableName = "textTable";
        sparkSession.sql("create table " + tableName + " (c1 string, c2 int, c3 string) USING CSV " +
                "partitioned by (c3) clustered by (c2) sorted by (c2 ASC) into 1 buckets  tblproperties('lms_name'='lms')")
            .collect();

        sparkSession.sql("insert into " + tableName + " partition(c3='chengdu') values ('a', 1), ('b',4)").collect();

        Row[] rows = (Row[]) sparkSession.sql("select * from " + tableName + " cluster by (c2)").collect();
        Assertions.assertEquals(2, rows.length);
        Assertions.assertEquals("[a,1,chengdu]", rows[0].toString());
        Assertions.assertEquals("[b,4,chengdu]", rows[1].toString());
        sparkSession.sql("drop table " + tableName).collect();
    }

    @Test
    void alterTablePartitionTest() {
        String tableName = "partitionTable";

        sparkSession.sql("create table " + tableName + " (c1 string, c2 int)" +
            " partitioned by (c3 string) tblproperties('lms_name'='lms')").collect();

        sparkSession.sql("insert into " + tableName + " partition(c3='chengdu') values ('a', 1), ('b',4)").collect();

        sparkSession.sql("insert into " + tableName + " partition(c3='shenzhen') values ('a', 2), ('b',5)").collect();

        Row[] rows = (Row[]) sparkSession.sql("show partitions " + tableName).collect();
        Assertions.assertEquals(2, rows.length);
        Assertions.assertEquals("[c3=chengdu]", rows[0].toString());
        Assertions.assertEquals("[c3=shenzhen]", rows[1].toString());

        sparkSession.sql("alter table " + tableName +
            " partition (c3='chengdu') rename to partition (c3='beijing')").collect();
        Row[] rows1 = (Row[]) sparkSession.sql("show partitions " + tableName).collect();
        Assertions.assertEquals(2, rows1.length);
        Assertions.assertEquals("[c3=beijing]", rows1[0].toString());
        Assertions.assertEquals("[c3=shenzhen]", rows1[1].toString());

        sparkSession.sql("drop table " + tableName).collect();
    }
}
