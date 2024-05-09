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
package io.polycat.catalog.spark;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import io.polycat.catalog.client.util.Constants;
import io.polycat.catalog.common.Logger;

import io.polycat.catalog.spark.SparkCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Example {
    static private final Logger logger = Logger.getLogger(Example.class);

    // static String defaultLocation = "s3://xxxxxxxx.tmp.ap-southeast-1/Default/user1/";
     static String defaultLocation = "/tmp/hive/default/";
    public static void main(String[] args) {
        List<String> fileOutputAlgorithmVersions = Arrays.asList("1", "2");
        List<String> partitionOverwriteModes = Arrays.asList("STATIC", "DYNAMIC");

        for (int i = 0; i < fileOutputAlgorithmVersions.size(); i++) {
            for (int j = 0; j < partitionOverwriteModes.size(); j++) {
                executeSpark(fileOutputAlgorithmVersions.get(i), partitionOverwriteModes.get(j));
            }
        }


    }

    private static void assertEqual(boolean b) {
        if (!b) {
            throw new AssertionError(b);
        }
    }

    private static void executeSpark(String fileOutputAlgorithmVersion, String partitionOverwriteMode) {
        logger.info("============================================= Start fileOutputAlgorithmVersion={} partitionOverwriteMode={} =============================================", fileOutputAlgorithmVersion, partitionOverwriteMode);
        // String sparkCatalogName = "test_default_catalog1";
        String sparkCatalogName = "xxxxxxxx_ue1";
        String icebergCatalogName = "iceberg";
        logger.info("Example starts");
        logger.info("io.polycat.catalog.spark.SparkCatalog.class: {}", SparkCatalog.class.getCanonicalName());
        SparkSession session = SparkSession.builder()
            .appName("test")
            .master("local[2]")
            .enableHiveSupport()
//                               .config("spark.sql.catalogImplementation", "in-memory")
            .config("spark.sql.sources.partitionOverwriteMode", partitionOverwriteMode)
            .config("spark.sql.warehouse.dir", "file:/tmp/hive/warehouse")
            .config("catalog.sql.warehouse.dir", "file:/tmp/hive/warehouse")
            .config("spark.sql.defaultCatalog", sparkCatalogName)
//                .config("spark.sql.catalog." + sparkCatalogName, "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog." + sparkCatalogName, SparkCatalog.class.getCanonicalName())
            .config("spark.sql.catalog." + sparkCatalogName + ".catalog-impl", "PolyCatCatalog")
            // .config("spark.sql.catalog." + sparkCatalogName + ".type", "hive")
//                 .config("spark.sql.catalog." + sparkCatalogName + ".uri", "thrift://hms-ue1.uxxxxxxxx.org:9083")
            .config("spark.hadoop.hive.metastore.uris", "thrift://hms-ue1.uxxxxxxxx.org:9083")
            .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
            .config("spark.hadoop." + Constants.POLYCAT_CLIENT_HOST, "k8s-bdp-catalogm-dcc4da73d0-8e4964afc83f6e35.elb.ap-southeast-1.amazonaws.com")
            .config("spark.hadoop." + Constants.POLYCAT_CLIENT_PORT, "80")
            .config("spark.hadoop." + Constants.POLYCAT_CLIENT_PROJECT_ID, "xxxxxxxx")
            .config("spark.hadoop." + Constants.POLYCAT_CLIENT_USERNAME, "user1")
            .config("spark.sql.extensions", "io.polycat.catalog.spark.extension.PolyCatSparkTableExtensions")
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", fileOutputAlgorithmVersion)
            .config("spark.sql.catalog." + icebergCatalogName, "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog." + icebergCatalogName + ".uri", "thrift://hms-ue1.uxxxxxxxx.org:9083")
            .config("spark.sql.catalog." + icebergCatalogName + ".type", "hive")
            .config("spark.sql.catalog." + icebergCatalogName + ".cache-enabled", "false")
            .getOrCreate();


        /*executePrintSql(session, "CREATE TABLE IF NOT EXISTS test_db2.part_t4_textfile (id string COMMENT 'ID' , name string COMMENT '名称', dt string COMMENT '分区时间yyyy-MM-dd', hour string COMMENT '分区时间yyyy-MM-dd') using text COMMENT '测试表' PARTITIONED BY (dt, hour) ");
        executePrintSql(session, "insert overwrite test_db2.part_t4_textfile partition (dt='1111', hour) select 'amy', 32, '20210140' union all select 'amy', 32, '20210141' union all select 'amy', 32, '20210142'");

        executePrintSql(session, "select * from test_db2.part_t4_textfile ").show(1000, false);
*/


       /* // 多分区
        executePrintSql(session, "CREATE TABLE IF NOT EXISTS test_db2.part_t3 (id string COMMENT 'ID' , name string COMMENT '名称', dt string COMMENT '分区时间yyyy-MM-dd', hour string COMMENT '分区时间yyyy-MM-dd') USING parquet COMMENT '测试表' PARTITIONED BY (dt, hour) ");

        executePrintSql(session, "insert overwrite test_db2.part_t3 partition (dt='1111', hour) select 'amy', 32, '20210140' union all select 'amy', 32, '20210141' union all select 'amy', 32, '20210142'");
        executePrintSql(session, "select * from test_db2.part_t3 where dt='20210140'").show(1000, false);
        executePrintSql(session, "select * from test_db2.part_t3 ").show(1000, false);

        executePrintSql(session, "insert overwrite test_db2.part_t3 partition (dt, hour) select 'amy', 32, '1111', '20210140' union all select 'amy', 32, '1112', '20210140' union all select 'amy', 33, '1112', '20210146' union all select 'amy', 33, '1112', '20210145' union all select 'amy', 33, '1112', '20210142' union all select 'amy', 33, '1112', '20210143' union all select 'amy', 33, '1112', '20210144'");
        executePrintSql(session, "select * from test_db2.part_t3 where dt='1112'").show(1000, false);
        executePrintSql(session, "select * from test_db2.part_t3 ").show(1000, false);*/

         /*executePrintSql(session, "insert overwrite test_db2.part_t3 partition (dt, hour) select 'amy', 32, '1111', '20210140' union all select 'amy', 32, '1112', '20210140' union all select 'amy', 33, '1112', '20210146' union all select 'amy', 33, '1112', '20210145' union all select 'amy', 33, '1112', '20210142' union all select 'amy', 33, '1112', '20210143' union all select 'amy', 33, '1112', '20210144'");

        executePrintSql(session, "insert overwrite test_db2.part_t3 partition (dt='1113', hour) select id,name,hour from test_db2.part_t3 where dt='1112'").show(1000, false);
        executePrintSql(session, "select * from test_db2.part_t3 where dt='1113'").show(1000, false);*/
        /*
        // static partition test
        executePrintSql(session, "insert overwrite test_db2.part_t3 partition (dt='1114', hour='1111') select 'amy', 32").show(1000, false);
        executePrintSql(session, "select * from test_db2.part_t3 where dt='1114'").show(1000, false);*/

        // executePrintSql(session, "insert overwrite test_db2.part_t3 partition (dt='1114', hour) select id,name,hour from test_db2.part_t3 where dt='1112' or dt='1113'").show(1000, false);
        // executePrintSql(session, "insert overwrite test_db2.part_t3 partition (dt='1115', hour='1111') select * from (select 'amy', 32) t1 location '/tmp/hive/warehouse/test_db2.db/direct/part_t3/dt=1115/hour=1111';").show(1000, false);

        //executePrintSql(session, "select * from test_db2.part_t3 where dt='1115'").show(1000, false);

        //PolyCatFileIndex test
        //session.sql("alter table test_db2.part_t3 add partition(dt='1114', hour='01') location '/tmp/hive/warehouse/test_db2.db/part_t2/dt=1112/hour=20210140'");

        //executePrintSql(session, "select * from test_db2.part_t3 ").show(1000, false);
        // insert overwrite/insert into 不支持后面追加 location

        // executePrintSql(session, "select * from test_db2.part_t3 ").show(1000, false);
        // executePrintSql(session, "select * from test_db2.part_t3 where dt='2222' ").show(1000, false);
         //executePrintSql(session, "select * from spark_catalog.test_db2.part_t3 where dt='1114' ").show(1000, false);
//         executePrintSql(session, "select t1.* from (select * from test_db2.part_t3 where dt='1114' and name='32') t1 join test_db2.part_t3 t2 on t1.name=t2.name where concat(t2.dt, t2.hour) = '11131111' ").show(1000, false);
        //executePrintSql(session, "select t1.* from (select * from spark_catalog.test_db2.part_t3 where dt='1114' and name='32') t1 join spark_catalog.test_db2.part_t3 t2 on t1.name=t2.name where concat(t2.dt, t2.hour) = '11131111' ").show(1000, false);

        /*executePrintSql(session, "insert overwrite test_db2.part_t3 partition (dt='1113', hour) select id,name,hour from test_db2.part_t3 where dt='1111'").show(1000, false);
        executePrintSql(session, "select * from test_db2.part_t3 where dt='1113'").show(1000, false);
        executePrintSql(session, "insert overwrite test_db2.part_t3 partition (dt='1114', hour) select id,name,hour from test_db2.part_t3 where dt='1111' or dt='1113'").show(1000, false);
        executePrintSql(session, "select * from test_db2.part_t3 where dt='1114'").show(1000, false);*/
        // executePrintSql(session, "select * from test_db2.part_t3 ").show(1000, false);
//         System.exit(0);



        // List<String> tableFormats = Arrays.asList("PARQUET", "CSV", "ORC", "JSON", "AVRO");
        List<String> tableFormats = Arrays.asList("PARQUET", "CSV", "ORC", "JSON");

        String tableName = null;
        String tableFormat = null;
        for (int i = 0; i < tableFormats.size(); i++) {
            tableFormat = tableFormats.get(i).toLowerCase(Locale.ROOT);
            tableName = "default.test_tbl_" + tableFormat;
            // testExecuteDelegateFirst(session, "test_db2.part_doesnotexists_path", "csv");
            testExecute(session, tableName, tableFormat, true);
            testExecute(session, tableName, tableFormat, false);
        }

        testExecuteIceberg(session, "default.test_tbl_iceberg_polycat", "iceberg", true);
        testExecuteIceberg(session, "default.test_tbl_iceberg_iceberg", "iceberg", false);

        /*
        // function
        session.sql("CREATE TEMPORARY FUNCTION weighted_average3 AS 'com.udf.WeightedAverage' USING JAR '/Download/spark-udf-1.0-SNAPSHOT.jar'");
        session.sql("select weighted_average3(id, 2) from " + testTable).show(1000, false);
*/

        /*session.sql("SHOW functions").show(1000, false);
        session.sql("SHOW CURRENT NAMESPACE").show(1000, false);
        session.sql("SHOW databases").show(1000, false);
        session.sql("show tables").show(1000, false);
        session.sql("create database if not exists test_db").show(1000, false);
        session.sql("show tables from test_db").show(1000, false);
        session.sql("create database if not exists test_db2").show(1000, false);
        session.sql("CREATE TABLE IF NOT EXISTS test_db2.tbl_02 (id string COMMENT 'ID' , name string COMMENT '名称', dt string COMMENT '分区时间yyyy-MM-dd') USING parquet COMMENT '测试表' ");
        session.sql("CREATE TABLE IF NOT EXISTS test_db2.part_t1 (id string COMMENT 'ID' , name string COMMENT '名称', dt string COMMENT '分区时间yyyy-MM-dd') USING parquet COMMENT '测试表' PARTITIONED BY (dt) ");
        session.sql("set abc.aaa=aaa");
        session.sql("CREATE TABLE IF NOT EXISTS test_db2.part_t2 (id string COMMENT 'ID' , name string COMMENT '名称', dt string COMMENT '分区时间yyyy-MM-dd', hour string) USING parquet COMMENT '测试表' PARTITIONED BY (dt,hour) ");

        session.sql("select * from test_db2.part_t2");
        System.out.println("===========================================");
        session.sql("show partitions test_db2.part_t1").show(1000, false);
        session.sql("show partitions test_db2.part_t1");
        System.out.println("===========================================");

        session.sql("alter table test_db2.part_t1 add if not exists partition (dt='20210131')");
        session.sql("alter table test_db2.part_t1 add if not exists partition (dt='202101311')");
        // table overwrite
        session.sql("alter table test_db2.part_t1 add if not exists partition (dt='20210201')");*/

        /*session.sql("insert overwrite test_db2.part_t1 partition (dt='20210139') select 'amy', 32");

        session.sql("select * from test_db2.part_t1 where dt='20210139'").show(1000, false);

        session.sql("create table if not exists test_db2.test_t1 (name string, age int)");
        session.sql("insert overwrite test_db2.test_t1 values ('amy', 32), ('bob', 31)");
        session.sql("select * from test_db2.test_t1 ").show(1000, false);

        session.sql("insert into test_db2.test_t1 values ('amy', 32), ('bob', 31)");
        session.sql("select * from test_db2.test_t1 ").show(1000, false);
        session.sql("create database if not exists test_db2").show(1000, false);*/

        /*// 单分区
        session.sql("DROP TABLE IF EXISTS test_db2.part_t1 ");
        session.sql("CREATE TABLE IF NOT EXISTS test_db2.part_t1 (id string COMMENT 'ID' , name string COMMENT '名称', dt string COMMENT '分区时间yyyy-MM-dd') USING csv COMMENT '测试表' PARTITIONED BY (dt) ");

        session.sql("insert overwrite test_db2.part_t1 partition (dt) select 'amy', 32, '20210140' union all select 'amy', 32, '20210141' union all select 'amy', 32, '20210142'");
        session.sql("select * from test_db2.part_t1 where dt='20210140'").show(1000, false);
        session.sql("select * from test_db2.part_t1 ").show(1000, false);

        session.sql("insert overwrite test_db2.part_t1 partition (dt) select 'amy', 32, '20210140'");
        session.sql("select * from test_db2.part_t1 where dt='20210140'").show(1000, false);
        session.sql("select * from test_db2.part_t1 ").show(1000, false);
        // 多分区
        session.sql("CREATE TABLE IF NOT EXISTS test_db2.part_t2 (id string COMMENT 'ID' , name string COMMENT '名称', dt string COMMENT '分区时间yyyy-MM-dd', hour string COMMENT '分区时间yyyy-MM-dd') USING parquet COMMENT '测试表' PARTITIONED BY (dt, hour) ");

        session.sql("insert overwrite test_db2.part_t2 partition (dt='1111', hour) select 'amy', 32, '20210140' union all select 'amy', 32, '20210141' union all select 'amy', 32, '20210142'");
        session.sql("select * from test_db2.part_t2 where dt='20210140'").show(1000, false);
        session.sql("select * from test_db2.part_t2 ").show(1000, false);

        session.sql("insert overwrite test_db2.part_t2 partition (dt, hour) select 'amy', 32, '1111', '20210140' union all select 'amy', 32, '1112', '20210140' union all select 'amy', 33, '1112', '20210140'");
        session.sql("select * from test_db2.part_t2 where dt='1112'").show(1000, false);
        session.sql("select * from test_db2.part_t2 ").show(1000, false);




        session.sql("alter table test_db2.part_t2 add if not exists partition (dt='20210131',hour='01')");
        session.sql("show partitions test_db2.part_t1").show(1000, false);
        session.sql("show partitions test_db2.part_t2").show(1000, false);
        session.sql("create table if not exists default.t1 (name string, age int) using parquet");*/

/*
        String textFormat = "TEXT";
        String textTable = "test_tbl_text";
        session.sql("drop table if exists " + textTable);
        // Text data source supports only a single column, and you have 2 columns.
        session.sql("create table if not exists " + textTable + " (name string) using " + textFormat);
        session.sql("insert overwrite " + textTable + " values ('amy'), ('31')");
        session.sql("select * from " + textTable).show(1000, false);

        // List<String> tableFormats = Arrays.asList("PARQUET", "CSV", "ORC", "JSON", "AVRO");
        List<String> tableFormats = Arrays.asList("ICEBERG");
        String tableName = null;
        String tableFormat = null;
        for (int i = 0; i < tableFormats.size(); i++) {
            tableFormat = tableFormats.get(i).toLowerCase(Locale.ROOT);
            tableName = "default.test_tbl_" + tableFormat;
            session.sql("drop table if exists " + tableName);
            session.sql("create table if not exists " + tableName + " (name string, age int) using " + tableFormat);
            session.sql("insert overwrite " + tableName + " values ('amy', 32), ('bob', 31)");
            session.sql("select * from " + tableName).show(1000, false);
            // ANALYZE TABLE is not supported for v2 tables
            // session.sql("ANALYZE TABLE " + tableName + " COMPUTE STATISTICS").show(1000, false);

            // single partition key
            session.sql("DROP TABLE IF EXISTS test_db2.part_t1 ");
            session.sql("CREATE TABLE IF NOT EXISTS test_db2.part_t1 (id string COMMENT 'ID' , name string COMMENT '名称', dt string COMMENT '分区时间yyyy-MM-dd') USING csv COMMENT '测试表' PARTITIONED BY (dt) ");

            session.sql("insert overwrite test_db2.part_t1 partition (dt) select 'amy', 32, '20210140' union all select 'amy', 32, '20210141' union all select 'amy', 32, '20210142'");
            session.sql("select * from test_db2.part_t1 where dt='20210140'").show(1000, false);
            session.sql("select * from test_db2.part_t1 ").show(1000, false);

            session.sql("insert overwrite test_db2.part_t1 partition (dt) select 'amy', 32, '20210140'");
            session.sql("select * from test_db2.part_t1 where dt='20210140'").show(1000, false);
            session.sql("select * from test_db2.part_t1 ").show(1000, false);
        }

        // org.apache.hadoop.fs.FutureDataInputStreamBuilder
        session.sql("create table if not exists default.t2 (name string, age int)");
        session.sql("insert into default.t2 values ('amy', 32), ('bob', 31)");
        session.sql("select * from default.t2").show(1000, false);


        session.sql("CREATE TABLE IF NOT EXISTS default.part_t3 (id string COMMENT 'ID' , name string COMMENT '名称', dt string COMMENT '分区时间yyyy-MM-dd') USING parquet COMMENT '测试表' PARTITIONED BY (dt) ");
        session.sql("insert overwrite table default.part_t3 partition(dt='2023-04-29') values ('amy', 32), ('bob', 31)");
        session.sql("select * from default.part_t3 where dt='2023-04-29'").show(1000, false);*/

        // session.close();
        logger.info("============================================= End fileOutputAlgorithmVersion={} partitionOverwriteMode={} =============================================", fileOutputAlgorithmVersion, partitionOverwriteMode);

    }


    private static void testExecute(SparkSession session, String testTableName, String fileFormat, boolean sparkCatalogFirst) {

        String testTable = String.format(" %s ", testTableName);
        String delegateCatalogTable = String.format(" spark_catalog.%s ", testTableName);
        if (!sparkCatalogFirst) {
            testTable = String.format(" spark_catalog.%s ", testTableName);
            delegateCatalogTable = String.format(" %s ", testTableName);
        }

        testExecuteCommon(session, testTable, delegateCatalogTable, fileFormat, sparkCatalogFirst);

    }

    private static void testExecuteCommon(SparkSession session, String testTable, String delegateCatalogTable, String fileFormat, boolean sparkCatalogFirst) {
        String location = defaultLocation + testTable.split("\\.")[1].trim().toLowerCase(Locale.ROOT);
        fileFormat = fileFormat.toLowerCase(Locale.ROOT);
        session.sql("select '=========================================== Start " + fileFormat + ", testTable: " + testTable + ",delegateCatalogTable: " + delegateCatalogTable + "  =========================================='").show(1000, false);

        if ("iceberg".equalsIgnoreCase(fileFormat)) {
            executePrintSql(session, "DROP TABLE IF EXISTS " + testTable + " purge");
        } else {
            executePrintSql(session, "DROP TABLE IF EXISTS " + testTable + " ");
        }

        if (!sparkCatalogFirst && Arrays.asList("csv", "json").contains(fileFormat)) {
            if ("csv".equalsIgnoreCase(fileFormat)) {
                executePrintSql(session, "CREATE TABLE IF NOT EXISTS " + testTable + " (id string COMMENT 'ID' , name string COMMENT '名称') COMMENT '测试表' PARTITIONED BY (dt string) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' \n"
                    + "    STORED AS INPUTFORMAT \n"
                    + "  'org.apache.hadoop.mapred.TextInputFormat' \n"
                    + "OUTPUTFORMAT \n"
                    + "  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' location '" + location + "'");

            } else if ("json".equalsIgnoreCase(fileFormat)) {
                executePrintSql(session, "CREATE TABLE IF NOT EXISTS " + testTable + " (id string COMMENT 'ID' , name string COMMENT '名称') COMMENT '测试表' PARTITIONED BY (dt string) ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' \n"
                    + "    STORED AS INPUTFORMAT \n"
                    + "  'org.apache.hadoop.mapred.TextInputFormat' \n"
                    + "OUTPUTFORMAT \n"
                    + "  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' location '" + location + "'");
            }

            Row[] asSerde = (Row[])executePrintSql(session, "show create table " + testTable + " as SERDE").collect();
            logger.info("show create table collect asSerde: " + Arrays.toString(asSerde));

        } else {
            executePrintSql(session, "CREATE TABLE IF NOT EXISTS " + testTable + " (id string COMMENT 'ID' , name string COMMENT '名称', dt string COMMENT '分区时间yyyy-MM-dd') USING " + fileFormat + " COMMENT '测试表' PARTITIONED BY (dt) location '" + location + "'");

            Row[] collect = (Row[])executePrintSql(session, "show create table " + testTable).collect();
            logger.info("show create table collect: " + Arrays.toString(collect));
        }
        executePrintSql(session, "desc extended " + testTable).show(1000, false);

        executePrintSql(session, "insert overwrite " + testTable + " partition (dt) select 'amy', 32, '20210140' union all select 'amy', 32, '20210141' union all select 'amy', 32, '20210142'");
        executePrintSql(session, "desc extended " + testTable).show(1000, false);

//        // test add column
//        session.sql("alter table " + testTable + " add column c3 int");
//        session.sql("desc extended " + testTable).show(1000, false);

        assertEqual( executePrintSqlCollectResult(session,
            "select * from " + testTable + " where dt='20210142' and id=32 ") ==
            executePrintSqlCollectResult(session,
            "select * from " + delegateCatalogTable + " where dt='20210142' and id=32 "));

//        need refresh table: FileStatusCache
//        assertEqual(  executePrintSqlCollectResult(session, "select * from " + testTable) ==
//            executePrintSqlCollectResult(session, "select * from " + delegateCatalogTable));

        executePrintSql(session, "insert overwrite " + testTable + " partition (dt) select 'amy', 32, '20210140'");
        assertEqual(  executePrintSqlCollectResult(session, "select * from" + testTable + " where dt='20210140'") ==
            executePrintSqlCollectResult(session, "select * from" + delegateCatalogTable + " where dt='20210140'"));
//        assertEqual(  executePrintSqlCollectResult(session, "select * from " + testTable) ==
//            executePrintSqlCollectResult(session, "select * from " + delegateCatalogTable));

        // Read and write test together
        executePrintSql(session, "insert overwrite " + delegateCatalogTable + " partition (dt) select 'amy', 32, '20210141'");
        executePrintSql(session, "insert into " + testTable + " partition (dt) select 'amy', 32, '20210141'");
        assertEqual(  executePrintSqlCollectResult(session, "select * from" + delegateCatalogTable + " where dt='20210141'") ==
            executePrintSqlCollectResult(session, "select * from" + testTable + " where dt='20210141'"));
//        assertEqual(  executePrintSqlCollectResult(session, "select * from " + delegateCatalogTable) ==
//            executePrintSqlCollectResult(session, "select * from " + testTable));


        assertEqual(  executePrintSqlCollectResult(session, "select * from " + delegateCatalogTable) ==
            executePrintSqlCollectResult(session, "select * from " + testTable));
        session.sql("select '=========================================== End " + fileFormat + ", testTable: " + testTable + ",delegateCatalogTable: " + delegateCatalogTable + "  =========================================='").show(1000, false);

    }

    private static Dataset<Row> executePrintSql(SparkSession session, String sqlContext) {
        logger.info("executePrintSql: " + sqlContext);
        return session.sql(sqlContext);
    }

    private static int executePrintSqlCollectResult(SparkSession session, String sqlContext) {
        logger.info("executePrintSqlCollectResult: " + sqlContext);
        Row[] rows = (Row[]) session.sql(sqlContext).collect();
        return rows.length;
    }

    private static void testExecuteIceberg(SparkSession session, String testTableName, String fileFormat, boolean sparkCatalogFirst) {

        String testTable = String.format(" %s ", testTableName);
        String delegateCatalogTable = String.format(" iceberg.%s ", testTableName);
        if (!sparkCatalogFirst) {
            testTable = String.format(" iceberg.%s ", testTableName);
            delegateCatalogTable = String.format(" %s ", testTableName);
        }

        testExecuteCommon(session, testTable, delegateCatalogTable, fileFormat, sparkCatalogFirst);

    }
}
