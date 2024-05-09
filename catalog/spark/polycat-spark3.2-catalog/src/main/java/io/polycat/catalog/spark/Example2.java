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

import io.polycat.catalog.client.util.Constants;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.spark.SparkCatalog;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import java.lang.reflect.InvocationTargetException;

/**
 * @author liangyouze
 * @date 2024/2/21
 */
public class Example2 {
    static private final Logger logger = Logger.getLogger(Example2.class);

    public static void main(String[] args) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException, NoSuchTableException {
        String sparkCatalogName = "xxxxxxxx_ue1";
        logger.info("Example2 starts");
        SparkSession session = SparkSession.builder()
                .appName("test")
                .master("local[2]")
                .enableHiveSupport()
 //               .config("spark.sql.catalogImplementation", "in-memory")
                .config("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")
                .config("spark.sql.warehouse.dir", "file:/tmp/hive/warehouse")
                .config("catalog.sql.warehouse.dir", "file:/tmp/hive/warehouse")
//                .config("spark.sql.defaultCatalog", sparkCatalogName)
                .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog." + sparkCatalogName, SparkCatalog.class.getCanonicalName())
                // .config("spark.sql.catalog.iceberg" + ".catalog-impl", "PolyCatCatalog")
                .config("spark.sql.catalog.iceberg" + ".type", "hive")
                .config("spark.sql.catalog.iceberg" + ".uri", "thrift://hms-ue1.uxxxxxxxx.org:9083")
               //  .config("spark.hadoop.hive.metastore.uris", "thrift://k8s-bdp-hmssg2mi-23d5098ca7-0b9d691c4f1b9553.elb.ap-southeast-1.amazonaws.com:9083")
                .config("spark.hadoop.hive.metastore.uris", "thrift://hms-ue1.uxxxxxxxx.org:9083")
                .config("spark.hadoop." + Constants.POLYCAT_CLIENT_HOST, "k8s-bdp-catalogm-dcc4da73d0-8e4964afc83f6e35.elb.ap-southeast-1.amazonaws.com")
                .config("spark.hadoop." + Constants.POLYCAT_CLIENT_PORT, "80")
                .config("spark.hadoop." + Constants.POLYCAT_CLIENT_PROJECT_ID, "xxxxxxxx")
                .config("spark.hadoop." + Constants.POLYCAT_CLIENT_USERNAME, "user1")
                .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.sql.extensions", "io.polycat.catalog.spark.extension.PolyCatSparkTableExtensions")
                .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
                .getOrCreate();

        // session.sql("create table iceberg.test.test_iceberg1(id string, name string, dt string)  \n" +
        //             "using iceberg partitioned by (dt) location 'file:///tmp/test_iceberg' ");
        // session.sql("alter table xxxxxxxx_ue1.test.test_iceberg add columns (age string comment 'test_comment')");
        // session.sql("insert overwrite iceberg.test.test_iceberg1 partition (dt) select '33', 'amy','20240221'");
        // session.sql("insert overwrite xxxxxxxx_ue1.test.test_iceberg1 partition (dt) select '33', 'amy','20240221'");
        // session.sql("select * from spark_catalog.test.test_iceberg where dt='20240221'").show();
        // session.sql("drop table spark_catalog.test.test_iceberg purge");
        // session.sql("show namespaces").show();
        // session.sql("create view test_view as select * from test.partition_test1");
        // session.sql("show partitions xxxxxxxx_ue1.test.partition_test1").show();

        // session.sql("drop table xxxxxxxx_ue1.test.partition_test1");
        // session.sql("create table xxxxxxxx_ue1.test.partition_test1(id int, name string, dt string)" +
        //         "using parquet partitioned by (dt) location 'file:///tmp/partition_test1'");
        // session.sql("insert overwrite spark_catalog.test.partition_test1 partition (dt='20240221') select 32, 'amy' ");
        // session.sql("insert overwrite xxxxxxxx_ue1.test.partition_test1 partition (dt='20240221') values (32, 'amy'), (33, 'bob')");
        // session.sql("select * from xxxxxxxx_ue1.test.partition_test1 where dt='20240221' ").show();
        // session.sql("alter table xxxxxxxx_ue1.test.partition_test1 recover partitions");

        // session.sql("create table xxxxxxxx_ue1.test.partition_test5(id string, name string, dt string) using csv partitioned by (dt) location 'file:///tmp/partition_test5'");
        // session.sql("insert overwrite spark_catalog.test.partition_test3 partition (dt) select '32', 'amy', '20240221'");
        // session.sql("select * from spark_catalog.default.partition_test4 where dt='20240221'").show();
        // session.sql("create table spark_catalog.test.json_test(id string, name string, dt string)" +
        //         "using json partitioned by (dt) location 'file:///tmp/json_test'");

        // session.sql("drop table xxxxxxxx_ue1.test.partition_test1");
        // session.sql("CREATE TABLE IF NOT EXISTS xxxxxxxx_ue1.test.partition_test1 (id string COMMENT 'ID' , name string COMMENT '名称', dt string COMMENT '分区时间yyyy-MM-dd', hour string COMMENT '分区时间yyyy-MM-dd') USING parquet COMMENT '测试表' PARTITIONED BY (dt, hour) location '/tmp/partition_test1'");

        // session.sql("insert overwrite xxxxxxxx_ue1.test.partition_test1 partition (dt, hour) select 'amy', 32, '1111', '20210140' union all select 'amy', 32, '1112', '20210140' union all select 'amy', 33, '1112', '20210146' union all select 'amy', 33, '1112', '20210145' union all select 'amy', 33, '1112', '20210142' union all select 'amy', 33, '1112', '20210143' union all select 'amy', 33, '1112', '20210144'");
        // session.sql("insert overwrite xxxxxxxx_ue1.test.partition_test1 partition (dt='1113', hour) select id, name, hour from xxxxxxxx_ue1.test.partition_test1 where dt = '1112'");
        // session.sql("insert overwrite xxxxxxxx_ue1.test.partition_test1 partition (dt='1114', hour) select id, name, hour from xxxxxxxx_ue1.test.partition_test1 where dt = '1112' or dt = '1113'");

        // session.sql("insert overwrite xxxxxxxx_ue1.test.partition_test1 partition (dt='1111', hour='01') select '32', 'amy'");
        // session.sql("insert overwrite xxxxxxxx_ue1.test.partition_test1 partition (dt, hour) select '32', 'amy', '1111', '01'");
        // session.sql("insert overwrite xxxxxxxx_ue1.test.partition_test1 partition (dt='1111', hour) select '32', 'amy', '01'");
        // session.sql("drop table spark_catalog.test.partition_test2");
        // session.sql("CREATE TABLE IF NOT EXISTS spark_catalog.test.partition_test2 (id string COMMENT 'ID' , name string COMMENT '名称', dt string COMMENT '分区时间yyyy-MM-dd', hour string COMMENT '分区时间yyyy-MM-dd') USING parquet COMMENT '测试表' PARTITIONED BY (dt, hour) location '/tmp/partition_test1'");
//
        // session.sql("insert overwrite spark_catalog.test.partition_test2 partition (dt, hour) select 'amy', 32, '1111', '20210140' union all select 'amy', 32, '1112', '20210140' union all select 'amy', 33, '1112', '20210146' union all select 'amy', 33, '1112', '20210145' union all select 'amy', 33, '1112', '20210142' union all select 'amy', 33, '1112', '20210143' union all select 'amy', 33, '1112', '20210144'");
        // session.sql("insert overwrite spark_catalog.test.partition_test2 partition (dt='1113', hour) select id, name, hour from spark_catalog.test.partition_test2 where dt = '1112'");

        // session.sql("insert overwrite spark_catalog.test.partition_test1 partition (dt='1114', hour) select id, name, hour from spark_catalog.test.partition_test1 where dt = '1112' or dt = '1113'");

        // session.sql("alter table spark_catalog.test.partition_test1 add partition(dt='1115', hour='01') location '/tmp/partition_test2/dt=1115/hour=01'");
        // session.sql("insert overwrite spark_catalog.test.partition_test1 partition (dt, hour) select '32', 'amy', '1115', '01'");
        // session.sql("create table xxxxxxxx_ue1.test.no_partition_test(id string, name string) using parquet location '/tmp/no_partition_test'");
        // session.sql("insert overwrite xxxxxxxx_ue1.test.no_partition_test select '30', 'amy'");

        // session.sql("alter table spark_catalog.test.partition_test1 add partition(dt='1112', hour='01') location '/tmp/partition_test2/dt=1112/hour=01'");
        // session.sql("select * from spark_catalog.test.partition_test1 where concat(dt, hour) = '111201'").show();
        // staticQuery(session);

/*        session.sql("CREATE EXTERNAL TABLE spark_catalog.test.test_text(\n" +
                "  `id` STRING COMMENT '',\n" +
                "  `name` STRING COMMENT '')\n" +
                "partitioned by (dt string)" +
                "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'\n" +
                "WITH SERDEPROPERTIES (\n" +
                "  'serialization.format' = '\t',\n" +
                "  'field.delim' = '\t')\n" +
                "STORED AS\n" +
                "  INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'\n" +
                "  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'\n" +
                "LOCATION 'file:///tmp/test_text'");
        session.sql("insert overwrite xxxxxxxx_ue1.test.test_text partition(dt='1114') select '33', 'amy'");*/
        session.sql("select * from xxxxxxxx_ue1.test.test_text where dt='1114'").show();
    }

    public static void dynamicAndStatic(SparkSession session) {
        session.sql("insert overwrite xxxxxxxx_ue1.test.partition_test1 partition (dt='1112', hour) select '32', 'amy', '01' union all select '33', 'bob', '02'");
        session.sql("select * from  xxxxxxxx_ue1.test.partition_test1 where dt = '1112'").show();
    }

    public static void staticQuery(SparkSession session) {
        session.sql("insert overwrite xxxxxxxx_ue1.test.partition_test1 partition (dt='1112', hour='01') select '32', 'amy' union all select '33', 'bob'");
        session.sql("select * from  spark_catalog.test.partition_test1 where dt = '1112' and hour='02'").show(1000);
    }

    public static void dynamicQuery(SparkSession session) {
        session.sql("insert overwrite xxxxxxxx_ue1.test.partition_test1 partition (dt, hour) select '32', 'amy', '1111', '01' union all select '33', 'bob', '1112', '02'");
        session.sql("select * from  xxxxxxxx_ue1.test.partition_test1 where dt = '1112'").show(1000);
    }


}
