/**
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
package io.polycat.catalog.spark

import org.apache.spark.sql.{Row, SparkSession}
import io.polycat.catalog.client.util.Constants

/**
 * @author liangyouze
 * @date 2024/3/11
 */

object Example3 {
  def main(args: Array[String]): Unit = {
    val sparkCatalogName = "xxxxxxxx_ue1";
    val session = SparkSession.builder()
      .appName("test")
      .master("local[2]")
      .enableHiveSupport()
      //               .config("spark.sql.catalogImplementation", "in-memory")
      // .config("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")
      .config("spark.sql.warehouse.dir", "file:/tmp/hive/warehouse")
      .config("catalog.sql.warehouse.dir", "file:/tmp/hive/warehouse")
      //                .config("spark.sql.defaultCatalog", sparkCatalogName)
      //                .config("spark.sql.catalog." + sparkCatalogName, "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog." + sparkCatalogName, "io.polycat.catalog.spark.SparkCatalog")
    // .config("spark.sql.catalog." + sparkCatalogName + ".catalog-impl", "PolyCatCatalog")
    // .config("spark.sql.catalog." + sparkCatalogName + ".type", "hive")
    // .config("spark.sql.catalog." + sparkCatalogName + ".uri", "thrift://hms-ue1.uxxxxxxxx.org:9083")
    // .config("spark.hadoop.hive.metastore.uris", "thrift://k8s-bdp-hmssg2mi-23d5098ca7-0b9d691c4f1b9553.elb.ap-southeast-1.amazonaws.com:9083")
      .config("spark.hadoop.hive.metastore.uris", "thrift://hms-ue1.uxxxxxxxx.org:9083")
      .config("spark.hadoop." + Constants.POLYCAT_CLIENT_HOST, "k8s-bdp-catalogm-dcc4da73d0-8e4964afc83f6e35.elb.ap-southeast-1.amazonaws.com")
      .config("spark.hadoop." + Constants.POLYCAT_CLIENT_PORT, "80")
      .config("spark.hadoop." + Constants.POLYCAT_CLIENT_PROJECT_ID, "xxxxxxxx")
      .config("spark.hadoop." + Constants.POLYCAT_CLIENT_USERNAME, "user1")
      // .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.sql.extensions", "io.polycat.catalog.spark.extension.PolyCatSparkTableExtensions")
      .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .getOrCreate();

    session.sql("drop table spark_catalog.test.test_csv")
    session.sql("create table spark_catalog.test.test_csv(id int, name string, dt string) using csv partitioned by (dt) location 's3://xxxxxxxx.tmp.us-east-1/Default/user1/test_csv/'")
    session.sql("insert overwrite spark_catalog.test.test_csv partition (dt) select 32, 'amy', '20240221'")
    session.sql("select * from spark_catalog.test.test_csv where dt='20240221'").collect.foreach(row => {
      val id = row.getAs[Int]("id")
      println(id)
    })

  }

}
