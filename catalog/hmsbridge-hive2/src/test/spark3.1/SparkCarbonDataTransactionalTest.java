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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import io.polycat.catalog.client.CatalogUserInformation;
import io.polycat.catalog.client.PolyCatClient;
import io.polycat.catalog.common.GlobalConfig;
import io.polycat.catalog.common.PolyCatConf;
import io.polycat.catalog.common.model.Table;
import io.polycat.catalog.common.plugin.request.CreateCatalogRequest;
import io.polycat.catalog.common.plugin.request.CreateDatabaseRequest;
import io.polycat.catalog.common.plugin.request.GetTableRequest;
import io.polycat.catalog.common.plugin.request.input.CatalogInput;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;
import io.polycat.catalog.hms.hive2.HMSBridgeStore;
import io.polycat.catalog.server.CatalogApplication;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.spark.sql.CarbonSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This test class is to test CarbonData integration with PolyCat via HMSBridgeStore where only
 * table metadata is stored in PolyCat and not the version or commit history.
 */
public class SparkCarbonDataTransactionalTest {

  private static SparkSession spark;
  private static ConfigurableApplicationContext catalogServer;
  private static PolyCatClient catalogClient;
  private static String targetPath;
  private static String userName;
  private static HMSBridgeStore bridgeStore;

  @BeforeAll
  static void beforeAll() throws IOException {
    GlobalConfig.set(GlobalConfig.CONF_DIR, "../../conf");
    targetPath = new File(SparkCarbonDataTransactionalTest.class.getResource("/").getPath() + "/../").getCanonicalPath();
    HMSBridgeTestEnv.clean(targetPath + "/spark-warehouse", targetPath + "/metastore_db");
    userName = "test";
    spark = SparkSession
        .builder()
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.metrics.staticSources.enabled", "false")
        .config("spark.hadoop.hive.metastore.rawstore.impl",
            "io.polycat.catalog.hms.hive2.HMSBridgeStore")
        .config("spark.hadoop." + CatalogUserInformation.POLYCAT_USER_NAME, userName)
        .config("spark.hadoop." + CatalogUserInformation.POLYCAT_USER_PASSWORD, "dash")
        .config("spark.hadoop." + PolyCatConf.POLYCAT_CONFI_DIR, "../../conf")
        .config("spark.sql.warehouse.dir", targetPath + "/spark-warehouse")
        .config("spark.hadoop.javax.jdo.option.ConnectionURL",
            "jdbc:derby:;databaseName=" + targetPath + "/metastore_db;create=true")
        .config("IS_SCHEMA_EVOLUTION_CASE_SENSITIVE", "false")
        .config("hive.exec.dynamic.partition", "true")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .config("spark.sql.extensions","org.apache.spark.sql.CarbonExtensions")
        .enableHiveSupport()
        .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");

    catalogServer = SpringApplication.run(CatalogApplication.class);

    catalogClient = PolyCatClient.getInstance(spark.sessionState().newHadoopConf());
    bridgeStore = new HMSBridgeStore(spark.sessionState().newHadoopConf());

    // create catalog in lms
    CreateCatalogRequest createCatalogRequest = new CreateCatalogRequest();
    CatalogInput catalogInput = new CatalogInput();
    catalogInput.setCatalogName("carbon_lms");
    createCatalogRequest.setInput(catalogInput);
    createCatalogRequest.setProjectId(catalogClient.getProjectId());
    createCatalogRequest.getInput().setOwner(userName);
    catalogClient.createCatalog(createCatalogRequest);

    // create database in lms
    CreateDatabaseRequest createDatabaseRequest = new CreateDatabaseRequest();
    DatabaseInput databaseInput = new DatabaseInput();
    databaseInput.setDatabaseName("default");
    databaseInput.setLocationUri("target/default");
    createDatabaseRequest.setInput(databaseInput);
    createDatabaseRequest.setProjectId(catalogClient.getProjectId());
    createDatabaseRequest.getInput().setOwner(userName);
    createDatabaseRequest.setCatalogName("carbon_lms");
    catalogClient.createDatabase(createDatabaseRequest);

  }

  @Test
  public void testCarbonDataCreateLoadAndQuery() {
    spark.sql("drop table if exists carbon");
    spark.sql("create table carbon(name string, age int) stored as carbondata tblproperties('lms_name'='carbon_lms')");
    spark.sql("insert into carbon select 'ross',29");
    spark.sql("insert into carbon select 'joey',30");

    // check metadata in lms
    GetTableRequest
        getTableRequest = new GetTableRequest(catalogClient.getProjectId(), "carbon_lms", "default", "carbon");
    Table table = catalogClient.getTable(getTableRequest);
    Assertions.assertNotNull(table);

    // check output from query
    Row[] rows1 = (Row[]) spark.sql("select * from carbon order by name").collect();
    Assertions.assertEquals(2, rows1.length);
    Assertions.assertEquals("[joey,30]", rows1[0].toString());
    Assertions.assertEquals("[ross,29]", rows1[1].toString());

    // check filter queries
    Row[] rows2 = (Row[]) spark.sql("select age from carbon where name = 'ross'").collect();
    Assertions.assertEquals(1, rows2.length);
    Assertions.assertEquals(29, rows2[0].getInt(0));

    // check store path according to carbondata datasource
    File tablePath = new File(targetPath + "/spark-warehouse/carbon");
    Assertions.assertTrue(tablePath.exists() && tablePath.isDirectory());
    File factPath = new File(targetPath + "/spark-warehouse/carbon/Fact");
    Assertions.assertTrue(factPath.exists() && factPath.isDirectory());
    File metaPath = new File(targetPath + "/spark-warehouse/carbon/Metadata");
    Assertions.assertTrue(metaPath.exists() && metaPath.isDirectory());
    spark.sql("drop table if exists carbon");
  }

  @Test
  public void testCarbonDataUpdateAndDelete() {
    spark.sql("drop table if exists carbon_update");
    spark.sql("create table carbon_update(name string, age int, salary double, city string) stored as carbondata tblproperties('lms_name'='carbon_lms')");
    spark.sql("insert into carbon_update values('mike',40,5000.0,'scranton'),('dwight',32,2000.0,'berlin'),('jim',29,6000.0,'niagara')");
    Row[] count = (Row[]) spark.sql("select count(*) from carbon_update").collect();
    Assertions.assertEquals(3, count[0].getLong(0));
    spark.sql("update carbon_update set(salary) = (10000.0) where name = 'mike'");
    spark.sql("delete from carbon_update where name = 'dwight'");

    // check metadata in lms
    GetTableRequest
        getTableRequest = new GetTableRequest(catalogClient.getProjectId(), "carbon_lms", "default", "carbon_update");
    Table table = catalogClient.getTable(getTableRequest);
    Assertions.assertNotNull(table);

    Row[] countAfterUpdate = (Row[]) spark.sql("select count(*) from carbon_update").collect();
    Assertions.assertEquals(2, countAfterUpdate[0].getLong(0));
    Row[] filter = (Row[]) spark.sql("select city,name from carbon_update where salary = 10000.0").collect();
    Assertions.assertEquals("scranton", filter[0].getString(0));
    Assertions.assertEquals("mike", filter[0].getString(1));
    spark.sql("drop table if exists carbon_update");
  }

  @Test
  public void testCarbonDataPartitionTable() throws MetaException, NoSuchObjectException {
    spark.sql("drop table if exists partition");
    spark.sql("create table partition(name string, age int, salary double) partitioned by (city string) stored as carbondata tblproperties('lms_name'='carbon_lms')");
    spark.sql("insert into partition values('mike',40,5000.0,'scranton'),('dwight',32,2000.0,'berlin'),('jim',29,6000.0,'niagara')");
    Row[] count = (Row[]) spark.sql("select count(*) from partition").collect();
    Assertions.assertEquals(3, count[0].getLong(0));

    List<Partition> partitionsByFilter = bridgeStore.getPartitionsByFilter("default", "partition", "city='berlin'", (short) 0);
    Assertions.assertEquals(1, partitionsByFilter.size());

    List<String> partNames = new ArrayList<>();
    partNames.add("city=niagara");
    partNames.add("city=scranton");
    List<Partition> partitionsByNames = bridgeStore.getPartitionsByNames("default", "partition", partNames);
    Assertions.assertEquals(2, partitionsByNames.size());
    assertTrue(partitionsByNames.get(0).getSd().getLocation().contains("city=niagara"));
    assertTrue(partitionsByNames.get(1).getSd().getLocation().contains("city=scranton"));
    spark.sql("drop table if exists partition");
  }

  @Test
  public void testCarbonDataMergeSQL() {
    spark.sql("drop table if exists A");
    spark.sql("drop table if exists B");
    createTableForMerge();
    spark.sql("select * from A").show();
    spark.sql("select * from A").show();
    spark.sql(
        "MERGE INTO A USING B ON A.ID=B.ID WHEN MATCHED AND A.ID=2 THEN DELETE "
            + "WHEN MATCHED AND A.ID=1 THEN UPDATE SET  A.id=9, A.price=100, A.state='hahaha'");
    // check metadata in lms
    GetTableRequest
        getTableRequest1 = new GetTableRequest(catalogClient.getProjectId(), "carbon_lms", "default", "a");
    Table table1 = catalogClient.getTable(getTableRequest1);
    GetTableRequest
        getTableRequest2 = new GetTableRequest(catalogClient.getProjectId(), "carbon_lms", "default", "b");
    Table table2 = catalogClient.getTable(getTableRequest2);
    Assertions.assertNotNull(table1);
    Assertions.assertNotNull(table2);
    Row[] count = (Row[]) spark.sql("select count(*) from A").collect();
    Assertions.assertEquals(3, count[0].getLong(0));

    Row[] rows = (Row[])spark.sql("select * from A order by id").collect();
    Assertions.assertEquals("[3,300,NH]", rows[0].toString());
    Assertions.assertEquals("[4,400,FL]", rows[1].toString());
    Assertions.assertEquals("[9,100,hahaha]", rows[2].toString());
    spark.sql("drop table if exists A");
    spark.sql("drop table if exists B");
  }

  @Test
  public void testCarbonDataMergeAPIs() {
    spark.sql("drop table if exists target");
    ArrayList<Row> data = new ArrayList<>();
    data.add(RowFactory.create("a", "0"));
    data.add(RowFactory.create("b", "1"));
    data.add(RowFactory.create("c", "2"));
    data.add(RowFactory.create("d", "3"));

    List<org.apache.spark.sql.types.StructField> listOfStructField= new ArrayList<>();
    listOfStructField.add
        (DataTypes.createStructField("key", DataTypes.StringType, true));
    listOfStructField.add
        (DataTypes.createStructField("value", DataTypes.StringType, true));
    StructType structType=DataTypes.createStructType(listOfStructField);
    Dataset<Row> dataset = spark.createDataFrame(data,structType);

    dataset.write().format("carbondata").option("tableName", "target").option("lms_name", "carbon_lms").mode(SaveMode.Overwrite).save();
    Dataset<Row> target = spark.sqlContext().read().format("carbondata").option("tableName", "target").load();

    ArrayList<Row> cdcdata = new ArrayList<>();
    cdcdata.add(RowFactory.create("a", "0"));
    cdcdata.add(RowFactory.create("b", null));
    cdcdata.add(RowFactory.create("g", null));
    cdcdata.add(RowFactory.create("e", "3"));

    Dataset<Row> cdc = spark.createDataFrame(cdcdata, structType);
    // upsert API
    new CarbonSession.DataSetMerge(target.as("A")).upsert(cdc.as("B"), "key").execute();

    cdcdata.clear();
    cdcdata.add(RowFactory.create("a", "7"));
    cdcdata.add(RowFactory.create("e", "3"));
    cdc = spark.createDataFrame(cdcdata, structType);
    // delete API
    new CarbonSession.DataSetMerge(target.as("A")).delete(cdc.as("B"), "key").execute();

    cdcdata.clear();
    cdcdata.add(RowFactory.create("g", "56"));
    cdc = spark.createDataFrame(cdcdata, structType);
    // update API
    new CarbonSession.DataSetMerge(target.as("A")).update(cdc.as("B"), "key").execute();

    cdcdata.clear();
    cdcdata.add(RowFactory.create("z", "234"));
    cdcdata.add(RowFactory.create("x", "2"));
    cdc = spark.createDataFrame(cdcdata, structType);
    // insert API
    new CarbonSession.DataSetMerge(target.as("A")).insert(cdc.as("B"), "key").execute();

    // TODO: need to fix CarbonDataFrameWriter, as it wont consider all properties, so its not considering lms_name property
    // check metadata in lms
//    GetTableRequest
//        getTableRequest = new GetTableRequest(authResult.getProjectId(), "carbon_lms", "default", "target",
//        authResult.getToken());
//    Table table = catalogClient.getTable(getTableRequest);
//    Assertions.assertNotNull(table);

    Row[] rows = (Row[]) spark.sql("select * from target order by key").collect();

    Assertions.assertEquals("[b,null]", rows[0].toString());
    Assertions.assertEquals("[c,2]", rows[1].toString());
    Assertions.assertEquals("[d,3]", rows[2].toString());
    Assertions.assertEquals("[g,56]", rows[3].toString());
    Assertions.assertEquals("[x,2]", rows[4].toString());
    Assertions.assertEquals("[z,234]", rows[5].toString());
  }


  private void createTableForMerge() {
    spark.sql(" CREATE TABLE IF NOT EXISTS A(id Int,price Int,state String) STORED AS carbondata tblproperties('lms_name'='carbon_lms')");
    spark.sql("CREATE TABLE IF NOT EXISTS B(id Int,price Int,state String) STORED AS carbondata tblproperties('lms_name'='carbon_lms')");

    spark.sql("INSERT INTO A VALUES (1,100,'MA')");
    spark.sql("INSERT INTO A VALUES (2,200,'NY')");
    spark.sql("INSERT INTO A VALUES (3,300,'NH')");
    spark.sql("INSERT INTO A VALUES (4,400,'FL')");

    spark.sql("INSERT INTO B VALUES (1,1,'MA (updated)')");
    spark.sql("INSERT INTO B VALUES (2,3,'NY (updated)')");
    spark.sql("INSERT INTO B VALUES (3,3,'CA (updated)')");
    spark.sql("INSERT INTO B VALUES (5,5,'TX (updated)')");
    spark.sql("INSERT INTO B VALUES (7,7,'LO (updated)')");
  }

  @AfterAll
  static void afterAll() {
    try {
      if (spark != null) {
        try {
          spark.close();
        } catch (Exception ignored) {

        }
      }
      if (catalogServer != null) {
        try {
          catalogServer.close();
        } catch (Exception ignored) {

        }
      }
    } finally {
      HMSBridgeTestEnv.clean(targetPath + "/spark-warehouse", targetPath + "/metastore_db");
    }
  }
  }
