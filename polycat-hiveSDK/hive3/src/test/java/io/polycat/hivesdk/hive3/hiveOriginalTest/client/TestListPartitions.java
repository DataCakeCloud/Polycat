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
package io.polycat.hivesdk.hive3.hiveOriginalTest.client;

import static java.util.stream.Collectors.joining;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import io.polycat.hivesdk.hive3.PolyCatTestUtils;
import io.polycat.hivesdk.hive3.MetaStoreTestUtils;
import io.polycat.hivesdk.hive3.hiveOriginalTest.annotation.MetastoreCheckinTest;
import io.polycat.hivesdk.hive3.hiveOriginalTest.minihms.AbstractMetaStoreService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.PartitionValuesRequest;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.PartitionValuesRow;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.CatalogBuilder;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * API tests for HMS client's listPartitions methods.
 */
@RunWith(Parameterized.class)
@Ignore("some Hive feature, Catalog not support yet.")
@Category(MetastoreCheckinTest.class)
public class TestListPartitions extends MetaStoreClientTest {

  private static final String CATALOG_NAME = "list_partition_catalog";
  private AbstractMetaStoreService metaStore;
  public static final String TEST_TYPE = "polyCat";

  private static final String DB_NAME = "testpartdb";
  private static final String TABLE_NAME = "testparttable";

  public TestListPartitions(String name, AbstractMetaStoreService metaStore) {
    this.metaStore = metaStore;
  }

  private static IMetaStoreClient client;
  @BeforeClass
  public static void setUp() throws Exception {
    // Get new client
    // Get new client
    PolyCatTestUtils.runCatalogApp(TEST_TYPE);
    client = PolyCatTestUtils.createHMSClient(TEST_TYPE);
  }
  @Before
  public void cleanUp() throws TException {
    // Clean up the database
    client.dropDatabase(DB_NAME, true, true, true);
    createDB(DB_NAME);

    // clear other catalog
    for (String dbNames : client.getAllDatabases(CATALOG_NAME)) {
      if (dbNames.equals("default")) {
        continue;
      }
      client.dropDatabase(CATALOG_NAME, dbNames, true, true, true);
    }

    try {
      client.dropCatalog(CATALOG_NAME);
    } catch (TException e) {
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    try {
      if (client != null) {
        client.close();
      }
    } finally {
      client = null;
    }
  }

  private void createDB(String dbName) throws TException {
    new DatabaseBuilder().
            setName(dbName).
            create(client, metaStore.getConf());
  }

  private Table createTestTable(IMetaStoreClient client, String dbName, String tableName,
                                       List<String> partCols) throws Exception {

    return createTestTable(client, dbName, tableName, partCols, false);
  }


  private Table createTestTable(IMetaStoreClient client, String dbName, String tableName,
                                       List<String> partCols, boolean setPartitionLevelPrivilages)
          throws TException {
    TableBuilder builder = new TableBuilder()
            .setDbName(dbName)
            .setTableName(tableName)
            .addCol("id", "int")
            .addCol("name", "string");

    partCols.forEach(col -> builder.addPartCol(col, "string"));
    Table table = builder.build(metaStore.getConf());

    if (setPartitionLevelPrivilages) {
      table.putToParameters("PARTITION_LEVEL_PRIVILEGE", "true");
    }

    client.createTable(table);
    return table;
  }

  private void addPartition(IMetaStoreClient client, Table table, List<String> values)
          throws TException {
    PartitionBuilder partitionBuilder = new PartitionBuilder().inTable(table);
    values.forEach(val -> partitionBuilder.addValue(val));
    client.add_partition(partitionBuilder.build(metaStore.getConf()));
  }

  private void createTable3PartCols1PartGeneric(IMetaStoreClient client, boolean authOn)
          throws TException {
    Table t = createTestTable(client, DB_NAME, TABLE_NAME, Lists.newArrayList("yyyy", "mm",
            "dd"), authOn);
    addPartition(client, t, Lists.newArrayList("1997", "05", "16"));
  }

  private void createTable3PartCols1Part(IMetaStoreClient client) throws TException {
    createTable3PartCols1PartGeneric(client, false);
  }

  private List<List<String>> createTable4PartColsPartsGeneric(IMetaStoreClient client,
                                                                     boolean authOn) throws
          Exception {
    Table t = createTestTable(client, DB_NAME, TABLE_NAME, Lists.newArrayList("yyyy", "mm", "dd"),
            authOn);
    List<List<String>> testValues = Lists.newArrayList(
            Lists.newArrayList("1999", "01", "02"),
            Lists.newArrayList("2009", "02", "10"),
            Lists.newArrayList("2017", "10", "26"),
            Lists.newArrayList("2017", "11", "27"));

    for(List<String> vals : testValues) {
      addPartition(client, t, vals);
    }

    return testValues;
  }

  private List<List<String>> createTable4PartColsParts(IMetaStoreClient client) throws
          Exception {
    return createTable4PartColsPartsGeneric(client, false);
  }

  private List<List<String>> createTable4PartColsPartsAuthOn(IMetaStoreClient client) throws
          Exception {
    return createTable4PartColsPartsGeneric(client, true);
  }

  private static void assertAuthInfoReturned(String user, String group, Partition partition) {
    assertNotNull(partition.getPrivileges());
    assertEquals(Lists.newArrayList(),
            partition.getPrivileges().getUserPrivileges().get(user));
    assertEquals(Lists.newArrayList(),
            partition.getPrivileges().getGroupPrivileges().get(group));
    assertEquals(Lists.newArrayList(),
            partition.getPrivileges().getRolePrivileges().get("public"));
  }

  private static void assertPartitionsHaveCorrectValues(List<Partition> partitions,
                                               List<List<String>> testValues) throws Exception {
    assertEquals(testValues.size(), partitions.size());
    for (int i = 0; i < partitions.size(); ++i) {
      assertEquals(testValues.get(i), partitions.get(i).getValues());
    }
  }

  private static void assertCorrectPartitionNames(List<String> names,
                                                  List<List<String>> testValues,
                                                  List<String>partCols) throws Exception {
    assertEquals(testValues.size(), names.size());
    for (int i = 0; i < names.size(); ++i) {
      List<String> expectedKVPairs = new ArrayList<>();
      for (int j = 0; j < partCols.size(); ++j) {
        expectedKVPairs.add(partCols.get(j) + "=" + testValues.get(i).get(j));
      }
      assertEquals(expectedKVPairs.stream().collect(joining("/")), names.get(i));
    }
  }

  private static void assertPartitionsSpecProxy(PartitionSpecProxy partSpecProxy,
                                                List<List<String>> testValues) throws Exception {
    assertEquals(testValues.size(), partSpecProxy.size());
    List<PartitionSpec> partitionSpecs = partSpecProxy.toPartitionSpec();
    List<Partition> partitions = partitionSpecs.get(0).getPartitionList().getPartitions();
    assertEquals(testValues.size(), partitions.size());

    for (int i = 0; i < partitions.size(); ++i) {
      assertEquals(testValues.get(i), partitions.get(i).getValues());
    }
  }

  private static void assertCorrectPartitionValuesResponse(List<List<String>> testValues,
                                         PartitionValuesResponse resp) throws Exception {
    assertEquals(testValues.size(), resp.getPartitionValuesSize());
    List<PartitionValuesRow> rowList = resp.getPartitionValues();
    for (int i = 0; i < rowList.size(); ++i) {
      PartitionValuesRow pvr = rowList.get(i);
      List<String> values = pvr.getRow();
      for (int j = 0; j < values.size(); ++j) {
        assertEquals(testValues.get(i).get(j), values.get(j));
      }
    }
  }



  /**
   * Testing listPartitions(String,String,short) ->
   *         get_partitions(String,String,short).
   */
  @Test
  public void testListPartitionsAll() throws Exception {
    List<List<String>> testValues = createTable4PartColsParts(client);
    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    assertPartitionsHaveCorrectValues(partitions, testValues);

    partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)1);
    assertPartitionsHaveCorrectValues(partitions, testValues.subList(0, 1));

    // HIVE-18977
    if (MetastoreConf.getBoolVar(metaStore.getConf(), MetastoreConf.ConfVars.TRY_DIRECT_SQL)) {
      partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short) 0);
      assertTrue(partitions.isEmpty());
    }

  }

  @Test(expected = MetaException.class)
  public void testListPartitionsAllHighMaxParts() throws Exception {
    createTable3PartCols1Part(client);
    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)101);
    assertTrue(partitions.isEmpty());
  }

  @Test
  public void testListPartitionsAllNoParts() throws Exception {
    Table t = createTestTable(client, DB_NAME, TABLE_NAME, Lists.newArrayList("yyyy", "mm", "dd"));
    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    assertTrue(partitions.isEmpty());
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsAllNoTable() throws Exception {
    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsAllNoDb() throws Exception {
    client.dropDatabase(DB_NAME);
    client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
  }

  @Ignore("tblName/dbName is empty")

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsAllNoDbName() throws Exception {
    createTable3PartCols1Part(client);
    client.listPartitions("", TABLE_NAME, (short)-1);
  }

  @Ignore("tblName/dbName is empty")
  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsAllNoTblName() throws Exception {
    createTable3PartCols1Part(client);
    client.listPartitions(DB_NAME, "", (short)-1);
  }

  @Ignore("tblName/dbName is null")
  @Test
  public void testListPartitionsAllNullTblName() throws Exception {
    try {
      createTable3PartCols1Part(client);
      List<Partition> partitions = client.listPartitions(DB_NAME,
          (String)null, (short)-1);
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Ignore("tblName/dbName is null")
  @Test
  public void testListPartitionsAllNullDbName() throws Exception {
    try {
      createTable3PartCols1Part(client);
      client.listPartitions(null, TABLE_NAME, (short)-1);
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }



  /**
   * Testing listPartitions(String,String,List(String),short) ->
   *         get_partitions(String,String,List(String),short).
   */
  @Test
  public void testListPartitionsByValues() throws Exception {
    List<List<String>> testValues = createTable4PartColsParts(client);

    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME,
            Lists.newArrayList("2017"), (short)-1);
    assertEquals(2, partitions.size());
    assertEquals(testValues.get(2), partitions.get(0).getValues());
    assertEquals(testValues.get(3), partitions.get(1).getValues());

    partitions = client.listPartitions(DB_NAME, TABLE_NAME,
            Lists.newArrayList("2017", "11"), (short)-1);
    assertEquals(1, partitions.size());
    assertEquals(testValues.get(3), partitions.get(0).getValues());

    partitions = client.listPartitions(DB_NAME, TABLE_NAME,
            Lists.newArrayList("20177", "11"), (short)-1);
    assertEquals(0, partitions.size());
  }

  @Ignore("client should check List is not empty")
  @Test(expected = MetaException.class)
  public void testListPartitionsByValuesNoVals() throws Exception {
    createTable3PartCols1Part(client);
    client.listPartitions(DB_NAME, TABLE_NAME, Lists.newArrayList(), (short)-1);
  }

  @Test(expected = MetaException.class)
  public void testListPartitionsByValuesTooManyVals() throws Exception {
    createTable3PartCols1Part(client);
    client.listPartitions(DB_NAME, TABLE_NAME, Lists.newArrayList("0", "1", "2", "3"), (short)-1);
  }

  @Ignore("dbName is empty")
  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsByValuesNoDbName() throws Exception {
    createTable3PartCols1Part(client);
    client.listPartitions("", TABLE_NAME, Lists.newArrayList("1999"), (short)-1);
  }

  @Ignore("tblName is null")
  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsByValuesNoTblName() throws Exception {
    createTable3PartCols1Part(client);
    client.listPartitions(DB_NAME, "", Lists.newArrayList("1999"), (short)-1);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsByValuesNoTable() throws Exception {
    client.listPartitions(DB_NAME, TABLE_NAME, Lists.newArrayList("1999"), (short)-1);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsByValuesNoDb() throws Exception {
    client.dropDatabase(DB_NAME);
    client.listPartitions(DB_NAME, TABLE_NAME, Lists.newArrayList("1999"), (short)-1);
  }

  @Ignore("tblName/dbName is null")
  @Test(expected = MetaException.class)
  public void testListPartitionsByValuesNullDbName() throws Exception {
    createTable3PartCols1Part(client);
    client.listPartitions(null, TABLE_NAME, Lists.newArrayList("1999"), (short)-1);
  }

  @Ignore("tblName/dbName is null")
  @Test(expected = MetaException.class)
  public void testListPartitionsByValuesNullTblName() throws Exception {
    createTable3PartCols1Part(client);
    client.listPartitions(DB_NAME, null, Lists.newArrayList("1999"), (short)-1);
  }

  @Ignore("client should check List is not empty")
  @Test(expected = MetaException.class)
  public void testListPartitionsByValuesNullValues() throws Exception {
    createTable3PartCols1Part(client);
    client.listPartitions(DB_NAME, TABLE_NAME, (List<String>)null, (short)-1);
  }



  /**
   * Testing listPartitionSpecs(String,String,int) ->
   *         get_partitions_pspec(String,String,int).
   */
  @Ignore("PartitionSpecs not support")
  @Test
  public void testListPartitionSpecs() throws Exception {
    List<List<String>> testValues = createTable4PartColsParts(client);

    PartitionSpecProxy partSpecProxy = client.listPartitionSpecs(DB_NAME, TABLE_NAME, -1);
    assertPartitionsSpecProxy(partSpecProxy, testValues);

    partSpecProxy = client.listPartitionSpecs(DB_NAME, TABLE_NAME, 2);
    assertPartitionsSpecProxy(partSpecProxy, testValues.subList(0, 2));

    // HIVE-18977
    if (MetastoreConf.getBoolVar(metaStore.getConf(), MetastoreConf.ConfVars.TRY_DIRECT_SQL)) {
      partSpecProxy = client.listPartitionSpecs(DB_NAME, TABLE_NAME, 0);
      assertPartitionsSpecProxy(partSpecProxy, testValues.subList(0, 0));
    }
  }

  @Ignore("PartitionSpecs not support")
  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionSpecsNoTable() throws Exception {
    client.listPartitionSpecs(DB_NAME, TABLE_NAME, -1);
  }

  @Ignore("PartitionSpecs not support")
  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionSpecsNoDb() throws Exception {
    client.dropDatabase(DB_NAME);
    client.listPartitionSpecs(DB_NAME, TABLE_NAME, -1);
  }

  @Ignore("PartitionSpecs not support")
  @Test(expected = MetaException.class)
  public void testListPartitionSpecsHighMaxParts() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionSpecs(DB_NAME, TABLE_NAME, 101);
  }

  @Ignore("dbName is empty")
  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionSpecsNoDbName() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionSpecs("", TABLE_NAME, -1);
  }

  @Ignore("tblName/dbName is empty")
  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionSpecsNoTblName() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionSpecs(DB_NAME, "", -1);
  }

  @Ignore("tblName/dbName is null")
  @Test
  public void testListPartitionSpecsNullDbName() throws Exception {
    try {
      createTable4PartColsParts(client);
      client.listPartitionSpecs(null, TABLE_NAME,  -1);
      fail("Should have thrown exception");
    } catch (MetaException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Ignore("PartitionSpecs not support")
  @Test
  public void testListPartitionSpecsNullTblName() throws Exception {
    try {
      createTable4PartColsParts(client);
      client.listPartitionSpecs(DB_NAME, null, -1);
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }



  /**
   * Testing listPartitionsWithAuthInfo(String,String,short,String,List(String)) ->
   *         get_partitions_with_auth(String,String,short,String,List(String)).
   */
  @Ignore("auth info not support")
  @Test
  public void testListPartitionsWithAuth() throws Exception {
    List<List<String>> partValues = createTable4PartColsPartsAuthOn(client);
    String user = "user0";
    List<String> groups = Lists.newArrayList("group0");
    List<Partition> partitions = client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, (short)-1,
            user, groups);

    assertEquals(4, partitions.size());
    assertPartitionsHaveCorrectValues(partitions, partValues);
    partitions.forEach(partition -> assertAuthInfoReturned(user, groups.get(0), partition));

    partitions = client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, (short)2, user, groups);
    assertEquals(2, partitions.size());
    assertPartitionsHaveCorrectValues(partitions, partValues.subList(0, 2));
    partitions.forEach(partition -> assertAuthInfoReturned(user, groups.get(0), partition));
  }

  @Ignore("auth info not support")
  @Test(expected = MetaException.class)
  public void testListPartitionsWithAuthHighMaxParts() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, (short)101, "", Lists.newArrayList());
  }

  @Test
  public void testListPartitionsWithAuthNoPrivilegesSet() throws Exception {
    List<List<String>> partValues = createTable4PartColsParts(client);
    List<Partition> partitions = client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, (short)-1,
            "", Lists.newArrayList());

    assertEquals(4, partitions.size());
    assertPartitionsHaveCorrectValues(partitions, partValues);
    partitions.forEach(partition -> assertNull(partition.getPrivileges()));
  }

  @Ignore("auth info not support")
  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsWithAuthNoDbName() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionsWithAuthInfo("", TABLE_NAME, (short)-1, "", Lists.newArrayList());
  }

  @Ignore("auth info not support")
  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsWithAuthNoTblName() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionsWithAuthInfo(DB_NAME, "", (short)-1, "", Lists.newArrayList());
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsWithAuthNoTable() throws Exception {
    client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, (short)-1, "", Lists.newArrayList());
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsWithAuthNoDb() throws Exception {
    client.dropDatabase(DB_NAME);
    client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, (short)-1, "", Lists.newArrayList());
  }

  @Ignore("auth info not support")
  @Test(expected = MetaException.class)
  public void testListPartitionsWithAuthNullDbName() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionsWithAuthInfo(null, TABLE_NAME, (short)-1, "", Lists.newArrayList());
  }

  @Ignore("auth info not support")
  @Test
  public void testListPartitionsWithAuthNullTblName() throws Exception {
    try {
      createTable4PartColsParts(client);
      client.listPartitionsWithAuthInfo(DB_NAME, (String)null, (short)-1, "",
          Lists.newArrayList());
      fail("Should have thrown exception");
    } catch (MetaException| TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Test
  public void testListPartitionsWithAuthNullUser() throws Exception {
    createTable4PartColsPartsAuthOn(client);
    client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, (short)-1, null, Lists.newArrayList());
  }

  @Test
  public void testListPartitionsWithAuthNullGroup() throws Exception {
    createTable4PartColsPartsAuthOn(client);
    client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, (short)-1, "user0", null);
  }



  /**
   * Testing listPartitionsWithAuthInfo(String,String,List(String),short,String,List(String)) ->
   *         get_partitions_ps_with_auth(String,String,List(String),short,String,List(String)).
   */
  @Ignore("not support auth info")
  @Test
  public void testListPartitionsWithAuthByValues() throws Exception {
    List<List<String>> partValues = createTable4PartColsPartsAuthOn(client);
    String user = "user0";
    List<String> groups = Lists.newArrayList("group0");

    List<Partition> partitions = client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, Lists
            .newArrayList("2017", "11", "27"), (short)-1, user, groups);
    assertEquals(1, partitions.size());
    assertPartitionsHaveCorrectValues(partitions, partValues.subList(3, 4));
    partitions.forEach(partition -> assertAuthInfoReturned(user, groups.get(0), partition));

    partitions = client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, Lists
            .newArrayList("2017"), (short)-1, user, groups);
    assertEquals(2, partitions.size());
    assertPartitionsHaveCorrectValues(partitions, partValues.subList(2, 4));
    partitions.forEach(partition -> assertAuthInfoReturned(user, groups.get(0), partition));

    partitions = client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, Lists
            .newArrayList("2017"), (short)1, user, groups);
    assertEquals(1, partitions.size());
    assertPartitionsHaveCorrectValues(partitions, partValues.subList(2, 3));
    partitions.forEach(partition -> assertAuthInfoReturned(user, groups.get(0), partition));

    partitions = client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, Lists
            .newArrayList("2013"), (short)-1, user, groups);
    assertTrue(partitions.isEmpty());
  }

  @Ignore("auth info not support")
  @Test(expected = MetaException.class)
  public void testListPartitionsWithAuthByValuesNoVals() throws Exception {
    createTable4PartColsPartsAuthOn(client);
    client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, Lists
            .newArrayList(), (short)-1, "", Lists.newArrayList());
  }


  @Test(expected = MetaException.class)
  public void testListPartitionsWithAuthByValuesTooManyVals() throws Exception {
    createTable4PartColsPartsAuthOn(client);
    client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, Lists
            .newArrayList("0", "1", "2", "3"), (short)-1, "", Lists.newArrayList());
  }

  @Test
  public void testListPartitionsWithAuthByValuesHighMaxParts() throws Exception {
    List<List<String>> partValues = createTable4PartColsParts(client);
    //This doesn't throw MetaException when setting to high max part count
    List<Partition> partitions = client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, Lists
            .newArrayList("2017"), (short)101, "", Lists.newArrayList());
    assertPartitionsHaveCorrectValues(partitions, partValues.subList(2, 4));
  }

  @Test(expected = MetaException.class)
  public void testListPartitionsWithAuthByValuesTooManyValsHighMaxParts() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, Lists
            .newArrayList("0", "1", "2", "3"), (short)101, "", Lists.newArrayList());
  }

  @Ignore("auth info not support")
  @Test
  public void testListPartitionsWithAuthByValuesNoPrivilegesSet() throws Exception {
    List<List<String>> partValues = createTable4PartColsPartsAuthOn(client);
    String user = "user0";
    List<String> groups = Lists.newArrayList("group0");
    List<Partition> partitions = client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, Lists
            .newArrayList("2017", "11", "27"), (short)-1, user, groups);

    assertEquals(1, partitions.size());
    assertPartitionsHaveCorrectValues(partitions, partValues.subList(3, 4));
    partitions.forEach(partition -> assertAuthInfoReturned(user, groups.get(0), partition));
  }

  @Ignore("tblName/dbName is null")
  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsWithAuthByValuesNoDbName() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionsWithAuthInfo("", TABLE_NAME, Lists
            .newArrayList("2017", "11", "27"), (short)-1, "", Lists.newArrayList());
  }

  @Ignore("auth info not support")
  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsWithAuthByValuesNoTblName() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionsWithAuthInfo(DB_NAME, "", Lists
            .newArrayList("2017", "11", "27"), (short)-1, "", Lists.newArrayList());
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsWithAuthByValuesNoTable() throws Exception {
    client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, Lists
            .newArrayList("2017", "11", "27"), (short)-1, "", Lists.newArrayList());
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsWithAuthByValuesNoDb() throws Exception {
    client.dropDatabase(DB_NAME);
    client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, Lists
            .newArrayList("2017", "11", "27"), (short)-1, "", Lists.newArrayList());
  }

  @Ignore("auth info not support")
  @Test
  public void testListPartitionsWithAuthByValuesNullDbName() throws Exception {
    try {
      createTable4PartColsParts(client);
      client.listPartitionsWithAuthInfo(null, TABLE_NAME, Lists
              .newArrayList("2017", "11", "27"), (short)-1, "", Lists.newArrayList());
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Ignore("auth info not support")
  @Test
  public void testListPartitionsWithAuthByValuesNullTblName() throws Exception {
    try {
      createTable4PartColsParts(client);
      client.listPartitionsWithAuthInfo(DB_NAME, null, Lists
              .newArrayList("2017", "11", "27"), (short)-1, "", Lists.newArrayList());
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Ignore("auth info not support")
  @Test(expected = MetaException.class)
  public void testListPartitionsWithAuthByValuesNullValues() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, (List<String>)null,
            (short)-1, "", Lists.newArrayList());
  }

  @Test
  public void testListPartitionsWithAuthByValuesNullUser() throws Exception {
    List<List<String>> partValues = createTable4PartColsPartsAuthOn(client);
    List<Partition> partitions = client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, Lists
            .newArrayList("2017", "11", "27"), (short)-1, null, Lists.newArrayList());
    assertPartitionsHaveCorrectValues(partitions, partValues.subList(3, 4));
  }

  @Test
  public void testListPartitionsWithAuthByValuesNullGroup() throws Exception {
    List<List<String>> partValues = createTable4PartColsPartsAuthOn(client);
    List<Partition> partitions = client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, Lists
            .newArrayList("2017", "11", "27"), (short)-1, "", null);
    assertPartitionsHaveCorrectValues(partitions, partValues.subList(3, 4));
  }



  /**
   * Testing listPartitionsByFilter(String,String,String,short) ->
   *         get_partitions_by_filter(String,String,String,short).
   */
  @Test
  public void testListPartitionsByFilter() throws Exception {
    List<List<String>> partValues = createTable4PartColsParts(client);
    List<Partition> partitions = client.listPartitionsByFilter(DB_NAME, TABLE_NAME,
            "yyyy=\"2017\" OR " + "mm=\"02\"", (short)-1);
    assertEquals(3, partitions.size());
    assertPartitionsHaveCorrectValues(partitions, partValues.subList(1, 4));

    partitions = client.listPartitionsByFilter(DB_NAME, TABLE_NAME,
            "yyyy=\"2017\" OR " + "mm=\"02\"", (short)2);
    assertEquals(2, partitions.size());
    assertPartitionsHaveCorrectValues(partitions, partValues.subList(1, 3));

    partitions = client.listPartitionsByFilter(DB_NAME, TABLE_NAME,
            "yyyy=\"2017\" OR " + "mm=\"02\"", (short)0);
    assertTrue(partitions.isEmpty());

    // HIVE-18977
    if (MetastoreConf.getBoolVar(metaStore.getConf(), MetastoreConf.ConfVars.TRY_DIRECT_SQL)) {
      partitions = client.listPartitionsByFilter(DB_NAME, TABLE_NAME,
          "yYyY=\"2017\"", (short) -1);
      assertPartitionsHaveCorrectValues(partitions, partValues.subList(2, 4));
    }

    partitions = client.listPartitionsByFilter(DB_NAME, TABLE_NAME,
            "yyyy=\"2017\" AND mm=\"99\"", (short)-1);
    assertTrue(partitions.isEmpty());
  }

  @Test(expected = MetaException.class)
  public void testListPartitionsByFilterInvalidFilter() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionsByFilter(DB_NAME, TABLE_NAME, "yyy=\"2017\"", (short)101);
  }

  @Test(expected = MetaException.class)
  public void testListPartitionsByFilterHighMaxParts() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionsByFilter(DB_NAME, TABLE_NAME, "yyyy=\"2017\"", (short)101);
  }

  @Ignore("tblName/dbName is empty")
  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsByFilterNoTblName() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionsByFilter(DB_NAME, "", "yyyy=\"2017\"", (short)-1);
  }

  @Ignore("tblName/dbName is empty")
  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsByFilterNoDbName() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionsByFilter("", TABLE_NAME, "yyyy=\"2017\"", (short)-1);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsByFilterNoTable() throws Exception {
    client.listPartitionsByFilter(DB_NAME, TABLE_NAME, "yyyy=\"2017\"", (short)-1);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsByFilterNoDb() throws Exception {
    client.dropDatabase(DB_NAME);
    client.listPartitionsByFilter(DB_NAME, TABLE_NAME, "yyyy=\"2017\"", (short)-1);
  }

  @Ignore("tblName/dbName is null")
  @Test
  public void testListPartitionsByFilterNullTblName() throws Exception {
    try {
      createTable4PartColsParts(client);
      client.listPartitionsByFilter(DB_NAME, null, "yyyy=\"2017\"", (short)-1);
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Ignore("tblName/dbName is null")
  @Test
  public void testListPartitionsByFilterNullDbName() throws Exception {
    try {
      createTable4PartColsParts(client);
      client.listPartitionsByFilter(null, TABLE_NAME, "yyyy=\"2017\"", (short)-1);
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Test
  public void testListPartitionsByFilterNullFilter() throws Exception {
    createTable4PartColsParts(client);
    List<Partition> partitions = client.listPartitionsByFilter(DB_NAME, TABLE_NAME, null,
            (short)-1);
    assertEquals(4, partitions.size());
  }

  @Test
  public void testListPartitionsByFilterEmptyFilter() throws Exception {
    createTable4PartColsParts(client);
    List<Partition> partitions = client.listPartitionsByFilter(DB_NAME, TABLE_NAME, "", (short)-1);
    assertEquals(4, partitions.size());
  }



  /**
   * Testing listPartitionSpecsByFilter(String,String,String,int) ->
   *         get_part_specs_by_filter(String,String,String,int).
   */
  @Ignore("PartitionSpecs not support")
  @Test
  public void testListPartitionsSpecsByFilter() throws Exception {
    List<List<String>> testValues = createTable4PartColsParts(client);
    PartitionSpecProxy partSpecProxy = client.listPartitionSpecsByFilter(DB_NAME, TABLE_NAME,
            "yyyy=\"2017\" OR " + "mm=\"02\"", -1);

    assertPartitionsSpecProxy(partSpecProxy, testValues.subList(1, 4));

    partSpecProxy = client.listPartitionSpecsByFilter(DB_NAME, TABLE_NAME,
            "yyyy=\"2017\" OR " + "mm=\"02\"", 2);
    assertPartitionsSpecProxy(partSpecProxy, testValues.subList(1, 3));

    partSpecProxy = client.listPartitionSpecsByFilter(DB_NAME, TABLE_NAME,
            "yyyy=\"2017\" OR " + "mm=\"02\"", 0);
    assertPartitionsSpecProxy(partSpecProxy, Lists.newArrayList());

    partSpecProxy = client.listPartitionSpecsByFilter(DB_NAME, TABLE_NAME,
            "yyyy=\"20177\"", -1);
    assertPartitionsSpecProxy(partSpecProxy, Lists.newArrayList());

    // HIVE-18977
    if (MetastoreConf.getBoolVar(metaStore.getConf(), MetastoreConf.ConfVars.TRY_DIRECT_SQL)) {
      partSpecProxy = client.listPartitionSpecsByFilter(DB_NAME, TABLE_NAME,
          "yYyY=\"2017\"", -1);
      assertPartitionsSpecProxy(partSpecProxy, testValues.subList(2, 4));
    }

    partSpecProxy = client.listPartitionSpecsByFilter(DB_NAME, TABLE_NAME,
            "yyyy=\"2017\" AND mm=\"99\"", -1);
    assertPartitionsSpecProxy(partSpecProxy, Lists.newArrayList());
  }

  @Ignore("PartitionSpecs not support")
  @Test(expected = MetaException.class)
  public void testListPartitionSpecsByFilterInvalidFilter() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionSpecsByFilter(DB_NAME, TABLE_NAME, "yyy=\"2017\"", 101);
  }

  @Ignore("PartitionSpecs not support")
  @Test(expected = MetaException.class)
  public void testListPartitionSpecsByFilterHighMaxParts() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionSpecsByFilter(DB_NAME, TABLE_NAME, "yyyy=\"2017\"", 101);
  }

  @Ignore("tblName/dbName is empty")
  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionSpecsByFilterNoTblName() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionSpecsByFilter(DB_NAME, "", "yyyy=\"2017\"", -1);
  }

  @Ignore("PartitionSpecs not support")
  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionSpecsByFilterNoDbName() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionSpecsByFilter("", TABLE_NAME, "yyyy=\"2017\"", -1);
  }

  @Ignore("PartitionSpecs not support")
  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionSpecsByFilterNoTable() throws Exception {
    client.listPartitionSpecsByFilter(DB_NAME, TABLE_NAME, "yyyy=\"2017\"", -1);
  }

  @Ignore("PartitionSpecs not support")
  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionSpecsByFilterNoDb() throws Exception {
    client.dropDatabase(DB_NAME);
    client.listPartitionSpecsByFilter(DB_NAME, TABLE_NAME, "yyyy=\"2017\"", -1);
  }

  @Ignore("PartitionSpecs not support")
  @Test(expected = MetaException.class)
  public void testListPartitionSpecsByFilterNullTblName() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionSpecsByFilter(DB_NAME, null, "yyyy=\"2017\"", -1);
  }

  @Ignore("PartitionSpecs not support")
  @Test(expected = MetaException.class)
  public void testListPartitionSpecsByFilterNullDbName() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionSpecsByFilter(null, TABLE_NAME, "yyyy=\"2017\"", -1);
  }

  @Ignore("PartitionSpecs not support")
  @Test
  public void testListPartitionSpecsByFilterNullFilter() throws Exception {
    List<List<String>> values = createTable4PartColsParts(client);
    PartitionSpecProxy pproxy = client.listPartitionSpecsByFilter(DB_NAME, TABLE_NAME, null, -1);
    assertPartitionsSpecProxy(pproxy, values);
  }

  @Ignore("PartitionSpecs not support")
  @Test
  public void testListPartitionSpecsByFilterEmptyFilter() throws Exception {
    List<List<String>> values = createTable4PartColsParts(client);
    PartitionSpecProxy pproxy = client.listPartitionSpecsByFilter(DB_NAME, TABLE_NAME, "", -1);
    assertPartitionsSpecProxy(pproxy, values);
  }



  /**
   * Testing getNumPartitionsByFilter(String,String,String) ->
   *         get_num_partitions_by_filter(String,String,String).
   */
  @Test
  public void testGetNumPartitionsByFilter() throws Exception {
    createTable4PartColsParts(client);
    int n = client.getNumPartitionsByFilter(DB_NAME, TABLE_NAME, "yyyy=\"2017\" OR " +
            "mm=\"02\"");
    assertEquals(3, n);

    n = client.getNumPartitionsByFilter(DB_NAME, TABLE_NAME, "");
    assertEquals(4, n);

    n = client.getNumPartitionsByFilter(DB_NAME, TABLE_NAME, "yyyy=\"20177\"");
    assertEquals(0, n);

    // HIVE-18977
    if (MetastoreConf.getBoolVar(metaStore.getConf(), MetastoreConf.ConfVars.TRY_DIRECT_SQL)) {
      n = client.getNumPartitionsByFilter(DB_NAME, TABLE_NAME, "yYyY=\"2017\"");
      assertEquals(2, n);
    }

    n = client.getNumPartitionsByFilter(DB_NAME, TABLE_NAME, "yyyy=\"2017\" AND mm=\"99\"");
    assertEquals(0, n);

  }

  @Test(expected = MetaException.class)
  public void testGetNumPartitionsByFilterInvalidFilter() throws Exception {
    createTable4PartColsParts(client);
    client.getNumPartitionsByFilter(DB_NAME, TABLE_NAME, "yyy=\"2017\"");
  }

  @Ignore("tblName/dbName is empty")
  @Test(expected = NoSuchObjectException.class)
  public void testGetNumPartitionsByFilterNoTblName() throws Exception {
    createTable4PartColsParts(client);
    client.getNumPartitionsByFilter(DB_NAME, "", "yyyy=\"2017\"");
  }

  @Ignore("db is null")
  @Test(expected = NoSuchObjectException.class)
  public void testGetNumPartitionsByFilterNoDbName() throws Exception {
    createTable4PartColsParts(client);
    client.getNumPartitionsByFilter("", TABLE_NAME, "yyyy=\"2017\"");
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetNumPartitionsByFilterNoTable() throws Exception {
    client.getNumPartitionsByFilter(DB_NAME, TABLE_NAME, "yyyy=\"2017\"");
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetNumPartitionsByFilterNoDb() throws Exception {
    client.dropDatabase(DB_NAME);
    client.getNumPartitionsByFilter(DB_NAME, TABLE_NAME, "yyyy=\"2017\"");
  }

  @Ignore("tblName/dbName is null")
  @Test
  public void testGetNumPartitionsByFilterNullTblName() throws Exception {
    try {
      createTable4PartColsParts(client);
      client.getNumPartitionsByFilter(DB_NAME, null, "yyyy=\"2017\"");
      fail("Should have thrown exception");
    } catch (MetaException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Ignore("tblName/dbName is null")
  @Test(expected = MetaException.class)
  public void testGetNumPartitionsByFilterNullDbName() throws Exception {
    createTable4PartColsParts(client);
    client.getNumPartitionsByFilter(null, TABLE_NAME, "yyyy=\"2017\"");
  }

  @Test
  public void testGetNumPartitionsByFilterNullFilter() throws Exception {
    createTable4PartColsParts(client);
    int n = client.getNumPartitionsByFilter(DB_NAME, TABLE_NAME, null);
    assertEquals(4, n);
  }



  /**
   * Testing listPartitionNames(String,String,short) ->
   *         get_partition_names(String,String,short).
   */
  @Test
  public void testListPartitionNames() throws Exception {
    List<List<String>> testValues = createTable4PartColsParts(client);
    List<String> partitionNames = client.listPartitionNames(DB_NAME, TABLE_NAME, (short)-1);
    assertCorrectPartitionNames(partitionNames, testValues, Lists.newArrayList("yyyy", "mm",
            "dd"));

    partitionNames = client.listPartitionNames(DB_NAME, TABLE_NAME, (short)2);
    assertCorrectPartitionNames(partitionNames, testValues.subList(0, 2),
            Lists.newArrayList("yyyy", "mm", "dd"));

    partitionNames = client.listPartitionNames(DB_NAME, TABLE_NAME, (short)0);
    assertTrue(partitionNames.isEmpty());

    //This method does not depend on MetastoreConf.LIMIT_PARTITION_REQUEST setting:
    partitionNames = client.listPartitionNames(DB_NAME, TABLE_NAME, (short)101);
    assertCorrectPartitionNames(partitionNames, testValues, Lists.newArrayList("yyyy", "mm",
            "dd"));

  }

  @Ignore("tblName/dbName is empty")
  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionNamesNoDbName() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionNames("", TABLE_NAME, (short)-1);
  }

  @Ignore("tblName/dbName is empty")
  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionNamesNoTblName() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionNames(DB_NAME, "", (short)-1);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionNamesNoTable() throws Exception {
    client.listPartitionNames(DB_NAME, TABLE_NAME, (short)-1);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionNamesNoDb() throws Exception {
    client.dropDatabase(DB_NAME);
    client.listPartitionNames(DB_NAME, TABLE_NAME, (short)-1);
  }

  @Ignore("tblName/dbName is null")
  @Test
  public void testListPartitionNamesNullDbName() throws Exception {
    try {
      createTable4PartColsParts(client);
      client.listPartitionNames(null, TABLE_NAME, (short)-1);
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Ignore("tblName/dbName is null")
  @Test
  public void testListPartitionNamesNullTblName() throws Exception {
    try {
      createTable4PartColsParts(client);
      client.listPartitionNames(DB_NAME, (String)null, (short)-1);
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }



  /**
   * Testing listPartitionNames(String,String,List(String),short) ->
   *         get_partition_names_ps(String,String,List(String),short).
   */
  @Test
  public void testListPartitionNamesByValues() throws Exception {
    List<List<String>> testValues = createTable4PartColsParts(client);
    List<String> partitionNames = client.listPartitionNames(DB_NAME, TABLE_NAME,
            Lists.newArrayList("2017"), (short)-1);
    assertCorrectPartitionNames(partitionNames, testValues.subList(2, 4),
            Lists.newArrayList("yyyy", "mm", "dd"));

    partitionNames = client.listPartitionNames(DB_NAME, TABLE_NAME,
            Lists.newArrayList("2017"), (short)101);
    assertCorrectPartitionNames(partitionNames, testValues.subList(2, 4),
            Lists.newArrayList("yyyy", "mm", "dd"));

    partitionNames = client.listPartitionNames(DB_NAME, TABLE_NAME,
            Lists.newArrayList("2017"), (short)1);
    assertCorrectPartitionNames(partitionNames, testValues.subList(2, 3),
            Lists.newArrayList("yyyy", "mm", "dd"));

    partitionNames = client.listPartitionNames(DB_NAME, TABLE_NAME,
            Lists.newArrayList("2017"), (short)0);
    assertTrue(partitionNames.isEmpty());

    partitionNames = client.listPartitionNames(DB_NAME, TABLE_NAME,
            Lists.newArrayList("2017", "10"), (short)-1);
    assertCorrectPartitionNames(partitionNames, testValues.subList(2, 3),
            Lists.newArrayList("yyyy", "mm", "dd"));

  }

  @Test
  public void testListPartitionNamesByValuesMaxPartCountUnlimited() throws Exception {
    List<List<String>> testValues = createTable4PartColsParts(client);
    //TODO: due to value 101 this probably should throw an exception
    List<String> partitionNames = client.listPartitionNames(DB_NAME, TABLE_NAME,
            Lists.newArrayList("2017"), (short) 101);
    assertCorrectPartitionNames(partitionNames, testValues.subList(2, 4),
            Lists.newArrayList("yyyy", "mm", "dd"));
  }

  @Ignore("client should check List is not empty")
  @Test(expected = MetaException.class)
  public void testListPartitionNamesByValuesNoPartVals() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionNames(DB_NAME, TABLE_NAME, Lists.newArrayList(), (short)-1);
  }

  @Test(expected = MetaException.class)
  public void testListPartitionNamesByValuesTooManyVals() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionNames(DB_NAME, TABLE_NAME, Lists.newArrayList("1", "2", "3", "4"),
            (short)-1);
  }

  @Ignore("tblName/dbName is empty")

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionNamesByValuesNoDbName() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionNames("", TABLE_NAME, Lists.newArrayList("2017"), (short)-1);
  }

  @Ignore("tblName/dbName is empty")
  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionNamesByValuesNoTblName() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionNames(DB_NAME, "", Lists.newArrayList("2017"), (short)-1);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionNamesByValuesNoTable() throws Exception {
    client.listPartitionNames(DB_NAME, TABLE_NAME, Lists.newArrayList("2017"), (short)-1);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionNamesByValuesNoDb() throws Exception {
    client.dropDatabase(DB_NAME);
    client.listPartitionNames(DB_NAME, TABLE_NAME, Lists.newArrayList("2017"), (short)-1);
  }

  @Ignore("tblName/dbName is null")
  @Test
  public void testListPartitionNamesByValuesNullDbName() throws Exception {
    try {
      createTable4PartColsParts(client);
      client.listPartitionNames(null, TABLE_NAME, Lists.newArrayList("2017"), (short) -1);
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Ignore("tblName/dbName is null")
  @Test
  public void testListPartitionNamesByValuesNullTblName() throws Exception {
    try {
      createTable4PartColsParts(client);
      client.listPartitionNames(DB_NAME, null, Lists.newArrayList("2017"), (short)-1);
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Ignore("PartitionSpecs not support")
  @Test(expected = MetaException.class)
  public void testListPartitionNamesByValuesNullValues() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionNames(DB_NAME, TABLE_NAME, (List<String>)null, (short)-1);
  }



  /**
   * Testing listPartitionValues(PartitionValuesRequest) ->
   *         get_partition_values(PartitionValuesRequest).
   */
  @Ignore("not support")
  @Test
  public void testListPartitionValues() throws Exception {
    List<List<String>> testValues = createTable4PartColsParts(client);
    List<FieldSchema> partitionSchema = Lists.newArrayList(
            new FieldSchema("yyyy", "string", ""),
            new FieldSchema("mm", "string", ""));

    PartitionValuesRequest request = new PartitionValuesRequest(DB_NAME, TABLE_NAME,
            partitionSchema);
    PartitionValuesResponse response = client.listPartitionValues(request);
    assertCorrectPartitionValuesResponse(testValues, response);

  }

  @Ignore("listPartition values not support")
  @Test
  public void testListPartitionValuesEmptySchema() throws Exception {
    try {
      List<List<String>> testValues = createTable4PartColsParts(client);
      List<FieldSchema> partitionSchema = Lists.newArrayList();

      PartitionValuesRequest request = new PartitionValuesRequest(DB_NAME, TABLE_NAME,
              partitionSchema);
      client.listPartitionValues(request);
      fail("Should have thrown exception");
    } catch (IndexOutOfBoundsException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Ignore("tblName/dbName is empty")
  @Test(expected = MetaException.class)
  public void testListPartitionValuesNoDbName() throws Exception {
    createTable4PartColsParts(client);
    List<FieldSchema> partitionSchema = Lists.newArrayList(
            new FieldSchema("yyyy", "string", ""),
            new FieldSchema("mm", "string", ""));

    PartitionValuesRequest request = new PartitionValuesRequest("", TABLE_NAME,
            partitionSchema);
    client.listPartitionValues(request);
  }

  @Ignore("tblName/dbName is empty")
  @Test(expected = MetaException.class)
  public void testListPartitionValuesNoTblName() throws Exception {
    createTable4PartColsParts(client);
    List<FieldSchema> partitionSchema = Lists.newArrayList(
            new FieldSchema("yyyy", "string", ""),
            new FieldSchema("mm", "string", ""));

    PartitionValuesRequest request = new PartitionValuesRequest(DB_NAME, "",
            partitionSchema);
    client.listPartitionValues(request);
  }

  @Ignore("not support")
  @Test(expected = MetaException.class)
  public void testListPartitionValuesNoTable() throws Exception {
    List<FieldSchema> partitionSchema = Lists.newArrayList(
            new FieldSchema("yyyy", "string", ""),
            new FieldSchema("mm", "string", ""));

    PartitionValuesRequest request = new PartitionValuesRequest(DB_NAME, TABLE_NAME,
            partitionSchema);
    client.listPartitionValues(request);
  }

  @Ignore("listPartition values not support")
  @Test(expected = MetaException.class)
  public void testListPartitionValuesNoDb() throws Exception {
    client.dropDatabase(DB_NAME);
    List<FieldSchema> partitionSchema = Lists.newArrayList(
            new FieldSchema("yyyy", "string", ""),
            new FieldSchema("mm", "string", ""));

    PartitionValuesRequest request = new PartitionValuesRequest(DB_NAME, TABLE_NAME,
            partitionSchema);
    client.listPartitionValues(request);
  }

  @Ignore("listPartitionValues not support")
  @Test
  public void testListPartitionValuesNullDbName() throws Exception {
    try {
      createTable4PartColsParts(client);
      List<FieldSchema> partitionSchema = Lists.newArrayList(
              new FieldSchema("yyyy", "string", ""),
              new FieldSchema("mm", "string", ""));

      PartitionValuesRequest request = new PartitionValuesRequest(null, TABLE_NAME,
              partitionSchema);
      client.listPartitionValues(request);
      fail("Should have thrown exception");
    } catch (NullPointerException | TProtocolException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Ignore("listPartition values not support")
  @Test
  public void testListPartitionValuesNullTblName() throws Exception {
    try {
      createTable4PartColsParts(client);
      List<FieldSchema> partitionSchema = Lists.newArrayList(
              new FieldSchema("yyyy", "string", ""),
              new FieldSchema("mm", "string", ""));

      PartitionValuesRequest request = new PartitionValuesRequest(DB_NAME, null,
              partitionSchema);
      client.listPartitionValues(request);
      fail("Should have thrown exception");
    } catch (NullPointerException | TProtocolException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Ignore("listPartition values not support")
  @Test
  public void testListPartitionValuesNullSchema() throws Exception {
    try {
      createTable4PartColsParts(client);
      PartitionValuesRequest request = new PartitionValuesRequest(DB_NAME, TABLE_NAME,
              null);
      client.listPartitionValues(request);
      fail("Should have thrown exception");
    } catch (NullPointerException | TProtocolException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Ignore("not support")
  @Test
  public void testListPartitionValuesNullRequest() throws Exception {
    try {
      createTable4PartColsParts(client);
      client.listPartitionValues(null);
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Test
  public void otherCatalog() throws TException {
    String catName = "list_partition_catalog";
    Catalog cat = new CatalogBuilder()
        .setName(catName)
        .setLocation(MetaStoreTestUtils.getTestWarehouseDir(catName))
        .build();
    client.createCatalog(cat);

    String dbName = "list_partition_database_in_other_catalog";
    Database db = new DatabaseBuilder()
        .setName(dbName)
        .setCatalogName(catName)
        .create(client, metaStore.getConf());

    String tableName = "table_in_other_catalog";
    Table table = new TableBuilder()
        .inDb(db)
        .setTableName(tableName)
        .addCol("id", "int")
        .addCol("name", "string")
        .addPartCol("partcol", "string")
        .create(client, metaStore.getConf());

    Partition[] parts = new Partition[5];
    for (int i = 0; i < parts.length; i++) {
      parts[i] = new PartitionBuilder()
          .inTable(table)
          .addValue("a" + i)
          .build(metaStore.getConf());
    }
    client.add_partitions(Arrays.asList(parts));

    List<Partition> fetched = client.listPartitions(catName, dbName, tableName, -1);
    Assert.assertEquals(parts.length, fetched.size());
    Assert.assertEquals(catName, fetched.get(0).getCatName());

    fetched = client.listPartitions(catName, dbName, tableName,
        Collections.singletonList("a0"), -1);
    Assert.assertEquals(1, fetched.size());
    Assert.assertEquals(catName, fetched.get(0).getCatName());

//    PartitionSpecProxy proxy = client.listPartitionSpecs(catName, dbName, tableName, -1);
//    Assert.assertEquals(parts.length, proxy.size());
//    Assert.assertEquals(catName, proxy.getCatName());

    fetched = client.listPartitionsByFilter(catName, dbName, tableName, "partcol=\"a0\"", -1);
    Assert.assertEquals(1, fetched.size());
    Assert.assertEquals(catName, fetched.get(0).getCatName());

//    proxy = client.listPartitionSpecsByFilter(catName, dbName, tableName, "partcol=\"a0\"", -1);
//    Assert.assertEquals(1, proxy.size());
//    Assert.assertEquals(catName, proxy.getCatName());

    Assert.assertEquals(1, client.getNumPartitionsByFilter(catName, dbName, tableName,
        "partcol=\"a0\""));

    List<String> names = client.listPartitionNames(catName, dbName, tableName, 57);
    Assert.assertEquals(parts.length, names.size());

    names = client.listPartitionNames(catName, dbName, tableName, Collections.singletonList("a0"),
        Short.MAX_VALUE + 1);
    Assert.assertEquals(1, names.size());

    PartitionValuesRequest rqst = new PartitionValuesRequest(dbName,
        tableName, Lists.newArrayList(new FieldSchema("partcol", "string", "")));
    rqst.setCatName(catName);
//    PartitionValuesResponse rsp = client.listPartitionValues(rqst);
//    Assert.assertEquals(5, rsp.getPartitionValuesSize());
  }

  @Test(expected = NoSuchObjectException.class)
  public void listPartitionsBogusCatalog() throws TException {
    createTable3PartCols1Part(client);
    client.listPartitions("bogus", DB_NAME, TABLE_NAME, -1);
  }

  @Test(expected = NoSuchObjectException.class)
  public void listPartitionsWithPartialValuesBogusCatalog() throws TException {
    createTable3PartCols1Part(client);
    client.listPartitions("bogus", DB_NAME, TABLE_NAME, Collections.singletonList("a0"), -1);
  }

  @Ignore("PartitionSpecs not support")
  @Test(expected = NoSuchObjectException.class)
  public void listPartitionsSpecsBogusCatalog() throws TException {
    createTable3PartCols1Part(client);
    client.listPartitionSpecs("bogus", DB_NAME, TABLE_NAME, -1);
  }

  @Test(expected = NoSuchObjectException.class)
  public void listPartitionsByFilterBogusCatalog() throws TException {
    createTable3PartCols1Part(client);
    client.listPartitionsByFilter("bogus", DB_NAME, TABLE_NAME, "partcol=\"a0\"", -1);
  }

  @Ignore("PartitionSpecs not support")
  @Test(expected = NoSuchObjectException.class)
  public void listPartitionSpecsByFilterBogusCatalog() throws TException {
    createTable3PartCols1Part(client);
    client.listPartitionSpecsByFilter("bogus", DB_NAME, TABLE_NAME, "partcol=\"a0\"", -1);
  }

  @Test(expected = NoSuchObjectException.class)
  public void getNumPartitionsByFilterBogusCatalog() throws TException {
    createTable3PartCols1Part(client);
    client.getNumPartitionsByFilter("bogus", DB_NAME, TABLE_NAME, "partcol=\"a0\"");
  }

  @Test(expected = NoSuchObjectException.class)
  public void listPartitionNamesBogusCatalog() throws TException {
    createTable3PartCols1Part(client);
    client.listPartitionNames("bogus", DB_NAME, TABLE_NAME, -1);
  }

  @Test(expected = NoSuchObjectException.class)
  public void listPartitionNamesPartialValsBogusCatalog() throws TException {
    createTable3PartCols1Part(client);
    client.listPartitionNames("bogus", DB_NAME, TABLE_NAME, Collections.singletonList("a0"), -1);
  }

  @Ignore("catalog catalog not check")
  @Test(expected = MetaException.class)
  public void listPartitionValuesBogusCatalog() throws TException {
    createTable3PartCols1Part(client);
    PartitionValuesRequest rqst = new PartitionValuesRequest(DB_NAME,
        TABLE_NAME, Lists.newArrayList(new FieldSchema("partcol", "string", "")));
    rqst.setCatName("bogus");
    client.listPartitionValues(rqst);
  }
}
