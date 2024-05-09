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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.util.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestEmbeddedHmsBridge extends TestHiveMetaStore {

  @Override
  public void testSingleMethod() throws Throwable {
//    super.testNameMethods();
    super.testPartition();
//    super.testListPartitions();
//    super.testListPartitionsWihtLimitEnabled();
//    super.testAlterTableCascade();
//    super.testListPartitionNames();
//    super.testDropTable();
//    super.testAlterViewParititon();

//    super.testAlterPartition();
//    //super.testRenamePartition();
//    super.testDatabase();
//    super.testDatabaseLocationWithPermissionProblems();
//    super.testDatabaseLocation();
//    super.testSimpleTypeApi();
//    super.testComplexTypeApi();
//    super.testSimpleTable();
//    super.testStatsFastTrivial();
//    super.testColumnStatistics();

//    super.testGetSchemaWithNoClassDefFoundError();
//    super.testAlterTable();
//    super.testComplexTable();
//    super.testTableDatabase();
//    super.testGetConfigValue();
//    super.testPartitionFilter();
//    super.testFilterSinglePartition();
//    super.testFilterLastPartition();
//    super.testSynchronized();
//    super.testTableFilter();
//    super.testConcurrentMetastores();
//    super.testSimpleFunction();
//    super.testFunctionWithResources();
//    super.testDBOwner();
//    super.testDBOwnerChange();
//    super.testGetTableObjects();

    // TODO MV relation db and table doesn't delete.
//    super.testDropDatabaseCascadeMVMultiDB();
//    super.testDBLocationChange();
//    super.testRetriableClientWithConnLifetime();
//    super.testJDOPersistanceManagerCleanup();
//    super.testValidateTableCols();
//    super.testGetMetastoreUuid();
//    super.testGetUUIDInParallel();
//    super.testPartitionOps_statistics();
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    warehouse = new Warehouse(conf);
    try {
      // 此参数避开未实现的接口，临时方案  -1   10
      conf.setInt(HiveConf.ConfVars.METASTORE_LIMIT_PARTITION_REQUEST.varname, DEFAULT_LIMIT_PARTITION_REQUEST);
      conf.setInt("metastore.limit.partition.request", DEFAULT_LIMIT_PARTITION_REQUEST);
      client = new HiveMetaStoreClient(conf);
      System.out.println("HiveMetaStoreClient create success.");
    } catch (Throwable e) {
      System.err.println("Unable to open the metastore");
      System.err.println(StringUtils.stringifyException(e));
      throw new Exception(e);
    }
  }

  @After
  public void tearDown() throws Exception {
    client.close();
  }


}
