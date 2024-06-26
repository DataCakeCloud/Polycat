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

import static java.lang.Thread.sleep;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.polycat.hivesdk.hive3.PolyCatMetaStoreClientTest;
import io.polycat.hivesdk.hive3.PolyCatTestUtils;
import io.polycat.hivesdk.hive3.MetaStoreTestUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.CatalogBuilder;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ConfigurableApplicationContext;

@Disabled("some Hive feature, Catalog not support yet.")
public class HiveOriginalTest {

    private static final String NO_CAT = "DO_NOT_USE_A_CATALOG!";
    private static final Logger LOG = LoggerFactory.getLogger(PolyCatMetaStoreClientTest.class);
    public static final String LOCATION = "/location/uri";
    public static final String TBL_TEST_LOCATION = "/location/uri";
    public static final String TEST_DESCRIPTION = "alter description";
    public static final String SERDE_LIB = "hive.serde.lib";
    public static final String SERDE_NAME = "";
    public static final String OUTPUT_FORMAT = "output.format";
    public static final String INPUT_FORMAT = "input.format";
    public static final String TEST_COL1 = "col1";
    public static final String COL1_TYPE = "string";
    public static final String COL1_COMMENT = "empty";
    public static final String TEST_COL2 = "col2";
    public static final String COL2_TYPE = "string";
    public static final String COL2_COMMENT = "empty";
    public static final String OWNER = "owner";
    public static final String DEFAULT_CATALOG_NAME = "hive";
    public static final String TEST_TYPE = "polyCat";
    protected static IMetaStoreClient client;
    protected static Warehouse warehouse;
    protected static Configuration conf;
    protected static ConfigurableApplicationContext catalogApp;

    private static final String CATALOG_TEST_NAME = "testc1";
    private static final String DB_TEST_NAME = "testdb1";
    private static final String TBL_TEST_NAME = "testtbl1";
    private static final String CATALOG_NAME = "c1";
    private static final String DB1_NAME = "db1";
    private static final String DB2_NAME = "db2";
    private static final String DEFAULT_DB = "default";
    private static final String TBL1_NAME = "tbl1";
    private static final String TBL2_NAME = "tbl2";
    private static final String DESCRIPTION = "description";
    private static final String DB1_LOCATION = LOCATION + "/" + DB1_NAME;
    private static final String DB_TEST_LOCATION = LOCATION + "/" + DB_TEST_NAME;
    private static final String DB2_LOCATION = LOCATION + "/" + DB2_NAME;

    protected boolean isThriftClient = false;

    @BeforeAll
    public static void setupEnv() throws Exception {
        PolyCatTestUtils.runHiveMetaStore(TEST_TYPE);
        catalogApp = PolyCatTestUtils.runCatalogApp(TEST_TYPE);
        conf = PolyCatTestUtils.createConf(TEST_TYPE);
        warehouse = PolyCatTestUtils.createWarehouse(TEST_TYPE);
        client = PolyCatTestUtils.createHMSClient(TEST_TYPE);
        PolyCatTestUtils.clearTestEnv(TEST_TYPE);
    }
    @AfterAll
    public static void tearDown() throws TException {
        client.close();
        catalogApp.close();
        try {
            sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @BeforeEach
    public void deleteEnv() {
        try {
            MetaStoreTestUtils.dropCatalogCascade(client, "cat_table_stats");
            MetaStoreTestUtils.dropCatalogCascade(client, NO_CAT);
        } catch (TException e) {
        }
    }

    @Test
    public void testAlterTable() throws Exception {
        String dbName = "alterdb";
        String invTblName = "alter-tbl";
        String tblName = "altertbl";

        try {
            client.dropTable(dbName, tblName);
            silentDropDatabase(dbName);

            new DatabaseBuilder()
                .setName(dbName)
                .create(client, conf);

            ArrayList<FieldSchema> invCols = new ArrayList<>(2);
            invCols.add(new FieldSchema("n-ame", ColumnType.STRING_TYPE_NAME, ""));
            invCols.add(new FieldSchema("in.come", ColumnType.INT_TYPE_NAME, ""));

            Table tbl = new TableBuilder()
                .setDbName(dbName)
                .setTableName(invTblName)
                .setCols(invCols)
                .build(conf);

            boolean failed = false;
            try {
                client.createTable(tbl);
            } catch (InvalidObjectException ex) {
                failed = true;
            }
            if (!failed) {
                assertTrue("Able to create table with invalid name: " + invTblName,
                    false);
            }

            // create an invalid table which has wrong column type
            ArrayList<FieldSchema> invColsInvType = new ArrayList<>(2);
            invColsInvType.add(new FieldSchema("name", ColumnType.STRING_TYPE_NAME, ""));
            invColsInvType.add(new FieldSchema("income", "xyz", ""));
            tbl.setTableName(tblName);
            tbl.getSd().setCols(invColsInvType);
            boolean failChecker = false;
            try {
                client.createTable(tbl);
            } catch (InvalidObjectException ex) {
                failChecker = true;
            }
            if (!failChecker) {
                assertTrue("Able to create table with invalid column type: " + invTblName,
                    false);
            }

            ArrayList<FieldSchema> cols = new ArrayList<>(2);
            cols.add(new FieldSchema("name", ColumnType.STRING_TYPE_NAME, ""));
            cols.add(new FieldSchema("income", ColumnType.INT_TYPE_NAME, ""));

            // create a valid table
            tbl.setTableName(tblName);
            tbl.getSd().setCols(cols);
            client.createTable(tbl);

            if (isThriftClient) {
                tbl = client.getTable(tbl.getDbName(), tbl.getTableName());
            }

            // now try to invalid alter table
            Table tbl2 = client.getTable(dbName, tblName);
            failed = false;
            try {
                tbl2.setTableName(invTblName);
                tbl2.getSd().setCols(invCols);
                client.alter_table(dbName, tblName, tbl2);
            } catch (InvalidOperationException ex) {
                failed = true;
            }
            if (!failed) {
                assertTrue("Able to rename table with invalid name: " + invTblName,
                    false);
            }

            //try an invalid alter table with partition key name
            Table tbl_pk = client.getTable(tbl.getDbName(), tbl.getTableName());
            List<FieldSchema> partitionKeys = tbl_pk.getPartitionKeys();
            for (FieldSchema fs : partitionKeys) {
                fs.setName("invalid_to_change_name");
                fs.setComment("can_change_comment");
            }
            tbl_pk.setPartitionKeys(partitionKeys);
            try {
                client.alter_table(dbName, tblName, tbl_pk);
            } catch (InvalidOperationException ex) {
                failed = true;
            }
            assertTrue("Should not have succeeded in altering partition key name", failed);

            //try a valid alter table partition key comment
            failed = false;
            tbl_pk = client.getTable(tbl.getDbName(), tbl.getTableName());
            partitionKeys = tbl_pk.getPartitionKeys();
            for (FieldSchema fs : partitionKeys) {
                fs.setComment("can_change_comment");
            }
            tbl_pk.setPartitionKeys(partitionKeys);
            try {
                client.alter_table(dbName, tblName, tbl_pk);
            } catch (InvalidOperationException ex) {
                failed = true;
            }
            assertFalse("Should not have failed alter table partition comment", failed);
            Table newT = client.getTable(tbl.getDbName(), tbl.getTableName());
            assertEquals(partitionKeys, newT.getPartitionKeys());

            // try a valid alter table
            tbl2.setTableName(tblName + "_renamed");
            tbl2.getSd().setCols(cols);
            tbl2.getSd().setNumBuckets(32);
            client.alter_table(dbName, tblName, tbl2);
            Table tbl3 = client.getTable(dbName, tbl2.getTableName());
            assertEquals("Alter table didn't succeed. Num buckets is different ",
                tbl2.getSd().getNumBuckets(), tbl3.getSd().getNumBuckets());
            // check that data has moved
            FileSystem fs = FileSystem.get((new Path(tbl.getSd().getLocation())).toUri(), conf);
            assertFalse("old table location still exists", fs.exists(new Path(tbl
                .getSd().getLocation())));
            assertTrue("data did not move to new location", fs.exists(new Path(tbl3
                .getSd().getLocation())));

            if (!isThriftClient) {
                assertEquals("alter table didn't move data correct location", tbl3
                    .getSd().getLocation(), tbl2.getSd().getLocation());
            }

            // alter table with invalid column type
            tbl_pk.getSd().setCols(invColsInvType);
            failed = false;
            try {
                client.alter_table(dbName, tbl2.getTableName(), tbl_pk);
            } catch (InvalidOperationException ex) {
                failed = true;
            }
            assertTrue("Should not have succeeded in altering column", failed);

        } catch (Exception e) {
            System.err.println(StringUtils.stringifyException(e));
            System.err.println("testSimpleTable() failed.");
            throw e;
        } finally {
            silentDropDatabase(dbName);
        }
    }

    private static void silentDropDatabase(String dbName) throws TException {
        try {
            for (String tableName : client.getTables(dbName, "*")) {
                client.dropTable(dbName, tableName);
            }
            client.dropDatabase(dbName);
        } catch (NoSuchObjectException |InvalidOperationException e) {
            // NOP
        }
    }


    private Map<String, Column> buildAllColumns() {
        Map<String, Column> colMap = new HashMap<>(6);
        Column[] cols = { new BinaryColumn(), new BooleanColumn(), new DateColumn(),
            new DoubleColumn(),  new LongColumn(), new StringColumn() };
        for (Column c : cols) colMap.put(c.colName, c);
        return colMap;
    }

    private List<String> createMetadata(String catName, String dbName, String tableName,
        String partKey, List<String> partVals,
        Map<String, Column> colMap)
        throws TException {
        if (!DEFAULT_CATALOG_NAME.equals(catName) && !NO_CAT.equals(catName)) {
            Catalog cat = new CatalogBuilder()
                .setName(catName)
                .setLocation(MetaStoreTestUtils.getTestWarehouseDir(catName))
                .build();
            client.createCatalog(cat);
        }

        Database db;
        if (!DEFAULT_DATABASE_NAME.equals(dbName)) {
            DatabaseBuilder dbBuilder = new DatabaseBuilder()
                .setName(dbName);
            if (!NO_CAT.equals(catName)) dbBuilder.setCatalogName(catName);
            db = dbBuilder.create(client, conf);
        } else {
            db = client.getDatabase(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME);
        }

        TableBuilder tb = new TableBuilder()
            .inDb(db)
            .setTableName(tableName);

        for (Column col : colMap.values()) tb.addCol(col.colName, col.colType);

        if (partKey != null) {
            assert partVals != null && !partVals.isEmpty() :
                "Must provide partition values for partitioned table";
            tb.addPartCol(partKey, ColumnType.STRING_TYPE_NAME);
        }
        Table table = tb.create(client, conf);

        if (partKey != null) {
            for (String partVal : partVals) {
                new PartitionBuilder()
                    .inTable(table)
                    .addValue(partVal)
                    .addToTable(client, conf);
            }
        }

        SetPartitionsStatsRequest rqst = new SetPartitionsStatsRequest();
        List<String> partNames = new ArrayList<>();
        if (partKey == null) {
            rqst.addToColStats(buildStatsForOneTableOrPartition(catName, dbName, tableName, null,
                colMap.values()));
        } else {
            for (String partVal : partVals) {
                String partName = partKey + "=" + partVal;
                rqst.addToColStats(buildStatsForOneTableOrPartition(catName, dbName, tableName, partName,
                    colMap.values()));
                partNames.add(partName);
            }
        }
        client.setPartitionColumnStatistics(rqst);
        return partNames;
    }

    private ColumnStatistics buildStatsForOneTableOrPartition(String catName, String dbName,
        String tableName, String partName,
        Collection<Column> cols) {
        ColumnStatisticsDesc desc = new ColumnStatisticsDesc(partName == null, dbName, tableName);
        if (!NO_CAT.equals(catName)) desc.setCatName(catName);
        if (partName != null) desc.setPartName(partName);

        List<ColumnStatisticsObj> objs = new ArrayList<>(cols.size());

        for (Column col : cols) objs.add(col.generate());

        return new ColumnStatistics(desc, objs);
    }

    private void dropStats(String catName, String dbName, String tableName, String partName,
        Collection<String> colNames)
        throws TException {
        for (String colName : colNames) {
            if (partName == null) {
                if (NO_CAT.equals(catName)) client.deleteTableColumnStatistics(dbName, tableName, colName);
                else client.deleteTableColumnStatistics(catName, dbName, tableName, colName);
            } else {
                if (NO_CAT.equals(catName)) client.deletePartitionColumnStatistics(dbName, tableName, partName, colName);
                else client.deletePartitionColumnStatistics(catName, dbName, tableName, partName, colName);
            }
        }
    }

    private void compareStatsForTable(String catName, String dbName, String tableName,
        Map<String, Column> colMap) throws TException {
        List<ColumnStatisticsObj> objs = catName.equals(NO_CAT) ?
            client.getTableColumnStatistics(dbName, tableName, new ArrayList<>(colMap.keySet())) :
            client.getTableColumnStatistics(catName, dbName, tableName, new ArrayList<>(colMap.keySet()));
        compareStatsForOneTableOrPartition(objs, 0, colMap);
    }

    private void compareStatsForPartitions(String catName, String dbName, String tableName,
        List<String> partNames, final Map<String, Column> colMap)
        throws TException {
        Map<String, List<ColumnStatisticsObj>> partObjs = catName.equals(NO_CAT) ?
            client.getPartitionColumnStatistics(dbName, tableName, partNames, new ArrayList<>(colMap.keySet())) :
            client.getPartitionColumnStatistics(catName, dbName, tableName, partNames, new ArrayList<>(colMap.keySet()));
        for (int i = 0; i < partNames.size(); i++) {
            compareStatsForOneTableOrPartition(partObjs.get(partNames.get(i)), i, colMap);
        }
        AggrStats aggr = catName.equals(NO_CAT) ?
            client.getAggrColStatsFor(dbName, tableName, new ArrayList<>(colMap.keySet()), partNames) :
            client.getAggrColStatsFor(catName, dbName, tableName, new ArrayList<>(colMap.keySet()), partNames);
        Assert.assertEquals(partNames.size(), aggr.getPartsFound());
        Assert.assertEquals(colMap.size(), aggr.getColStatsSize());
        aggr.getColStats().forEach(cso -> colMap.get(cso.getColName()).compareAggr(cso));
    }

    private void compareStatsForOneTableOrPartition(List<ColumnStatisticsObj> objs,
        final int partOffset,
        final Map<String, Column> colMap)
        throws TException {
        Assert.assertEquals(objs.size(), colMap.size());
        objs.forEach(cso -> colMap.get(cso.getColName()).compare(cso, partOffset));
    }

    @Test
    public void tableInHiveCatalog() throws TException {
        String dbName = "db_table_stats";
        String tableName = "table_in_default_db_stats";
        Map<String, Column> colMap = buildAllColumns();
        createMetadata(DEFAULT_CATALOG_NAME, dbName, tableName, null, null, colMap);
        compareStatsForTable(DEFAULT_CATALOG_NAME, dbName, tableName, colMap);
        dropStats(DEFAULT_CATALOG_NAME, dbName, tableName, null, colMap.keySet());
    }

    @Ignore("HIVE-19509: Disable tests that are failing continuously")
    @Test
    public void partitionedTableInHiveCatalog() throws TException {
        String dbName = "db_part_stats";
        client.dropDatabase(dbName, true, true, true);
        String tableName = "partitioned_table_in_default_db_stats";
        Map<String, Column> colMap = buildAllColumns();
        List<String> partNames = createMetadata(DEFAULT_CATALOG_NAME, dbName, tableName, "pk",
            Arrays.asList("a1", "a2", "a3"), colMap);
        compareStatsForPartitions(DEFAULT_CATALOG_NAME, dbName, tableName, partNames, colMap);
        for (String partName : partNames) {
            dropStats(DEFAULT_CATALOG_NAME, dbName, tableName, partName, colMap.keySet());
        }
    }

    @Test
    public void tableOtherCatalog() throws TException {
        String catName = "cat_table_stats";
        String dbName = "other_cat_db_table_stats";
        String tableName = "table_in_default_db_stats";
        Map<String, Column> colMap = buildAllColumns();
        createMetadata(catName, dbName, tableName, null, null, colMap);
        compareStatsForTable(catName, dbName, tableName, colMap);
        dropStats(catName, dbName, tableName, null, colMap.keySet());
    }

    @Ignore("HIVE-19509: Disable tests that are failing continuously")
    @Test
    public void partitionedTableOtherCatalog() throws TException {
        String catName = "cat_table_stats";
        String dbName = "other_cat_db_part_stats";
        String tableName = "partitioned_table_in_default_db_stats";
        Map<String, Column> colMap = buildAllColumns();
        List<String> partNames = createMetadata(catName, dbName, tableName, "pk",
            Arrays.asList("a1", "a2", "a3"), colMap);
        compareStatsForPartitions(catName, dbName, tableName, partNames, colMap);
        for (String partName : partNames) {
            dropStats(catName, dbName, tableName, partName, colMap.keySet());
        }
    }

    @Test
    public void tableDeprecatedCalls() throws TException {
        String dbName = "old_db_table_stats";
        String tableName = "table_in_default_db_stats";
        Map<String, Column> colMap = buildAllColumns();
        createMetadata(NO_CAT, dbName, tableName, null, null, colMap);
        compareStatsForTable(NO_CAT, dbName, tableName, colMap);
        dropStats(NO_CAT, dbName, tableName, null, colMap.keySet());
    }

    @Ignore("HIVE-19509: Disable tests that are failing continuously")
    @Test
    public void partitionedTableDeprecatedCalls() throws TException {
        String dbName = "old_db_part_stats";
        String tableName = "partitioned_table_in_default_db_stats";
        Map<String, Column> colMap = buildAllColumns();
        List<String> partNames = createMetadata(NO_CAT, dbName, tableName, "pk",
            Arrays.asList("a1", "a2", "a3"), colMap);
        compareStatsForPartitions(NO_CAT, dbName, tableName, partNames, colMap);
        for (String partName : partNames) {
            dropStats(NO_CAT, dbName, tableName, partName, colMap.keySet());
        }
    }

    private abstract class Column {
        final String colName;
        final String colType;

        Random rand = new Random();

        List<Long> maxLens, numNulls, numDvs;
        List<Double> avgLens;


        public Column(String colName, String colType) {
            this.colName = colName;
            this.colType = colType;
            maxLens = new ArrayList<>();
            numNulls = new ArrayList<>();
            avgLens = new ArrayList<>();
            numDvs = new ArrayList<>();
        }

        abstract ColumnStatisticsObj generate();
        abstract void compare(ColumnStatisticsObj obj, int offset);
        abstract void compareAggr(ColumnStatisticsObj obj);

        void compareCommon(ColumnStatisticsObj obj) {
            Assert.assertEquals(colName, obj.getColName());
            Assert.assertEquals(colType, obj.getColType());
        }

        long genMaxLen() {
            return genPositiveLong(maxLens);
        }

        long getMaxLen() {
            return maxLong(maxLens);
        }

        long genNumNulls() {
            return genPositiveLong(numNulls);
        }

        long genNumDvs() {
            return genPositiveLong(numDvs);
        }

        long getNumNulls() {
            return sumLong(numNulls);
        }

        long getNumDvs() {
            return maxLong(numDvs);
        }

        double genAvgLens() {
            return genDouble(avgLens);
        }

        double getAvgLen() {
            return maxDouble(avgLens);
        }

        protected long genNegativeLong(List<Long> addTo) {
            long val = rand.nextInt(100);
            if (val > 0) val *= -1;
            addTo.add(val);
            return val;
        }

        protected long genPositiveLong(List<Long> addTo) {
            long val = rand.nextInt(100);
            val = Math.abs(val) + 1; // make sure it isn't 0
            addTo.add(val);
            return val;
        }

        protected long maxLong(List<Long> maxOf) {
            long max = Long.MIN_VALUE;
            for (long maybe : maxOf) max = Math.max(max, maybe);
            return max;
        }

        protected long sumLong(List<Long> sumOf) {
            long sum = 0;
            for (long element : sumOf) sum += element;
            return sum;
        }

        protected double genDouble(List<Double> addTo) {
            double val = rand.nextDouble() * rand.nextInt(100);
            addTo.add(val);
            return val;
        }

        protected double maxDouble(List<Double> maxOf) {
            double max = Double.MIN_VALUE;
            for (double maybe : maxOf) max = Math.max(max, maybe);
            return max;
        }

    }

    private class BinaryColumn extends Column {
        public BinaryColumn() {
            super("bincol", ColumnType.BINARY_TYPE_NAME);
        }

        @Override
        ColumnStatisticsObj generate() {
            BinaryColumnStatsData binData = new BinaryColumnStatsData(genMaxLen(), genAvgLens(), genNumNulls());
            ColumnStatisticsData data = new ColumnStatisticsData();
            data.setBinaryStats(binData);
            return new ColumnStatisticsObj(colName, colType, data);
        }

        @Override
        void compare(ColumnStatisticsObj obj, int offset) {
            compareCommon(obj);
            Assert.assertEquals("binary max length", maxLens.get(offset),
                (Long)obj.getStatsData().getBinaryStats().getMaxColLen());
            Assert.assertEquals("binary min length", avgLens.get(offset), obj.getStatsData().getBinaryStats().getAvgColLen(), 0.01);
            Assert.assertEquals("binary num nulls", numNulls.get(offset), (Long)obj.getStatsData().getBinaryStats().getNumNulls());
        }

        @Override
        void compareAggr(ColumnStatisticsObj obj) {
            compareCommon(obj);
            Assert.assertEquals("aggr binary max length", getMaxLen(), obj.getStatsData().getBinaryStats().getMaxColLen());
            Assert.assertEquals("aggr binary min length", getAvgLen(), obj.getStatsData().getBinaryStats().getAvgColLen(), 0.01);
            Assert.assertEquals("aggr binary num nulls", getNumNulls(), obj.getStatsData().getBinaryStats().getNumNulls());
        }
    }

    private class BooleanColumn extends Column {
        private List<Long> numTrues, numFalses;

        public BooleanColumn() {
            super("boolcol", ColumnType.BOOLEAN_TYPE_NAME);
            numTrues = new ArrayList<>();
            numFalses = new ArrayList<>();
        }

        @Override
        ColumnStatisticsObj generate() {
            BooleanColumnStatsData
                boolData = new BooleanColumnStatsData(genNumTrues(), genNumFalses(), genNumNulls());
            ColumnStatisticsData data = new ColumnStatisticsData();
            data.setBooleanStats(boolData);
            return new ColumnStatisticsObj(colName, colType, data);
        }

        @Override
        void compare(ColumnStatisticsObj obj, int offset) {
            compareCommon(obj);
            Assert.assertEquals("boolean num trues", numTrues.get(offset), (Long)obj.getStatsData().getBooleanStats().getNumTrues());
            Assert.assertEquals("boolean num falses", numFalses.get(offset), (Long)obj.getStatsData().getBooleanStats().getNumFalses());
            Assert.assertEquals("boolean num nulls", numNulls.get(offset), (Long)obj.getStatsData().getBooleanStats().getNumNulls());
        }

        @Override
        void compareAggr(ColumnStatisticsObj obj) {
            compareCommon(obj);
            Assert.assertEquals("aggr boolean num trues", getNumTrues(), obj.getStatsData().getBooleanStats().getNumTrues());
            Assert.assertEquals("aggr boolean num falses", getNumFalses(), obj.getStatsData().getBooleanStats().getNumFalses());
            Assert.assertEquals("aggr boolean num nulls", getNumNulls(), obj.getStatsData().getBooleanStats().getNumNulls());
        }

        private long genNumTrues() {
            return genPositiveLong(numTrues);
        }

        private long genNumFalses() {
            return genPositiveLong(numFalses);
        }

        private long getNumTrues() {
            return sumLong(numTrues);
        }

        private long getNumFalses() {
            return sumLong(numFalses);
        }
    }

    private class DateColumn extends Column {
        private List<Date> lowVals, highVals;

        public DateColumn() {
            super("datecol", ColumnType.DATE_TYPE_NAME);
            lowVals = new ArrayList<>();
            highVals = new ArrayList<>();
        }

        @Override
        ColumnStatisticsObj generate() {
            DateColumnStatsData dateData = new DateColumnStatsData(genNumNulls(), genNumDvs());
            dateData.setLowValue(genLowValue());
            dateData.setHighValue(genHighValue());
            ColumnStatisticsData data = new ColumnStatisticsData();
            data.setDateStats(dateData);
            return new ColumnStatisticsObj(colName, colType, data);
        }

        @Override
        void compare(ColumnStatisticsObj obj, int offset) {
            compareCommon(obj);
            Assert.assertEquals("date num nulls", numNulls.get(offset), (Long)obj.getStatsData().getDateStats().getNumNulls());
            Assert.assertEquals("date num dvs", numDvs.get(offset), (Long)obj.getStatsData().getDateStats().getNumDVs());
            Assert.assertEquals("date low val", lowVals.get(offset), obj.getStatsData().getDateStats().getLowValue());
            Assert.assertEquals("date high val", highVals.get(offset), obj.getStatsData().getDateStats().getHighValue());
        }

        @Override
        void compareAggr(ColumnStatisticsObj obj) {
            compareCommon(obj);
            Assert.assertEquals("aggr date num nulls", getNumNulls(), obj.getStatsData().getDateStats().getNumNulls());
            Assert.assertEquals("aggr date num dvs", getNumDvs(), obj.getStatsData().getDateStats().getNumDVs());
            Assert.assertEquals("aggr date low val", getLowVal(), obj.getStatsData().getDateStats().getLowValue());
            Assert.assertEquals("aggr date high val", getHighVal(), obj.getStatsData().getDateStats().getHighValue());
        }

        private Date genLowValue() {
            Date d = new Date(rand.nextInt(100) * -1);
            lowVals.add(d);
            return d;
        }

        private Date genHighValue() {
            Date d = new Date(rand.nextInt(200));
            highVals.add(d);
            return d;
        }

        private Date getLowVal() {
            long min = Long.MAX_VALUE;
            for (Date d : lowVals) min = Math.min(min, d.getDaysSinceEpoch());
            return new Date(min);
        }

        private Date getHighVal() {
            long max = Long.MIN_VALUE;
            for (Date d : highVals) max = Math.max(max, d.getDaysSinceEpoch());
            return new Date(max);
        }
    }

    private class DoubleColumn extends Column {
        List<Double> lowVals, highVals;

        public DoubleColumn() {
            super("doublecol", ColumnType.DOUBLE_TYPE_NAME);
            lowVals = new ArrayList<>();
            highVals = new ArrayList<>();
        }

        @Override
        ColumnStatisticsObj generate() {
            DoubleColumnStatsData doubleData = new DoubleColumnStatsData(genNumNulls(), genNumDvs());
            doubleData.setLowValue(genLowVal());
            doubleData.setHighValue(genHighVal());
            ColumnStatisticsData data = new ColumnStatisticsData();
            data.setDoubleStats(doubleData);
            return new ColumnStatisticsObj(colName, colType, data);
        }

        @Override
        void compare(ColumnStatisticsObj obj, int offset) {
            compareCommon(obj);
            Assert.assertEquals("double num nulls", numNulls.get(offset),
                (Long)obj.getStatsData().getDoubleStats().getNumNulls());
            Assert.assertEquals("double num dvs", numDvs.get(offset),
                (Long)obj.getStatsData().getDoubleStats().getNumDVs());
            Assert.assertEquals("double low val", lowVals.get(offset),
                obj.getStatsData().getDoubleStats().getLowValue(), 0.01);
            Assert.assertEquals("double high val", highVals.get(offset),
                obj.getStatsData().getDoubleStats().getHighValue(), 0.01);
        }

        @Override
        void compareAggr(ColumnStatisticsObj obj) {
            compareCommon(obj);
            Assert.assertEquals("aggr double num nulls", getNumNulls(),
                obj.getStatsData().getDoubleStats().getNumNulls());
            Assert.assertEquals("aggr double num dvs", getNumDvs(),
                obj.getStatsData().getDoubleStats().getNumDVs());
            Assert.assertEquals("aggr double low val", getLowVal(),
                obj.getStatsData().getDoubleStats().getLowValue(), 0.01);
            Assert.assertEquals("aggr double high val", getHighVal(),
                obj.getStatsData().getDoubleStats().getHighValue(), 0.01);

        }

        private double genLowVal() {
            return genDouble(lowVals);
        }

        private double genHighVal() {
            return genDouble(highVals);
        }

        private double getLowVal() {
            double min = Double.MAX_VALUE;
            for (Double d : lowVals) min = Math.min(min, d);
            return min;
        }

        private double getHighVal() {
            return maxDouble(highVals);
        }
    }

    private class LongColumn extends Column {
        List<Long> lowVals, highVals;

        public LongColumn() {
            super("bigintcol", ColumnType.BIGINT_TYPE_NAME);
            lowVals = new ArrayList<>();
            highVals = new ArrayList<>();
        }

        @Override
        ColumnStatisticsObj generate() {
            LongColumnStatsData longData = new LongColumnStatsData(genNumNulls(), genNumDvs());
            longData.setLowValue(genLowVal());
            longData.setHighValue(genHighVal());
            ColumnStatisticsData data = new ColumnStatisticsData();
            data.setLongStats(longData);
            return new ColumnStatisticsObj(colName, colType, data);
        }

        @Override
        void compare(ColumnStatisticsObj obj, int offset) {
            compareCommon(obj);
            Assert.assertEquals("long num nulls", numNulls.get(offset),
                (Long)obj.getStatsData().getLongStats().getNumNulls());
            Assert.assertEquals("long num dvs", numDvs.get(offset),
                (Long)obj.getStatsData().getLongStats().getNumDVs());
            Assert.assertEquals("long low val", (long)lowVals.get(offset),
                obj.getStatsData().getLongStats().getLowValue());
            Assert.assertEquals("long high val", (long)highVals.get(offset),
                obj.getStatsData().getLongStats().getHighValue());
        }

        @Override
        void compareAggr(ColumnStatisticsObj obj) {
            compareCommon(obj);
            Assert.assertEquals("aggr long num nulls", getNumNulls(),
                obj.getStatsData().getLongStats().getNumNulls());
            Assert.assertEquals("aggr long num dvs", getNumDvs(),
                obj.getStatsData().getLongStats().getNumDVs());
            Assert.assertEquals("aggr long low val", getLowVal(),
                obj.getStatsData().getLongStats().getLowValue());
            Assert.assertEquals("aggr long high val", getHighVal(),
                obj.getStatsData().getLongStats().getHighValue());
        }

        private long genLowVal() {
            return genNegativeLong(lowVals);
        }

        private long genHighVal() {
            return genPositiveLong(highVals);
        }

        private long getLowVal() {
            long min = Long.MAX_VALUE;
            for (Long val : lowVals) min = Math.min(min, val);
            return min;
        }

        private long getHighVal() {
            return maxLong(highVals);
        }
    }

    private class StringColumn extends Column {
        public StringColumn() {
            super("strcol", ColumnType.STRING_TYPE_NAME);
        }

        @Override
        ColumnStatisticsObj generate() {
            StringColumnStatsData strData = new StringColumnStatsData(genMaxLen(), genAvgLens(),
                genNumNulls(), genNumDvs());
            ColumnStatisticsData data = new ColumnStatisticsData();
            data.setStringStats(strData);
            return new ColumnStatisticsObj(colName, colType, data);
        }

        @Override
        void compare(ColumnStatisticsObj obj, int offset) {
            compareCommon(obj);
            Assert.assertEquals("str num nulls", numNulls.get(offset),
                (Long)obj.getStatsData().getStringStats().getNumNulls());
            Assert.assertEquals("str num dvs", numDvs.get(offset),
                (Long)obj.getStatsData().getStringStats().getNumDVs());
            Assert.assertEquals("str low val", (long)maxLens.get(offset),
                obj.getStatsData().getStringStats().getMaxColLen());
            Assert.assertEquals("str high val", avgLens.get(offset),
                obj.getStatsData().getStringStats().getAvgColLen(), 0.01);
        }

        @Override
        void compareAggr(ColumnStatisticsObj obj) {
            compareCommon(obj);
            Assert.assertEquals("aggr str num nulls", getNumNulls(),
                obj.getStatsData().getStringStats().getNumNulls());
            Assert.assertEquals("aggr str num dvs", getNumDvs(),
                obj.getStatsData().getStringStats().getNumDVs());
            Assert.assertEquals("aggr str low val", getMaxLen(),
                obj.getStatsData().getStringStats().getMaxColLen());
            Assert.assertEquals("aggr str high val", getAvgLen(),
                obj.getStatsData().getStringStats().getAvgColLen(), 0.01);

        }
    }
    // statsTest
}
