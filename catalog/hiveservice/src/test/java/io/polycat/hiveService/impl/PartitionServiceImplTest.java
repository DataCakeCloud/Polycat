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
package io.polycat.hiveService.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.model.PartitionAlterContext;
import io.polycat.catalog.common.model.Column;
import io.polycat.catalog.common.model.StorageDescriptor;
import io.polycat.catalog.common.model.TableName;
import io.polycat.catalog.common.model.Partition;
import io.polycat.catalog.common.model.base.PartitionInput;
import io.polycat.catalog.common.model.stats.AggrStatisticData;
import io.polycat.catalog.common.model.stats.ColumnStatistics;
import io.polycat.catalog.common.model.stats.ColumnStatisticsDesc;
import io.polycat.catalog.common.model.stats.ColumnStatisticsObj;
import io.polycat.catalog.common.model.stats.Decimal;
import io.polycat.catalog.common.model.stats.DecimalColumnStatsData;
import io.polycat.catalog.common.model.stats.StringColumnStatsData;
import io.polycat.catalog.common.plugin.request.input.AlterPartitionInput;
import io.polycat.catalog.common.plugin.request.input.ColumnStatisticsInput;
import io.polycat.catalog.common.plugin.request.input.DropPartitionByValuesInput;
import io.polycat.catalog.common.plugin.request.input.DropPartitionInput;
import io.polycat.catalog.common.plugin.request.input.GetPartitionWithAuthInput;
import io.polycat.catalog.common.plugin.request.input.GetPartitionsWithAuthInput;
import io.polycat.catalog.common.plugin.request.input.PartitionDescriptorInput;
import io.polycat.catalog.common.plugin.request.input.PartitionFilterInput;
import io.polycat.catalog.common.plugin.request.input.AddPartitionInput;
import io.polycat.catalog.common.plugin.request.input.SetPartitionColumnStatisticsInput;
import io.polycat.catalog.common.utils.PartitionUtil;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

// listPartitionByExpr not test
public class PartitionServiceImplTest extends TestUtil {

    public static final String PARTITION_PART_URL = "column1=value1/column2=value20";
    public static final String PARTITION_PART_URL2 = "column1=value1/column2=value21";
    public static final String PARTITION_PART_URL3 = "column1=value1/column2=value22";
    public static final String FULL_PATH = "/full/path/";
    public static final String INPUT_FORMAT = "input.format";
    public static final String OUTPUT_FORMAT = "output.format";
    public static final String LOCATION_FILE_PREFIX = "file:%s";
    public static final String COLUMN1_VALUE = "value1";
    public static final ArrayList<String> PARTITION1_VALUES = new ArrayList<String>() {{
        add("value1");
        add("value20");
    }};
    public static final ArrayList<String> PARTITION2_VALUES = new ArrayList<String>() {{
        add("value1");
        add("value21");
    }};

    @Test
    public void should_create_partition_success() {
        TableName tableName = new TableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, TABLE_NAME);
        String[] partNames = new String[]{PARTITION_PART_URL};
        AddPartitionInput partInput = buildPartitionInput(tableName, partNames);
        partitionService.addPartition(tableName, partInput);
        Partition actual = partitionService.getPartitionByName(tableName, PARTITION_PART_URL);
        assertSamePartition(partInput.getPartitions()[0], actual);
    }

    @Test
    public void should_append_partition_success() {
        TableName tableName = new TableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, TABLE_NAME);
        PartitionDescriptorInput input = new PartitionDescriptorInput();
        input.setPartName(PARTITION_PART_URL);
        // append partition 1
        partitionService.appendPartition(tableName, input);

        // append partition 2
        input.setValues(PARTITION2_VALUES);
        partitionService.appendPartition(tableName, input);

        // then
        Partition actual = partitionService.getPartitionByName(tableName, PARTITION_PART_URL);
        assertEquals(PartitionUtil.convertNameToVals(PARTITION_PART_URL), actual.getPartitionValues());
        actual = partitionService.getPartitionByName(tableName, PARTITION_PART_URL2);
        assertEquals(PartitionUtil.convertNameToVals(PARTITION_PART_URL2), actual.getPartitionValues());
    }

    @Test
    public void should_add_partitions_by_name_success() {
        TableName tableName = new TableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, TABLE_NAME);
        String[] partitionNames = new String[]{PARTITION_PART_URL, PARTITION_PART_URL2, PARTITION_PART_URL3};
        AddPartitionInput partInput = buildPartitionInput(tableName, partitionNames);
        partitionService.addPartitionsBackResult(tableName, partInput);

        PartitionFilterInput filter = new PartitionFilterInput();
        filter.setMaxParts((short) 2);
        filter.setPartNames(new String[]{PARTITION_PART_URL, PARTITION_PART_URL2, PARTITION_PART_URL3});
        Partition[] actuals = partitionService.getPartitionsByNames(tableName, filter);

        assertEquals(3, actuals.length);
        PartitionInput[] expectBases = partInput.getPartitions();
        for (int i = 0; i < 3; i++) {
            assertSamePartition(expectBases[i], actuals[i]);
        }
    }

    @Test
    public void should_add_partition_with_auth_success() {
        TableName tableName = new TableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, TABLE_NAME);
        String[] partitionNames = new String[]{PARTITION_PART_URL, PARTITION_PART_URL2, PARTITION_PART_URL3};
        AddPartitionInput partInput = buildPartitionInput(tableName, partitionNames);
        partitionService.addPartitions(tableName, partInput);

        GetPartitionWithAuthInput filter = new GetPartitionWithAuthInput();
        filter.setPartVals(PARTITION1_VALUES);
        filter.setGroupNames(Collections.emptyList());
        filter.setUserName("");
        Partition actual = partitionService.getPartitionWithAuth(tableName, filter);

        assertSamePartition(partInput.getPartitions()[0], actual);
    }

    @Test
    public void should_add_partitions_by_name_no_return_success() {
        TableName tableName = new TableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, TABLE_NAME);
        String[] partitionNames = new String[]{PARTITION_PART_URL, PARTITION_PART_URL2, PARTITION_PART_URL3};
        AddPartitionInput partInput = buildPartitionInput(tableName, partitionNames);
        partitionService.addPartitions(tableName, partInput);

        PartitionFilterInput filter = new PartitionFilterInput();
        filter.setMaxParts((short) 2);
        filter.setPartNames(new String[]{PARTITION_PART_URL, PARTITION_PART_URL2, PARTITION_PART_URL3});
        Partition[] actuals = partitionService.getPartitionsByNames(tableName, filter);

        assertEquals(3, actuals.length);
        PartitionInput[] expectBases = partInput.getPartitions();
        for (int i = 0; i < 3; i++) {
            assertSamePartition(expectBases[i], actuals[i]);
        }
    }

    @Test
    public void should_list_partitions_without_values_success() {
        TableName tableName = new TableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, TABLE_NAME);
        String[] partitionNames = new String[]{PARTITION_PART_URL, PARTITION_PART_URL2, PARTITION_PART_URL3};
        AddPartitionInput partInput = buildPartitionInput(tableName, partitionNames);
        partitionService.addPartitions(tableName, partInput);

        PartitionFilterInput filter = new PartitionFilterInput();
        filter.setMaxParts((short) 10);
        Partition[] actuals = partitionService.listPartitions(tableName, filter);

        assertEquals(3, actuals.length);
        PartitionInput[] expectBases = partInput.getPartitions();
        for (int i = 0; i < 3; i++) {
            assertSamePartition(expectBases[i], actuals[i]);
        }
    }

    @Test
    public void should_list_partitions_with_values_success() {
        TableName tableName = new TableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, TABLE_NAME);
        String[] partitionNames = new String[]{PARTITION_PART_URL, PARTITION_PART_URL2, PARTITION_PART_URL3};
        AddPartitionInput partInput = buildPartitionInput(tableName, partitionNames);
        partitionService.addPartitions(tableName, partInput);

        PartitionFilterInput filter = new PartitionFilterInput();
        filter.setMaxParts((short) 10);
        filter.setValues(new String[]{"value1"});
        Partition[] actuals = partitionService.listPartitions(tableName, filter);

        assertEquals(3, actuals.length);
        PartitionInput[] expectBases = partInput.getPartitions();
        for (int i = 0; i < 3; i++) {
            assertSamePartition(expectBases[i], actuals[i]);
        }
    }

    @Test
    public void should_list_partitions_names_success() {
        TableName tableName = new TableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, TABLE_NAME);
        String[] partitionNames = new String[]{PARTITION_PART_URL, PARTITION_PART_URL2, PARTITION_PART_URL3};
        AddPartitionInput partInput = buildPartitionInput(tableName, partitionNames);
        partitionService.addPartitions(tableName, partInput);
        PartitionFilterInput filterInput = new PartitionFilterInput();
        filterInput.setMaxParts(10);
        String[] actuals = partitionService.listPartitionNames(tableName, filterInput, false);

        assertEquals(3, actuals.length);
        for (int i = 0; i < 3; i++) {
            assertEquals(partitionNames[i], actuals[i]);
        }
    }

    @Test
    public void should_list_partitions_names_with_values_success() {
        TableName tableName = new TableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, TABLE_NAME);
        String[] partitionNames = new String[]{PARTITION_PART_URL, PARTITION_PART_URL2, PARTITION_PART_URL3};
        AddPartitionInput partInput = buildPartitionInput(tableName, partitionNames);
        partitionService.addPartitions(tableName, partInput);

        PartitionFilterInput filter = new PartitionFilterInput();
        filter.setMaxParts((short) 10);
        filter.setValues(new String[]{"value1"});
        String[] actuals = partitionService.listPartitionNamesPs(tableName, filter);

        assertEquals(3, actuals.length);
        for (int i = 0; i < 3; i++) {
            assertEquals(partitionNames[i], actuals[i]);
        }
    }

    @Test
    public void should_list_partitions_with_auth_success() {
        TableName tableName = new TableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, TABLE_NAME);
        String[] partitionNames = new String[]{PARTITION_PART_URL, PARTITION_PART_URL2, PARTITION_PART_URL3};
        AddPartitionInput partInput = buildPartitionInput(tableName, partitionNames);
        partitionService.addPartitions(tableName, partInput);

        GetPartitionsWithAuthInput filter = new GetPartitionsWithAuthInput();
        filter.setMaxParts((short) 10);
        filter.setValues(Collections.EMPTY_LIST);
        filter.setGroupNames(Collections.EMPTY_LIST);
        filter.setUserName("");
        Partition[] actuals = partitionService.listPartitionsPsWithAuth(tableName, filter);

        assertEquals(3, actuals.length);
        PartitionInput[] expectBases = partInput.getPartitions();
        for (int i = 0; i < 3; i++) {
            assertSamePartition(expectBases[i], actuals[i]);
        }
    }

    @Test
    public void should_list_partitions_with_values_and_auth_success() {
        TableName tableName = new TableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, TABLE_NAME);
        String[] partitionNames = new String[]{PARTITION_PART_URL, PARTITION_PART_URL2, PARTITION_PART_URL3};
        AddPartitionInput partInput = buildPartitionInput(tableName, partitionNames);
        partitionService.addPartitions(tableName, partInput);

        GetPartitionsWithAuthInput filter = new GetPartitionsWithAuthInput();
        filter.setMaxParts((short) 10);
        filter.setValues(Collections.singletonList(COLUMN1_VALUE));
        filter.setGroupNames(Collections.EMPTY_LIST);
        filter.setUserName("");
        Partition[] actuals = partitionService.listPartitionsPsWithAuth(tableName, filter);

        assertEquals(3, actuals.length);
        PartitionInput[] expectBases = partInput.getPartitions();
        for (int i = 0; i < 3; i++) {
            assertSamePartition(expectBases[i], actuals[i]);
        }
    }

    @Test
    public void should_get_partitions_by_filter_success() {
        TableName tableName = new TableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, TABLE_NAME);
        String[] partitionNames = new String[]{PARTITION_PART_URL, PARTITION_PART_URL2, PARTITION_PART_URL3};
        AddPartitionInput partInput = buildPartitionInput(tableName, partitionNames);
        partitionService.addPartitions(tableName, partInput);

        PartitionFilterInput filter = new PartitionFilterInput();
        filter.setMaxParts((short) 10);
        filter.setFilter("column1=\"value2\"");
        Partition[] actuals = partitionService.getPartitionsByFilter(tableName, filter);
        assertEquals(0, actuals.length);

        filter.setFilter("column1=\"value1\"");
        actuals = partitionService.getPartitionsByFilter(tableName, filter);
        assertEquals(3, actuals.length);
        PartitionInput[] expectBases = partInput.getPartitions();
        for (int i = 0; i < 3; i++) {
            assertSamePartition(expectBases[i], actuals[i]);
        }
    }

    @Test
    public void should_delete_partitions_by_name_success() {
        TableName tableName = new TableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, TABLE_NAME);
        String[] partitionNames = new String[]{PARTITION_PART_URL, PARTITION_PART_URL2, PARTITION_PART_URL3};
        AddPartitionInput partInput = buildPartitionInput(tableName, partitionNames);
        partitionService.addPartitions(tableName, partInput);

        DropPartitionInput dropPartitionInput = new DropPartitionInput();
        List<String> partitionName = Collections.singletonList(PARTITION_PART_URL);
        dropPartitionInput.setPartitionNames(partitionName);
        dropPartitionInput.setDeleteData(true);
        partitionService.dropPartition(tableName, dropPartitionInput);
        Throwable exception = assertThrows(
            CatalogServerException.class, ()->partitionService.getPartitionByName(tableName, PARTITION_PART_URL));
        assertEquals("NoSuchObjectException(message:partition values=[value1, value20])", exception.getMessage());
        DropPartitionByValuesInput dropPartitionByValuesInput = new DropPartitionByValuesInput();
        List<String> delValues = new ArrayList<String>() {{
            add("value1");
            add("value21");
        }};
        dropPartitionByValuesInput.setValues(delValues);
        dropPartitionByValuesInput.setDeleteData(true);
        dropPartitionByValuesInput.setIfExists(false);
        dropPartitionByValuesInput.setPurgeData(false);
        dropPartitionByValuesInput.setReturnResults(false);
        partitionService.dropPartition(tableName, dropPartitionByValuesInput);

        // test Exprs need serialize kryo
//        partitionService.dropPartitionsByExprs(tableName, dropPartitionsByExprsInput);

        PartitionFilterInput filter = new PartitionFilterInput();
        filter.setPartNames(new String[]{PARTITION_PART_URL, PARTITION_PART_URL2, PARTITION_PART_URL3});
        Partition[] actuals = partitionService.getPartitionsByNames(tableName, filter);

        assertEquals(1, actuals.length);
        PartitionInput[] expectBases = partInput.getPartitions();
        assertSamePartition(expectBases[2], actuals[0]);
    }

    @Test
    public void should_alter_partitions_success() {
        TableName tableName = new TableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, TABLE_NAME);
        String[] partNames = new String[]{PARTITION_PART_URL};
        AddPartitionInput partInput = buildPartitionInput(tableName, partNames);
        partitionService.addPartition(tableName, partInput);

        // alter Partition
        AlterPartitionInput input = new AlterPartitionInput();
        PartitionAlterContext[] contexts = {new PartitionAlterContext()};
        contexts[0].setOldValues(PARTITION1_VALUES);
        contexts[0].setNewValues(PARTITION1_VALUES);
        contexts[0].setParameters(null);
        contexts[0].setInputFormat(INPUT_FORMAT + "different");
        contexts[0].setOutputFormat(OUTPUT_FORMAT + "different");
        contexts[0].setLocation(MOCK_LOCATION);

        input.setPartitionContexts(contexts);
        partitionService.alterPartitions(tableName, input);

        Partition actual = partitionService.getPartitionByName(tableName, PARTITION_PART_URL);
        assertEquals(INPUT_FORMAT + "different", actual.getStorageDescriptor().getInputFormat());
        assertEquals(OUTPUT_FORMAT + "different", actual.getStorageDescriptor().getOutputFormat());
        assertEquals(MOCK_LOCATION, actual.getStorageDescriptor().getLocation());
    }

    @Test
    public void should_alter_partition_success() {
        TableName tableName = new TableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, TABLE_NAME);
        String[] partNames = new String[]{PARTITION_PART_URL};
        AddPartitionInput partInput = buildPartitionInput(tableName, partNames);
        partitionService.addPartition(tableName, partInput);

        // alter Partition
        AlterPartitionInput input = new AlterPartitionInput();
        PartitionAlterContext[] contexts = {new PartitionAlterContext()};
        contexts[0].setOldValues(PARTITION1_VALUES);
        contexts[0].setNewValues(PARTITION1_VALUES);
        contexts[0].setParameters(null);
        contexts[0].setInputFormat(INPUT_FORMAT + "different");
        contexts[0].setOutputFormat(OUTPUT_FORMAT + "different");
        contexts[0].setLocation(MOCK_LOCATION);

        input.setPartitionContexts(contexts);
        partitionService.alterPartition(tableName, input);

        Partition actual = partitionService.getPartitionByName(tableName, PARTITION_PART_URL);
        assertEquals(INPUT_FORMAT + "different", actual.getStorageDescriptor().getInputFormat());
        assertEquals(OUTPUT_FORMAT + "different", actual.getStorageDescriptor().getOutputFormat());
        assertEquals(String.format(Locale.ROOT, LOCATION_FILE_PREFIX, MOCK_LOCATION),
            actual.getStorageDescriptor().getLocation());
    }

    @Test
    public void should_get_partition_column_stats_success() {
        try {
            TableName tableName = new TableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, TABLE_NAME);
            String[] partNames = new String[]{PARTITION_PART_URL};
            AddPartitionInput partInput = buildPartitionInput(tableName, partNames);
            partitionService.addPartition(tableName, partInput);

            // Add ColumnStatistics for tbl to metastore DB via ObjectStore
            ColumnStatistics stats = generateColumnStatistics(PARTITION_PART_URL);

            // Save to DB
            partitionService.updatePartitionColumnStatistics(tableName, new ColumnStatisticsInput(stats));
            List<String> columns = new ArrayList<String>() {{
                add(COLUMN_1);
                add(COLUMN_2);
            }};
            Map<String, List<ColumnStatisticsObj>> statsObjMap = partitionService.getPartitionColumnStatistics(tableName,
                Collections.singletonList(PARTITION_PART_URL), columns).getStatisticsResults();

            List<ColumnStatisticsObj> statsObj = statsObjMap.get(PARTITION_PART_URL);
            assertEquals(2, statsObj.size());
            StringColumnStatsData actualString = (StringColumnStatsData) (statsObj.get(0).getDataValue());
            assertEquals(10, actualString.getAvgColLen());
            assertEquals(20, actualString.getMaxColLen());
            assertEquals(10, actualString.getNumDVs());
            assertEquals(10, actualString.getNumNulls());
            assertEquals(HIVE_DECIMAL_TYPE, statsObj.get(1).getColType());
            DecimalColumnStatsData actualDecimal = (DecimalColumnStatsData) (statsObj.get(1).getDataValue());
            assertEquals(10, actualDecimal.getLowValue().getScale());
            assertEquals(20, actualDecimal.getHighValue().getScale());
            assertEquals(10, actualDecimal.getNumDVs());
            assertEquals(10, actualDecimal.getNumNulls());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void should_delete_column_stats_success() {
        try {
            TableName tableName = new TableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, TABLE_NAME);
            String[] partNames = new String[]{PARTITION_PART_URL};
            AddPartitionInput partInput = buildPartitionInput(tableName, partNames);
            partitionService.addPartition(tableName, partInput);

            // Add ColumnStatistics for tbl to metastore DB via ObjectStore
            ColumnStatistics stats = generateColumnStatistics(PARTITION_PART_URL);

            // Save to DB
            partitionService.updatePartitionColumnStatistics(tableName, new ColumnStatisticsInput(stats));
            List<String> columns = new ArrayList<String>() {{
                add(COLUMN_1);
                add(COLUMN_2);
            }};

            partitionService.deletePartitionColumnStatistics(tableName, PARTITION_PART_URL, COLUMN_1);
            List<ColumnStatisticsObj> statsObj = partitionService.getPartitionColumnStatistics(tableName, Arrays.asList(partNames), columns)
                .getStatisticsResults().get(PARTITION_PART_URL);
            assertEquals(1, statsObj.size());
            partitionService.deletePartitionColumnStatistics(tableName, PARTITION_PART_URL, COLUMN_2);
            assertNull(partitionService.getPartitionColumnStatistics(tableName, Arrays.asList(partNames), columns).getStatisticsResults()
                .get(PARTITION_PART_URL));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void should_add_column_stats_merge_success() {
        try {
            TableName tableName = new TableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, TABLE_NAME);
            String[] partNames = new String[]{PARTITION_PART_URL};
            AddPartitionInput partInput = buildPartitionInput(tableName, partNames);
            partitionService.addPartition(tableName, partInput);

            // Add ColumnStatistics for tbl to metastore DB via ObjectStore
            ColumnStatistics stats = generateColumnStatistics(PARTITION_PART_URL);

            // Save to DB
            partitionService.updatePartitionColumnStatistics(tableName, new ColumnStatisticsInput(stats));
            SetPartitionColumnStatisticsInput input = new SetPartitionColumnStatisticsInput();
            input.setStats(Collections.singletonList(stats));
            input.setNeedMerge(true);
            partitionService.setPartitionColumnStatistics(tableName, input);
            List<String> columns = new ArrayList<String>() {{
                add(COLUMN_1);
                add(COLUMN_2);
            }};
            Map<String, List<ColumnStatisticsObj>> statsObjMap = partitionService.getPartitionColumnStatistics(tableName,
                Arrays.asList(partNames), columns).getStatisticsResults();

            List<ColumnStatisticsObj> statsObj = statsObjMap.get(PARTITION_PART_URL);
            assertEquals(2, statsObj.size());
            StringColumnStatsData actualString = (StringColumnStatsData) (statsObj.get(0).getDataValue());
            assertEquals(10, actualString.getAvgColLen());
            assertEquals(20, actualString.getMaxColLen());
            assertEquals(10, actualString.getNumDVs());
            assertEquals(20, actualString.getNumNulls());
            assertEquals(HIVE_DECIMAL_TYPE, statsObj.get(1).getColType());
            DecimalColumnStatsData actualDecimal = (DecimalColumnStatsData) (statsObj.get(1).getDataValue());
            assertEquals(10, actualDecimal.getLowValue().getScale());
            assertEquals(20, actualDecimal.getHighValue().getScale());
            assertEquals(10, actualDecimal.getNumDVs());
            assertEquals(20, actualDecimal.getNumNulls());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }


    @Test
    public void should_get_column_stats_for_success() {
        try {
            TableName tableName = new TableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, TABLE_NAME);
            String[] partNames = new String[]{PARTITION_PART_URL, PARTITION_PART_URL2};
            AddPartitionInput partInput = buildPartitionInput(tableName, partNames);
            partitionService.addPartitions(tableName, partInput);

            // Add ColumnStatistics for tbl to metastore DB via ObjectStore
            partitionService.updatePartitionColumnStatistics(tableName,
                new ColumnStatisticsInput(generateColumnStatistics(PARTITION_PART_URL)));
            partitionService.updatePartitionColumnStatistics(tableName,
                new ColumnStatisticsInput(generateColumnStatistics(PARTITION_PART_URL2)));

            List<String> columns = new ArrayList<String>() {{
                add(COLUMN_1);
                add(COLUMN_2);
            }};

            AggrStatisticData statsResults = partitionService.getAggrColStatsFor(tableName, Arrays.asList(partNames), columns);
            assertEquals(2, statsResults.getPartsFound());

            List<ColumnStatisticsObj> statsInfo = statsResults.getColumnStatistics();
            assertEquals(2, statsInfo.size());
            StringColumnStatsData actualString = (StringColumnStatsData) (statsInfo.get(0).getDataValue());
            assertEquals(10, actualString.getAvgColLen());
            assertEquals(20, actualString.getMaxColLen());
            assertEquals(10, actualString.getNumDVs());
            assertEquals(20, actualString.getNumNulls());
            assertEquals(HIVE_DECIMAL_TYPE, statsInfo.get(1).getColType());
            DecimalColumnStatsData actualDecimal = (DecimalColumnStatsData) (statsInfo.get(1).getDataValue());
//          decimal min max args lost accuracy
//            assertEquals(10, actualDecimal.getLowValue().getScale());
//            assertEquals(20, actualDecimal.getHighValue().getScale());
            assertEquals(10, actualDecimal.getNumDVs());
            assertEquals(20, actualDecimal.getNumNulls());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    private ColumnStatistics generateColumnStatistics(String partName) throws Exception {
        ColumnStatistics stats = new ColumnStatistics();
        ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc(false, CATALOG_NAME, DATABASE_NAME, TABLE_NAME);
        statsDesc.setPartName(partName);
        List<ColumnStatisticsObj> colStatObjs = new ArrayList<>();

        // Col1
        StringColumnStatsData data1 = new StringColumnStatsData();
        ColumnStatisticsObj col1Stats = new ColumnStatisticsObj();
        data1.setAvgColLen(10);
        data1.setMaxColLen(20);
        data1.setNumNulls(10);
        data1.setNumDVs(10);
        col1Stats.setColName(COLUMN_1);
        col1Stats.setColType(HIVE_STR_TYPE);
        col1Stats.setDataType("stringStats");
        col1Stats.setDataValue(obj2Map(data1));
        colStatObjs.add(col1Stats);

        // Col2
        DecimalColumnStatsData data2 = new DecimalColumnStatsData();
        ColumnStatisticsObj col2Stats = new ColumnStatisticsObj();
        ByteBuffer unscared = ByteBuffer.allocate(4);
        unscared.asIntBuffer().put(1);
        data2.setHighValue(new Decimal((short) 20, unscared.array()));
        data2.setLowValue(new Decimal((short) 10, unscared.array()));
        data2.setNumNulls(10);
        data2.setNumDVs(10);

        col2Stats.setColName(COLUMN_2);
        col2Stats.setColType(HIVE_DECIMAL_TYPE);
        col2Stats.setDataType("decimalStats");
        col2Stats.setDataValue(obj2Map(data2));
        colStatObjs.add(col2Stats);

        stats.setColumnStatisticsDesc(statsDesc);
        stats.setColumnStatisticsObjs(colStatObjs);
        return stats;
    }

    //@Test
    // hive has a bug / can't rename partition not in default catalog
    public void should_rename_partition_success() {
        TableName tableName = new TableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, TABLE_NAME);
        String[] partNames = new String[]{PARTITION_PART_URL};
        AddPartitionInput partInput = buildPartitionInput(tableName, partNames);
        partitionService.addPartition(tableName, partInput);

        // alter Partition
        AlterPartitionInput input = new AlterPartitionInput();
        PartitionAlterContext[] contexts = {new PartitionAlterContext()};
        contexts[0].setOldValues(PARTITION1_VALUES);
        contexts[0].setNewValues(PARTITION2_VALUES);
        contexts[0].setParameters(null);
        contexts[0].setInputFormat(INPUT_FORMAT + "different");
        contexts[0].setOutputFormat(OUTPUT_FORMAT + "different");
        contexts[0].setLocation(MOCK_LOCATION);

        input.setPartitionContexts(contexts);
        partitionService.renamePartition(tableName, input);

        Partition actual = partitionService.getPartitionByName(tableName, PARTITION_PART_URL2);
        assertEquals(INPUT_FORMAT + "different", actual.getStorageDescriptor().getInputFormat());
        assertEquals(OUTPUT_FORMAT + "different", actual.getStorageDescriptor().getOutputFormat());
        assertEquals(String.format(Locale.ROOT, LOCATION_FILE_PREFIX, MOCK_LOCATION),
            actual.getStorageDescriptor().getLocation());
    }

    private void assertSamePartition(PartitionInput expect, Partition actual) {
        assertEquals(expect.getPartitionValues(), actual.getPartitionValues());

        assertEquals(expect.getStorageDescriptor().getInputFormat(), actual.getStorageDescriptor().getInputFormat());
        assertEquals(expect.getStorageDescriptor().getOutputFormat(), actual.getStorageDescriptor().getOutputFormat());
        checkColumns(expect.getStorageDescriptor().getColumns(), actual.getStorageDescriptor().getColumns());
        assertEquals(String.format(Locale.ROOT, LOCATION_FILE_PREFIX, expect.getStorageDescriptor().getLocation()),
            actual.getStorageDescriptor().getLocation());
    }

    private void checkColumns(List<Column> expect, List<Column> actual) {
        assertEquals(expect.size(), actual.size());
        for (int i = 0; i < expect.size(); i++) {
            assertEquals(expect.get(i).getColumnName(), actual.get(i).getColumnName());
            assertEquals(expect.get(i).getColType(), actual.get(i).getColType());
        }
    }

    private AddPartitionInput buildPartitionInput(TableName tableName, String[] partitionName) {
        AddPartitionInput partitionInput = new AddPartitionInput();
        PartitionInput[] partitionBases = new PartitionInput[partitionName.length];
        partitionInput.setOverwrite(false);
        partitionInput.setFileFormat("parquet");
        for (int i = 0; i < partitionName.length; i++) {
            partitionBases[i] = new PartitionInput();
            partitionBases[i].setPartitionValues(PartitionUtil.convertNameToVals(partitionName[i]));
            partitionBases[i].setCatalogName(tableName.getCatalogName());
            partitionBases[i].setDatabaseName(tableName.getDatabaseName());
            partitionBases[i].setTableName(tableName.getTableName());
            partitionBases[i].setFiles(null);
            partitionBases[i].setIndex(null);
            partitionBases[i].setFileIndexUrl(null);
            StorageDescriptor sd = new StorageDescriptor();
            sd.setLocation(FULL_PATH + partitionName[i]);
            sd.setInputFormat(INPUT_FORMAT);
            sd.setOutputFormat(OUTPUT_FORMAT);
            sd.setColumns(COLUMNS);
            sd.setCompressed(false);
            sd.setSerdeInfo(SER_DE_INFO);
            partitionBases[i].setStorageDescriptor(sd);
            partitionBases[i].setCreateTime(System.currentTimeMillis() / 1000);
            partitionBases[i].setLastAccessTime(System.currentTimeMillis() / 1000);
        }
        partitionInput.setPartitions(partitionBases);

        return partitionInput;
    }



    @AfterEach
    private void cleanEnv() {
        deleteTestDataBaseAfterTest();
    }

    @BeforeEach
    private void makeEnv() {
        createTestTableBeforeTest();
    }
}
