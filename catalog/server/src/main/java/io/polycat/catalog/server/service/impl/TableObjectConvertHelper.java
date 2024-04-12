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
package io.polycat.catalog.server.service.impl;

import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.model.Column;
import io.polycat.catalog.common.model.ColumnObject;
import io.polycat.catalog.common.model.ColumnStatisticsAggrObject;
import io.polycat.catalog.common.model.ColumnStatisticsObject;
import io.polycat.catalog.common.model.SerDeInfo;
import io.polycat.catalog.common.model.StorageDescriptor;
import io.polycat.catalog.common.model.Table;
import io.polycat.catalog.common.model.TableObject;
import io.polycat.catalog.common.model.TableStorageObject;
import io.polycat.catalog.common.model.stats.BinaryColumnStatsData;
import io.polycat.catalog.common.model.stats.BooleanColumnStatsData;
import io.polycat.catalog.common.model.stats.ColumnStatisticsDataType;
import io.polycat.catalog.common.model.stats.ColumnStatisticsObj;
import io.polycat.catalog.common.model.stats.DateColumnStatsData;
import io.polycat.catalog.common.model.stats.DecimalColumnStatsData;
import io.polycat.catalog.common.model.stats.DoubleColumnStatsData;
import io.polycat.catalog.common.model.stats.LongColumnStatsData;
import io.polycat.catalog.common.model.stats.StringColumnStatsData;
import io.polycat.catalog.common.plugin.request.input.TableInput;
import io.polycat.catalog.common.plugin.request.input.TableTypeInput;
import io.polycat.catalog.common.utils.DataTypeUtil;
import io.polycat.catalog.common.utils.UuidUtil;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.logging.log4j.util.Strings;

@Slf4j
public class TableObjectConvertHelper {

    private static final String dateFormat = "yyyy-MM-dd HH:mm:ss";
    private static final SimpleDateFormat SDF = new SimpleDateFormat(dateFormat);

    public enum SpecialSerdeFormatInfo {
        /**
         * Hbase handler
         */
        HBASESERDE("org.apache.hadoop.hive.hbase.HBaseSerDe", "org.apache.hadoop.hive.hbase.HiveHBaseTableInputFormat", "org.apache.hadoop.hive.hbase.HiveHBaseTableOutputFormat"),
        ;

        public static final Map<String, SpecialSerdeFormatInfo> serdeFormatMap = new HashMap<>();
        private String serde;
        private String inputFormat;
        private String outputFormat;

        static {
            for (SpecialSerdeFormatInfo specialSerdeFormatInfo: SpecialSerdeFormatInfo.values()) {
                serdeFormatMap.put(specialSerdeFormatInfo.serde, specialSerdeFormatInfo);
            }
        }

        SpecialSerdeFormatInfo(String serde, String inputFormat, String outputFormat) {
            this.serde = serde;
            this.inputFormat = inputFormat;
            this.outputFormat = outputFormat;
        }

        public static SpecialSerdeFormatInfo getSpecialSerdeFormatInfo(String serde) {
            return serdeFormatMap.get(serde);
        }

        public String getInputFormat() {
            return inputFormat;
        }

        public String getSerde() {
            return serde;
        }

        public String getOutputFormat() {
            return outputFormat;
        }
    }

    public static Table toTableModel(TableObject tableObject) {
        if (null == tableObject) {
            return null;
        }

        Table table = new Table();
        table.setCatalogName(tableObject.getCatalogName());
        table.setTableId(tableObject.getTableId());
        table.setDatabaseName(tableObject.getDatabaseName());
        table.setTableName(tableObject.getName());
        table.setOwner(tableObject.getTableBaseObject().getOwner());
        table.setLmsMvcc(tableObject.getTableBaseObject().isLmsMvcc());
        table.setTableType(tableObject.getTableBaseObject().getTableType());
        table.setCreateTime(tableObject.getTableBaseObject().getCreateTime());
        table.setAccountId(Strings.EMPTY);
        table.setAuthSourceType(Strings.EMPTY);
        table.setOwnerType("USER");
        table.setRetention(0L);
        table.setViewExpandedText(tableObject.getTableBaseObject().getViewExpandedText());
        table.setViewOriginalText(tableObject.getTableBaseObject().getViewOriginalText());
        table.setLastAccessTime(System.currentTimeMillis());
        table.setDescription(tableObject.getTableBaseObject().getDescription());
        if (tableObject.getDroppedTime() != 0) {
            table.setDroppedTime(tableObject.getDroppedTime());
        } else {
            table.setPartitionKeys(convertColumn(tableObject.getTableSchemaObject().getPartitionKeys()));
            table.setStorageDescriptor(toStorageDescriptor(tableObject));
            table.setParameters(tableObject.getTableBaseObject().getParameters());
        }
        return table;
    }


    public static List<Column> convertColumn(List<ColumnObject> columns) {
        List<Column> columnOutputs = new ArrayList<>(columns.size());
        for (ColumnObject column : columns) {
            String dataType;
            dataType = column.getDataType().toString();
            columnOutputs.add(new Column(column.getName(), dataType, column.getComment()));
        }
        return columnOutputs;
    }

    private static StorageDescriptor toStorageDescriptor(TableObject tableObject) {
        TableStorageObject tableStorageObject = tableObject.getTableStorageObject();
        if (tableObject.getTableSchemaObject().getColumns() == null && tableStorageObject == null) {
            return null;
        }

        StorageDescriptor tableStorage = new StorageDescriptor();
        tableStorage.setColumns(convertColumn(tableObject.getTableSchemaObject().getColumns()));
        if (tableStorageObject != null) {
            // compatibility history proto storage_info.proto field=location: view location=null
            if (TableTypeInput.VIRTUAL_VIEW.name().equals(tableObject.getTableBaseObject().getTableType())) {
                tableStorage.setLocation(null);
            } else {
                tableStorage.setLocation(tableStorageObject.getLocation());
            }
            tableStorage.setSourceShortName(tableStorageObject.getSourceShortName());
            tableStorage.setSerdeInfo(toSerDeInfo(tableStorageObject));
            tableStorage.setFileFormat(tableStorageObject.getFileFormat());
            tableStorage.setInputFormat(tableStorageObject.getInputFormat());
            tableStorage.setOutputFormat(tableStorageObject.getOutputFormat());
            if (TableStorageObject.DEFAULT_SD_FILE_FORMAT.equalsIgnoreCase(tableStorage.getOutputFormat()) && tableStorage.getSerdeInfo().getSerializationLibrary() != null) {
                SpecialSerdeFormatInfo serdeFormatInfo = SpecialSerdeFormatInfo.getSpecialSerdeFormatInfo(tableStorage.getSerdeInfo().getSerializationLibrary());
                if (serdeFormatInfo != null) {
                    tableStorage.setFileFormat(serdeFormatInfo.getSerde());
                    tableStorage.setInputFormat(serdeFormatInfo.getInputFormat());
                    tableStorage.setOutputFormat(serdeFormatInfo.getOutputFormat());
                }
            }
            tableStorage.setParameters(tableStorageObject.getParameters());

            // todo: set based on stored table object once underlying structure complement these fields
            tableStorage.setCompressed(tableStorageObject.getCompressed());
            tableStorage.setNumberOfBuckets(tableStorageObject.getNumberOfBuckets());
            tableStorage.setBucketColumns(tableStorageObject.getBucketColumns());
            tableStorage.setSortColumns(tableStorageObject.getSortColumns());
            tableStorage.setSkewedInfo(tableStorageObject.getSkewedInfo());
            tableStorage.setStoredAsSubDirectories(false);
        }

        return tableStorage;
    }

    private static SerDeInfo toSerDeInfo(TableStorageObject tableStorageObject) {
        SerDeInfo serDeInfo = null;
        if (tableStorageObject != null) {
            serDeInfo = tableStorageObject.getSerdeInfo();
        }
        return serDeInfo;
    }

    public static ColumnStatisticsObject toPartitionColumnStatisticsObject(Table table,
            ColumnStatisticsObj statsObj, String partName, String partitionId, long lastAnalyzed) {
        ColumnStatisticsObject columnStatisticsObject = toTableColumnStatisticsObject(table,
                statsObj, lastAnalyzed);
        columnStatisticsObject.setPartitionName(partName);
        columnStatisticsObject.setPartitionId(partitionId);
        columnStatisticsObject.setPcsId(UuidUtil.generateUUID32());
        return columnStatisticsObject;
    }

    public static ColumnStatisticsObject toTableColumnStatisticsObject(Table table,
            ColumnStatisticsObj cso, long lastAnalyzed) {
        if (cso == null) {
            return null;
        }
        String colType = cso.getColType();
        Object dataValue = cso.getDataValue();
        ColumnStatisticsObject stat = new ColumnStatisticsObject();
        stat.setCatalogName(table.getCatalogName());
        stat.setDatabaseName(table.getDatabaseName());
        stat.setTableName(table.getTableName());
        stat.setTableId(table.getTableId());
        stat.setColumnName(cso.getColName());
        stat.setColumnType(colType);
        stat.setTcsId(UuidUtil.generateUUID32());
        stat.setLastAnalyzed(lastAnalyzed);
        setColumnStatisticsData(stat, dataValue, colType);
        return stat;
    }

    private static void setColumnStatisticsData(ColumnStatisticsObject stat, Object dataValue, String colType) {
        ColumnStatisticsDataType statisticsDataType = ColumnStatisticsDataType.findByColumnType(colType);
        switch (statisticsDataType) {
            case DATE_STATS:
                setDateTableColumnStatisticsObject(stat, dataValue);
                break;
            case LONG_STATS:
                setLongTableColumnStatisticsObject(stat, dataValue);
                break;
            case BINARY_STATS:
                setBinaryTableColumnStatisticsObject(stat, dataValue);
                break;
            case DOUBLE_STATS:
                setDoubleTableColumnStatisticsObject(stat, dataValue);
                break;
            case STRING_STATS:
                setStringTableColumnStatisticsObject(stat, dataValue);
                break;
            case BOOLEAN_STATS:
                setBooleanTableColumnStatisticsObject(stat, dataValue);
                break;
            case DECIMAL_STATS:
                setDecimalTableColumnStatisticsObject(stat, dataValue);
                break;
            default:
                break;
        }
    }

    private static void setDecimalTableColumnStatisticsObject(ColumnStatisticsObject stat, Object dataValue) {
        DecimalColumnStatsData statsData = DataTypeUtil.mapToColumnStatsData(dataValue, DecimalColumnStatsData.class);
        stat.setBitVector(statsData.getBitVectors());
        stat.setDecimalLowValue(DataTypeUtil.toCatalogDecimalString(statsData.getLowValue()));
        stat.setDecimalHighValue(DataTypeUtil.toCatalogDecimalString(statsData.getHighValue()));
        stat.setNumDistincts(statsData.getNumDVs());
        stat.setNumNulls(statsData.getNumNulls());
    }

    private static void setBooleanTableColumnStatisticsObject(ColumnStatisticsObject stat, Object dataValue) {
        BooleanColumnStatsData statsData = DataTypeUtil.mapToColumnStatsData(dataValue, BooleanColumnStatsData.class);
        stat.setBitVector(statsData.getBitVectors());
        stat.setNumFalses(statsData.getNumFalses());
        stat.setNumTrues(statsData.getNumTrues());
        stat.setNumNulls(statsData.getNumNulls());
    }

    private static void setStringTableColumnStatisticsObject(ColumnStatisticsObject stat, Object dataValue) {
        StringColumnStatsData statsData = DataTypeUtil.mapToColumnStatsData(dataValue, StringColumnStatsData.class);
        stat.setBitVector(statsData.getBitVectors());
        stat.setAvgColLen(statsData.getAvgColLen());
        stat.setMaxColLen(statsData.getMaxColLen());
        stat.setNumDistincts(statsData.getNumDVs());
        stat.setNumNulls(statsData.getNumNulls());
    }

    private static void setDoubleTableColumnStatisticsObject(ColumnStatisticsObject stat, Object dataValue) {
        DoubleColumnStatsData statsData = DataTypeUtil.mapToColumnStatsData(dataValue, DoubleColumnStatsData.class);
        stat.setBitVector(statsData.getBitVectors());
        stat.setDoubleLowValue(statsData.getLowValue());
        stat.setDoubleHighValue(statsData.getHighValue());
        stat.setNumDistincts(statsData.getNumDVs());
        stat.setNumNulls(statsData.getNumNulls());
    }

    private static void setBinaryTableColumnStatisticsObject(ColumnStatisticsObject stat, Object dataValue) {
        BinaryColumnStatsData statsData = DataTypeUtil.mapToColumnStatsData(dataValue, BinaryColumnStatsData.class);
        stat.setBitVector(statsData.getBitVectors());
        stat.setAvgColLen(statsData.getAvgColLen());
        stat.setMaxColLen(statsData.getMaxColLen());
        stat.setNumNulls(statsData.getNumNulls());
    }

    private static void setLongTableColumnStatisticsObject(ColumnStatisticsObject stat, Object dataValue) {
        LongColumnStatsData statsData = DataTypeUtil.mapToColumnStatsData(dataValue, LongColumnStatsData.class);
        stat.setBitVector(statsData.getBitVectors());
        stat.setLongHighValue(statsData.getHighValue());
        stat.setLongLowValue(statsData.getLowValue());
        stat.setNumDistincts(statsData.getNumDVs());
        stat.setNumNulls(statsData.getNumNulls());
    }

    private static void setDateTableColumnStatisticsObject(ColumnStatisticsObject stat, Object dataValue) {
        DateColumnStatsData statsData = DataTypeUtil.mapToColumnStatsData(dataValue, DateColumnStatsData.class);
        stat.setBitVector(statsData.getBitVectors());
        stat.setLongHighValue(statsData.getHighValue());
        stat.setLongLowValue(statsData.getLowValue());
        stat.setNumDistincts(statsData.getNumDVs());
        stat.setNumNulls(statsData.getNumNulls());
    }

    public static ColumnStatisticsObj toColumnStatisticsData(ColumnStatisticsObject ts) {
        if (ts == null) {
            return null;
        }
        ColumnStatisticsDataType statisticsDataType = ColumnStatisticsDataType.findByColumnType(ts.getColumnType());
        ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj();
        columnStatisticsObj.setColName(ts.getColumnName());
        columnStatisticsObj.setColType(ts.getColumnType());
        columnStatisticsObj.setDataType(statisticsDataType.getTypeName());
        switch (statisticsDataType) {
            case DATE_STATS:
                setDateStatisticsData(ts, columnStatisticsObj);
                break;
            case LONG_STATS:
                setLongStatisticsData(ts, columnStatisticsObj);
                break;
            case BINARY_STATS:
                setBinaryStatisticsData(ts, columnStatisticsObj);
                break;
            case DOUBLE_STATS:
                setDoubleStatisticsData(ts, columnStatisticsObj);
                break;
            case STRING_STATS:
                setStringStatisticsData(ts, columnStatisticsObj);
                break;
            case BOOLEAN_STATS:
                setBooleanStatisticsData(ts, columnStatisticsObj);
                break;
            case DECIMAL_STATS:
                setDecimalStatisticsData(ts, columnStatisticsObj);
                break;
            default:
                break;
        }
        return columnStatisticsObj;
    }

    private static void setDecimalStatisticsData(ColumnStatisticsObject ts, ColumnStatisticsObj columnStatisticsObj) {
        setDecimalStatisticsData(columnStatisticsObj, ts.getBitVector(), ts.getDecimalLowValue(),
                ts.getDecimalHighValue(),
                ts.getNumDistincts(), ts.getNumNulls());
    }

    private static void setDecimalStatisticsData(ColumnStatisticsObj columnStatisticsObj, byte[] bitVectors,
            String decimalLowValue, String decimalHighValue, long numDVs, long numNulls) {
        DecimalColumnStatsData statsData = new DecimalColumnStatsData();
        statsData.setBitVectors(bitVectors);
        statsData.setLowValue(DataTypeUtil.toCatalogDecimal(decimalLowValue));
        statsData.setHighValue(DataTypeUtil.toCatalogDecimal(decimalHighValue));
        statsData.setNumDVs(numDVs);
        statsData.setNumNulls(numNulls);
        columnStatisticsObj.setDataValue(statsData);
    }

    private static void setBooleanStatisticsData(ColumnStatisticsObject ts, ColumnStatisticsObj columnStatisticsObj) {
        setBooleanStatisticsData(columnStatisticsObj, ts.getBitVector(), ts.getNumFalses(), ts.getNumTrues(),
                ts.getNumNulls());
    }

    private static void setBooleanStatisticsData(ColumnStatisticsObj columnStatisticsObj, byte[] bitVectors,
            long numFalses, long numTrues, long numNulls) {
        BooleanColumnStatsData statsData = new BooleanColumnStatsData();
        statsData.setBitVectors(bitVectors);
        statsData.setNumFalses(numFalses);
        statsData.setNumTrues(numTrues);
        statsData.setNumNulls(numNulls);
        columnStatisticsObj.setDataValue(statsData);
    }

    private static void setStringStatisticsData(ColumnStatisticsObject ts, ColumnStatisticsObj columnStatisticsObj) {
        setStringStatisticsData(columnStatisticsObj, ts.getBitVector(), ts.getAvgColLen(), ts.getMaxColLen(),
                ts.getNumDistincts(), ts.getNumNulls());
    }

    private static void setStringStatisticsData(ColumnStatisticsObj columnStatisticsObj, byte[] bitVectors,
            double avgColLen, long maxColLen, long numDVs, long numNulls) {
        StringColumnStatsData statsData = new StringColumnStatsData();
        statsData.setBitVectors(bitVectors);
        statsData.setAvgColLen(avgColLen);
        statsData.setMaxColLen(maxColLen);
        statsData.setNumDVs(numDVs);
        statsData.setNumNulls(numNulls);
        columnStatisticsObj.setDataValue(statsData);
    }

    private static void setDoubleStatisticsData(ColumnStatisticsObject ts, ColumnStatisticsObj columnStatisticsObj) {
        setDoubleStatisticsData(columnStatisticsObj, ts.getBitVector(), ts.getDoubleLowValue(), ts.getDoubleHighValue(),
                ts.getNumDistincts(), ts.getNumNulls());
    }

    private static void setDoubleStatisticsData(ColumnStatisticsObj columnStatisticsObj, byte[] bitVectors,
            double doubleLowValue, double doubleHighValue, long numDVs, long numNulls) {
        DoubleColumnStatsData statsData = new DoubleColumnStatsData();
        statsData.setBitVectors(bitVectors);
        statsData.setLowValue(doubleLowValue);
        statsData.setHighValue(doubleHighValue);
        statsData.setNumDVs(numDVs);
        statsData.setNumNulls(numNulls);
        columnStatisticsObj.setDataValue(statsData);
    }

    private static void setBinaryStatisticsData(ColumnStatisticsObject ts, ColumnStatisticsObj columnStatisticsObj) {
        setBinaryStatisticsData(columnStatisticsObj, ts.getBitVector(), ts.getAvgColLen(), ts.getMaxColLen(),
                ts.getNumNulls());
    }

    private static void setBinaryStatisticsData(ColumnStatisticsObj columnStatisticsObj, byte[] bitVectors,
            double avgColLen, long maxColLen, long numNulls) {
        BinaryColumnStatsData statsData = new BinaryColumnStatsData();
        statsData.setBitVectors(bitVectors);
        statsData.setAvgColLen(avgColLen);
        statsData.setMaxColLen(maxColLen);
        statsData.setNumNulls(numNulls);
        columnStatisticsObj.setDataValue(statsData);
    }

    private static void setLongStatisticsData(ColumnStatisticsObject ts, ColumnStatisticsObj columnStatisticsObj) {
        setLongStatisticsData(columnStatisticsObj, ts.getBitVector(), ts.getLongLowValue(), ts.getLongHighValue(),
                ts.getNumDistincts(), ts.getNumNulls());
    }

    private static void setLongStatisticsData(ColumnStatisticsObj columnStatisticsObj, byte[] bitVectors,
            long longLowValue, long longHighValue, long numDVs, long numNulls) {
        LongColumnStatsData statsData = new LongColumnStatsData();
        statsData.setBitVectors(bitVectors);
        statsData.setLowValue(longLowValue);
        statsData.setHighValue(longHighValue);
        statsData.setNumDVs(numDVs);
        statsData.setNumNulls(numNulls);
        columnStatisticsObj.setDataValue(statsData);
    }

    private static void setDateStatisticsData(ColumnStatisticsObject ts, ColumnStatisticsObj columnStatisticsObj) {
        setDateStatisticsData(columnStatisticsObj, ts.getBitVector(), ts.getLongLowValue(), ts.getLongHighValue(),
                ts.getNumDistincts(), ts.getNumNulls());
    }

    private static void setDateStatisticsData(ColumnStatisticsObj columnStatisticsObj, byte[] bitVectors,
            long longLowValue, long longHighValue, long numDVs, long numNulls) {
        DateColumnStatsData statsData = new DateColumnStatsData();
        statsData.setBitVectors(bitVectors);
        statsData.setHighValue(longLowValue);
        statsData.setLowValue(longHighValue);
        statsData.setNumDVs(numDVs);
        statsData.setNumNulls(numNulls);
        columnStatisticsObj.setDataValue(statsData);
    }

    public static TableInput toTableInput(Table table) {
        try {
            TableInput tableInput = new TableInput();
            BeanUtils.copyProperties(tableInput, table);
            return tableInput;
        } catch (Exception e) {
            e.printStackTrace();
            throw new CatalogException("Build table input error: " + e.getMessage());
        }
    }

    public static ColumnStatisticsObj toColumnStatisticsObjForAggrObject(ColumnStatisticsAggrObject aggrObj,
            boolean useDensityFunctionForNDVEstimation, float ndvTuner) {
        ColumnStatisticsDataType statisticsDataType = ColumnStatisticsDataType.findByColumnType(
                aggrObj.getColumnType());
        ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj();
        columnStatisticsObj.setColName(aggrObj.getColumnName());
        columnStatisticsObj.setColType(aggrObj.getColumnType());
        columnStatisticsObj.setDataType(statisticsDataType.getTypeName());
        switch (statisticsDataType) {
            case DATE_STATS:
                setDateStatisticsData(columnStatisticsObj, null, aggrObj.getLongLowValue(), aggrObj.getLongHighValue(),
                        getLongEstimationNDV(aggrObj, useDensityFunctionForNDVEstimation, ndvTuner),
                        aggrObj.getNumNulls());
                break;
            case LONG_STATS:
                setLongStatisticsData(columnStatisticsObj, null, aggrObj.getLongLowValue(), aggrObj.getLongHighValue(),
                        getLongEstimationNDV(aggrObj, useDensityFunctionForNDVEstimation, ndvTuner),
                        aggrObj.getNumNulls());
                break;
            case BINARY_STATS:
                setBinaryStatisticsData(columnStatisticsObj, null, aggrObj.getAvgColLen(), aggrObj.getMaxColLen(),
                        aggrObj.getNumNulls());
                break;
            case DOUBLE_STATS:
                setDoubleStatisticsData(columnStatisticsObj, null, aggrObj.getDoubleLowValue(),
                        aggrObj.getDoubleHighValue(),
                        getDoubleEstimationNDV(aggrObj, useDensityFunctionForNDVEstimation, ndvTuner),
                        aggrObj.getNumNulls());
                break;
            case STRING_STATS:
                setStringStatisticsData(columnStatisticsObj, null, aggrObj.getAvgColLen(), aggrObj.getMaxColLen(),
                        aggrObj.getLowerNumDistincts(), aggrObj.getNumNulls());
                break;
            case BOOLEAN_STATS:
                setBooleanStatisticsData(columnStatisticsObj, null, aggrObj.getNumFalses(), aggrObj.getNumTrues(),
                        aggrObj.getNumNulls());
                break;
            case DECIMAL_STATS:
                setDecimalStatisticsData(columnStatisticsObj, null, aggrObj.getDecimalLowValue(),
                        aggrObj.getDecimalHighValue(),
                        getDecimalEstimationNDV(aggrObj, useDensityFunctionForNDVEstimation, ndvTuner),
                        aggrObj.getNumNulls());
                break;
            default:
                break;
        }
        return columnStatisticsObj;
    }

    private static long getDoubleEstimationNDV(ColumnStatisticsAggrObject aggrObj,
            boolean useDensityFunctionForNDVEstimation, float ndvTuner) {
        long lowerBound = aggrObj.getLowerNumDistincts();
        long higherBound = aggrObj.getHigherNumDistincts();
        if (!useDensityFunctionForNDVEstimation) {
            return (long) (lowerBound + (higherBound - lowerBound) * ndvTuner);
        }
        {
            // TODO useDensityFunctionForNDVEstimation
        }
        return 0L;
    }

    private static long getDecimalEstimationNDV(ColumnStatisticsAggrObject aggrObj,
            boolean useDensityFunctionForNDVEstimation, float ndvTuner) {
        long lowerBound = aggrObj.getLowerNumDistincts();
        long higherBound = aggrObj.getHigherNumDistincts();
        if (!useDensityFunctionForNDVEstimation) {
            return (long) (lowerBound + (higherBound - lowerBound) * ndvTuner);
        }
        {
            // TODO useDensityFunctionForNDVEstimation
        }
        return 0L;
    }

    private static long getLongEstimationNDV(ColumnStatisticsAggrObject aggrObj,
            boolean useDensityFunctionForNDVEstimation, float ndvTuner) {
        long lowerBound = aggrObj.getLowerNumDistincts();
        long higherBound = aggrObj.getHigherNumDistincts();
        long linearBound = Long.MAX_VALUE;
        if (aggrObj.getLongLowValue() != null && aggrObj.getLongHighValue() != null) {
            linearBound = aggrObj.getLongHighValue() - aggrObj.getLongLowValue() + 1;
        }
        if (!useDensityFunctionForNDVEstimation) {
            return (long) Math.min(lowerBound + (higherBound - lowerBound) * ndvTuner, linearBound);
        }
        {
            // TODO useDensityFunctionForNDVEstimation
        }
        return 0L;
    }
}
