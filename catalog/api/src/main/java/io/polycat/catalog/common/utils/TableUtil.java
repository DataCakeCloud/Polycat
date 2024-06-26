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
package io.polycat.catalog.common.utils;

import com.google.common.collect.Maps;
import io.polycat.catalog.common.Constants;
import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.model.Column;
import io.polycat.catalog.common.model.Table;
import io.polycat.catalog.common.model.stats.ColumnStatisticsObj;
import jakarta.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hive.common.util.HiveStringUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toList;

public class TableUtil {

    /**
     * The current partition level and table level only perform verification of table level fields
     * @param table
     * @param colNames
     */
    public static void validateTableColumns(Table table, List<String> colNames) {
        List<Column> columns = table.getStorageDescriptor().getColumns();
        for (String colName : colNames) {
            boolean foundCol = false;
            for (Column column : columns) {
                if (column.getColumnName().equals(colName)) {
                    foundCol = true;
                    break;
                }
            }
            if (!foundCol) {
                throw new CatalogException("Column "+ colName + " not found.");
            }
        }
    }

    public static Map<String, ColumnStatisticsObj> getColumnStatisticsMap(
            ColumnStatisticsObj[] columnStatistics) {
        Map<String, ColumnStatisticsObj> tableColumnStatisticsMap = Maps.newHashMap();
        for (ColumnStatisticsObj statisticsObj: columnStatistics) {
            tableColumnStatisticsMap.put(statisticsObj.getColName(), statisticsObj);
        }
        return tableColumnStatisticsMap;
    }

    public static List<String> getColNamesFromColumnStatistics(List<ColumnStatisticsObj> statsObjs) {
        if (statsObjs == null) {
            return new ArrayList<>();
        }
        return statsObjs.stream().map(ColumnStatisticsObj::getColName).collect(Collectors.toList());
    }

    /**
     * assert iceberg table_type via table params.
     * @param tableParams
     * @return
     */
    public static boolean isIcebergTableByParams(Map<String, String> tableParams) {
        return tableParams != null && Constants.ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(
                tableParams.get(Constants.TABLE_TYPE_PROP));
    }

    /**
     * hive like grammar schema matcher.
     * @param filter
     * @return
     */
    public static Pattern getFilterPattern(@NotNull String filter) {
        filter = HiveStringUtils.normalizeIdentifier(filter);
        List<String> filterRegex = Arrays.stream(filter.split("\\|")).map(x -> "(?i)" + x.replaceAll("\\*", ".*")).collect(toList());
        return Pattern.compile(String.join("|", filterRegex));
    }

    public static Collection<String> filterPattern(List<String> result, String filter) {
        if (StringUtils.isEmpty(filter)) {
            return result;
        }
        Pattern pattern = TableUtil.getFilterPattern(filter);
        return result.stream().filter(x -> pattern.matcher(x).matches()).collect(toList());
    }

    public static boolean filterPattern(String name, String filter) {
        if (StringUtils.isEmpty(filter)) {
            return true;
        }
        Pattern pattern = TableUtil.getFilterPattern(filter);
        return pattern.matcher(name).matches();
    }
}
