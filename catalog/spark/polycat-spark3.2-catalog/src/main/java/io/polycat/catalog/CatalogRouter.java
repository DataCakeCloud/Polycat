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
package io.polycat.catalog;

import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.polycat.catalog.client.Client;
import io.polycat.catalog.client.PolyCatClientHelper;
import io.polycat.catalog.common.TableFormatType;
import io.polycat.catalog.common.model.Table;
import io.polycat.catalog.common.options.PolyCatOptions;
import io.polycat.catalog.common.utils.ConfUtil;
import io.polycat.catalog.spark.PolyCatClientSparkHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;

@Slf4j
public class CatalogRouter {

    private static final Set<String> DEFAULT_NS_KEYS = ImmutableSet.of(TableCatalog.PROP_OWNER);
    private static final Splitter COMMA = Splitter.on(",");
    private static final Pattern AT_TIMESTAMP = Pattern.compile("at_timestamp_(\\d+)");
    private static final Pattern SNAPSHOT_ID = Pattern.compile("snapshot_id_(\\d+)");
    public static final String TABLE_TYPE_PROP_KEY = "table_type";

    public static final String PAIMON_INPUT_FORMAT_CLASS_NAME =
            "org.apache.paimon.hive.mapred.PaimonInputFormat";
    public static final String PAIMON_OUTPUT_FORMAT_CLASS_NAME =
            "org.apache.paimon.hive.mapred.PaimonOutputFormat";
    public static final String SPARK_SOURCE_PREFIX = "spark.sql.sources.";
    public static final String PROVIDER = "provider";
    public static final String SPARK_SOURCE_PROVIDER = SPARK_SOURCE_PREFIX + PROVIDER;

    public static TableFormatType routeCatalogViaLoad(Client client, String catalogName, Identifier ident) {
        TableFormatType routeTableFormatType = TableFormatType.HIVE;
        if (PolyCatClientSparkHelper.isPathIdentifier(ident)) {
            // TODO tmp
            routeTableFormatType = TableFormatType.ICEBERG;
        } else {
            Table polyCatTable = PolyCatClientHelper.getTable(client, PolyCatClientSparkHelper.getTableName(client.getProjectId(), catalogName, ident));
            routeTableFormatType = routeViaTable(polyCatTable);
        }
        log.info("Route catalog to tableFormat={}.", routeTableFormatType.toString());
        return routeTableFormatType;
    }

    public static TableFormatType routeViaTable(Table table) {
        Map<String, String> properties = table.getParameters();
        if (isIcebergTable(properties)) {
            return TableFormatType.ICEBERG;
        } else if (isHoodieTable(properties)) {
            return TableFormatType.DELTA;
        } else if (isDeltaTable(properties)) {
            return TableFormatType.HUDI;
        }  else if (isPaimonTable(table)) {
            return TableFormatType.PAIMON;
        } else {
            return TableFormatType.HIVE;
        }
    }

    public static TableFormatType routeViaTableProps(Map<String, String> properties) {
        String provider = getTableProvider(properties);
        String tableFormat = PolyCatOptions.getValueViaPrefixKey(properties, PolyCatOptions.TABLE_FORMAT_TYPE);
        if (TableFormatType.isTableFormatType(provider)) {
            if (TableFormatType.isTableFormatType(tableFormat) && !tableFormat.equalsIgnoreCase(provider)) {
                throw new IllegalArgumentException(String.format("Create table param provide: %s and %s: %s inconsistent.",
                        provider, PolyCatOptions.TABLE_FORMAT_TYPE.key(), tableFormat));
            } else {
                return TableFormatType.valueOf(provider.toUpperCase(Locale.ROOT));
            }
        }
        if (tableFormat == null) {
            return TableFormatType.HIVE;
        }
        return TableFormatType.valueOf(tableFormat.toUpperCase(Locale.ROOT));
    }

    public static boolean isPaimonTable(Table table) {
        if (isPaimonTable(table.getParameters())) {
            return true;
        }
        return PAIMON_INPUT_FORMAT_CLASS_NAME.equals(table.getStorageDescriptor().getInputFormat())
                && PAIMON_OUTPUT_FORMAT_CLASS_NAME.equals(table.getStorageDescriptor().getOutputFormat());
    }

    public static boolean isDeltaTable(Map<String, String> properties) {
        return TableFormatType.DELTA.name().equalsIgnoreCase(getTableProvider(properties));
    }

    public static boolean isHoodieTable(Map<String, String> properties) {
        return TableFormatType.HUDI.name().equalsIgnoreCase(getTableProvider(properties));
    }

    private static boolean isPaimonTable(Map<String, String> properties) {
        // paimon #2492
        return TableFormatType.PAIMON.name().equalsIgnoreCase(getTableType(properties));
    }

    public static boolean isIcebergTable(Map<String, String> properties) {
        return TableFormatType.ICEBERG.name().equalsIgnoreCase(getTableType(properties));
    }

    public static String getTableProvider(Map<String, String> properties) {
        if (MapUtils.isNotEmpty(properties) && properties.containsKey(PROVIDER)) {
            return ConfUtil.getMapPropVal(properties, PROVIDER);
        }
        return ConfUtil.getMapPropVal(properties, SPARK_SOURCE_PROVIDER);
    }

    public static String getTableType(Map<String, String> properties) {
        return ConfUtil.getMapPropVal(properties, TABLE_TYPE_PROP_KEY);
    }
}
