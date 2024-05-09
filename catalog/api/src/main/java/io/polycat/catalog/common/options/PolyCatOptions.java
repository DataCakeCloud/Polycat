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
package io.polycat.catalog.common.options;

import java.util.Map;

import io.polycat.catalog.common.Constants;
import io.polycat.catalog.common.TableFormatType;
import io.polycat.catalog.common.utils.ConfUtil;

public class PolyCatOptions {

    public static final String IDENTIFIER =  "polycat";

    public static final String POLYCAT_PREFIX = IDENTIFIER + ".";

    /**
     * Value can be tableFormat type , refer: {@link TableFormatType}
     * Decide which tableFormat logic to use.
     */
    public static final ConfigOption<String> TABLE_FORMAT_TYPE =
            ConfigOptions.key("table-format-type")
                    .stringType()
                    .defaultValue(null)
                    .withDescription(
                            "Value can be tableFormat type, decide which tableFormat logic to use.");

    public static final ConfigOption<Boolean> PARTITION_OVERWRITE =
            ConfigOptions.key(Constants.PARTITION_OVERWRITE)
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to overwrite if partition exists.");

    public static final ConfigOption<String> ICEBERG_CATALOG_CLASS =
            ConfigOptions.key("iceberg-catalog-impl")
                    .stringType()
                    .defaultValue("io.polycat.catalog.iceberg.PolyCatCatalog")
                    .withDescription(
                            "Value can be tableFormat type, decide which tableFormat logic to use.");

    public static final ConfigOption<String> DEFAULT_WAREHOUSE =
            ConfigOptions.key("default-warehouse")
                    .stringType()
                    .defaultValue("/tmp/polycat/warehouse")
                    .withDescription(
                            "default warehouse location");

    public static <T> T getValueViaPrefixKey(Map<String, String> options, ConfigOption<T> configOption) {
        T value = ConfUtil.convertValue(options.get(POLYCAT_PREFIX + configOption.key()), configOption.getClazz());
        if (value == null) {
            return configOption.defaultValue();
        }
        return value;
    }

    public static <T> T getValue(Map<String, String> options, ConfigOption<T> configOption) {
        T value = ConfUtil.convertValue(options.get(configOption.key()), configOption.getClazz());
        if (value == null) {
            return configOption.defaultValue();
        }
        return value;
    }

    private PolyCatOptions() {}
}
