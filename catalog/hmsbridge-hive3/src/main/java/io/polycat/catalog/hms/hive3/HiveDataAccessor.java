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
package io.polycat.catalog.hms.hive3;

import java.security.InvalidParameterException;
import java.util.*;
import java.util.stream.Collectors;

import io.polycat.catalog.common.model.Column;

import org.apache.hadoop.hive.metastore.api.*;

public class HiveDataAccessor {


    private static int toInteger(Long longNum) {
        if (longNum == null) {
            return 0;
        }
        return longNum.intValue();
    }

    private static StorageDescriptor convertToStorageDescriptor(
        io.polycat.catalog.common.model.StorageDescriptor lmsSd) {
        if (Objects.nonNull(lmsSd)) {
            StorageDescriptor hiveSd = new StorageDescriptor();
            hiveSd.setCols(convertToColumns(lmsSd.getColumns()));
            hiveSd.setLocation(lmsSd.getLocation());
            hiveSd.setInputFormat(lmsSd.getInputFormat());
            hiveSd.setOutputFormat(lmsSd.getOutputFormat());
            hiveSd.setCompressed(lmsSd.getCompressed());
            hiveSd.setNumBuckets(lmsSd.getNumberOfBuckets());
            hiveSd.setSerdeInfo(convertToSerdeInfo(lmsSd.getSerdeInfo()));
            hiveSd.setBucketCols(lmsSd.getBucketColumns());
            hiveSd.setSortCols(convertToSortColumns(lmsSd.getSortColumns()));
            hiveSd.setParameters(lmsSd.getParameters());
            hiveSd.setSkewedInfo(convertToSkewInfo(lmsSd.getSkewedInfo()));
            return hiveSd;
        }
        return null;
    }

    private static SkewedInfo convertToSkewInfo(io.polycat.catalog.common.model.SkewedInfo lmsSkewInfo) {
        if (Objects.nonNull(lmsSkewInfo)) {
            return new SkewedInfo(lmsSkewInfo.getSkewedColumnNames()
                , lmsSkewInfo.getSkewedColumnValues(), getSkewedValueLocationMap(lmsSkewInfo));
        }
        return null;
    }

    private static Map<List<String>, String> getSkewedValueLocationMap(
        io.polycat.catalog.common.model.SkewedInfo lmsSkewInfo) {
        Map<List<String>, String> hiveMap = new HashMap<>();
        Map<String, String> lsmSkewMap = lmsSkewInfo.getSkewedColumnValueLocationMaps();
        lsmSkewMap.keySet().forEach(valStr -> hiveMap.put(convertToValueList(valStr), lsmSkewMap.get(valStr)));
        return hiveMap;
    }

    public static List<String> convertToValueList(final String valStr) {
        boolean isValid = false;
        if (valStr == null) {
            return null;
        }
        List<String> valueList = new ArrayList<>();
        try {
            for (int i = 0; i < valStr.length(); ) {
                StringBuilder length = new StringBuilder();
                for (int j = i; j < valStr.length(); j++) {
                    if (valStr.charAt(j) != '$') {
                        length.append(valStr.charAt(j));
                        i++;
                        isValid = false;
                    } else {
                        int lengthOfString = Integer.parseInt(length.toString());
                        valueList.add(valStr.substring(j + 1, j + 1 + lengthOfString));
                        i = j + 1 + lengthOfString;
                        isValid = true;
                        break;
                    }
                }
            }
        } catch (NumberFormatException | StringIndexOutOfBoundsException e) {
            isValid = false;
        }

        if (!isValid) {
            throw new InvalidParameterException(String.format("str %s is illegal", valStr));
        }
        return valueList;
    }

    private static List<Order> convertToSortColumns(List<io.polycat.catalog.common.model.Order> lmsSortCols) {
        if (Objects.nonNull(lmsSortCols)) {
            return lmsSortCols.stream()
                .map(lmsCol -> new Order(lmsCol.getColumn(), lmsCol.getSortOrder()))
                .collect(Collectors.toList());
        }
        return null;
    }

    private static SerDeInfo convertToSerdeInfo(io.polycat.catalog.common.model.SerDeInfo lmsSerDe) {
        if (Objects.nonNull(lmsSerDe)) {
            SerDeInfo serde = new SerDeInfo();
            serde.setName(lmsSerDe.getName());
            serde.setSerializationLib(lmsSerDe.getSerializationLibrary());
            serde.setParameters(lmsSerDe.getParameters());
            return serde;
        }
        return null;
    }

    private static List<FieldSchema> convertToColumns(List<Column> columns) {
        if (Objects.isNull(columns) || columns.isEmpty()) {
            return null;
        }
        return columns.stream()
            .map(column -> new FieldSchema(column.getColumnName(), column.getColType(), column.getComment()))
            .collect(Collectors.toList());
    }

}
