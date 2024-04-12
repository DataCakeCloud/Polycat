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
package io.polycat.catalog.common.model;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.polycat.catalog.common.plugin.request.input.TableInput;
import io.polycat.catalog.store.protos.common.StorageInfo;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;


@Data
public class TableStorageObject {
    public static final String DEFAULT_SD_FILE_FORMAT = "parquet";
    private String location = "";
    private String sourceShortName = "";
    private String fileFormat = "parquet";
    private String inputFormat = "parquet";
    private String outputFormat = "parquet";
    private Map<String, String> parameters = Collections.emptyMap();
    private Boolean compressed = false;
    private Integer numberOfBuckets = 0;
    private List<String> bucketColumns = new ArrayList<>();
    private SerDeInfo serdeInfo;
    private List<Order> sortColumns = new ArrayList<>();
    private Boolean storedAsSubDirectories = false;
    private SkewedInfo skewedInfo = new SkewedInfo();

    public TableStorageObject(TableInput tableInput, String location) {
        StorageDescriptor storageDescriptor = tableInput.getStorageDescriptor();
        this.location = location;
        if (StringUtils.isNotBlank(storageDescriptor.getSourceShortName())) {
            this.sourceShortName = storageDescriptor.getSourceShortName();
        }
        if (StringUtils.isNotBlank(storageDescriptor.getFileFormat())) {
            this.fileFormat = storageDescriptor.getFileFormat();
        }
        if (StringUtils.isNotBlank(storageDescriptor.getInputFormat())) {
            this.inputFormat = storageDescriptor.getInputFormat();
        }
        if (StringUtils.isNotBlank(storageDescriptor.getOutputFormat())) {
            this.outputFormat = storageDescriptor.getOutputFormat();
        }
        this.parameters = storageDescriptor.getParameters();
        if (storageDescriptor.getCompressed() != null) {
            this.compressed = storageDescriptor.getCompressed();
        }
        if (storageDescriptor.getNumberOfBuckets() != null) {
            this.numberOfBuckets = storageDescriptor.getNumberOfBuckets();
        }
        if (storageDescriptor.getBucketColumns() != null) {
            this.bucketColumns = storageDescriptor.getBucketColumns();
        }
        if (storageDescriptor.getSerdeInfo() != null) {
            this.serdeInfo = storageDescriptor.getSerdeInfo();
        } else {
            this.serdeInfo = new SerDeInfo();
        }
        if (storageDescriptor.getSortColumns() != null) {
            this.sortColumns = storageDescriptor.getSortColumns();
        }
        if (storageDescriptor.getStoredAsSubDirectories() != null) {
            this.storedAsSubDirectories = storageDescriptor.getStoredAsSubDirectories();
        }
        if (storageDescriptor.getSkewedInfo() != null) {
            this.skewedInfo = storageDescriptor.getSkewedInfo();
        }
    }

    public TableStorageObject(StorageInfo storageInfo) {
        this.location = storageInfo.getLocation();
        this.sourceShortName = storageInfo.getSourceShortName();
        this.fileFormat = storageInfo.getFileFormat();
        this.inputFormat = storageInfo.getInputFormat();
        this.outputFormat = storageInfo.getOutputFormat();
        this.parameters = storageInfo.getParametersMap();
        this.compressed = storageInfo.getCompressed();
        this.numberOfBuckets = storageInfo.getNumberOfBuckets();
        this.bucketColumns = storageInfo.getBucketColumnsList();
        this.serdeInfo = new SerDeInfo(storageInfo.getSerdeInfo().getName(),
            storageInfo.getSerdeInfo().getSerializationLibrary(), storageInfo.getSerdeInfo().getParametersMap());
        this.sortColumns = new ArrayList<>(storageInfo.getSortColumnsList().size());
        storageInfo.getSortColumnsList().forEach(order -> {
            this.sortColumns.add(new Order(order.getColumn(), order.getSortOrder()));
        });
        this.storedAsSubDirectories = storageInfo.getStoredAsSubDirectories();
        this.skewedInfo = constructSkewedInfo(storageInfo.getSkewedInfo());
    }

    private SkewedInfo constructSkewedInfo(io.polycat.catalog.store.protos.common.SkewedInfo skewedInfo) {
        SkewedInfo si = new SkewedInfo();
        if (skewedInfo == null) {
            return si;
        }
        si.setSkewedColumnNames(skewedInfo.getSkewedColumnNamesList());
        if (skewedInfo.getSkewedColumnValuesCount() > 0) {
            List<List<String>> skewedValues = Lists.newArrayList();
            for (int i = 0; i < skewedInfo.getSkewedColumnValuesCount(); i++) {
                skewedValues.add(new ArrayList<String>(skewedInfo.getSkewedColumnValues(i).getValuesList()));
            }
            si.setSkewedColumnValues(skewedValues);
        }
        si.setSkewedColumnValueLocationMaps(skewedInfo.getSkewedColumnValueLocationMapsMap());
        return si;
    }

}
