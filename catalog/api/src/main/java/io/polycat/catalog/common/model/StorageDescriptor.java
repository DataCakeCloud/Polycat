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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.logging.log4j.util.Strings;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ApiModel(value = "storage descriptor")
public class StorageDescriptor {

    /**
     * <p>
     * The physical location of the table. By default this takes the form of the warehouse location, followed by the
     * database location in the warehouse, followed by the table name.
     * </p>
     */
    @ApiModelProperty(value = "location")
    private String location;

    @ApiModelProperty(value = "source short name", required = true, example = "carbon")
    private String sourceShortName = "";

    @ApiModelProperty(value = "file format", required = true, example = "parquet")
    private String fileFormat = "parquet";
    /**
     * <p>
     * The input format: <code>SequenceFileInputFormat</code> (binary), or <code>TextInputFormat</code>, or a custom
     * format.
     * </p>
     */
    @ApiModelProperty(value = "input format")
    private String inputFormat = Strings.EMPTY;
    /**
     * <p>
     * The output format: <code>SequenceFileOutputFormat</code> (binary), or <code>IgnoreKeyTextOutputFormat</code>, or
     * a custom format.
     * </p>
     */
    @ApiModelProperty(value = "output format")
    private String outputFormat= Strings.EMPTY;

    /**
     * <p>
     * User-supplied properties in key-value form.
     * </p>
     */
    @ApiModelProperty(value = "parameters")
    private Map<String, String> parameters = Collections.emptyMap();

    @ApiModelProperty(value = "column")
    private List<Column> columns;

    /**
     * <p>
     * True if the data in the table is compressed, or False if not.
     * </p>
     */
    @ApiModelProperty(value = "compressed flag")
    private Boolean compressed = false;

    /**
     * <p>
     * Must be specified if the table contains any dimension columns.
     * </p>
     */
    @ApiModelProperty(value = "number of buckets")
    private Integer numberOfBuckets = -1;

    @ApiModelProperty(value = "bucket key columns")
    private List<String> bucketColumns = Collections.emptyList();

    @ApiModelProperty(value = "serializable and deserializable info", required = true)
    private SerDeInfo serdeInfo;

    @ApiModelProperty(value = "sort order")
    private List<Order> sortColumns = Collections.emptyList();

    @ApiModelProperty(value = "skewed info")
    private SkewedInfo skewedInfo;

    @ApiModelProperty(value = "stored as sub directories")
    private Boolean storedAsSubDirectories = false;

    public StorageDescriptor(StorageDescriptor other) {
        this.location = other.getLocation();
        this.sourceShortName = other.getSourceShortName();
        this.fileFormat = other.getFileFormat();
        this.inputFormat = other.getInputFormat();
        this.outputFormat = other.getOutputFormat();
        this.compressed = other.getCompressed();
        this.numberOfBuckets = other.getNumberOfBuckets();
        this.storedAsSubDirectories = other.getStoredAsSubDirectories();
        this.parameters = other.getParameters();
        this.columns = other.getColumns();
        this.bucketColumns = other.getBucketColumns();
        this.serdeInfo = other.getSerdeInfo();
        this.sortColumns = other.getSortColumns();
        this.skewedInfo = other.getSkewedInfo();
    }
    
    public StorageDescriptor deepCopy() {
        StorageDescriptor newSd = new StorageDescriptor();
        newSd.setLocation(location);
        newSd.setSourceShortName(sourceShortName);
        newSd.setFileFormat(fileFormat);
        newSd.setInputFormat(inputFormat);
        newSd.setOutputFormat(outputFormat);
        newSd.setOutputFormat(outputFormat);
        newSd.setNumberOfBuckets(numberOfBuckets);
        if (parameters != null) {
            Map<String, String> params = new HashMap<>();
            params.putAll(parameters);
            newSd.setParameters(params);
        }
        newSd.setColumns(columns == null ? null : columns.stream().map(Column::new).collect(Collectors.toList()));
        newSd.setSerdeInfo(serdeInfo == null ? null : serdeInfo.deepCopy());;
        newSd.skewedInfo = getSkewedInfo() == null ? null : getSkewedInfo().deepCopy();
        newSd.setStoredAsSubDirectories(storedAsSubDirectories);
        newSd.setBucketColumns(bucketColumns == null ? null
            : bucketColumns.stream().map(String::new).collect(Collectors.toList()));
        newSd.sortColumns = getSortColumns() == null ? null
            : getSortColumns().stream().map(Order::new).collect(Collectors.toList());
        return newSd;
    }

    public Map<String, String> getParameters() {
        if (parameters != null) {
            return parameters;
        }
        return Collections.emptyMap();
    }
}
