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
import java.io.Serializable;
import java.util.ArrayList;
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

@Data
@NoArgsConstructor
@AllArgsConstructor
@ApiModel
public class SkewedInfo implements Serializable {
    @ApiModelProperty(value = "skewed column names")
    private List<String> skewedColumnNames;

    @ApiModelProperty(value = "A list of values that appear so frequently as to be considered skewed.")
    private List<List<String>> skewedColumnValues;

    @ApiModelProperty(value = "A mapping of skewed values to the columns that contain them.")
    private Map<String, String> skewedColumnValueLocationMaps;

    public SkewedInfo(SkewedInfo other) {
        skewedColumnNames = other.getSkewedColumnNames();
        skewedColumnValues = other.getSkewedColumnValues();
        skewedColumnValueLocationMaps = other.getSkewedColumnValueLocationMaps();
    }

    @ApiModelProperty(hidden = true)
    public SkewedInfo deepCopy() {
        SkewedInfo cpy = new SkewedInfo();
        cpy.setSkewedColumnNames(skewedColumnNames == null ? null : new ArrayList<>(skewedColumnNames));
        cpy.setSkewedColumnValues(skewedColumnValues == null ?
            null : skewedColumnValues.stream().map(ArrayList::new).collect(Collectors.toList()));
        cpy.setSkewedColumnValueLocationMaps(new HashMap<>(skewedColumnValueLocationMaps));
        return cpy;
    }
}