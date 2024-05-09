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

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ColumnStatisticsAggrObject {
    private String columnName;
    private String columnType;
    private Long longLowValue;
    private Long longHighValue;
    private Long estimationLongNumDistincts;
    private Double doubleLowValue;
    private Double doubleHighValue;
    private Long estimationDoubleNumDistincts;
    private String decimalLowValue;
    private String decimalHighValue;
    private String estimationDecimalNumDistincts;
    private Long numNulls;
    private Long lowerNumDistincts;
    private Long higherNumDistincts;
    private Double avgColLen;
    private Long maxColLen;
    private Long numTrues;
    private Long numFalses;
}
