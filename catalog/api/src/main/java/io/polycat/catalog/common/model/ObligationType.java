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

import lombok.Getter;

/* obligations
NONE;
COLUMN_FILTER:(column_name, column_name,column_name):include
ROW_FILTER:() and () or ();
DATA_MASK:column_name:mask;
*/
public enum ObligationType{
    NONE("None", 0),
    COLUMN_FILTER("Column Filter", 2),
    ROW_FILTER("Row Filter", 2),
    DATA_MASK("Data Mask", 3);

    @Getter
    private final String printName;

    @Getter
    private final int length;

    ObligationType(String printName, int length){
        this.printName = printName;
        this.length = length;
    }
}
