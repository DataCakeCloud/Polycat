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

import java.util.ArrayList;
import java.util.List;

import io.polycat.catalog.store.protos.IndexPartitionSet;
import io.polycat.catalog.store.protos.common.TableIndexPartitionSetInfo;
import lombok.Data;

@Data
public class IndexPartitionSetObject {
    private String setId;
    private String catalogId;
    private String databaseId;
    private String tableId;
    private String curSetId;   //标识当前使用的DataPartitionSet ID
    private List<String> setIds;

    public IndexPartitionSetObject() {
        this.setIds = new ArrayList<>();
    }

    public IndexPartitionSetObject(String setId, String catalogId, String databaseId, String tableId,
        TableIndexPartitionSetInfo tableIndexInfo) {
        this.setId = setId;
        this.catalogId = catalogId;
        this.databaseId = databaseId;
        this.tableId = tableId;
        this.curSetId = tableIndexInfo.getCurSetId();
        this.setIds = new ArrayList<>();
        this.setIds.addAll(tableIndexInfo.getSetIdsList());
    }

    public void addSetId(String setId) {
        this.setIds.add(setId);
    }

}
