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
package io.polycat.catalog.common.plugin.request.input;

import java.util.List;
import java.util.Map;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

/**
 * This class will be used as an input to create index (either materialized view
 * or secondary index) request
 */
@Data
@ApiModel(description = "index input")
public class IndexInput {

  @ApiModelProperty(value = "index name", required = true, example = "t1")
  private String name;

  @ApiModelProperty(value = "description")
  private String description;

  @ApiModelProperty(value = "owner", required = true, example = "zhangsan")
  private String userId;

  @ApiModelProperty(value = "properties")
  private Map<String, String> properties;

  @ApiModelProperty(value = "columnOrderMap")
  private Map<String, List<String>> columnOrderMap;

  @ApiModelProperty(value = "factTables")
  private List<TableNameInput> factTables;

  @ApiModelProperty(value = "querySql")
  private String querySql;

  @ApiModelProperty(value = "isHMSTable")
  private String isHMSTable;

}
