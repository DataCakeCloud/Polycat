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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.polycat.catalog.common.model.base.IndexBase;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

/**
 * This Class will be used to store index information. Index can be materialized view or
 * secondary index
 */
@ApiModel(description = "index")
@Data
public class IndexInfo extends IndexBase {

  @ApiModelProperty(value = "query sql", required = true)
  private String querySql;

  @ApiModelProperty(value = "parent table ident", required = true)
  private List<TableName> tableNames;

  @ApiModelProperty(value = "create time", required = true)
  private long createTime;

  @ApiModelProperty(value = "owner", required = false)
  private String owner;

  @ApiModelProperty(value = "dropped time", required = true)
  //@JsonInclude(Include.NON_NULL)
  private String droppedTime;

  @ApiModelProperty(value = "properties")
  private Map<String, String> properties = new HashMap<>();

}
