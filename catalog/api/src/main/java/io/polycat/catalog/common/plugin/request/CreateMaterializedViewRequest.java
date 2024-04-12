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
package io.polycat.catalog.common.plugin.request;

import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.plugin.request.base.DatabaseRequestBase;
import io.polycat.catalog.common.plugin.request.input.IndexInput;
import io.polycat.catalog.common.plugin.CatalogContext;
import lombok.Data;

@Data
public class CreateMaterializedViewRequest extends DatabaseRequestBase<IndexInput> {

  @Override
  public void initContext(CatalogContext context) {
    super.initContext(context);
    if (input.getUserId() == null) {
      input.setUserId(context.getUserName());
    }
  }

  @Override
  public Operation getOperation() {
    return Operation.CREATE_MATERIALIZED_VIEW;
  }
}
