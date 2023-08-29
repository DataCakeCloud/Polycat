/**
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
package org.apache.spark.adapter

import org.apache.spark.sql.catalyst.plans.logical.{DropTable, LogicalPlan}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.command.ResetCommand

object MatchResetCommand {

  def unapply(plan: LogicalPlan): Option[Boolean] = {
    if (plan == ResetCommand) {
      Some(true)
    } else {
      None
    }
  }

}


object SparkAdapter {

  def getTable(dropTable: DropTable): Identifier = {
    dropTable.ident
  }

  def getTableCatalog(dropTable: DropTable): TableCatalog = {
    dropTable.catalog
  }

}
