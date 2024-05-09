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
package org.apache.spark.sql

import io.polycat.catalog.common.{GlobalConfig, PolyCatConf}
import org.apache.spark.sql.catalyst.parser.ParserInterface

import java.io.File

class PolyCatExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {

    initConfig()

    extensions.injectParser((_, parser: ParserInterface) => new PolyCatCommandParser(parser))

    extensions.injectPostHocResolutionRule((sparkSession: SparkSession) => new PolyCatCheckPermission(sparkSession))

  }

  private def initConfig() = {
    var confPath = System.getenv("SPARK_HOME") + "/conf/gateway.conf"
    val confFile = new File(confPath)
    if (!confFile.exists) confPath = "../../conf/gateway.conf"
    val conf = new PolyCatConf(confPath)
    GlobalConfig.set(GlobalConfig.WAREHOUSE, conf.getWarehouse)
    GlobalConfig.set(GlobalConfig.OBS_AK, conf.getObsAk)
    GlobalConfig.set(GlobalConfig.OBS_SK, conf.getObsSk)
    GlobalConfig.set(GlobalConfig.OBS_ENDPOINT, conf.getObsEndpoint)
  }

}
