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
package io.polycat.catalog.spark

import io.polycat.catalog.client.Client
import io.polycat.catalog.common.TableFormatType
import io.polycat.catalog.common.exception.NoSuchObjectException
import io.polycat.catalog.common.model.TableName
import io.polycat.catalog.{CatalogAdapter, CatalogRouter}
import io.polycat.catalog.CatalogRouter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.hudi.catalog.HoodieCatalog
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

class SparkCatalog extends StagingTableCatalog with SupportsNamespaces {

  private var catalogName: String = "spark_catalog"

  override val defaultNamespace: Array[String] = Array(PolyCatClientSparkHelper.DEFAULT_DATABASE)

  private var client: Client = null

  private val icebergCatalog: org.apache.iceberg.spark.SparkCatalog = new org.apache.iceberg.spark.SparkCatalog

  private val hoodieCatalog: HoodieCatalog = new HoodieCatalog

  override def purgeTable(ident: Identifier): Boolean = {
    CatalogRouter.routeCatalogViaLoad(client, name(), ident) match {
      case TableFormatType.ICEBERG =>
        icebergCatalog.purgeTable(ident)
      case TableFormatType.HUDI =>
        hoodieCatalog.purgeTable(ident)
      case _ =>
        super.purgeTable(ident)
    }
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    namespace match {
      case Array(db) =>
        PolyCatClientSparkHelper
                  .listTables(client, name(), Array(db), defaultNamespace)
      case _ =>
        throw new NoSuchNamespaceException(namespace)
    }
  }

  override def loadTable(ident: Identifier): Table = {
    try {
      CatalogRouter.routeCatalogViaLoad(client, name(), ident) match {
        case TableFormatType.ICEBERG =>
          icebergCatalog.loadTable(ident)
        case TableFormatType.HUDI =>
          hoodieCatalog.loadTable(ident)
        case TableFormatType.HIVE =>
          PolyCatClientSparkHelper.getSparkTable(client, getTableName(ident))
        case _ => throw new UnsupportedOperationException
      }
    } catch {
      case e: NoSuchObjectException => throw new NoSuchTableException(ident)
    }
  }

  private def getTableName(ident: Identifier): TableName = {
    PolyCatClientSparkHelper.getTableName(client.getProjectId, name(), ident)
  }

  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): Table = {
    CatalogRouter.routeViaTableProps(properties) match {
      case TableFormatType.ICEBERG =>
        icebergCatalog.createTable(ident, schema, partitions, properties)
      case TableFormatType.HUDI =>
        hoodieCatalog.createTable(ident, schema, partitions, properties)
      case TableFormatType.HIVE =>
        PolyCatClientSparkHelper.createTable(client, getTableName(ident), schema, partitions, properties)
      case _ => throw new UnsupportedOperationException
    }
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    val table: Table = ensureTable(ident)
    CatalogRouter.routeViaTableProps(table.properties()) match {
      case TableFormatType.ICEBERG =>
        icebergCatalog.alterTable(ident, changes:_*)
      case TableFormatType.HUDI =>
        hoodieCatalog.alterTable(ident, changes:_*)
      case TableFormatType.HIVE =>
        PolyCatClientSparkHelper.alterTable(client, getTableName(ident), changes.toArray)
      case _ => throw new UnsupportedOperationException
    }

  }

  def ensureTable(ident: Identifier): Table = {
    val table: Table = loadTable(ident)
    if(table == null) {
      throw new NoSuchTableException(ident)
    }
    table
  }

  override def dropTable(ident: Identifier): Boolean = {
    val table: Table = ensureTable(ident)
    CatalogRouter.routeViaTableProps(table.properties()) match {
      case TableFormatType.ICEBERG =>
        icebergCatalog.dropTable(ident)
      case TableFormatType.HUDI =>
        hoodieCatalog.dropTable(ident)
      case TableFormatType.HIVE =>
        PolyCatClientSparkHelper.dropTable(client, getTableName(ident))
      case _ => throw new UnsupportedOperationException
    }
  }

  // TODO
  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    val table: Table = ensureTable(oldIdent)
    CatalogRouter.routeViaTableProps(table.properties()) match {
      case TableFormatType.ICEBERG =>
        icebergCatalog.dropTable(oldIdent)
      case TableFormatType.HUDI =>
        hoodieCatalog.dropTable(oldIdent)
      case TableFormatType.HIVE =>
        PolyCatClientSparkHelper.dropTable(client, getTableName(oldIdent))
      case _ => throw new UnsupportedOperationException
    }
  }

  override def listNamespaces(): Array[Array[String]] = {
    PolyCatClientSparkHelper.listDatabases(client, name())
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    namespace match {
      case Array() =>
        listNamespaces()
      case Array(db) if PolyCatClientSparkHelper.databaseExists(client, name(), db) =>
        Array()
      case _ =>
        throw new NoSuchNamespaceException(namespace)
    }
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    PolyCatClientSparkHelper.getDatabaseProperties(client, name(), namespace(0))
  }

  override def createNamespace(namespace: Array[String], metadata: util.Map[String, String]): Unit = {
    PolyCatClientSparkHelper.createDatabase(client, name(), namespace(0), metadata)
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    PolyCatClientSparkHelper.alterDatabase(client, name(), namespace(0), changes.toArray)
  }

  override def dropNamespace(namespace: Array[String]): Boolean = {
    PolyCatClientSparkHelper.dropDatabase(client, name(), namespace(0))
  }

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    val conf = SparkSession.getActiveSession.get.sparkContext.getConf
    options.forEach((k, v) => {conf.set(k, v)})
    PolyCatClientSparkHelper.initCatalogNameMapping(conf)
    catalogName = PolyCatClientSparkHelper.getCatalogMappingName(name)
    val optionsMap: util.TreeMap[String, String] = new util.TreeMap(String.CASE_INSENSITIVE_ORDER);
    optionsMap.putAll(options.asCaseSensitiveMap())
    CatalogAdapter.configAdaptIceberg(optionsMap)
    val adaptedOptions = new CaseInsensitiveStringMap(optionsMap)
    client = PolyCatClientSparkHelper.getClient
    icebergCatalog.initialize(catalogName, adaptedOptions)
    hoodieCatalog.initialize(catalogName, adaptedOptions)

  }

  override def name(): String = {
    catalogName
  }

  override def stageCreate(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): StagedTable = {
    CatalogRouter.routeViaTableProps(properties) match {
      case TableFormatType.ICEBERG =>
        icebergCatalog.stageCreate(ident, schema, partitions, properties)
      case TableFormatType.HUDI =>
        hoodieCatalog.stageCreate(ident, schema, partitions, properties)
      case TableFormatType.HIVE =>
        PolyCatClientSparkHelper.createStagedTable(client, getTableName(ident), schema, partitions, properties)
      case _ => throw new UnsupportedOperationException
    }
  }

  override def stageReplace(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): StagedTable = {
    CatalogRouter.routeViaTableProps(properties) match {
      case TableFormatType.ICEBERG =>
        icebergCatalog.stageReplace(ident, schema, partitions, properties)
      case TableFormatType.HUDI =>
        hoodieCatalog.stageReplace(ident, schema, partitions, properties)
      case _ => throw new UnsupportedOperationException
    }
  }

  override def stageCreateOrReplace(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): StagedTable = {
    CatalogRouter.routeViaTableProps(properties) match {
      case TableFormatType.ICEBERG =>
        icebergCatalog.stageCreateOrReplace(ident, schema, partitions, properties)
      case TableFormatType.HUDI =>
        hoodieCatalog.stageCreateOrReplace(ident, schema, partitions, properties)
      case _ => throw new UnsupportedOperationException
    }
  }
}
