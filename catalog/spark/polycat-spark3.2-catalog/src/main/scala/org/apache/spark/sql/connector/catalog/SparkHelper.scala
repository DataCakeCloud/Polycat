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
package org.apache.spark.sql.connector.catalog

import io.polycat.catalog.common.model.{Column, Partition}
import io.polycat.catalog.common.types.{DataTypes => LmsDataTypes}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, BinaryComparison, BoundReference, Cast, Contains, EndsWith, EqualNullSafe, EqualTo, Expression, GreaterThanOrEqual, In, InSet, LessThanOrEqual, Literal, Not, Or, Predicate, StartsWith}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateFormatter, DateTimeUtils, TypeUtils}
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform, LogicalExpressions, Transform}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, PartitionPath}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ThreadUtils

import java.util
import java.util.concurrent.ForkJoinPool
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

object SparkHelper {

  def createIdentifier(namespace: Array[String], name: String) = Identifier.of(namespace, name)

  def convertDataType(dataType: String): DataType = {
    var result = dataType.toUpperCase.trim
    if (result.equals(LmsDataTypes.BOOLEAN.getName)) {
      return DataTypes.BooleanType
    } else if (result.equals(LmsDataTypes.TINYINT.getName)) {
      return DataTypes.ByteType
    } else if (result.equals(LmsDataTypes.BYTE.getName)) {
      return DataTypes.ByteType
    } else if (result.equals(LmsDataTypes.SMALLINT.getName)) {
      return DataTypes.ShortType
    } else if (result.equals(LmsDataTypes.SHORT.getName)) {
      return DataTypes.ShortType
    } else if (result.equals(LmsDataTypes.INTEGER.getName)) {
      return DataTypes.IntegerType
    } else if (result.equals(LmsDataTypes.INT.getName)) {
      return DataTypes.IntegerType
    } else if (result.equals(LmsDataTypes.BIGINT.getName)) {
      return DataTypes.LongType
    } else if (result.equals(LmsDataTypes.LONG.getName)) {
      return DataTypes.LongType
    } else if (result.equals(LmsDataTypes.FLOAT.getName)) {
      return DataTypes.FloatType
    } else if (result.equals(LmsDataTypes.DOUBLE.getName)) {
      return DataTypes.DoubleType
    } else if (result.equals(LmsDataTypes.STRING.getName)) {
      return DataTypes.StringType
    } else if (result.equals(LmsDataTypes.VARCHAR.getName)) {
      return DataTypes.StringType
    } else if (result.equals(LmsDataTypes.TIMESTAMP.getName)) {
      return DataTypes.TimestampType
    } else if (result.equals(LmsDataTypes.DATE.getName)) {
      return DataTypes.DateType
    } else if (result.equals(LmsDataTypes.BLOB.getName)) {
      return DataTypes.BinaryType
    } else if (result.equals(LmsDataTypes.BINARY.getName)) {
      return DataTypes.BinaryType
    } else if (result.startsWith("DECIMAL(") && result.endsWith(")")) {
      return parseDecimal(result)
    } else if (result.startsWith("ARRAY<")) {
      return DataType.fromDDL(result)
    } else {
      return DataType.fromDDL(result)
    }
    throw new UnsupportedOperationException("failed to convert " + dataType)
  }

  def parseDecimal(dataType: String): DataType = {
    val i = dataType.indexOf("(")
    val j = dataType.indexOf(",")
    val k = dataType.indexOf(")")
    if (k != dataType.length - 1) {
      throw new UnsupportedOperationException("failed to convert " + dataType)
    }

    val str1 = dataType.substring(i + 1, j)
    val str2 = dataType.substring(j + 1, k)

    val precision = str1.toInt
    val scale = str2.toInt
    return DataTypes.createDecimalType(precision, scale)
  }


  def createTransform(columnName: String): Transform = {
    // IdentityTransform(FieldReference(Seq(columnName)))
    LogicalExpressions.identity(LogicalExpressions.reference(Seq(columnName)))
  }

  def convertField(x: Column) = {
    StructField(x.getColumnName, convertDataType(x.getColType), true, Metadata.empty)
  }

  def convertAttribute(x: StructType): Seq[AttributeReference] = {
    x.toAttributes
  }

  def getPaths(map: CaseInsensitiveStringMap): Seq[String] = {
    Seq(map.get(TableCatalog.PROP_LOCATION))
  }

  def getOptionsWithoutPaths(map: CaseInsensitiveStringMap): CaseInsensitiveStringMap = {
    val withoutPath = map.asCaseSensitiveMap().asScala.filterKeys { k =>
      !k.equalsIgnoreCase("path") && !k.equalsIgnoreCase("paths")
    }
    new CaseInsensitiveStringMap(withoutPath.toMap.asJava)
  }

  def getPathFilter(hadoopConf: Configuration): PathFilter = {
    // Dummy jobconf to get to the pathFilter defined in configuration
    // It's very expensive to create a JobConf(ClassUtil.findContainingJar() is slow)
    val jobConf = new JobConf(hadoopConf, this.getClass)
    val pathFilter = FileInputFormat.getInputPathFilter(jobConf)
    path: Path => {
      val name = path.getName
      if (name != "_SUCCESS" && name != "_temporary" && !name.startsWith(".")) {
        pathFilter == null || pathFilter.accept(path)
      } else {
        false
      }
    }
  }

  def throwCmdOnlyWorksOnPartitionedTablesError(cmd: String, tableIdentWithDB: String): Throwable = {
    throw QueryCompilationErrors.cmdOnlyWorksOnPartitionedTablesError(cmd, tableIdentWithDB)
  }

  def throwCmdOnlyWorksOnTableWithLocationError(cmd: String, tableIdentWithDB: String): Throwable = {
    throw QueryCompilationErrors.cmdOnlyWorksOnTableWithLocationError(cmd, tableIdentWithDB)
  }

  def parmap[I, O](in: Seq[I], prefix: String, maxThreads: Int)(f: I => O): Seq[O] = {
    ThreadUtils.parmap(in, prefix, maxThreads)(f)
  }

  def newForkJoinPool(prefix: String, maxThreadNumber: Int): ForkJoinPool = {
    ThreadUtils.newForkJoinPool(prefix, maxThreadNumber)
  }

  def convertFilters(filters: Seq[Expression]): String = {
    lazy val dateFormatter = DateFormatter()

    /**
     * An extractor that matches all binary comparison operators except null-safe equality.
     *
     * Null-safe equality is not supported by Hive metastore partition predicate pushdown
     */
    object SpecialBinaryComparison {
      def unapply(e: BinaryComparison): Option[(Expression, Expression)] = e match {
        case _: EqualNullSafe => None
        case _ => Some((e.left, e.right))
      }
    }

    object ExtractableLiteral {
      def unapply(expr: Expression): Option[String] = expr match {
        case Literal(null, _) => None // `null`s can be cast as other types; we want to avoid NPEs.
        case Literal(value, _: IntegralType) => Some(value.toString)
        case Literal(value, _: StringType) => Some(quoteStringLiteral(value.toString))
        case Literal(value, _: DateType) =>
          Some(dateFormatter.format(value.asInstanceOf[Int]))
        case _ => None
      }
    }

    object ExtractableLiterals {
      def unapply(exprs: Seq[Expression]): Option[Seq[String]] = {
        // SPARK-24879: The Hive metastore filter parser does not support "null", but we still want
        // to push down as many predicates as we can while still maintaining correctness.
        // In SQL, the `IN` expression evaluates as follows:
        //  > `1 in (2, NULL)` -> NULL
        //  > `1 in (1, NULL)` -> true
        //  > `1 in (2)` -> false
        // Since Hive metastore filters are NULL-intolerant binary operations joined only by
        // `AND` and `OR`, we can treat `NULL` as `false` and thus rewrite `1 in (2, NULL)` as
        // `1 in (2)`.
        // If the Hive metastore begins supporting NULL-tolerant predicates and Spark starts
        // pushing down these predicates, then this optimization will become incorrect and need
        // to be changed.
        val extractables = exprs
          .filter {
            case Literal(null, _) => false
            case _ => true
          }.map(ExtractableLiteral.unapply)
        if (extractables.nonEmpty && extractables.forall(_.isDefined)) {
          Some(extractables.map(_.get))
        } else {
          None
        }
      }
    }

    object ExtractableValues {
      private lazy val valueToLiteralString: PartialFunction[Any, String] = {
        case value: Byte => value.toString
        case value: Short => value.toString
        case value: Int => value.toString
        case value: Long => value.toString
        case value: UTF8String => quoteStringLiteral(value.toString)
      }

      def unapply(values: Set[Any]): Option[Seq[String]] = {
        val extractables = values.filter(_ != null).toSeq.map(valueToLiteralString.lift)
        if (extractables.nonEmpty && extractables.forall(_.isDefined)) {
          Some(extractables.map(_.get))
        } else {
          None
        }
      }
    }

    object ExtractableDateValues {
      private lazy val valueToLiteralString: PartialFunction[Any, String] = {
        case value: Int => dateFormatter.format(value)
      }

      def unapply(values: Set[Any]): Option[Seq[String]] = {
        val extractables = values.filter(_ != null).toSeq.map(valueToLiteralString.lift)
        if (extractables.nonEmpty && extractables.forall(_.isDefined)) {
          Some(extractables.map(_.get))
        } else {
          None
        }
      }
    }

    object SupportedAttribute {
      def unapply(attr: Attribute): Option[String] = {
        if (attr.dataType.isInstanceOf[IntegralType] || attr.dataType == StringType ||
          attr.dataType == DateType) {
          Some(attr.name)
        } else {
          None
        }
      }
    }

    def convertInToOr(name: String, values: Seq[String]): String = {
      values.map(value => s"$name = $value").mkString("(", " or ", ")")
    }

    def convertNotInToAnd(name: String, values: Seq[String]): String = {
      values.map(value => s"$name != $value").mkString("(", " and ", ")")
    }

    def hasNullLiteral(list: Seq[Expression]): Boolean = list.exists {
      case Literal(null, _) => true
      case _ => false
    }

    val useAdvanced = SQLConf.get.advancedPartitionPredicatePushdownEnabled
    val inSetThreshold = SQLConf.get.metastorePartitionPruningInSetThreshold

    object ExtractAttribute {
      def unapply(expr: Expression): Option[Attribute] = {
        expr match {
          case attr: Attribute => Some(attr)
          case Cast(child@IntegralType(), dt: IntegralType, _, _)
            if Cast.canUpCast(child.dataType.asInstanceOf[AtomicType], dt) => unapply(child)
          case _ => None
        }
      }
    }

    def convert(expr: Expression): Option[String] = expr match {
      case Not(InSet(_, values)) if values.size > inSetThreshold =>
        None

      case Not(In(_, list)) if hasNullLiteral(list) => None
      case Not(InSet(_, list)) if list.contains(null) => None

      case In(ExtractAttribute(SupportedAttribute(name)), ExtractableLiterals(values))
        if useAdvanced =>
        Some(convertInToOr(name, values))

      case Not(In(ExtractAttribute(SupportedAttribute(name)), ExtractableLiterals(values)))
        if useAdvanced =>
        Some(convertNotInToAnd(name, values))

      case InSet(child, values) if useAdvanced && values.size > inSetThreshold =>
        val dataType = child.dataType
        // Skip null here is safe, more details could see at ExtractableLiterals.
        val sortedValues = values.filter(_ != null).toSeq
          .sorted(TypeUtils.getInterpretedOrdering(dataType))
        convert(And(GreaterThanOrEqual(child, Literal(sortedValues.head, dataType)),
          LessThanOrEqual(child, Literal(sortedValues.last, dataType))))

      case InSet(child@ExtractAttribute(SupportedAttribute(name)), ExtractableDateValues(values))
        if useAdvanced && child.dataType == DateType =>
        Some(convertInToOr(name, values))

      case Not(InSet(child@ExtractAttribute(SupportedAttribute(name)),
      ExtractableDateValues(values))) if useAdvanced && child.dataType == DateType =>
        Some(convertNotInToAnd(name, values))

      case InSet(ExtractAttribute(SupportedAttribute(name)), ExtractableValues(values))
        if useAdvanced =>
        Some(convertInToOr(name, values))

      case Not(InSet(ExtractAttribute(SupportedAttribute(name)), ExtractableValues(values)))
        if useAdvanced =>
        Some(convertNotInToAnd(name, values))

      case op@SpecialBinaryComparison(
      ExtractAttribute(SupportedAttribute(name)), ExtractableLiteral(value)) =>
        Some(s"$name ${op.symbol} $value")

      case op@SpecialBinaryComparison(
      ExtractableLiteral(value), ExtractAttribute(SupportedAttribute(name))) =>
        Some(s"$value ${op.symbol} $name")

      case Contains(ExtractAttribute(SupportedAttribute(name)), ExtractableLiteral(value)) =>
        Some(s"$name like " + (("\".*" + value.drop(1)).dropRight(1) + ".*\""))

      case StartsWith(ExtractAttribute(SupportedAttribute(name)), ExtractableLiteral(value)) =>
        Some(s"$name like " + (value.dropRight(1) + ".*\""))

      case EndsWith(ExtractAttribute(SupportedAttribute(name)), ExtractableLiteral(value)) =>
        Some(s"$name like " + ("\".*" + value.drop(1)))

      case And(expr1, expr2) if useAdvanced =>
        val converted = convert(expr1) ++ convert(expr2)
        if (converted.isEmpty) {
          None
        } else {
          Some(converted.mkString("(", " and ", ")"))
        }

      case Or(expr1, expr2) if useAdvanced =>
        for {
          left <- convert(expr1)
          right <- convert(expr2)
        } yield s"($left or $right)"

      case Not(EqualTo(
      ExtractAttribute(SupportedAttribute(name)), ExtractableLiteral(value))) if useAdvanced =>
        Some(s"$name != $value")

      case Not(EqualTo(
      ExtractableLiteral(value), ExtractAttribute(SupportedAttribute(name)))) if useAdvanced =>
        Some(s"$value != $name")

      case _ => None
    }

    filters.flatMap(convert).mkString(" and ")
  }

  private def quoteStringLiteral(str: String): String = {
    if (!str.contains("\"")) {
      s""""$str""""
    } else if (!str.contains("'")) {
      s"""'$str'"""
    } else {
      throw QueryExecutionErrors.invalidPartitionFilterError()
    }
  }

  def normalizeExprs(
                      exprs: Seq[Expression],
                      attributes: Seq[Attribute]): Seq[Expression] = {
    DataSourceStrategy.normalizeExprs(exprs, attributes)
  }

  def prunePartitionsByFilter(table: PolyCatPartitionTable, fs: FileSystem, partitions: util.List[Partition], predicates: Seq[Expression]): Seq[PartitionPath] = {
    val defaultTimeZoneId = table.getSparkSession.sessionState.conf.sessionLocalTimeZone
    val partitionSchema = table.partitionSchema()
    if (predicates.isEmpty) {
      partitions.map(p => PartitionPath(toRow(p, defaultTimeZoneId, table), new Path(p.getStorageDescriptor.getLocation).makeQualified(
        fs.getUri, fs.getWorkingDirectory))).toSeq
    } else {
      val partitionNames = partitionSchema.names.toSet
      val nonPartitionPruningPredicates = predicates.filterNot {
        _.references.map(_.name).toSet.subsetOf(partitionNames)
      }
      if (nonPartitionPruningPredicates.nonEmpty) {
        throw QueryCompilationErrors.nonPartitionPruningPredicatesNotExpectedError(
          nonPartitionPruningPredicates)
      }

      val boundPredicate =
        Predicate.createInterpreted(predicates.reduce(And).transform {
          case att: AttributeReference =>
            val index = partitionSchema.indexWhere(_.name == att.name)
            BoundReference(index, partitionSchema(index).dataType, nullable = true)
        })

      partitions
        .map(p => (toRow(p, defaultTimeZoneId, table), p.getStorageDescriptor.getLocation))
        .filter { p => boundPredicate.eval(p._1) }
        .map(p => PartitionPath(p._1, new Path(p._2).makeQualified(fs.getUri, fs.getWorkingDirectory))).toSeq
    }
  }

  def toRow(partition: Partition, defaultTimeZoneId: String, table: PolyCatPartitionTable): InternalRow = {
    val partitionTuples = table.partitionSchema().zip(partition.getPartitionValues)
    InternalRow.fromSeq(partitionTuples.map { part =>
      Cast(Literal(part._2), part._1.dataType, Option(defaultTimeZoneId)).eval()
    })
  }
}
