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

import org.apache.spark.sql.connector.catalog.SupportFileFormat._
import org.apache.spark.sql.types.{AnsiIntervalType, ArrayType, AtomicType, DataType, MapType, NullType, StringType, StructType, UserDefinedType}

object WriteHelper {

  def supportsDataType(fileFormat: String, dataType: DataType): Boolean = {
    SupportFileFormat.apply(fileFormat) match {
      case JSON => JsonSupportStructType(dataType)
      case TEXT | TEXTFILE => TextSupportStructType(dataType)
      case PARQUET => ParquetSupportStructType(dataType)
      case ORC => ParquetSupportStructType(dataType)
      case CSV => CsvSupportStructType(dataType)
      case AVRO => CsvSupportStructType(dataType)
      case _ => throw new UnsupportedOperationException("fileFormat: " + fileFormat)
    }
  }

  abstract class SupportStructType {

    def supportsDataType(dataType: DataType): Boolean

    def apply(dataType: DataType): Boolean = {
      supportsDataType(dataType)
    }
  }

  object JsonSupportStructType extends SupportStructType {

    override def supportsDataType(dataType: DataType): Boolean = dataType match {
      case _: AnsiIntervalType => false

      case _: AtomicType => true

      case st: StructType => st.forall { f => supportsDataType(f.dataType) }

      case ArrayType(elementType, _) => supportsDataType(elementType)

      case MapType(keyType, valueType, _) =>
        supportsDataType(keyType) && supportsDataType(valueType)

      case udt: UserDefinedType[_] => supportsDataType(udt.sqlType)

      case _: NullType => true

      case _ => false
    }
  }

  object ParquetSupportStructType extends SupportStructType {

    override def supportsDataType(dataType: DataType): Boolean = dataType match {
      case _: AnsiIntervalType => false

      case _: AtomicType => true

      case st: StructType => st.forall { f => supportsDataType(f.dataType) }

      case ArrayType(elementType, _) => supportsDataType(elementType)

      case MapType(keyType, valueType, _) =>
        supportsDataType(keyType) && supportsDataType(valueType)

      case udt: UserDefinedType[_] => supportsDataType(udt.sqlType)

      case _ => false
    }
  }

  object TextSupportStructType extends SupportStructType {

    override def supportsDataType(dataType: DataType): Boolean = dataType == StringType
  }

  object CsvSupportStructType extends SupportStructType {

    override def supportsDataType(dataType: DataType): Boolean = dataType match {
      case _: AnsiIntervalType => false

      case _: AtomicType => true

      case udt: UserDefinedType[_] => supportsDataType(udt.sqlType)

      case _ => false
    }
  }

  object AvroSupportStructType extends SupportStructType {

    override def supportsDataType(dataType: DataType): Boolean = dataType match {
      case _: AnsiIntervalType => false

      case _: AtomicType => true

      case st: StructType => st.forall { f => supportsDataType(f.dataType) }

      case ArrayType(elementType, _) => supportsDataType(elementType)

      case MapType(keyType, valueType, _) =>
        supportsDataType(keyType) && supportsDataType(valueType)

      case udt: UserDefinedType[_] => supportsDataType(udt.sqlType)

      case _: NullType => true

      case _ => false
    }
  }
}
