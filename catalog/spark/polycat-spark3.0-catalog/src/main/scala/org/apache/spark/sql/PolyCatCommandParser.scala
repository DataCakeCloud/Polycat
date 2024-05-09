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

import io.polycat.common.expression.{SyntaxErrorListener, UpperCaseCharStream}
import io.polycat.sql.{PolyCatSQLLexer, PolyCatSQLParser}
import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}
import org.apache.spark.adapter.ParserAdapter
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.types.{DataType, StructType}

class PolyCatCommandParser(otherParser: ParserInterface) extends ParserAdapter with Logging {

  override def parsePlan(sqlText: String): LogicalPlan = {
    try {
      parse(sqlText)
    } catch {
      case polyCatException: Throwable =>
        try {
          otherParser.parsePlan(sqlText)
        } catch {
          case otherException: Throwable =>
            logError("Failed to parse sql in PolyCat", polyCatException)
            logError("Parser [" + otherParser.getClass.getSimpleName + "] failed after PolyCat", otherException)
            throw otherException;
        }
    }
  }

  private def parse(sqlText: String): LogicalPlan = {
    val lexer = new PolyCatSQLLexer(new UpperCaseCharStream(CharStreams.fromString(sqlText)))
    lexer.removeErrorListeners()
    val errorListener = new SyntaxErrorListener
    lexer.addErrorListener(errorListener)
    val tokens = new CommonTokenStream(lexer)
    val parser = new PolyCatSQLParser(tokens)
    parser.removeErrorListeners()
    parser.addErrorListener(errorListener)
    // get grammar tree
    val evalVisitor = new PolyCatCommandBuilder()
    evalVisitor.visitCatalogStatement(parser.catalogStatement())
  }

  override def parseExpression(sqlText: String): Expression = ???

  override def parseTableIdentifier(sqlText: String): TableIdentifier = ???

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = ???

  override def parseMultipartIdentifier(sqlText: String): Seq[String] = {
    sqlText.split("\\.").toSeq
  }

  override def parseTableSchema(sqlText: String): StructType = ???

  override def parseDataType(sqlText: String): DataType = ???
}
