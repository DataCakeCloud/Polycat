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
package io.polycat.common.expression;

import io.polycat.sql.PolyCatSQLBaseVisitor;
import io.polycat.sql.PolyCatSQLLexer;
import io.polycat.sql.PolyCatSQLParser;
import jdk.nashorn.internal.runtime.regexp.joni.exception.SyntaxException;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

public class ExpressionParser {

        public Expression parse(String expText) {
            PolyCatSQLLexer lexer = new PolyCatSQLLexer(new UpperCaseCharStream(CharStreams.fromString(expText)));
            lexer.removeErrorListeners();
            SyntaxErrorListener errorListener = new SyntaxErrorListener();
            lexer.addErrorListener(errorListener);
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            PolyCatSQLParser parser = new PolyCatSQLParser(tokens);
            parser.removeErrorListeners();
            parser.addErrorListener(errorListener);
            // get grammar tree
            PolyCatSQLBaseVisitor<Expression> expVisitor = new ExpressionPlanBuilder();
            return expVisitor.visitExpression(parser.expression());
        }
}
