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

import io.polycat.catalog.common.exception.CarbonSqlException;
import io.polycat.catalog.common.model.record.*;
import io.polycat.catalog.common.types.DataType;
import io.polycat.catalog.common.types.DataTypes;
import io.polycat.common.expression.bool.AndExpression;
import io.polycat.common.expression.bool.OrExpression;
import io.polycat.common.expression.interval.IntervalUnit;
import io.polycat.common.expression.interval.TimeInterval;
import io.polycat.common.sql.ParserUtil;
import io.polycat.sql.PolyCatSQLBaseVisitor;
import io.polycat.sql.PolyCatSQLParser;
import io.polycat.sql.PolyCatSQLParser.*;

import io.polycat.common.expression.arithmetic.DivideExpression;
import io.polycat.common.expression.arithmetic.MinusExpression;
import io.polycat.common.expression.arithmetic.ModExpression;
import io.polycat.common.expression.arithmetic.PlusExpression;
import io.polycat.common.expression.arithmetic.TimesExpression;
import io.polycat.common.expression.comparison.EqualExpression;
import io.polycat.common.expression.comparison.GreaterThanExpression;
import io.polycat.common.expression.comparison.GreaterThanOrEqualExpression;
import io.polycat.common.expression.comparison.LessThanExpression;
import io.polycat.common.expression.comparison.LessThanOrEqualExpression;
import io.polycat.common.expression.comparison.NotEqualExpression;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class ExpressionPlanBuilder extends PolyCatSQLBaseVisitor<Expression> {

    protected String parseIdentifier(IdentifierContext ctx) {
        if (ctx == null) {
            return null;
        }
        String identifier = ctx.getText().toLowerCase();
        return StringUtils.unwrap(identifier, "\"");
    }

    protected String parseString(StringContext ctx) {
        if (ctx == null) {
            return null;
        }
        return StringUtils.unwrap(ctx.getText(), "'");
    }

    @Override
    public Expression visitSingleStatement(SingleStatementContext ctx) {
        return visit(ctx.statement());
    }

    @Override
    public Expression visitPredicated(PredicatedContext ctx) {
        if (ctx.predicate() == null) {
            if (ctx.valueExpression instanceof ValueExpressionDefaultContext) {
                return visitValueExpressionDefault((ValueExpressionDefaultContext) ctx.valueExpression);
            }
            return super.visitPredicated(ctx);
        } else {
            return visit(ctx.predicate());
        }
    }

    @Override
    public Expression visitComparison(ComparisonContext ctx) {
        ComparisonOperatorContext operator = ctx.comparisonOperator();
        Expression left = visit(ctx.value);
        Expression right = visit(ctx.right);
        if (operator.EQ() != null) {
            return new EqualExpression(left, right);
        }
        if (operator.NEQ() != null) {
            return new NotEqualExpression(left, right);
        }
        if (operator.LT() != null) {
            return new LessThanExpression(left, right);
        }
        if (operator.LTE() != null) {
            return new LessThanOrEqualExpression(left, right);
        }
        if (operator.GT() != null) {
            return new GreaterThanExpression(left, right);
        }
        if (operator.GTE() != null) {
            return new GreaterThanOrEqualExpression(left, right);
        }
        return super.visitComparison(ctx);
    }

    @Override
    public Expression visitLogicalBinary(LogicalBinaryContext ctx) {
        Expression left = visit(ctx.left);
        Expression right = visit(ctx.right);
        List<Expression> expressions = new ArrayList<>();
        expressions.add(left);
        expressions.add(right);
        if (ctx.operator.getText().equalsIgnoreCase("and")) {
            return new AndExpression(expressions, DataTypes.BOOLEAN);
        } else {
            return new OrExpression(expressions, DataTypes.BOOLEAN);
        }
    }

    @Override
    public Expression visitArithmeticBinary(ArithmeticBinaryContext ctx) {
        Expression left = visit(ctx.left);
        Expression right = visit(ctx.right);
        if (ctx.PLUS() != null) {
            return new PlusExpression(left, right);
        }
        if (ctx.MINUS() != null) {
            return new MinusExpression(left, right);
        }
        if (ctx.ASTERISK() != null) {
            return new TimesExpression(left, right);
        }
        if (ctx.SLASH() != null) {
            return new DivideExpression(left, right);
        }
        if (ctx.PERCENT() != null) {
            return new ModExpression(left, right);
        }
        return super.visitArithmeticBinary(ctx);
    }

    @Override
    public Expression visitParenthesizedExpression(ParenthesizedExpressionContext ctx) {
        return visitExpression(ctx.expression());
    }

    @Override
    public Expression visitArithmeticUnary(PolyCatSQLParser.ArithmeticUnaryContext ctx) {
        Expression expression = visit(ctx.valueExpression());
        if (ctx.MINUS() == null) {
            return expression;
        }
        ((LiteralExpression) expression).getValue().negate();
        return expression;
    }

    @Override
    public Expression visitInterval(IntervalContext ctx) {
        TimeInterval interval;
        IntervalUnit from = IntervalUnit.valueOf(ctx.from.getText().toUpperCase());
        IntervalFieldContext toCtx = ctx.to;
        String value = StringUtils.unwrap(ctx.intervalValue().getText(), "'").trim();
        if (toCtx == null) {
            interval = ParserUtil.unitInterval(value, from);
        } else {
            IntervalUnit to = IntervalUnit.valueOf(toCtx.getText().toUpperCase());
            interval = ParserUtil.unitToUnitInterval(value, from, to);
        }
        return new LiteralExpression(new ObjectWritable(interval), DataTypes.INTERVAL);
    }

    @Override
    public Expression visitTypeConstructor(TypeConstructorContext ctx) {
        Field field;
        DataType dataType;
        String identifier = parseIdentifier(ctx.identifier()).toUpperCase();
        if ("DATE".equals(identifier)) {
            field = ParserUtil.stringToDate(parseString(ctx.string()));
            dataType = DataTypes.DATE;
        } else if ("TIMESTAMP".equals(identifier)) {
            field = ParserUtil.stringToTimestamp(parseString(ctx.string()));
            dataType = DataTypes.TIMESTAMP;
        } else {
            throw new CarbonSqlException("Unsupported type: " + identifier);
        }
        return new LiteralExpression(field, dataType);
    }

    @Override
    public Expression visitBooleanLiteral(BooleanLiteralContext ctx) {
        return new LiteralExpression(new BooleanWritable(Boolean.parseBoolean(ctx.getText())), DataTypes.BOOLEAN);
    }

    @Override
    public Expression visitNullLiteral(NullLiteralContext ctx) {
        return new NullExpression(DataTypes.NULL);
    }

    @Override
    public Expression visitBasicStringLiteral(BasicStringLiteralContext ctx) {
        String unwrap = StringUtils.unwrap(ctx.getText(), "'");
        return new LiteralExpression(new StringWritable(unwrap), DataTypes.STRING);
    }

    @Override
    public Expression visitDecimalLiteral(DecimalLiteralContext ctx) {
        return new LiteralExpression(new DoubleWritable(Double.parseDouble(ctx.getText())), DataTypes.DOUBLE);
    }

    @Override
    public Expression visitDoubleLiteral(DoubleLiteralContext ctx) {
        return new LiteralExpression(new DoubleWritable(Double.parseDouble(ctx.getText())), DataTypes.DOUBLE);
    }

    @Override
    public Expression visitIntegerLiteral(IntegerLiteralContext ctx) {
        return new LiteralExpression(new IntWritable(Integer.parseInt(ctx.getText())), DataTypes.INTEGER);
    }

    @Override
    public Expression visitColumnReference(ColumnReferenceContext ctx) {
        String columnName = parseIdentifier(ctx.identifier());
        return new FieldExpression(columnName);
    }

    @Override
    public Expression visitDereference(DereferenceContext ctx) {
        FieldExpression qualifier = (FieldExpression) visit(ctx.base);
        String columnName = parseIdentifier(ctx.identifier());
        List<String> newQualifier = new ArrayList<>(3);
        newQualifier.addAll(qualifier.getQualifier());
        newQualifier.add(qualifier.getName());
        return new FieldExpression(columnName, newQualifier);
    }
}
