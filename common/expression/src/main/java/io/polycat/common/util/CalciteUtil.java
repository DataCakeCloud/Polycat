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
package io.polycat.common.util;

import com.google.common.collect.ImmutableList;
import io.polycat.catalog.common.types.DataTypes;

import io.polycat.common.expression.arithmetic.DivideExpression;
import io.polycat.common.expression.arithmetic.MinusExpression;
import io.polycat.common.expression.arithmetic.PlusExpression;
import io.polycat.common.expression.arithmetic.TimesExpression;
import io.polycat.common.expression.bool.AndExpression;
import io.polycat.common.expression.bool.IsNullExpression;
import io.polycat.common.expression.bool.NotExpression;
import io.polycat.common.expression.bool.OrExpression;
import io.polycat.common.expression.comparison.EqualExpression;
import io.polycat.common.expression.comparison.GreaterThanExpression;
import io.polycat.common.expression.comparison.GreaterThanOrEqualExpression;
import io.polycat.common.expression.comparison.LessThanExpression;
import io.polycat.common.expression.comparison.LessThanOrEqualExpression;
import io.polycat.common.expression.comparison.LikeExpression;
import io.polycat.common.expression.comparison.NotEqualExpression;
import io.polycat.common.expression.conditional.CaseExpression;
import io.polycat.common.expression.function.SubstringExpression;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import io.polycat.catalog.common.exception.CarbonSqlException;
import io.polycat.catalog.common.exception.ErrorItem;
import io.polycat.catalog.common.types.DataType;
import io.polycat.catalog.common.model.record.BooleanWritable;
import io.polycat.catalog.common.model.record.ByteWritable;
import io.polycat.catalog.common.model.record.DateWritable;
import io.polycat.catalog.common.model.record.DecimalWritable;
import io.polycat.catalog.common.model.record.DoubleWritable;
import io.polycat.catalog.common.model.record.Field;
import io.polycat.catalog.common.model.record.FloatWritable;
import io.polycat.catalog.common.model.record.IntWritable;
import io.polycat.catalog.common.model.record.LongWritable;
import io.polycat.catalog.common.model.record.NullWritable;
import io.polycat.catalog.common.model.record.ShortWritable;
import io.polycat.catalog.common.model.record.StringWritable;
import io.polycat.catalog.common.model.record.TimestampWritable;
import io.polycat.common.expression.CastExpression;
import io.polycat.common.expression.Direction;
import io.polycat.common.expression.Expression;
import io.polycat.common.expression.ExtractExpression;
import io.polycat.common.expression.FieldExpression;
import io.polycat.common.expression.JoinType;
import io.polycat.common.expression.LiteralExpression;
import io.polycat.common.expression.NullExpression;
import io.polycat.common.expression.UnSupportExpression;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.GregorianCalendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

// TODO need new sub module
public class CalciteUtil {

    public static String getTableName(RelOptTable table) {
        if (table.getQualifiedName().size() > 1) {
            return table.getQualifiedName().get(1);
        } else {
            return table.getQualifiedName().get(0);
        }
    }

    public static Optional<String> getDatabaseName(RelOptTable table) {
        if (table.getQualifiedName().size() > 1) {
            return Optional.of(table.getQualifiedName().get(0));
        } else {
            return Optional.empty();
        }
    }

    public static DataType convertToDataType(String sqlTypeName) {
        SqlTypeName sqlType = SqlTypeName.get(sqlTypeName);
        if (sqlType == SqlTypeName.VARCHAR || sqlType == SqlTypeName.CHAR) {
            return DataTypes.STRING;
        }
        return DataTypes.valueOf(sqlTypeName);
    }

    public static SqlTypeName convertToCalciteDataType(DataType dataType) {
        int sqlType = dataType.getSqlType();
        if (sqlType == DataTypes.STRING_SQL_TYPE) {
            return SqlTypeName.VARCHAR;
        } else if (sqlType == DataTypes.BOOLEAN_SQL_TYPE) {
            return SqlTypeName.BOOLEAN;
        } else if (sqlType == DataTypes.TINYINT_SQL_TYPE) {
            return SqlTypeName.TINYINT;
        } else if (sqlType == DataTypes.SMALLINT_SQL_TYPE) {
            return SqlTypeName.SMALLINT;
        } else if (sqlType == DataTypes.INTEGER_SQL_TYPE) {
            return SqlTypeName.INTEGER;
        } else if (sqlType == DataTypes.BIGINT_SQL_TYPE) {
            return SqlTypeName.BIGINT;
        } else if (sqlType == DataTypes.FLOAT_SQL_TYPE) {
            return SqlTypeName.FLOAT;
        } else if (sqlType == DataTypes.DOUBLE_SQL_TYPE) {
            return SqlTypeName.DOUBLE;
        } else if (sqlType == DataTypes.DECIMAL_SQL_TYPE) {
            return SqlTypeName.DECIMAL;
        } else if (sqlType == DataTypes.TIMESTAMP_SQL_TYPE) {
            return SqlTypeName.TIMESTAMP;
        } else if (sqlType == DataTypes.DATE_SQL_TYPE) {
            return SqlTypeName.DATE;
        }
        throw new UnsupportedOperationException(dataType.getName());
    }

    public static DataType convertFromCalciteDataType(SqlTypeName dataType) {
        switch (dataType) {
            case CHAR:
            case VARCHAR:
                return DataTypes.STRING;
            case BOOLEAN:
                return DataTypes.BOOLEAN;
            case TINYINT:
                return DataTypes.TINYINT;
            case SMALLINT:
                return DataTypes.SMALLINT;
            case INTEGER:
                return DataTypes.INTEGER;
            case BIGINT:
                return DataTypes.BIGINT;
            case FLOAT:
                return DataTypes.FLOAT;
            case DOUBLE:
                return DataTypes.DOUBLE;
            case DECIMAL:
                return DataTypes.DECIMAL;
            case DATE:
                return DataTypes.DATE;
            case TIMESTAMP:
                return DataTypes.TIMESTAMP;
            default:
                return DataTypes.STRING;
        }
    }

    /**
     * 将calcite的表达式转为enigma表达式
     */
    public static Expression convertExpression(RexNode rexNode) {
        if (rexNode instanceof RexCall) {
            return convertRexCallToExpression(rexNode);
        } else if (rexNode instanceof RexLiteral) {
            return convertRexLiteralToExpression(rexNode);
        } else if (rexNode instanceof RexInputRef) {
            // 字段表达式，取得字段下标
            RexInputRef rexInputRef = (RexInputRef) rexNode;
            int index = rexInputRef.getIndex();
            SqlTypeName calciteDataType = rexNode.getType().getSqlTypeName();
            DataType dataType = convertFromCalciteDataType(calciteDataType);
            return new FieldExpression(index, dataType);
        }
        throw new UnsupportedOperationException(rexNode.getKind().toString());
    }

    private static Expression convertRexLiteralToExpression(RexNode rexNode) {
        // 字面量表达式
        SqlTypeName sqlTypeName = rexNode.getType().getSqlTypeName();
        SqlTypeName calciteDataType = rexNode.getType().getSqlTypeName();
        DataType dataType = convertFromCalciteDataType(calciteDataType);
        Object object = ((RexLiteral) rexNode).getValue();
        if (object == null) {
            return new NullExpression(dataType);
        }

        Field field = CalciteUtil.convertLiteral(object, dataType);
        if (sqlTypeName == SqlTypeName.VARCHAR || sqlTypeName == SqlTypeName.CHAR) {
            return new LiteralExpression(field, dataType);
        } else if (sqlTypeName == SqlTypeName.DATE) {
            return new LiteralExpression(field, DataTypes.DATE);
        } else if (sqlTypeName == SqlTypeName.TIMESTAMP) {
            return new LiteralExpression(field, DataTypes.TIMESTAMP);
        } else if (sqlTypeName == SqlTypeName.SYMBOL) {
            return new LiteralExpression(field, dataType);
        } else if (sqlTypeName == SqlTypeName.BOOLEAN) {
            return new LiteralExpression(field, DataTypes.BOOLEAN);
        } else if (sqlTypeName == SqlTypeName.SMALLINT) {
            return new LiteralExpression(field, DataTypes.SMALLINT);
        } else if (sqlTypeName == SqlTypeName.INTEGER) {
            return new LiteralExpression(field, DataTypes.INTEGER);
        } else if (sqlTypeName == SqlTypeName.BIGINT) {
            return new LiteralExpression(field, DataTypes.BIGINT);
        } else if (sqlTypeName == SqlTypeName.DECIMAL) {
            return new LiteralExpression(field, DataTypes.DECIMAL);
        } else if (sqlTypeName == SqlTypeName.FLOAT) {
            return new LiteralExpression(field, DataTypes.FLOAT);
        } else if (sqlTypeName == SqlTypeName.DOUBLE) {
            return new LiteralExpression(field, DataTypes.DOUBLE);
        } else {
            return new LiteralExpression(field, dataType);
        }
    }

    private static Expression convertRexCallToExpression(RexNode rexNode) {
        ImmutableList<RexNode> operands = ((RexCall) rexNode).operands;
        SqlKind sqlKind = rexNode.getKind();

        // 先判断布尔表达式
        if (sqlKind == SqlKind.AND) {
            List<Expression> expressionList = new LinkedList<>();
            for (RexNode node : operands) {
                Expression expression = convertExpression(node);
                expressionList.add(expression);
            }
            return new AndExpression(expressionList, expressionList.get(0).getDataType());
        } else if (sqlKind == SqlKind.OR) {
            List<Expression> expressionList = new LinkedList<>();
            for (RexNode node : operands) {
                Expression expression = convertExpression(node);
                expressionList.add(expression);
            }
            return new OrExpression(expressionList, expressionList.get(0).getDataType());
        } else if (sqlKind == SqlKind.NOT || sqlKind == SqlKind.IS_NOT_TRUE) {
            Expression expression = convertExpression(operands.get(0));
            return new NotExpression(expression);
        } else if (sqlKind == SqlKind.CASE) {
            ImmutableList<RexNode> children = ((RexCall) rexNode).operands;
            assert (children.size() - 1) % 2 == 0;
            assert (children.size() - 1) / 2 >= 1;
            List<Expression> expressions =
                children.stream().map(CalciteUtil::convertExpression).collect(Collectors.toList());
            return new CaseExpression(expressions);
        } else if (sqlKind == SqlKind.CAST) {
            return new CastExpression(
                operands.stream().map(CalciteUtil::convertExpression).collect(Collectors.toList()),
                convertFromCalciteDataType(rexNode.getType().getSqlTypeName()));
        } else if (sqlKind == SqlKind.IS_NULL) {
            return new IsNullExpression(convertExpression(operands.get(0)), true);
        } else if (sqlKind == SqlKind.IS_NOT_NULL) {
            return new IsNullExpression(convertExpression(operands.get(0)), false);
        }

        // 两个运算符的表达式
        if (operands.size() == 2) {
            return convertTwoOprandsExpression((RexCall) rexNode, operands, sqlKind);
        }

        // 三个运算符的表达式, 例如substring(str, 1, 1)
        if (operands.size() == 3) {
            return convertThreeOperandsExpression((RexCall) rexNode, operands, sqlKind);
        }

        UnSupportExpression unSupportExpression = createUnsupportExpression((RexCall) rexNode);
        // 不支持的抛异常，否则查询结果错误的情况下原因不好定位
        throw new CarbonSqlException(ErrorItem.UNSUPPORT_EXPRESSION,
            "UnSupportExpression name = " + unSupportExpression.getExpressionName());
    }

    private static UnSupportExpression createUnsupportExpression(RexCall rexCall) {
        List<Expression> operands = rexCall.getOperands()
            .stream()
            .map(oprand -> CalciteUtil.convertExpression(oprand))
            .collect(Collectors.toList());
        DataType dataType = CalciteUtil.convertFromCalciteDataType(rexCall.getType().getSqlTypeName());
        String name;
        if (rexCall.getKind() != SqlKind.OTHER_FUNCTION) {
            name = rexCall.getKind().name();
        } else {
            name = rexCall.getOperator().getName();
        }
        return new UnSupportExpression(operands, dataType, name);
    }

    private static Expression convertTwoOprandsExpression(RexCall rexNode, ImmutableList<RexNode> operands,
        SqlKind sqlKind) {
        Expression left = convertExpression(operands.get(0));
        Expression right = convertExpression(operands.get(1));
        if (sqlKind == SqlKind.EQUALS) {
            return new EqualExpression(left, right);
        } else if (sqlKind == SqlKind.NOT_EQUALS) {
            return new NotEqualExpression(left, right);
        } else if (sqlKind == SqlKind.LESS_THAN) {
            return new LessThanExpression(left, right);
        } else if (sqlKind == SqlKind.GREATER_THAN) {
            return new GreaterThanExpression(left, right);
        } else if (sqlKind == SqlKind.LESS_THAN_OR_EQUAL) {
            return new LessThanOrEqualExpression(left, right);
        } else if (sqlKind == SqlKind.GREATER_THAN_OR_EQUAL) {
            return new GreaterThanOrEqualExpression(left, right);
        } else if (sqlKind == SqlKind.PLUS) {
            return new PlusExpression(left, right);
        } else if (sqlKind == SqlKind.MINUS) {
            return new MinusExpression(left, right);
        } else if (sqlKind == SqlKind.TIMES) {
            return new TimesExpression(left, right);
        } else if (sqlKind == SqlKind.DIVIDE) {
            return new DivideExpression(left, right);
        } else if (sqlKind == SqlKind.LIKE) {
            return new LikeExpression(left, right);
        } else if (sqlKind == SqlKind.EXTRACT) {
            return new ExtractExpression(left, right);
        } else {
            throw new CarbonSqlException(ErrorItem.UNSUPPORT_EXPRESSION, "UnSupportExpression name = " +
                createUnsupportExpression(rexNode).getExpressionName());
        }
    }

    private static Expression convertThreeOperandsExpression(RexCall rexNode, ImmutableList<RexNode> operands,
        SqlKind sqlKind) {
        List<Expression> expressions = new LinkedList<>();
        for (int i = 0; i < operands.size(); i++) {
            expressions.add(convertExpression(operands.get(i)));
        }

        if (sqlKind == SqlKind.OTHER_FUNCTION && rexNode.op.getName().equals("SUBSTRING")) {
            return new SubstringExpression(expressions);
        } else {
            throw new CarbonSqlException(ErrorItem.UNSUPPORT_EXPRESSION, "UnSupportExpression name = " +
                createUnsupportExpression(rexNode).getExpressionName());
        }
    }

    public static RexLiteral convertField(Field field) {
        RexBuilder rexBuilder = new RexBuilder(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
        int sqlType = field.getType().getSqlType();
        if (sqlType == DataTypes.TINYINT_SQL_TYPE || sqlType == DataTypes.SMALLINT_SQL_TYPE
            || sqlType == DataTypes.INTEGER_SQL_TYPE || sqlType == DataTypes.BIGINT_SQL_TYPE
            || sqlType == DataTypes.DECIMAL_SQL_TYPE || sqlType == DataTypes.FLOAT_SQL_TYPE
            || sqlType == DataTypes.DOUBLE_SQL_TYPE) {
            return rexBuilder.makeBigintLiteral(field.getDecimal());
        } else if (sqlType == DataTypes.BOOLEAN_SQL_TYPE) {
            return rexBuilder.makeLiteral(field.getBoolean());
        } else if (sqlType == DataTypes.STRING_SQL_TYPE) {
            return rexBuilder.makeLiteral(field.getString());
        } else if (sqlType == DataTypes.DATE_SQL_TYPE) {
            return rexBuilder.makeDateLiteral(new DateString(field.getString()));
        } else if (sqlType == DataTypes.TIMESTAMP_SQL_TYPE) {
            return rexBuilder.makeDateLiteral(new DateString(field.getString()));
        }
        throw new UnsupportedOperationException();
    }

    public static Field convertLiteral(RexLiteral rexNode) {
        // 字面量表达式
        SqlTypeName sqlTypeName = rexNode.getType().getSqlTypeName();
        Object object = rexNode.getValue();
        if (object == null) {
            return NullWritable.getInstance();
        } else if (sqlTypeName == SqlTypeName.VARCHAR || sqlTypeName == SqlTypeName.CHAR) {
            String value = ((NlsString) object).getValue();
            return new StringWritable(value);
        } else if (sqlTypeName == SqlTypeName.DATE) {
            Date date = new Date(((GregorianCalendar) object).getTimeInMillis());
            return new DateWritable(date);
        } else if (sqlTypeName == SqlTypeName.TIMESTAMP) {
            Timestamp timestamp = new Timestamp(((GregorianCalendar) object).getTimeInMillis());
            return new TimestampWritable(timestamp);
        } else if (sqlTypeName == SqlTypeName.BOOLEAN) {
            return new BooleanWritable((Boolean) object);
        } else if (sqlTypeName == SqlTypeName.TINYINT) {
            return new ShortWritable(((BigDecimal) object).byteValue());
        } else if (sqlTypeName == SqlTypeName.SMALLINT) {
            return new ShortWritable(((BigDecimal) object).shortValue());
        } else if (sqlTypeName == SqlTypeName.INTEGER) {
            return new IntWritable(((BigDecimal) object).intValue());
        } else if (sqlTypeName == SqlTypeName.BIGINT) {
            return new LongWritable(((BigDecimal) object).longValue());
        } else if (sqlTypeName == SqlTypeName.FLOAT) {
            return new FloatWritable(((BigDecimal) object).floatValue());
        } else if (sqlTypeName == SqlTypeName.DOUBLE) {
            return new DoubleWritable(((BigDecimal) object).doubleValue());
        } else if (sqlTypeName == SqlTypeName.DECIMAL) {
            return new DecimalWritable((BigDecimal) object);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    public static Field convertLiteral(Object calciteLiteral, DataType dataType) {
        int sqlType = dataType.getSqlType();
        if (sqlType == DataTypes.STRING_SQL_TYPE) {
            return new StringWritable(String.valueOf(calciteLiteral));
        } else if (sqlType == DataTypes.BOOLEAN_SQL_TYPE) {
            return new BooleanWritable((Boolean) calciteLiteral);
        } else if (sqlType == DataTypes.TINYINT_SQL_TYPE) {
            return new ByteWritable(((BigDecimal) calciteLiteral).byteValue());
        } else if (sqlType == DataTypes.SMALLINT_SQL_TYPE) {
            return new ShortWritable(((BigDecimal) calciteLiteral).shortValue());
        } else if (sqlType == DataTypes.INTEGER_SQL_TYPE) {
            return new IntWritable(((BigDecimal) calciteLiteral).intValue());
        } else if (sqlType == DataTypes.BIGINT_SQL_TYPE) {
            return new LongWritable(((BigDecimal) calciteLiteral).longValue());
        } else if (sqlType == DataTypes.FLOAT_SQL_TYPE) {
            return new FloatWritable(((BigDecimal) calciteLiteral).floatValue());
        } else if (sqlType == DataTypes.DOUBLE_SQL_TYPE) {
            return new DoubleWritable(((BigDecimal) calciteLiteral).doubleValue());
        } else if (sqlType == DataTypes.DECIMAL_SQL_TYPE) {
            return new DecimalWritable(((BigDecimal) calciteLiteral));
        } else if (sqlType == DataTypes.DATE_SQL_TYPE) {
            Date date = new Date(((GregorianCalendar) calciteLiteral).getTimeInMillis());
            return new DateWritable(date);
        } else if (sqlType == DataTypes.TIMESTAMP_SQL_TYPE) {
            Timestamp timestamp = new Timestamp(
                ((GregorianCalendar) calciteLiteral).getTimeInMillis());
            return new TimestampWritable(timestamp);
        }
        throw new UnsupportedOperationException(dataType.getName());
    }

    public static Direction valueOfDirection(RelFieldCollation.Direction dir) {
        if (dir == RelFieldCollation.Direction.ASCENDING) {
            return Direction.ASCENDING;
        } else if (dir == RelFieldCollation.Direction.DESCENDING) {
            return Direction.DESCENDING;
        } else {
            throw new UnsupportedOperationException("sort direction '" + dir + "' is not supported");
        }
    }

    public static JoinType trans2CarbonJoinType(JoinRelType joinRelType) {
        switch (joinRelType) {
            case FULL:
                return JoinType.FULL;
            case LEFT:
                return JoinType.LEFT;
            case RIGHT:
                return JoinType.RIGHT;
            case INNER:
                return JoinType.INNER;
            default:
                throw new CarbonSqlException("unsupport join type:" + joinRelType);
        }
    }

    public static JoinRelType trans2CalciteJoinType(JoinType joinType) {
        switch (joinType) {
            case FULL:
                return JoinRelType.FULL;
            case LEFT:
                return JoinRelType.LEFT;
            case RIGHT:
                return JoinRelType.RIGHT;
            case INNER:
                return JoinRelType.INNER;
            default:
                throw new CarbonSqlException("unsupport join type:" + joinType);
        }
    }

}
