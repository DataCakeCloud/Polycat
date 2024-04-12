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
package io.polycat.common.expression.rule;

import io.polycat.common.expression.bool.AndExpression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import io.polycat.common.expression.FieldExpression;
import io.polycat.common.expression.LiteralExpression;
import io.polycat.common.expression.comparison.EqualExpression;
import io.polycat.common.util.CalciteUtil;

import java.util.stream.Collectors;

public class TransformExpressionVisitor implements ToRexCallVisitor {
    private final static RelDataTypeFactory TYPE_FACTORY = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    private final static RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

    private final RelDataType relDataType;

    public TransformExpressionVisitor(RelDataType relDataType) {
        this.relDataType = relDataType;
    }

    @Override
    public RexNode visit(AndExpression expression) {
        // TODO ReturnTypes, InferTypes, OperandTypes ??
        SqlOperator operator = new SqlBinaryOperator(SqlKind.AND.name(), SqlKind.AND, 24, false,
            ReturnTypes.BOOLEAN, InferTypes.BOOLEAN, OperandTypes.family(SqlTypeFamily.BOOLEAN, SqlTypeFamily.BOOLEAN));

        return REX_BUILDER.makeCall(TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN), operator,
            expression.getOperands().stream().map(x -> x.accept(this)).collect(Collectors.toList()));
    }

    @Override
    public RexNode visit(FieldExpression expression) {
        return RexInputRef.of(expression.getFieldIndex(), relDataType);
    }

    @Override
    public RexNode visit(EqualExpression expression) {
        SqlOperator operator = new SqlBinaryOperator(SqlKind.EQUALS.name(), SqlKind.EQUALS, 30, false,
            ReturnTypes.BOOLEAN_NOT_NULL, InferTypes.FIRST_KNOWN,
            OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED);

        return REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(CalciteUtil.convertToCalciteDataType(expression.getDataType())), operator,
            expression.getOperands().stream().map(x -> x.accept(this)).collect(Collectors.toList()));
    }

    @Override
    public RexNode visit(LiteralExpression expression) {
        return CalciteUtil.convertField(expression.getValue());
    }

}
