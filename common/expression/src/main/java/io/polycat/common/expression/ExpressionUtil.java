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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import io.polycat.common.expression.arithmetic.DivideExpression;
import io.polycat.common.expression.arithmetic.MinusExpression;
import io.polycat.common.expression.arithmetic.PlusExpression;
import io.polycat.common.expression.arithmetic.TimesExpression;
import io.polycat.common.expression.bool.AndExpression;

import com.google.common.collect.Lists;

/**
 * 表达式的一些通用工具方法
 * @since 2021/3/9
 */
public class ExpressionUtil {

    private ExpressionUtil() {

    }

    // 获取该表达式中关联的所有字段索引
    public static List<Integer> getFieldIndexs(Expression expression) {
        if (expression instanceof FieldExpression) {
            FieldExpression fieldExpression = (FieldExpression)expression;
            return fieldExpression.getFieldIndexes();
        } else if (expression instanceof ExpressionBase) {
            ExpressionBase expressionBase = (ExpressionBase)expression;
            List<Integer> result = new ArrayList<>();
            for (Expression operand : expressionBase.getOperands()) {
                result.addAll(getFieldIndexs(operand));
            }
            return result;
        } else {
            return new ArrayList<>();
        }
    }

    // 是否是计算表达式,加减乘除
    public static boolean isCalculateExpression(Expression expression) {
        return expression instanceof PlusExpression
                || expression instanceof DivideExpression
                || expression instanceof MinusExpression
                || expression instanceof TimesExpression;
    }

    public static String nameWithQualifier(String name, List<String> qualifier) {
        if (qualifier.size() == 0) {
            return name;
        }
        return String.join(".", String.join(".", qualifier), name);
    }

    public static List<Expression> splitExpression(Expression expression) {
        List<Expression> expressionsList = new LinkedList<>();
        splitExpression(expression, expressionsList);
        return expressionsList;
    }

    public static void splitExpression(Expression expression, List<Expression> expressionsList) {
        if (expression instanceof AndExpression) {
            ((AndExpression) expression).getOperands().forEach(x -> {
                splitExpression(x, expressionsList);
            });
        } else {
            expressionsList.add(expression);
        }
    }

    public static Expression combineExpression(List<Expression> expressions) {
        if (expressions == null || expressions.isEmpty()) {
            return null;
        }
        if (expressions.size() == 1) {
            return expressions.get(0);
        }
        return new AndExpression(expressions, expressions.get(0).getDataType());
    }

    public static Expression combineExpression(Expression left, Expression right) {
        return new AndExpression(Lists.newArrayList(left, right), left.getDataType());
    }

    public static String name(Expression left, Expression right, String operation) {
        return ((NamedExpression) left).getName() + operation + ((NamedExpression) right).getName();
    }
}
