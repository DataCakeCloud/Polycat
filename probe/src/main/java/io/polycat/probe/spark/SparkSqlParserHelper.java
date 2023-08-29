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
package io.polycat.probe.spark;

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.expressions.Alias;
import org.apache.spark.sql.catalyst.expressions.And;
import org.apache.spark.sql.catalyst.expressions.BinaryArithmetic;
import org.apache.spark.sql.catalyst.expressions.BinaryComparison;
import org.apache.spark.sql.catalyst.expressions.BinaryOperator;
import org.apache.spark.sql.catalyst.expressions.CaseWhen;
import org.apache.spark.sql.catalyst.expressions.EqualNullSafe;
import org.apache.spark.sql.catalyst.expressions.EqualTo;
import org.apache.spark.sql.catalyst.expressions.Exists;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.GreaterThan;
import org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual;
import org.apache.spark.sql.catalyst.expressions.In;
import org.apache.spark.sql.catalyst.expressions.InSubquery;
import org.apache.spark.sql.catalyst.expressions.LessThan;
import org.apache.spark.sql.catalyst.expressions.LessThanOrEqual;
import org.apache.spark.sql.catalyst.expressions.ListQuery;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.Not;
import org.apache.spark.sql.catalyst.expressions.Or;
import org.apache.spark.sql.catalyst.expressions.ScalarSubquery;
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression;
import org.apache.spark.sql.catalyst.plans.logical.BinaryNode;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.InsertIntoStatement;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode;
import org.apache.spark.sql.catalyst.plans.logical.Union;
import org.apache.spark.sql.catalyst.plans.logical.UnresolvedWith;
import org.apache.spark.sql.catalyst.plans.logical.View;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import org.apache.commons.collections.CollectionUtils;

/**
 * Parse the sql table name and extract it to tableMaps
 *
 * @author renjianxu
 * @date 2023/3/28
 */
public class SparkSqlParserHelper {
    public static final String INPUT_TABLE = "inputTable";
    public static final String OUTPUT_TABLE = "outputTable";
    public static final String TEMP_TABLE = "tempTable";

    public static void visitedLogicalPlan(
            Map<String, Set<String>> tableMaps,
            LogicalPlan curLogicalPlan,
            LogicalPlan parentLogicalPlan) {
        if (curLogicalPlan instanceof InsertIntoStatement) {
            LogicalPlan table = ((InsertIntoStatement) curLogicalPlan).table();
            visitedLogicalPlan(tableMaps, table, curLogicalPlan);
            LogicalPlan query = ((InsertIntoStatement) curLogicalPlan).query();
            visitedLogicalPlan(tableMaps, query, curLogicalPlan);
        }
        if (curLogicalPlan instanceof UnresolvedRelation) {
            saveTableName(tableMaps, curLogicalPlan, parentLogicalPlan);
        }
        // deal union
        if (curLogicalPlan instanceof Union) {
            Seq<LogicalPlan> localChildren = curLogicalPlan.children();
            List<LogicalPlan> localPlans = JavaConverters.seqAsJavaList(localChildren);
            localPlans.forEach(
                    localLogicalPlan -> {
                        visitedLogicalPlan(tableMaps, localLogicalPlan, curLogicalPlan);
                    });
        }
        // deal view
        if (curLogicalPlan instanceof View) {
            LogicalPlan child = ((View) curLogicalPlan).child();
            visitedLogicalPlan(tableMaps, child, curLogicalPlan);
        }
        // deal node with a left and right child
        if (curLogicalPlan instanceof BinaryNode) {
            LogicalPlan left = ((BinaryNode) curLogicalPlan).left();
            if (left != null) {
                visitedLogicalPlan(tableMaps, left, curLogicalPlan);
            }
            LogicalPlan right = ((BinaryNode) curLogicalPlan).right();
            if (right != null) {
                visitedLogicalPlan(tableMaps, right, curLogicalPlan);
            }
            if (curLogicalPlan instanceof Join) {
                Option<Expression> condition = ((Join) (curLogicalPlan)).condition();
                if (!condition.isDefined()) {
                    return;
                }
                Expression expression = condition.get();
                if (expression != null) {
                    visitedExpression(tableMaps, expression);
                }
            }
        }
        // deal with query
        if (curLogicalPlan instanceof UnresolvedWith) {
            Seq<Tuple2<String, SubqueryAlias>> cteRelations =
                    ((UnresolvedWith) curLogicalPlan).cteRelations();
            for (Tuple2<String, SubqueryAlias> tuple2 :
                    JavaConverters.asJavaCollection(cteRelations)) {
                Set<String> tempTable = tableMaps.computeIfAbsent(TEMP_TABLE, key -> new HashSet());
                tempTable.add(tuple2._1);
                visitedLogicalPlan(tableMaps, tuple2._2, curLogicalPlan);
            }
            visitedLogicalPlan(
                    tableMaps, ((UnresolvedWith) curLogicalPlan).child(), curLogicalPlan);
        } else if (curLogicalPlan instanceof SubqueryAlias) {
            LogicalPlan child = ((SubqueryAlias) curLogicalPlan).child();
            visitedLogicalPlan(tableMaps, child, curLogicalPlan);
        } else if (curLogicalPlan instanceof UnaryNode) {
            LogicalPlan child = ((UnaryNode) curLogicalPlan).child();
            visitedLogicalPlan(tableMaps, child, curLogicalPlan);
            if (curLogicalPlan instanceof Filter) {
                Expression condition = ((Filter) curLogicalPlan).condition();
                visitedExpression(tableMaps, condition);
            }
            if (curLogicalPlan instanceof Project) {
                Seq<NamedExpression> namedExpressionSeq = ((Project) curLogicalPlan).projectList();
                Collection<NamedExpression> namedExpressions =
                        JavaConverters.asJavaCollection(namedExpressionSeq);
                namedExpressions.forEach(
                        express -> visitedExpression(tableMaps, (Expression) express));
            }
        }
    }

    private static void saveTableName(
            Map<String, Set<String>> tableMaps,
            LogicalPlan logicalPlan,
            LogicalPlan parentLogicalPlan) {
        Set<String> tableNames;
        if (parentLogicalPlan instanceof InsertIntoStatement) {
            tableNames = tableMaps.computeIfAbsent(OUTPUT_TABLE, key -> new HashSet());
        } else {
            tableNames = tableMaps.computeIfAbsent(INPUT_TABLE, key -> new HashSet());
        }
        Seq<String> tableIdentifier = ((UnresolvedRelation) logicalPlan).multipartIdentifier();
        tableNames.add(tableIdentifier.mkString("."));
    }

    public static Map<String, Set<String>> getRealIOTableMap(Map<String, Set<String>> tableMaps) {
        Set<String> cacheTable = tableMaps.get(TEMP_TABLE);
        Set<String> inputTable = tableMaps.get(INPUT_TABLE);
        if (CollectionUtils.isNotEmpty(cacheTable)) {
            inputTable.removeAll(cacheTable);
        }
        tableMaps.remove(TEMP_TABLE);
        return tableMaps;
    }

    private static void visitedExpression(
            Map<String, Set<String>> tableMaps, Expression curExpression) {
        if (curExpression instanceof In) {
            Seq<Expression> list = ((In) curExpression).list();
            if (CollectionUtils.isNotEmpty(Collections.singleton(list))) {
                handleExpressions(tableMaps, list);
            }
        } else if (curExpression instanceof SubqueryExpression) {
            LogicalPlan plan = getLogicalPlanOnExpression(curExpression);
            if (plan != null) {
                visitedLogicalPlan(tableMaps, plan, plan);
            }
            Seq<Expression> children = curExpression.children();
            if (CollectionUtils.isNotEmpty(Collections.singleton(children))) {
                handleExpressions(tableMaps, children);
            }
        } else if (curExpression instanceof Alias) {
            Alias expression = (Alias) curExpression;
            visitedExpression(tableMaps, expression.child());
        } else if (curExpression instanceof CaseWhen) {
            CaseWhen caseWhen = (CaseWhen) curExpression;
            Collection<Tuple2<Expression, Expression>> branches =
                    JavaConverters.asJavaCollection(((CaseWhen) curExpression).branches());
            for (Tuple2<Expression, Expression> tuple2 : branches) {
                if (!Objects.isNull(tuple2._1)) {
                    visitedExpression(tableMaps, tuple2._1);
                }
                if (!Objects.isNull(tuple2._2)) {
                    visitedExpression(tableMaps, tuple2._2);
                }
            }
            Option<Expression> expressionOption = caseWhen.elseValue();
            if (expressionOption.isDefined()) {
                visitedExpression(tableMaps, expressionOption.get());
            }
        } else if (curExpression instanceof InSubquery) {
            LogicalPlan plan = getLogicalPlanOnExpression(((InSubquery) curExpression).query());
            if (plan != null) {
                visitedLogicalPlan(tableMaps, plan, plan);
            }
        } else if (curExpression instanceof Not) {
            visitedExpression(tableMaps, ((Not) curExpression).child());
        } else if (curExpression instanceof BinaryArithmetic) {
            visitedExpression(tableMaps, ((BinaryArithmetic) curExpression).left());
            visitedExpression(tableMaps, ((BinaryArithmetic) curExpression).right());
        } else {
            Expression expressionRight = handleBinaryOperatorExpressionOnRight(curExpression);
            if (expressionRight != null) {
                visitedExpression(tableMaps, expressionRight);
            }
            Expression expressionLeft = handleBinaryOperatorExpressionOnLeft(curExpression);
            if (expressionLeft != null) {
                visitedExpression(tableMaps, expressionLeft);
            }
        }
    }

    private static Expression handleBinaryOperatorExpressionOnRight(Expression curExpression) {
        Expression right = null;
        if (curExpression instanceof BinaryOperator) {
            if (curExpression instanceof Or) {
                right = ((Or) curExpression).right();
            } else if (curExpression instanceof And) {
                right = ((And) curExpression).right();
            } else if (curExpression instanceof BinaryComparison) {
                if (curExpression instanceof EqualNullSafe) {
                    right = ((EqualNullSafe) curExpression).right();
                } else if (curExpression instanceof GreaterThanOrEqual) {
                    right = ((GreaterThanOrEqual) curExpression).right();
                } else if (curExpression instanceof LessThanOrEqual) {
                    right = ((LessThanOrEqual) curExpression).right();
                } else if (curExpression instanceof LessThan) {
                    right = ((LessThan) curExpression).right();
                } else if (curExpression instanceof GreaterThan) {
                    right = ((GreaterThan) curExpression).right();
                } else if (curExpression instanceof EqualTo) {
                    right = ((EqualTo) curExpression).right();
                }
            }
        }
        return right;
    }

    private static Expression handleBinaryOperatorExpressionOnLeft(Expression curExpression) {
        Expression left = null;
        if (curExpression instanceof BinaryOperator) {
            if (curExpression instanceof Or) {
                left = ((Or) curExpression).left();
            } else if (curExpression instanceof And) {
                left = ((And) curExpression).left();
            } else if (curExpression instanceof BinaryComparison) {
                if (curExpression instanceof EqualNullSafe) {
                    left = ((EqualNullSafe) curExpression).left();
                } else if (curExpression instanceof GreaterThanOrEqual) {
                    left = ((GreaterThanOrEqual) curExpression).left();
                } else if (curExpression instanceof LessThanOrEqual) {
                    left = ((LessThanOrEqual) curExpression).left();
                } else if (curExpression instanceof LessThan) {
                    left = ((LessThan) curExpression).left();
                } else if (curExpression instanceof GreaterThan) {
                    left = ((GreaterThan) curExpression).left();
                } else if (curExpression instanceof EqualTo) {
                    left = ((EqualTo) curExpression).left();
                }
            }
        }
        return left;
    }

    private static LogicalPlan getLogicalPlanOnExpression(Expression curExpression) {
        LogicalPlan plan = null;
        if (curExpression instanceof ListQuery) {
            plan = ((ListQuery) curExpression).plan();
        } else if (curExpression instanceof ScalarSubquery) {
            plan = ((ScalarSubquery) curExpression).plan();
        } else if (curExpression instanceof Exists) {
            plan = ((Exists) curExpression).plan();
        }
        return plan;
    }

    private static void handleExpressions(
            Map<String, Set<String>> tableMaps, Seq<Expression> seqExpression) {
        Collection<Expression> expressions = JavaConverters.asJavaCollection(seqExpression);
        expressions.forEach(
                expression -> {
                    visitedExpression(tableMaps, expression);
                });
    }
}
