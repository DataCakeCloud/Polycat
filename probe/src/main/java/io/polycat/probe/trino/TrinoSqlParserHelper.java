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
package io.polycat.probe.trino;

import io.trino.sql.tree.*;

import java.util.Optional;
import java.util.Set;

public class TrinoSqlParserHelper {

    /**
     * @param statement
     * @param targetTable
     * @param queryTables
     * @param tempTables
     */
    public static void extractTablesFromStatement(
            Statement statement,
            Set<String> targetTable,
            Set<String> queryTables,
            Set<String> tempTables) {
        if (statement instanceof Insert) {
            QualifiedName target = ((Insert) statement).getTarget();
            // fill in target table name
            targetTable.add(target.toString());
            extractTablesFromStatement(
                    ((Insert) statement).getQuery(), targetTable, queryTables, tempTables);
        } else if (statement instanceof Query) {
            // with subquery
            Optional<With> with = ((Query) statement).getWith();
            if (with.isPresent()) {
                with.get()
                        .getChildren()
                        .forEach(node -> extractTablesFromSqlNode(node, queryTables, tempTables));
            }

            QueryBody queryBody = ((Query) statement).getQueryBody();
            extractTablesFromSqlNode(queryBody, queryTables, tempTables);

            if (!(queryBody instanceof Union)) {
                // deal where
                Optional<Expression> whereExpress = ((QuerySpecification) queryBody).getWhere();
                if (whereExpress.isPresent()) {
                    Expression expression = whereExpress.get();
                    extractTablesFromSqlNode(expression, queryTables, tempTables);
                }
            }
        }
    }

    private static void extractTablesFromSqlNode(
            Node sqlNode, Set<String> queryTables, Set<String> tempTables) {
        if (sqlNode instanceof QuerySpecification) {
            // base query statement
            Optional<Relation> fromRelationOptional = ((QuerySpecification) sqlNode).getFrom();
            if (!fromRelationOptional.isPresent()) {
                return;
            }
            Relation table = fromRelationOptional.get();
            extractTablesFromSqlNode(table, queryTables, tempTables);

            Optional<Expression> where = ((QuerySpecification) sqlNode).getWhere();
            where.ifPresent(
                    whereExpress ->
                            extractTablesFromSqlNode(whereExpress, queryTables, tempTables));
        } else if (sqlNode instanceof TableSubquery) {
            // deal subquery
            Query query = ((TableSubquery) sqlNode).getQuery();
            extractTablesFromSqlNode(query, queryTables, tempTables);
        } else if (sqlNode instanceof Table) {
            // the final resolved table name
            QualifiedName tableName = ((Table) sqlNode).getName();
            queryTables.add(tableName.toString());
        } else if (sqlNode instanceof AliasedRelation) {
            // deal as relation, do not save temporary tables
            Relation relation = ((AliasedRelation) sqlNode).getRelation();
            extractTablesFromSqlNode(relation, queryTables, tempTables);
        } else if (sqlNode instanceof Query) {
            QueryBody queryBody = ((Query) sqlNode).getQueryBody();
            extractTablesFromSqlNode(queryBody, queryTables, tempTables);
        } else if (sqlNode instanceof Join) {
            extractTablesFromSqlNode(((Join) sqlNode).getLeft(), queryTables, tempTables);
            extractTablesFromSqlNode(((Join) sqlNode).getRight(), queryTables, tempTables);
        } else if (sqlNode instanceof WithQuery) {
            Identifier tempTableName = ((WithQuery) sqlNode).getName();
            tempTables.add(tempTableName.getValue());
            extractTablesFromSqlNode(((WithQuery) sqlNode).getQuery(), queryTables, tempTables);
        } else if (sqlNode instanceof ExistsPredicate) {
            // deal exist
            Expression subquery = ((ExistsPredicate) sqlNode).getSubquery();
            extractTablesFromSqlNode(subquery, queryTables, tempTables);
        } else if (sqlNode instanceof InPredicate) {
            // deal where in
            extractTablesFromSqlNode(
                    ((InPredicate) sqlNode).getValueList(), queryTables, tempTables);
        } else if (sqlNode instanceof SubqueryExpression) {
            extractTablesFromSqlNode(
                    ((SubqueryExpression) sqlNode).getQuery(), queryTables, tempTables);
        } else if (sqlNode instanceof ComparisonExpression
                || sqlNode instanceof Union
                || sqlNode instanceof Intersect
                || sqlNode instanceof Except) {
            // deal where xx = sub query or union
            sqlNode.getChildren()
                    .forEach(child -> extractTablesFromSqlNode(child, queryTables, tempTables));
        } else if (sqlNode instanceof LogicalBinaryExpression) {
            // deal where ..and ..and ..
            extractTablesFromSqlNode(
                    ((LogicalBinaryExpression) sqlNode).getLeft(), queryTables, tempTables);
            extractTablesFromSqlNode(
                    ((LogicalBinaryExpression) sqlNode).getRight(), queryTables, tempTables);
        }
    }
}
