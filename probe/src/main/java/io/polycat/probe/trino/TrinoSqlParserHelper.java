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

import io.polycat.catalog.common.Operation;

import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrinoSqlParserHelper {
    private static final Logger LOG = LoggerFactory.getLogger(TrinoSqlParserHelper.class);

    private Map<Operation, Set<String>> operationObject = new HashMap<>();

    public Map<Operation, Set<String>> parseSqlGetOperationObjects(String sqlText) {
        Statement statement;
        try {
            SqlParser sqlParser = new SqlParser();
            statement =
                    sqlParser.createStatement(
                            sqlText.toLowerCase(Locale.ROOT), new ParsingOptions(AS_DECIMAL));
        } catch (Exception e) {
            LOG.error(
                    "the trino sql syntax verification failed, please check the sql statement!", e);
            throw e;
        }
        visitTreeNodeFillOperationObject(statement);
        // remove temp table
        Set<String> tempTable = operationObject.get(Operation.CREATE_VIEW);
        Set<String> selectTable = operationObject.get(Operation.SELECT_TABLE);
        if (CollectionUtils.isNotEmpty(tempTable)) {
            selectTable.removeAll(tempTable);
        }
        operationObject.remove(Operation.CREATE_VIEW);
        return operationObject;
    }

    private void visitTreeNodeFillOperationObject(Statement statement) {
        if (statement instanceof CreateTable) {
            addToOperationSet(
                    Operation.CREATE_TABLE, ((CreateTable) statement).getName().toString());
        } else if (statement instanceof DropTable) {
            addToOperationSet(
                    Operation.DROP_TABLE, ((DropTable) statement).getTableName().toString());
        } else if (statement instanceof ShowCreate) {
            addToOperationSet(Operation.DESC_TABLE, ((ShowCreate) statement).getName().toString());
        } else if (statement instanceof ShowColumns) {
            addToOperationSet(
                    Operation.DESC_TABLE, ((ShowColumns) statement).getTable().toString());
        } else if (statement instanceof CreateTableAsSelect) {
            addToOperationSet(
                    Operation.CREATE_TABLE, ((CreateTableAsSelect) statement).getName().toString());
            visitTreeNodeFillOperationObject(((CreateTableAsSelect) statement).getQuery());
        } else if (statement instanceof RenameTable) {
            addToOperationSet(
                    Operation.RENAME_TABLE, ((RenameTable) statement).getSource().toString());
        } else if (statement instanceof AddColumn) {
            addToOperationSet(Operation.ADD_COLUMN, ((AddColumn) statement).getName().toString());
        } else if (statement instanceof DropColumn) {
            addToOperationSet(
                    Operation.DROP_COLUMN, ((DropColumn) statement).getTable().toString());
        } else if (statement instanceof RenameColumn) {
            addToOperationSet(
                    Operation.RENAME_COLUMN, ((RenameColumn) statement).getTable().toString());
        } else if (statement instanceof Merge) {
            addToOperationSet(
                    Operation.INSERT_TABLE, ((Merge) statement).getTable().getName().toString());
            extractTablesFromSqlNode(((Merge) statement).getRelation());
        } else if (statement instanceof Insert) {
            QualifiedName target = ((Insert) statement).getTarget();
            addToOperationSet(Operation.INSERT_TABLE, target.toString());
            visitTreeNodeFillOperationObject(((Insert) statement).getQuery());
        } else if (statement instanceof Query) {
            // with subquery
            Optional<With> with = ((Query) statement).getWith();
            with.ifPresent(value -> value.getChildren().forEach(this::extractTablesFromSqlNode));
            // dela select body
            QueryBody queryBody = ((Query) statement).getQueryBody();
            extractTablesFromSqlNode(queryBody);

            if (!(queryBody instanceof Union)) {
                // deal where
                Optional<Expression> whereExpress = ((QuerySpecification) queryBody).getWhere();
                if (whereExpress.isPresent()) {
                    Expression expression = whereExpress.get();
                    extractTablesFromSqlNode(expression);
                }
            }
        }
    }

    private void extractTablesFromSqlNode(Node sqlNode) {
        if (sqlNode instanceof QuerySpecification) {
            // base query statement
            Optional<Relation> fromRelationOptional = ((QuerySpecification) sqlNode).getFrom();
            if (!fromRelationOptional.isPresent()) {
                return;
            }
            // deal from
            extractTablesFromSqlNode(fromRelationOptional.get());
            // deal where
            ((QuerySpecification) sqlNode).getWhere().ifPresent(this::extractTablesFromSqlNode);
        } else if (sqlNode instanceof TableSubquery) {
            // deal subquery
            extractTablesFromSqlNode(((TableSubquery) sqlNode).getQuery());
        } else if (sqlNode instanceof Table) {
            // the final resolved table name
            QualifiedName tableName = ((Table) sqlNode).getName();
            addToOperationSet(Operation.SELECT_TABLE, tableName.toString());
        } else if (sqlNode instanceof AliasedRelation) {
            // deal as relation, do not save temporary tables
            extractTablesFromSqlNode(((AliasedRelation) sqlNode).getRelation());
        } else if (sqlNode instanceof Query) {
            extractTablesFromSqlNode(((Query) sqlNode).getQueryBody());
        } else if (sqlNode instanceof Join) {
            extractTablesFromSqlNode(((Join) sqlNode).getLeft());
            extractTablesFromSqlNode(((Join) sqlNode).getRight());
        } else if (sqlNode instanceof WithQuery) {
            Identifier tempTableName = ((WithQuery) sqlNode).getName();
            addToOperationSet(Operation.CREATE_VIEW, tempTableName.getValue());
            extractTablesFromSqlNode(((WithQuery) sqlNode).getQuery());
        } else if (sqlNode instanceof ExistsPredicate) {
            // deal exist
            extractTablesFromSqlNode(((ExistsPredicate) sqlNode).getSubquery());
        } else if (sqlNode instanceof InPredicate) {
            // deal where in
            extractTablesFromSqlNode(((InPredicate) sqlNode).getValueList());
        } else if (sqlNode instanceof SubqueryExpression) {
            extractTablesFromSqlNode(((SubqueryExpression) sqlNode).getQuery());
        } else if (sqlNode instanceof ComparisonExpression
                || sqlNode instanceof Union
                || sqlNode instanceof Intersect
                || sqlNode instanceof Except) {
            // deal where xx = sub query or union
            sqlNode.getChildren().forEach(this::extractTablesFromSqlNode);
        } else if (sqlNode instanceof LogicalBinaryExpression) {
            // deal where ..and ..and ..
            extractTablesFromSqlNode(((LogicalBinaryExpression) sqlNode).getLeft());
            extractTablesFromSqlNode(((LogicalBinaryExpression) sqlNode).getRight());
        }
    }

    private void addToOperationSet(Operation operation, String tableName) {
        Set<String> operationSet =
                operationObject.computeIfAbsent(operation, key -> new HashSet<>());
        operationSet.add(tableName);
    }
}
