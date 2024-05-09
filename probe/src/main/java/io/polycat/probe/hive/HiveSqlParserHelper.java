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
package io.polycat.probe.hive;

import io.polycat.catalog.common.Operation;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.Stack;


import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parse sql and store operation and table information into the operationObject collection.
 *
 * @author renjianxu
 * @date 2023/7/10
 */
public class HiveSqlParserHelper implements NodeProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(HiveSqlParserHelper.class);
    private Map<Operation, Set<String>> operationObject = new HashMap<>();

    @Override
    public Object process(
            Node node, Stack<Node> stack, NodeProcessorCtx nodeProcessorCtx, Object... objects) {
        ASTNode ast = (ASTNode) node;
        switch (ast.getToken().getType()) {
                // query table
            case HiveParser.TOK_TABREF:
                {
                    Set<String> inputTables =
                            operationObject.computeIfAbsent(
                                    Operation.SELECT_TABLE, key -> new HashSet());
                    ASTNode tabTree = (ASTNode) ast.getChild(0);
                    String tableName =
                            (tabTree.getChildCount() == 1)
                                    ? BaseSemanticAnalyzer.getUnescapedName(
                                            (ASTNode) tabTree.getChild(0))
                                    : BaseSemanticAnalyzer.getUnescapedName(
                                                    (ASTNode) tabTree.getChild(0))
                                            + "."
                                            + tabTree.getChild(1);
                    inputTables.add(tableName);
                    break;
                }
                // with temp table as create_view operation
            case HiveParser.TOK_CTE:
                {
                    for (int i = 0; i < ast.getChildCount(); i++) {
                        Set<String> tempTables =
                                operationObject.computeIfAbsent(
                                        Operation.CREATE_VIEW, key -> new HashSet());
                        ASTNode temp = (ASTNode) ast.getChild(i);
                        String tableName =
                                BaseSemanticAnalyzer.getUnescapedName((ASTNode) temp.getChild(1));
                        tempTables.add(tableName);
                    }
                    break;
                }
                // insert  table
            case HiveParser.TOK_TAB:
                {
                    addToOperationSet(Operation.INSERT_TABLE, (ASTNode) ast.getChild(0));
                    break;
                }
                // create table
            case HiveParser.TOK_CREATETABLE:
                {
                    addToOperationSet(Operation.CREATE_TABLE, (ASTNode) ast.getChild(0));
                    break;
                }
                // add partitions
            case HiveParser.TOK_ALTERTABLE_ADDPARTS:
                addToOperationSet(Operation.ADD_PARTITIONS, (ASTNode) ast.getParent().getChild(0));
                break;
                // drop partitions
            case HiveParser.TOK_ALTERTABLE_DROPPARTS:
                {
                    addToOperationSet(
                            Operation.DROP_PARTITION, (ASTNode) ast.getParent().getChild(0));
                    break;
                }
                // rename table
            case HiveParser.TOK_ALTERTABLE_RENAME:
                {
                    addToOperationSet(
                            Operation.RENAME_TABLE, (ASTNode) ast.getParent().getChild(0));
                    break;
                }
                // set properties
            case HiveParser.TOK_ALTERTABLE_PROPERTIES:
                {
                    addToOperationSet(
                            Operation.SET_PROPERTIES, (ASTNode) ast.getParent().getChild(0));
                    break;
                }
                // set location
            case HiveParser.TOK_ALTERTABLE_LOCATION:
                {
                    addToOperationSet(
                            Operation.SET_LOCATION, (ASTNode) ast.getParent().getChild(0));
                    break;
                }
                //  add columns
            case HiveParser.TOK_ALTERTABLE_ADDCOLS:
                {
                    addToOperationSet(Operation.ADD_COLUMN, (ASTNode) ast.getParent().getChild(0));
                    break;
                }
                //  rename columns
            case HiveParser.TOK_ALTERTABLE_RENAMECOL:
                {
                    addToOperationSet(
                            Operation.RENAME_COLUMN, (ASTNode) ast.getParent().getChild(0));
                    break;
                }
                // drop table
            case HiveParser.TOK_DROPTABLE:
                {
                    addToOperationSet(Operation.DROP_TABLE, (ASTNode) ast.getChild(0));
                    break;
                }
                // desc table
            case HiveParser.TOK_DESCTABLE:
                {
                    if (ast.getChildCount() >= 1) {
                        for (int i = 0; i < ast.getChildCount(); i++) {
                            ASTNode child = (ASTNode) ast.getChild(i);
                            if (child.getType() == HiveParser.TOK_TABTYPE) {
                                addToOperationSet(
                                        Operation.DESC_TABLE, (ASTNode) child.getChild(0));
                            }
                        }
                    }
                    break;
                }
                // show create table
            case HiveParser.TOK_SHOW_CREATETABLE:
                {
                    addToOperationSet(Operation.DESC_TABLE, (ASTNode) ast.getChild(0));
                    break;
                }
                // alter database
            case HiveParser.TOK_ALTERDATABASE_PROPERTIES:
            case HiveParser.TOK_ALTERDATABASE_OWNER:
                {
                    addToOperationSet(Operation.ALTER_DATABASE, (ASTNode) ast.getChild(0));
                    break;
                }
                // create database
            case HiveParser.TOK_CREATEDATABASE:
                {
                    addToOperationSet(Operation.CREATE_DATABASE, (ASTNode) ast.getChild(0));
                    break;
                }
                // drop database
            case HiveParser.TOK_DROPDATABASE:
                {
                    addToOperationSet(Operation.DROP_DATABASE, (ASTNode) ast.getChild(0));
                    break;
                }
        }
        return null;
    }

    private void addToOperationSet(Operation operation, ASTNode astNode) {
        Set<String> operationSet =
                operationObject.computeIfAbsent(operation, key -> new HashSet<>());
        operationSet.add(BaseSemanticAnalyzer.getUnescapedName(astNode));
    }

    public Map<Operation, Set<String>> parseSqlGetOperationObjects(String sqlText)
            throws ParseException {
        ParseDriver pd = new ParseDriver();
        ASTNode tree = null;
        try {
            tree = pd.parse(sqlText);
        } catch (ParseException e) {
            LOG.error("hive sql parse error!", e);
            throw e;
        }
        while ((tree.getToken() == null) && (tree.getChildCount() > 0)) {
            tree = (ASTNode) tree.getChild(0);
        }

        GraphWalker ogw =
                new DefaultGraphWalker(
                        new DefaultRuleDispatcher(this, new LinkedHashMap<>(), null));
        try {
            ogw.startWalking(Arrays.asList(tree), null);
        } catch (SemanticException e) {
            throw new RuntimeException(e);
        }
        // remove temp table
        Set<String> tempTable = operationObject.get(Operation.CREATE_VIEW);
        Set<String> inputTable = operationObject.get(Operation.SELECT_TABLE);
        if (CollectionUtils.isNotEmpty(tempTable)) {
            inputTable.removeAll(tempTable);
        }
        operationObject.remove(Operation.CREATE_VIEW);
        return operationObject;
    }
}
