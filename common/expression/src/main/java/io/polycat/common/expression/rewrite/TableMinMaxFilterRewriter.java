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
package io.polycat.common.expression.rewrite;

import java.util.Arrays;
import java.util.List;

import io.polycat.common.expression.RuleExecutor;
import io.polycat.common.expression.rule.CombineBinary;
import io.polycat.common.expression.rule.EliminateBoolean;
import io.polycat.common.expression.rule.ExpressionRule;
import io.polycat.common.expression.rule.FieldBeforeLiteral;
import io.polycat.common.expression.rule.MinMaxPruning;
import io.polycat.common.expression.rule.MinMaxTranslate;

public class TableMinMaxFilterRewriter extends RuleExecutor {

    private static class MinMaxPlannerHandler {
        private static final TableMinMaxFilterRewriter INSTANCE = new TableMinMaxFilterRewriter();
    }

    private final List<ExpressionRule> rules = Arrays.asList(
            new ExpressionRule(new MinMaxPruning(), 1),
            new ExpressionRule(new FieldBeforeLiteral(), 1),
            new ExpressionRule(new MinMaxTranslate(), 1),
            new ExpressionRule(new EliminateBoolean(), 1),
            new ExpressionRule(new CombineBinary(), 1));

    private TableMinMaxFilterRewriter() {

    }

    @Override
    public List<ExpressionRule> getRules() {
        return rules;
    }

    public static TableMinMaxFilterRewriter getInstance() {
        return MinMaxPlannerHandler.INSTANCE;
    }

}
