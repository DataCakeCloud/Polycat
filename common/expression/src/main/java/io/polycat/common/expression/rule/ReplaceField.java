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

import java.util.Map;

import io.polycat.common.expression.Expression;
import io.polycat.common.expression.FieldExpression;

public class ReplaceField implements ToExpressionVisitor {

    Map<Integer, Expression> absentFieldMap;

    public ReplaceField(Map<Integer, Expression> absentFieldMap) {
        this.absentFieldMap = absentFieldMap;
    }

    @Override
    public Expression visit(FieldExpression expression) {
        Expression absentExpression = absentFieldMap.get(expression.getFieldIndexes().get(0));
        return absentExpression != null ? absentExpression : expression;
    }
}
