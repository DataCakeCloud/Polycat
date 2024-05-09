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
package io.polycat.sql.common.expression.expression;

import java.util.ArrayList;
import java.util.List;

import io.polycat.catalog.common.model.record.IntWritable;
import io.polycat.catalog.common.model.record.StringWritable;
import io.polycat.catalog.common.types.DataTypes;
import io.polycat.common.expression.Expression;
import io.polycat.common.expression.FieldExpression;
import io.polycat.common.expression.LiteralExpression;
import io.polycat.common.expression.bool.AndExpression;
import io.polycat.common.expression.comparison.EqualExpression;
import io.polycat.common.expression.comparison.GreaterThanExpression;
import io.polycat.common.expression.rewrite.TableMinMaxFilterRewriter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TableMinMaxFilterRewriterTest {

    @Test
    public void smoke() {
        List<Expression> operands = new ArrayList<>();
        FieldExpression stringField = new FieldExpression(0, DataTypes.STRING);
        FieldExpression intField = new FieldExpression(1, DataTypes.INTEGER);
        EqualExpression equal = new EqualExpression(stringField, new LiteralExpression(new StringWritable("abc"), DataTypes.STRING));
        GreaterThanExpression greaterThan =
                new GreaterThanExpression(intField, new LiteralExpression(new IntWritable(10), DataTypes.INTEGER));
        operands.add(equal);
        operands.add(greaterThan);
        Expression expression = new AndExpression(operands, DataTypes.STRING);
        Expression newExpression = TableMinMaxFilterRewriter.getInstance().execute(expression);
        Assertions.assertNotNull(newExpression);
        Assertions.assertTrue(newExpression instanceof AndExpression);
        Assertions.assertEquals(3, ((AndExpression) newExpression).getOperands().size());
    }

}
