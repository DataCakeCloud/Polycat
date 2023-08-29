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

import lombok.Data;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;

@Data
public class SyntaxException extends RuntimeException {

    private String sql = "";

    private String msg;

    private int line;

    private int start;

    private int end;

    public SyntaxException(String msg, int line, int start, int end) {
        this.msg = msg;
        this.line = line;
        this.start = start;
        this.end = end;
    }

    @Override
    public String getMessage() {
        StringBuilder builder = new StringBuilder(msg);
        builder.append(" (column ").append(start).append(", line ").append(line).append(")\n");
        String[] split = sql.split("\n", -1);
        int index = line - 1;
        for (int i = 0; i < split.length; i++) {
            builder.append(split[i]).append("\n");
            if (i == index) {
                for (int j = 0; j < start; j++) {
                    builder.append(" ");
                }
                builder.append(withRedColor("^"));
            }
        }
        return builder.toString();
    }

    private String withRedColor(String content) {
        return new AttributedStringBuilder()
                .style(AttributedStyle.DEFAULT.foreground(AttributedStyle.RED))
                .append(content)
                .toAnsi();
    }
}
