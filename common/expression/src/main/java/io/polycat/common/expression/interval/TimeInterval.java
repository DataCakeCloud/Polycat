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
package io.polycat.common.expression.interval;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import lombok.Data;

@Data
public class TimeInterval implements Serializable {

    private int months;
    private int days;
    private int seconds;
    private long nano;

    public TimeInterval() {

    }

    public void add(long value, IntervalUnit unit) {
        switch (unit) {
            case YEAR:
                months = Math.addExact(months, Math.multiplyExact((int) value, 12));
                break;
            case MONTH:
                months = Math.addExact(months, (int) value);
                break;
            case DAY:
                days = Math.addExact(days, (int) value);
                break;
            case HOUR:
                seconds = Math.addExact(seconds, (int) TimeUnit.HOURS.toSeconds(value));
                break;
            case MINUTE:
                seconds = Math.addExact(seconds, (int) TimeUnit.MINUTES.toSeconds(value));
                break;
            case SECOND:
                seconds = Math.addExact(seconds, (int) value);
                break;
            case NANO:
                nano = Math.addExact(nano, value);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + unit);
        }
    }

    public void subtract(long value, IntervalUnit unit) {
        add(Math.subtractExact(0L, value), unit);
    }


    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder("TimeInterval");
        if (months != 0) {
            stringBuilder.append(" months=").append(months);
        }
        if (days != 0) {
            stringBuilder.append(" days=").append(days);
        }
        if (seconds != 0) {
            stringBuilder.append(" seconds=").append(seconds);
        }
        if (nano != 0) {
            stringBuilder.append(" nano=").append(nano);
        }
        return stringBuilder.toString();
    }
}
