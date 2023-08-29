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

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;

public class IntervalUtils {

    public static Date plusDate(Date date, TimeInterval interval) {
        LocalDate localDate = date.toLocalDate();
        localDate = localDate.plusMonths(interval.getMonths());
        localDate = localDate.plusDays(interval.getDays());
        return Date.valueOf(localDate);
    }

    public static Date minusDate(Date date, TimeInterval interval) {
        LocalDate localDate = date.toLocalDate();
        localDate = localDate.minusMonths(interval.getMonths());
        localDate = localDate.minusDays(interval.getDays());
        return Date.valueOf(localDate);
    }

    public static Timestamp plusTimestamp(Timestamp timestamp, TimeInterval interval) {
        LocalDateTime localDateTime = timestamp.toLocalDateTime();
        localDateTime = localDateTime.plusMonths(interval.getMonths());
        localDateTime = localDateTime.plusDays(interval.getDays());
        localDateTime = localDateTime.plusSeconds(interval.getSeconds());
        localDateTime = localDateTime.plusNanos(interval.getNano());
        return Timestamp.valueOf(localDateTime);
    }

    public static Timestamp minusTimestamp(Timestamp timestamp, TimeInterval interval) {
        LocalDateTime localDateTime = timestamp.toLocalDateTime();
        localDateTime = localDateTime.minusMonths(interval.getMonths());
        localDateTime = localDateTime.minusDays(interval.getDays());
        localDateTime = localDateTime.minusSeconds(interval.getSeconds());
        localDateTime = localDateTime.minusNanos(interval.getNano());
        return Timestamp.valueOf(localDateTime);
    }
}
