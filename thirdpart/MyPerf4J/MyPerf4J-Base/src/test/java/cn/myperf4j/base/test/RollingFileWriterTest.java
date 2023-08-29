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
package cn.myperf4j.base.test;

import cn.myperf4j.base.file.AutoRollingFileWriter;
import cn.myperf4j.base.file.DailyRollingFileWriter;
import cn.myperf4j.base.file.HourlyRollingFileWriter;
import cn.myperf4j.base.file.MinutelyRollingFileWriter;
import org.junit.Test;

/**
 * Created by LinShunkang on 2018/10/17
 */
public class RollingFileWriterTest {

    @Test
    public void test() {
        AutoRollingFileWriter writer1 = new MinutelyRollingFileWriter("/tmp/test1.log", 1);
        test(writer1);

        AutoRollingFileWriter writer2 = new HourlyRollingFileWriter("/tmp/test2.log", 1);
        test(writer2);

        AutoRollingFileWriter writer3 = new DailyRollingFileWriter("/tmp/test3.log", 1);
        test(writer3);
    }

    private void test(AutoRollingFileWriter writer) {
        writer.write("111111");
        writer.write("222222");
        writer.write("333333");
        writer.writeAndFlush("44444");
        writer.preCloseFile();
        writer.closeFile(true);
    }
}
