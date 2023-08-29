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

import org.junit.Assert;
import org.junit.Test;

import static cn.myperf4j.base.util.LineProtocolUtils.processTagOrField;

/**
 * Created by LinShunkang on 2018/10/17
 */
public class LineProtocolUtilsTest {

    @Test
    public void test() {
        String expect = "method_metrics\\,AppName\\=TestApp\\,ClassName\\=TestClass\\,Method\\=TestClass.test\\ " +
                "RPS\\=1i\\,Avg\\=0.00\\,Min\\=0i\\,Max\\=0i\\,StdDev\\=0.00\\,Count\\=17i\\,TP50\\=0i\\,TP90\\=0i\\," +
                "TP95\\=0i\\,TP99\\=0i\\,TP999\\=0i\\,TP9999\\=0i\\,TP99999\\=0i\\,TP100\\=0i\\ 1539705590006000000";
        Assert.assertEquals(expect,
                processTagOrField("method_metrics,AppName=TestApp,ClassName=TestClass,Method=TestClass.test " +
                        "RPS=1i,Avg=0.00,Min=0i,Max=0i,StdDev=0.00,Count=17i,TP50=0i,TP90=0i," +
                        "TP95=0i,TP99=0i,TP999=0i,TP9999=0i,TP99999=0i,TP100=0i 1539705590006000000"));
    }
}
