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
package io.polycat.catalog.spark;

import java.text.SimpleDateFormat;
import java.util.UUID;

import io.polycat.catalog.common.model.Table;
import io.polycat.catalog.common.plugin.request.GetTableRequest;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static io.polycat.catalog.spark.SparkCatalogTestEnv.catalogClient;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.createCatalog;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.createNamespace;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.sql;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith({SparkCatalogTestEnv.class})
public class ImcTest {
    private static String catalogName;
    private static String nameSpace;

    @BeforeEach
    public void databaseCommandBeforeEach() {
        catalogName = createCatalog();
        sql("use catalog " + catalogName);

        nameSpace = createNamespace(catalogName);
        sql("use namespace " + nameSpace);
    }

    @Test
    public void sqlForIMCTestWithoutPartition() {
        // left join、inner join
        // sum() count() limit avg
        // string_agg()
        // group by、order by、desc、 having
        // case when then end
        // sub query
        // create table by using 'lms_name' in spark
        sql("create table tbl1 (c1 string, c2 int) using parquet").collect();
        sql("desc table tbl1").show();

        // check metadata in lms
        GetTableRequest getTableRequest = new GetTableRequest(catalogClient.getProjectId(), catalogName, nameSpace, "tbl1");
        Table lmsTable = catalogClient.getTable(getTableRequest);
        Assertions.assertNotNull(lmsTable);

        sql("insert into tbl1 "
                + " values ('a', 1),('b',2),('c',3),('d',4),('e',4),('f',4)").collect();

        // check data from spark
        // support order by&limit&desc
        Row[] rows = (Row[]) sql("select c1,c2 from tbl1 order by c2 desc limit 2 ").collect();
        Assertions.assertEquals(2, rows.length);
        Assertions.assertEquals("d", rows[0].getString(0));
        Assertions.assertEquals(4, rows[0].getInt(1));

        // support count()
        Row[] rows1 = (Row[]) sql("select count(c2) from tbl1 where c2 > 2").collect();
        Assertions.assertEquals(1, rows1.length);
        Assertions.assertEquals(4, rows1[0].getLong(0));

        // support avg()
        Row[] rows2 = (Row[]) sql("select avg(c2) from tbl1").collect();
        Assertions.assertEquals(1, rows2.length);
        Assertions.assertEquals(3, rows2[0].getDouble(0));

        // support group by&&having
        Row[] rows3 = (Row[]) sql("select c2,first(c1) from tbl1 group by c2 having c2 = 2 ").collect();
        Assertions.assertEquals(1, rows3.length);
        Assertions.assertEquals(2, rows3[0].getInt(0));

        // support case when then end
        Row[] rows4 = (Row[]) sql("select c1, case  when c2 == 4 then 'yes' else 'false' end as newc2 from tbl1 ").collect();
        Assertions.assertEquals(6, rows4.length);
        Assertions.assertEquals("yes", rows4[0].getString(1));

        //support inner join&&left join
        sql("create table tbl2 (c1 string, c2 int) using parquet").collect();

        sql("insert into tbl2 "
                + " values ('a', 5),('a',4),('c',4),('d',7),('e',8),('f',9)").collect();

        Row[] rows5 = (Row[]) sql("select * from tbl2 left join tbl1 on tbl2.c2=tbl1.c2 ").collect();
        Assertions.assertEquals(10, rows5.length);
        Assertions.assertEquals("a", rows5[0].getString(0));

        Row[] rows6 = (Row[]) sql("select * from tbl2 inner join tbl1 on tbl2.c2=tbl1.c2 ").collect();
        Assertions.assertEquals(6, rows6.length);
        Assertions.assertEquals("a", rows6[0].getString(0));

        //support sub query
        Row[] rows7 = (Row[]) sql("select * from (select c1 from tbl2 where c2=4)").collect();
        Assertions.assertEquals(2, rows7.length);
        Assertions.assertEquals("a", rows7[0].getString(0));

        sql("drop table tbl1").collect();
        sql("drop table tbl2").collect();
    }

    @Test
    public void sqlForIMCTest() {
        // left join、inner join
        // sum() count() limit avg
        // string_agg()
        // group by、order by、desc、 having
        // case when then end
        // sub query

        // create table by using 'c3' in spark
        sql("create table t9 (c1 string, c2 int, c3 string) using parquet"
            + " partitioned by (c3)");

        String part1 = UUID.randomUUID().toString();
        sql("insert into t9 partition(c3='" + part1 + "')"
            + " values ('a', 1),('b',2),('c',3),('d',4),('e',4),('f',4)");

        sql("select * from t9").show();

        // check metadata in lms
        GetTableRequest getTableRequest = new GetTableRequest(catalogClient.getProjectId(), catalogName, nameSpace, "t9");
        Table lmsTable = catalogClient.getTable(getTableRequest);
        Assertions.assertNotNull(lmsTable);

        // check data from spark
        // support order by&limit&desc
        Row[] rows = (Row[]) sql("select c1,c2 from t9 order by c2 desc limit 2 ").collect();
        Assertions.assertEquals(2, rows.length);
        Assertions.assertEquals("d", rows[0].getString(0));
        Assertions.assertEquals(4, rows[0].getInt(1));

        // support count()
        Row[] rows1 = (Row[]) sql("select count(c2) from t9 where c2 > 2").collect();
        Assertions.assertEquals(1, rows1.length);
        Assertions.assertEquals(4, rows1[0].getLong(0));

        // support avg()
        Row[] rows2 = (Row[]) sql("select avg(c2) from t9").collect();
        Assertions.assertEquals(1, rows2.length);
        Assertions.assertEquals(3, rows2[0].getDouble(0));

        // support group by&&having
        Row[] rows3 = (Row[]) sql("select c2,first(c1) from t9 group by c2 having c2 = 2 ").collect();
        Assertions.assertEquals(1, rows3.length);
        Assertions.assertEquals(2, rows3[0].getInt(0));

        // support case when then end
        Row[] rows4 = (Row[]) sql("select c1, case  when c2 == 4 then 'yes' else 'false' end as newc2 from t9 ").collect();
        Assertions.assertEquals(6, rows4.length);
        Assertions.assertEquals("yes", rows4[0].getString(1));

        //support inner join&&left join
        sql("create table t10 (c1 string, c2 int, c3 string) using parquet"
            + " partitioned by (c3)").collect();

        String part2 = UUID.randomUUID().toString();
        sql("insert into t10 partition(c3='" + part2 + "')"
            + " values ('a', 5),('a',4),('c',4),('d',7),('e',8),('f',9)").collect();

        Row[] rows5 = (Row[]) sql("select * from t10 left join t9 on t10.c2=t9.c2 ").collect();
        Assertions.assertEquals(10, rows5.length);
        Assertions.assertEquals("a", rows5[0].getString(0));

        Row[] rows6 = (Row[]) sql("select * from t10 inner join t9 on t10.c2=t9.c2 ").collect();
        Assertions.assertEquals(6, rows6.length);
        Assertions.assertEquals("a", rows6[0].getString(0));

        //support sub query
        Row[] rows7 = (Row[]) sql("select * from (select c1 from t10 where c2=4)").collect();
        Assertions.assertEquals(2, rows7.length);
        Assertions.assertEquals("a", rows7[0].getString(0));

        sql("drop table t9");
        sql("drop table t10");
    }

    @Test
    void DataTypeTest() {
        // test for data type: int/string/date/float/double/date
        // char/varchar are not supported in the table schema since Spark 3.1
        String tableName = "testdatatype";
        // create table by using 'lms_name' in spark
        sql("create table " + tableName + " (c1 int, c2 string, c3 float, c4 double, " +
            "c5 date, c6 TIMESTAMP, c7 decimal(10,2)) " +
            "using parquet").collect();
        sql("DESC TABLE " + tableName).show();
        // desc table
        Row[] rowsTable = (Row[]) sql("DESC TABLE " + tableName).collect();
        Assertions.assertEquals(7+3, rowsTable.length);
        Assertions.assertEquals("int", rowsTable[0].getString(1));
        Assertions.assertEquals("string", rowsTable[1].getString(1));
        Assertions.assertEquals("float", rowsTable[2].getString(1));
        Assertions.assertEquals("double", rowsTable[3].getString(1));
        Assertions.assertEquals("date",   rowsTable[4].getString(1));
        Assertions.assertEquals("timestamp", rowsTable[5].getString(1));
        Assertions.assertEquals("decimal(10,2)", rowsTable[6].getString(1));
        Assertions.assertEquals("", rowsTable[7].getString(1));
        Assertions.assertEquals("# Partitioning", rowsTable[8].getString(0));
        Assertions.assertEquals("Not partitioned", rowsTable[9].getString(0));
        // insert table
        String part = UUID.randomUUID().toString();
        sql("insert into " + tableName
            + "  select 1, 'a', 3.1415, 3.141592654, to_date('2017-08-09 10:11:12','yyyy-MM-dd HH:mm:ss'),"
            + " to_timestamp('2017-08-09 10:11:12','yyyy-MM-dd HH:mm:ss'), 12.1").collect();
        double EPS1 = 1.0E-7;
        double EPS2 = 1.0E-16;
        Row[] rows = (Row[]) sql("select * from " + tableName).collect();
        Assertions.assertEquals(1, rows.length);
        Assertions.assertEquals(1, rows[0].getInt(0));
        Assertions.assertEquals("a", rows[0].getString(1));
        Assertions.assertTrue(Math.abs(rows[0].getFloat(2) - 3.1415) < EPS1);
        Assertions.assertTrue(Math.abs(rows[0].getDouble(3) - 3.141592654) < EPS2);
        SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd");
        Assertions.assertEquals( "2017-08-09", dateformat.format(rows[0].getDate(4)));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Assertions.assertEquals("2017-08-09 10:11:12", sdf.format(rows[0].getTimestamp(5)));
        Assertions.assertEquals(12.1, rows[0].getDecimal(6).doubleValue());

        sql("drop table " + tableName).collect();
    }

    @Test
    void decimalDataTypeTest() {
        String tableName = "decimal";

        AnalysisException e = assertThrows(AnalysisException.class,
            () -> sql(
                "create table " + tableName + " (tid int, c1 DECIMAL(15,20), c2 DECIMAL(20,15), c3 DECIMAL(10,8)) using csv " +
                    "partitioned by (c3)").collect());
        assertEquals(e.getMessage().contains("Decimal scale (20) cannot be greater than precision (15)."), true);

        sql(
            "create table " + tableName + " (tid int, c1 DECIMAL(10,2), c2 DECIMAL(6,5), c3 DECIMAL(4,1)) using csv " +
                "partitioned by (c3)").collect();

        ArithmeticException e1 = assertThrows(ArithmeticException.class,
            () -> sql("insert into " + tableName + " partition(c3=314.1) values (1, 123456789.12, 1.12)")
                .collect());
        assertEquals(
            e1.getMessage().contains("Decimal(expanded,123456789.12,11,2}) cannot be represented as Decimal(10, 2)."),
            true);

        sql("insert into " + tableName +
            " partition(c3=13.14) values (1, 12345678.12, 1.123)").collect();
        sql("insert into " + tableName +
            " partition(c3=14.14) values (2, 12345678.1234, 1.12345)").collect();

        Row[] rows = (Row[]) sql("select * from " + tableName + " order by tid ASC").collect();
        Assertions.assertEquals(2, rows.length);
        Assertions.assertEquals(1, rows[0].getInt(0));
        Assertions.assertEquals(12345678.12, rows[0].getDecimal(1).doubleValue());
        Assertions.assertEquals(1.123, rows[0].getDecimal(2).doubleValue());
        Assertions.assertEquals(13.1, rows[0].getDecimal(3).doubleValue());
        Assertions.assertEquals(2, rows[1].getInt(0));
        Assertions.assertEquals(12345678.12, rows[1].getDecimal(1).doubleValue());
        Assertions.assertEquals(1.12345, rows[1].getDecimal(2).doubleValue());
        Assertions.assertEquals(14.1, rows[1].getDecimal(3).doubleValue());
    }

}
