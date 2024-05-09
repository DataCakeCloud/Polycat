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


import io.polycat.catalog.client.CatalogUserInformation;
import io.polycat.catalog.common.plugin.CatalogPlugin;

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static io.polycat.catalog.spark.SparkCatalogTestEnv.batch;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.checkAnswer;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.getProjectId;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.row;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.spark;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.sql;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith({SparkCatalogTestEnv.class})
@Disabled
public class PolicyTest {

    private final String testAccount1 = "'testAccount1:testAccount1'";
    private final String testAccount2 = "'testAccount2:testAccount2'";
    private static String passwordNew = "huawei";
    private static String userLucy = "lucy";
    private static String userLilei = "lilei";


    @BeforeEach
    void beforeEach() {
        CatalogPlugin client = PolyCatClientHelper.getClientFromSession(spark);
        client.setContext(CatalogUserInformation.doAuth("test", "dash"));
    }

    @Test
    public void smoke_role() {
        sql("create role role1");
        sql("create role role2");
        sql("SHOW ROLES").show();
        sql("GRANT ROLE role1 TO USER IAM testUser1");
        sql("SHOW ROLES").show();
        sql("REVOKE ROLE role1 FROM USER IAM testUser1");
        sql("SHOW ROLES").show();
    }

    @Test
    public void smoke_role_policy() {
        sql("create catalog c1");
        sql("SHOW POLICIES OF USER IAM test").show();

        sql("create role role1");
        sql("GRANT DESC,DROP ON CATALOG c1 TO ROLE role1 EFFECT=TRUE "
            + " CONDITIONS ('IP'='0','TIME'='1') "
            + " ROWFILTER (c1 > 1 AND c1 < 10) OR (c2 > 10 AND c2 < 20) "
            + " COLFILTER c1,c2,c3:INCLUDING "
            + " COLMASK c1,c3:VOID;C2:VOID"
            + " WITH_GRANT");

        sql("DESC ROLE role1").show();
        sql("SHOW ROLES").show();
        sql("SHOW POLICIES OF ROLE role1").show();

        sql("REVOKE DESC ON CATALOG c1 FROM ROLE role1 EFFECT=TRUE ");
        sql("SHOW POLICIES OF ROLE role1").show();
        sql("SHOW ROLES").show();

        sql("drop role role1");
        sql("drop catalog c1");
    }

    @Test
    public void smoke_share_policy() {
        sql("CREATE CATALOG " + "c1");
        sql("USE CATALOG " + "c1");
        sql("CREATE DATABASE " + "db_test1");
        sql("USE DATABASE " + "db_test1");
        sql("CREATE TABLE " + "tb_test1" + " (c1 string, c2 int)  using parquet");
        sql("insert into " + "tb_test1" + " values('ab', 1)");

        sql("create share share1 on c1");
        sql("create share share2 on c1");
        sql("ALTER SHARE share1 ADD ACCOUNTS = " + "'tenantB:lucy'");
        sql("GRANT SELECT ON TABLE c1.db_test1.tb_test1 TO SHARE share1 EFFECT=TRUE "
            + " CONDITIONS ('IP'='0','TIME'='1') "
            + " ROWFILTER (c1 > 1 AND c1 < 10) OR (c2 > 10 AND c2 < 20) "
            + " COLFILTER c1,c2,c3:INCLUDING "
            + " COLMASK c1,c3:VOID;C2:VOID");
        sql("SHOW SHARES").show();
        Row[] rows = (Row[])sql("SHOW SHARES").collect();
        assertEquals(2, rows.length);

        CatalogPlugin client = PolyCatClientHelper.getClientFromSession(spark);
        client.setContext(CatalogUserInformation.doAuth(userLucy, passwordNew));
        spark.conf().set(CatalogUserInformation.POLYCAT_USER_NAME, "lucy");
        spark.conf().set(CatalogUserInformation.POLYCAT_USER_PASSWORD, "huawei");
        checkAnswer(
            sql("select c1,c2 from " + getProjectId() + "." + "share1" + "." + "db_test1" + "." + "tb_test1"),
            batch(row("ab", 1)));


        sql("GRANT SHARE " + getProjectId() + ".share1 TO USER IAM " + userLilei);
        rows = (Row[])sql("SHOW SHARES").collect();
        assertEquals(1, rows.length);

        sql("REVOKE SHARE " + getProjectId() + ".share1 FROM USER IAM " + userLilei);
        rows = (Row[])sql("SHOW SHARES").collect();
        assertEquals(1, rows.length);


        client.setContext(CatalogUserInformation.doAuth("test", "dash"));
        spark.conf().set(CatalogUserInformation.POLYCAT_USER_NAME, "test");
        spark.conf().set(CatalogUserInformation.POLYCAT_USER_PASSWORD, "dash");
        rows = (Row[])sql("SHOW POLICIES OF SHARE share1").collect();
        assertEquals(1, rows.length);

        sql("REVOKE SELECT ON TABLE c1.db_test1.tb_test1 FROM SHARE share1 EFFECT=TRUE ");
        rows = (Row[])sql("SHOW POLICIES OF SHARE share1").collect();
        assertEquals(0, rows.length);


        sql("drop share share1");
        sql("drop share share2");
        sql("drop table c1.db_test1.tb_test1");
        sql("drop database c1.db_test1");
        sql("drop catalog c1");
    }
}
