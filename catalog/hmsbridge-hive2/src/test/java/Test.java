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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.polycat.catalog.client.CatalogUserInformation;
import io.polycat.catalog.client.PolyCatClient;
import io.polycat.catalog.common.plugin.CatalogContext;
import io.polycat.catalog.common.plugin.request.ListTablePartitionsRequest;
import io.polycat.catalog.common.plugin.request.input.PartitionFilterInput;
import io.polycat.catalog.hms.hive2.CatalogStore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.QueryPlan;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.thrift.TException;
import scala.collection.Iterator;

public class Test {

    @org.junit.jupiter.api.Test
    public void testMSC() throws TException {
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.uris", "thrift://hms-polycat-region1.xxx.com:9083");
        HiveMetaStoreClient hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
        org.apache.hadoop.hive.metastore.api.Database temp_database = hiveMetaStoreClient.getDatabase("temp_database");
        System.out.println(temp_database);
        //hiveMetaStoreClient.dropTable("db", "tb", true, true);
    }

    @org.junit.jupiter.api.Test
    public void testSparkAST() throws ParseException {
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.hadoop.hive.metastore.uris", "thrift://hms-polycat-region1.xxx.com:9083");
        SparkSession sparkSession = SparkSession.builder().master("local").config(sparkConf).enableHiveSupport()
            .getOrCreate();
        String sql = "WITH assets_tbl AS\n"
            + "( /*每个用户当天是否做了某个功能任务*/\n"
            + "\tSELECT  replace(substr(cast(from_unixtime(cast( create_time AS double)/1000+28800) AS string),1,10),'-','') AS dt\n"
            + "\t       ,country                                                                                             AS nation\n"
            + "\t       ,beyla_id\n"
            + "\t       ,user_id\n"
            + "\t       ,message\n"
            + "\tFROM analyst.ods_buzznews_account_detail /* 全量表 */\n"
            + "\tWHERE dt='2022-05-24'\n"
            + "\tAND replace(substr(cast(from_unixtime(cast( create_time AS double)/1000+28800) AS string), 1, 10), '-', '') in ('20220523', '20220521', \n"
            + "      '20220517', '20220424')\n"
            + "\tAND app_id = 'com.lenovo.anyshare.gps'\n"
            + "\tAND country IN ( SELECT nation FROM analyst.dws_acti_nation_detail GROUP BY 1)\n"
            + "\tAND assets_type='coins'\n"
            + "\tAND type IN ('1') /* 增加1:领取金币, 增加1+待领取2：完成任务 */\n"
            + "\tAND status='1' /* 成功 */\n"
            + "\tAND source IN (select activity_code from analyst.dws_acti_activity_code_detail group by 1)\n"
            + "\tAND beyla_id is not null\n"
            + "\tAND beyla_id <> ''\n"
            + "\tGROUP BY  1\n"
            + "\t         ,2\n"
            + "\t         ,3\n"
            + "\t         ,4\n"
            + "\t         ,5\n"
            + "\n"
            + "\tunion all\n"
            + "\n"
            + "\tSELECT  replace(substr(cast(from_unixtime(cast( create_time AS double)/1000+28800) AS string),1,10),'-','') AS dt\n"
            + "\t       ,country                                                                                             AS nation\n"
            + "\t       ,beyla_id\n"
            + "\t       ,user_id\n"
            + "\t       ,message\n"
            + "\tFROM analyst.ods_buzznews_account_detail /* 全量表 */\n"
            + "\tWHERE dt='2022-05-18'/*有的分区中只含25天数据*/\n"
            + "\tAND replace(substr(cast(from_unixtime(cast( create_time AS double)/1000+28800) AS string),1,10),'-', '') = '20220424'\n"
            + "\tAND app_id = 'com.lenovo.anyshare.gps'\n"
            + "\tAND country IN ( SELECT nation FROM analyst.dws_acti_nation_detail GROUP BY 1)\n"
            + "\tAND assets_type='coins'\n"
            + "\tAND type IN ('1') /* 增加1:领取金币, 增加1+待领取2：完成任务 */\n"
            + "\tAND status='1' /* 成功 */\n"
            + "\tAND source IN (select activity_code from analyst.dws_acti_activity_code_detail group by 1)\n"
            + "\tAND beyla_id is not null\n"
            + "\tAND beyla_id <> ''\n"
            + "\tGROUP BY  1\n"
            + "\t         ,2\n"
            + "\t         ,3\n"
            + "\t         ,4\n"
            + "\t         ,5\n"
            + "\n"
            + ")\n"
            + "\n"
            + "INSERT OVERWRITE TABLE analyst.dwsabcdsign_acti_diff_task_fun_remain_inc_daily PARTITION (dt)\n"
            + "SELECT  distinct a.dt\n"
            + "       ,a.nation\n"
            + "       ,a.message\n"
            + "       ,a.task_uv\n"
            + "       ,CASE WHEN a.task_remain1_uv=0 THEN c.task_remain1_uv  ELSE a.task_remain1_uv END    AS task_remain1_uv\n"
            + "       ,CASE WHEN a.task_remain3_uv=0 THEN c.task_remain3_uv  ELSE a.task_remain3_uv END    AS task_remain3_uv\n"
            + "       ,CASE WHEN a.task_remain7_uv=0 THEN c.task_remain7_uv  ELSE a.task_remain7_uv END    AS task_remain7_uv\n"
            + "       ,CASE WHEN a.task_remain30_uv=0 THEN c.task_remain30_uv  ELSE a.task_remain30_uv END AS task_remain30_uv\n"
            + "       ,a.dt\n"
            + "FROM\n"
            + "(\n"
            + "\tSELECT  a.dt\n"
            + "\t       ,a.nation\n"
            + "\t       ,a.message\n"
            + "\t       ,COUNT(distinct a.beyla_id)                                                                                AS task_uv\n"
            + "\t       ,count(distinct CASE WHEN DATEDIFF(to_date(b.dt,'yyyyMMdd'),to_date(a.dt,'yyyyMMdd')) IN (1) THEN b.beyla_id  end)  AS task_remain1_uv\n"
            + "\t       ,count(distinct CASE WHEN DATEDIFF(to_date(b.dt,'yyyyMMdd'),to_date(a.dt,'yyyyMMdd')) IN (3) THEN b.beyla_id  end)  AS task_remain3_uv\n"
            + "\t       ,count(distinct CASE WHEN DATEDIFF(to_date(b.dt,'yyyyMMdd'),to_date(a.dt,'yyyyMMdd')) IN (7) THEN b.beyla_id  end)  AS task_remain7_uv\n"
            + "\t       ,count(distinct CASE WHEN DATEDIFF(to_date(b.dt,'yyyyMMdd'),to_date(a.dt,'yyyyMMdd')) IN (30) THEN b.beyla_id  end) AS task_remain30_uv\n"
            + "\tFROM\n"
            + "\t(/*当天是否做了某个功能任务*/\n"
            + "\t\tSELECT  *\n"
            + "\t\tFROM assets_tbl\n"
            + "\t\tWHERE  dt>='20220215'\n"
            + "\t\tAND dt IN ('20220523' , '20220521' , '20220517' , '20220424' ) \n"
            + "\t) a\n"
            + "\tLEFT JOIN\n"
            + "\t(/*后来是否做了某个功能任务*/\n"
            + "\t\tSELECT  *\n"
            + "\t\tFROM assets_tbl\n"
            + "\t\tWHERE  dt IN ('20220524') \n"
            + "\t) b\n"
            + "\tON a.beyla_id=b.beyla_id AND a.user_id=b.user_id AND DATEDIFF(to_date(b.dt, 'yyyyMMdd'), to_date(a.dt, 'yyyyMMdd')) IN (1, 3, 7, 30) AND a.message=b.message\n"
            + "\tGROUP BY  1\n"
            + "\t         ,2\n"
            + "\t         ,3\n"
            + ")a\n"
            + "LEFT JOIN\n"
            + "(\n"
            + "\tSELECT  *\n"
            + "\tFROM analyst.dwsabcdsign_acti_diff_task_fun_remain_inc_daily\n"
            + "\tWHERE dt IN ('20220523' , '20220521' , '20220517' , '20220424' ) \n"
            + "\n"
            + ")c\n"
            + "ON a.dt=c.dt AND a.message=c.message and a.nation=c.nation";

        //String sql = "alter table default.test_aaa change column name  n1 string";
        QueryPlan plan = sparkSession.sessionState().sqlParser().parsePlan(sql);
        QueryExecution queryExecution = sparkSession.sessionState().executePlan((LogicalPlan) plan);
        System.out.println(queryExecution.sparkPlan());
        ArrayList<List<String>> result = new ArrayList<>();
        travelTree(queryExecution.analyzed(), result, 0);
        for (List<String> nodes: result) {
            System.out.println(nodes);
        }

    }

    /*@org.junit.jupiter.api.Test
    public void testPresto() {
         //String sql = "insert into bd_dws.dws_beyla_device_active_inc_daily select app_token from bd_dws.dws_beyla_device_active_inc_daily limit 10";
        String sql = "SELECT app_token\n"
            + ",nation\n"
            + ",coalesce(user_type,'ALL') as user_type\n"
            + ",SUM(if(diff_day <= 7,revenue,0)) rev7 \n"
            + "FROM\n"
            + "(\n"
            + "SELECT usr.app_token\n"
            + ",nation\n"
            + ",user_type\n"
            + ",date_diff('day',to_date(usr.dt,'yyyymmdd'),to_date(rev.revenue_date,'yyyymmdd')) AS diff_day\n"
            + ",SUM(rev.revenue) AS revenue \n"
            + "FROM\n"
            + "(--活跃数据\n"
            + "\tSELECT dt\n"
            + "\t,nation\n"
            + "\t,user_type\n"
            + "\t,beyla_id\n"
            + "    ,app_token\n"
            + "\tfrom bd_dws.dws_beyla_device_active_inc_daily\n"
            + "\twhere dt  = '20220512' \n"
            + "\tand app_token IN ('VML','NEWS_BEES','WALL_PAP')\n"
            + "\tand nation in ('ID','PH','IN')\n"
            + "\tand beyla_id is not null \n"
            + "\tand beyla_id != '' \n"
            + "\tgroup by 1,2,3,4,5\n"
            + ") usr\n"
            + "LEFT JOIN \n"
            + "(--收入\n"
            + "\tSELECT dt as revenue_date\n"
            + "\t\t\t,app_token\n"
            + "\t       ,beyla_id\n"
            + "\t       ,SUM(revenue) AS revenue\n"
            + "\tFROM analyst.ads_beyla_revenue\n"
            + "\tWHERE app_token IN ('VML','WALL_PAP','NEWS_BEES')\n"
            + "\tand dt>='20220512' and dt <='20220519'\n"
            + "\tGROUP BY  1\n"
            + "\t         ,2\n"
            + "             ,3\n"
            + ") rev\n"
            + "on usr.beyla_id = rev.beyla_id and usr.app_token=rev.app_token AND rev.revenue_date>=usr.dt\n"
            + "GROUP BY 1,2,3,4\n"
            + ")\n"
            + "GROUP BY 1,2,cube(user_type)";
        SqlParser sqlParser = new SqlParser();
        Statement statement = sqlParser.createStatement(sql, new ParsingOptions(AS_DECIMAL));
        //System.out.println(statement);
        ArrayList<List<String>> result = new ArrayList<>();
        travelPrestoTree(statement, result, 0);
        for (List<String> nodes: result) {
            System.out.println(nodes);
        }
        //System.out.println(result);
    }*/
    /*private void travelPrestoTree(Node node, ArrayList<List<String>> result, int level) {

        System.out.println(node.getClass().getName() + ", " + level);
        List<String> list = level >= result.size()? new ArrayList<>(): result.get(level);
        list.add(node.getClass().getName());
        if (level >= result.size()) {
            result.add(list);
        }
        List<? extends Node> children = node.getChildren();
        if (children.isEmpty()) {
            return;
        }
        for (Node child: children) {
            *//*if (child instanceof io.trino.sql.tree.Table) {
                io.trino.sql.tree.Table s = (io.trino.sql.tree.Table) child;
                System.out.println("Table: " + s);
            }*//*
            travelPrestoTree(child, result, level+1);
        }
    }*/

    private void travelTree(QueryPlan plan, ArrayList<List<String>> result, int level) {
        System.out.println(plan.getClass().getName() + "," + level);
        //System.out.println(plan);
        List<String> list = level >= result.size()? new ArrayList<>(): result.get(level);
        list.add(plan.getClass().getName());
        if (level >= result.size()) {
            result.add(list);
        }
        if (plan.children().isEmpty()) {
            return;
        }
        Iterator<QueryPlan> iterator = plan.children().iterator();
        while (iterator.hasNext()) {
            QueryPlan queryPlan = iterator.next();
            travelTree(queryPlan, result, level+1);
        }

    }

    @org.junit.jupiter.api.Test
    public void testReflect() throws ClassNotFoundException {
        Class<?> sClass = Class.forName("java.lang.String");
        String s = "aaa";
        boolean b = sClass.isInstance(s);
        System.out.println(b);
    }

    @org.junit.jupiter.api.Test
    public void testMetastoreClient() throws TException {
        HiveConf conf = new HiveConf();
        conf.set("hive.metastore.uris", "thrift://10.32.17.40:9083");
        HiveMetaStoreClient hiveMetaStoreClient = new HiveMetaStoreClient(conf);
        org.apache.hadoop.hive.metastore.api.Database sprs_dwd_prod = hiveMetaStoreClient.getDatabase("sprs_dwd_prod");
        System.out.println(sprs_dwd_prod);
        Table table = hiveMetaStoreClient.getTable("sprs_dwd_prod", "dwd_push_user_info_all_daily");
        System.out.println(table);
        Partition partition = hiveMetaStoreClient.getPartition("sprs_dwd_prod", "dwd_push_user_info_all_daily", Arrays.asList("20201122", "hlaki", "content"));
        System.out.println(partition);
        hiveMetaStoreClient.getPartition("sprs_dwd_prod", "dwd_push_user_info_all_daily", "datepart=20201121/request_app=app1/biz_scene=content");
        hiveMetaStoreClient.getDatabases("*");
        hiveMetaStoreClient.listPartitions("sprs_dwd_prod", "dwd_push_user_info_all_daily", Arrays.asList("20201122", "hlaki", "content"), (short)10);
        hiveMetaStoreClient.listPartitions("sprs_dwd_prod", "dwd_push_user_info_all_daily", (short)10);
        hiveMetaStoreClient.listPartitionNames("sprs_dwd_prod", "dwd_push_user_info_all_daily", (short)10);
        hiveMetaStoreClient.listPartitionNames("sprs_dwd_prod", "dwd_push_user_info_all_daily", Arrays.asList("20201122", "hlaki", "content"), (short) 10);
        hiveMetaStoreClient.listTableNamesByFilter("sprs_dwd_prod", "", (short) 10);
        hiveMetaStoreClient.listPartitionSpecs("sprs_dwd_prod", "dwd_push_user_info_all_daily", (short)10);

    }

    @org.junit.jupiter.api.Test
    public void testDefault() throws TException {
        HiveConf conf = new HiveConf();
        conf.set("hive.metastore.uris", "thrift://10.32.16.36:9083");
        HiveMetaStoreClient hiveMetaStoreClient = new HiveMetaStoreClient(conf);
        final Table table = hiveMetaStoreClient.getTable("sprs_dwd_prod", "dwd_log_main_event_inc_hourly");
        final Partition partition = hiveMetaStoreClient.getPartition("sprs_dwd_prod", "dwd_log_main_event_inc_hourly",
            "datepart=20210512/hour=23/request_app=vwatchit/event=click_comment");
        final CatalogStore catalogStore = new CatalogStore();
        final Configuration configuration = new Configuration();
        configuration.set("polycat.user.project", "project1");
        configuration.set("polycat.user.name", "bdp");
        configuration.set("polycat.user.password", "bdp");
        configuration.set("polycat.user.tenant", "tenantA");
        catalogStore.setConf(configuration);
        final ArrayList<Partition> partitions = new ArrayList<>();
        partitions.add(partition);
        catalogStore.addPartitions("catalog_1", "sprs_dwd_prod", "dwd_log_main_event_inc_hourly", table, partitions);
        // catalogStore.addPartition("catalog_1", "sprs_dwd_prod", "dwd_log_main_event_inc_hourly", partition);
    }

    @org.junit.jupiter.api.Test
    public void testPartitions() throws TException {
        Configuration conf = new Configuration();
        conf.set("polycat.catalog.name", "catalog_1");
        conf.set(CatalogUserInformation.POLYCAT_USER_NAME, "bdp");
        conf.set(CatalogUserInformation.POLYCAT_USER_PASSWORD, "bdp");
        conf.set(CatalogUserInformation.POLYCAT_USER_PROJECT, "project1");
        conf.set(CatalogUserInformation.POLYCAT_USER_TENANT, "tenantA");
        // 配置里面要有 gateway.conf
        CatalogStore catalogStore = new CatalogStore(conf);
        final Table table = catalogStore.getTable("catalog_1", "analyst", "dwd_muslim_beyla_event_di");
        final List<Partition> partitionList = catalogStore.getPartitions("catalog_1", "analyst", "dwd_muslim_beyla_event_di", (short) 0);
        partitionList.forEach(partition -> {
            if (partition.getValues().contains("%2fCleanDetail%2fAccesstoUsagePermission") && partition.getValues().contains("20220701")) {
                System.out.println(partition);
                try {
                    List<String> partNames = new ArrayList<String>();
                    partNames.add(Warehouse.makePartName(table.getPartitionKeys(), partition.getValues()));
                    catalogStore.dropPartitions("catalog_1", "analyst", "dwd_muslim_beyla_event_di", partNames);
                } catch (MetaException metaException) {
                    metaException.printStackTrace();
                }

            }
        });
    }

    @org.junit.jupiter.api.Test
    public void testPartitionVals() throws TException {
        HiveConf conf = new HiveConf();
        conf.set("hive.metastore.uris", "thrift://a73a2c9c7c82a4a3c9c16c4687f573ff-1323847595.us-east-1.elb.amazonaws.com:9084");
        HiveMetaStoreClient hiveMetaStoreClient = new HiveMetaStoreClient(conf);
        //final List<Partition> partitionList = hiveMetaStoreClient.listPartitions("default", "mq_table_textxx_001", (short) -1);
        //System.out.println(partitionList);
        final String s = FileUtils.escapePathName("%2F20200599");
        final String s1 = FileUtils.unescapePathName("20200599");
        System.out.println(s1);

    }

    @org.junit.jupiter.api.Test
    public void testHMS() throws TException {
        HiveConf conf = new HiveConf();
        conf.set("hive.metastore.uris", "thrift://hms-polycat-region1.xxx.com:9083");
        HiveMetaStoreClient hiveMetaStoreClient = new HiveMetaStoreClient(conf);
        final org.apache.hadoop.hive.metastore.api.Database database = hiveMetaStoreClient.getDatabase("default");
        System.out.println(database);
    }

}
