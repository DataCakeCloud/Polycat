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
package io.polycat.probe;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.parser.ParseException;

public class TestSparkSqlParser {
    public static void main(String[] args) throws ParseException {
        String sql1 =
                "insert into qz_test.test_risk_02\n"
                        + "select t1.risk_params from qz_test.test_risk_03 t1 join qz_test.test_risk_04 t2 on t1.risk_params = t2.risk_params";

        String sql2 =
                ""
                        + "select t1.risk_params from qz_test.test_risk_03 t1 join qz_test.test_risk_04 t2 on t1.risk_params = t2.risk_params limit 10";

        String sql3 =
                "select * from analyst.event_add_xxxxxxxx_contacts_all_daily_partition_inc_daily";
        String sql4 =
                "CREATE TABLE ads_dmp.dim_ads_midas_dsp_t_campaign_daily_stats_part_v3_test (\n"
                        + "  id BIGINT COMMENT '主键id',\n"
                        + "  campaign_pk_id BIGINT COMMENT 'campaign主键id',\n"
                        + "  campaign_name STRING COMMENT 'campaign名称',\n"
                        + "  campaign_id STRING COMMENT 'campaignid',\n"
                        + "  country STRING COMMENT '国家',\n"
                        + "  date STRING COMMENT '日期 yyyyMMdd',\n"
                        + "  objective BIGINT COMMENT '',\n"
                        + "  consume BIGINT COMMENT '日消耗',\n"
                        + "  install BIGINT COMMENT '日转化数',\n"
                        + "  bid_price BIGINT COMMENT '日出价',\n"
                        + "  budget BIGINT COMMENT '当前预算',\n"
                        + "  create_time BIGINT COMMENT '创建时间',\n"
                        + "  update_time BIGINT COMMENT '更新时间',\n"
                        + "  account_id BIGINT COMMENT '账户id',\n"
                        + "  dt STRING COMMENT '分区字段')\n"
                        + "USING PARQUET\n"
                        + "PARTITIONED BY (dt)\n"
                        + "LOCATION 's3://xxxxxxxx.ads.us-east-1/dw/table/dim/dim_ads_midas_dsp_t_campaign_daily_stats_part_v3'\n"
                        + "";

        String sql5 = "SHOW TABLES from  testDB";
        String sql6 =
                "ALTER TABLE ads_dmp.dim_ads_midas_t_sku_group_relation_part_v3  ADD COLUMNS(noUserCol int) ";
        String sql7 = "drop TABLE testTable  ";
        String sql8 = "describe  TABLE ads_dmp.dim_ads_midas_t_sku_group_relation_part_v3  ";

        String sql9 =
                "insert\n"
                        + " overwrite table ads_dmp.dim_ads_midas_t_creative_info_v3 \n"
                        + "select\n"
                        + " cast(creative_id as bigint),\n"
                        + " cast(creative_type as int),\n"
                        + " creative_text,\n"
                        + " cast(img_type as int),\n"
                        + " img_url,\n"
                        + " img_remote_url,\n"
                        + " img_desc,\n"
                        + " title_desc,\n"
                        + " title,\n"
                        + " cast(flash_type as int),\n"
                        + " flash_url,\n"
                        + " flash_remote_url,\n"
                        + " backup_img_display,\n"
                        + " cast(backup_img_type as int),\n"
                        + " backup_img_url,\n"
                        + " cast(display_type as int),\n"
                        + " cast(user_id as bigint),\n"
                        + " cast(video_time as bigint),\n"
                        + " cast(create_time as bigint),\n"
                        + " cast(modify_time as bigint),\n"
                        + " creative_name,\n"
                        + " cast(creative_status as int),\n"
                        + " cast(format_id as bigint),\n"
                        + " feeds_title,\n"
                        + " feeds_url,\n"
                        + " feeds_desc,\n"
                        + " cast(site_id as bigint),\n"
                        + " icon_url,\n"
                        + " video_source,\n"
                        + " video_review,\n"
                        + " play_url,\n"
                        + " creative_format,\n"
                        + " btn_txt,\n"
                        + " cast(app_height as bigint),\n"
                        + " cast(app_width as bigint),\n"
                        + " js_tag,\n"
                        + " cast(display_prio as bigint),\n"
                        + " cast(edit_version as bigint),\n"
                        + " json_data,\n"
                        + " img_url2,\n"
                        + " origin_js_tag,\n"
                        + " cast(effect_type as bigint),\n"
                        + " thumb_icon_url,\n"
                        + " thumb_float_icon_url,\n"
                        + " cast(layout_type as bigint),\n"
                        + " cast(scale_type as bigint),\n"
                        + " cast(width as bigint),\n"
                        + " cast(height as bigint),\n"
                        + " status_bar_color,\n"
                        + " cast(need_cover as int),\n"
                        + " cast(countdown as bigint),\n"
                        + " cast(need_mraidjs as bigint),\n"
                        + " cast(need_preloadjs as bigint),\n"
                        + " cast(account_id as bigint),\n"
                        + " js_tag_tmpl_info,\n"
                        + " cast(has_tip as bigint),\n"
                        + " tip_guide_text,\n"
                        + " cast(style_type as bigint),\n"
                        + " pkg_name,\n"
                        + " cast(hotapp_flag as bigint),\n"
                        + " cast(hotapp_checked as bigint),\n"
                        + " cast(landing_page_id as bigint),\n"
                        + " hotapp_conf,\n"
                        + " cast(grade as double),\n"
                        + " cast(install_cnt as bigint),\n"
                        + " cast(original_id as bigint),\n"
                        + " cast(temporary_id as bigint),\n"
                        + " cast(approval_status as bigint),\n"
                        + " product_info,\n"
                        + " cast(product_type as bigint),\n"
                        + " imp_monitor_url,\n"
                        + " click_monitor_url,\n"
                        + " reject_reason,\n"
                        + " front_img_url,\n"
                        + " bg_img_url,\n"
                        + " cast(show_video_mute as bigint),\n"
                        + " cast(link_jump_mode as bigint),\n"
                        + " play_start_url,\n"
                        + " play_quarter_url,\n"
                        + " play_half_url,\n"
                        + " play_three_quarter_url,\n"
                        + " play_total_url,\n"
                        + " play_goon_url,\n"
                        + " off_impre_url,\n"
                        + " cast(need_anti_hijack as int),\n"
                        + " anti_hijack,\n"
                        + " app_title,\n"
                        + " app_package_name,\n"
                        + " click_url,\n"
                        + " deeplink_url,\n"
                        + " cast(xxxxxxxx_inner_func_type as bigint),\n"
                        + " xxxxxxxx_inner_func_type_name,\n"
                        + " xxxxxxxx_source_id,\n"
                        + " cast(target_page as bigint),\n"
                        + " track_urls,\n"
                        + " cast(off_report as bigint),\n"
                        + " cast(off_report_valid as bigint),\n"
                        + " cast(ad_logo as bigint),\n"
                        + " cache_pkg_name,\n"
                        + " cast(sub_action as bigint),\n"
                        + " wraped_info,\n"
                        + " cast(version as bigint),\n"
                        + " cast(skipable as int),\n"
                        + " pkg_size,\n"
                        + " img_url3,\n"
                        + " creative_element,\n"
                        + " off_click_url,\n"
                        + " landing_page_imp_monitor_url,\n"
                        + " landing_page_click_monitor_url,\n"
                        + " landing_page_offline_imp_monitor_url,\n"
                        + " landing_page_offline_click_monitor_url,\n"
                        + " package_download_url,\n"
                        + " cast(is_vast as bigint),\n"
                        + " vast_extra_info,\n"
                        + " cast(tag_status as bigint),\n"
                        + " cast(online_report as bigint),\n"
                        + " cast(need_landing_page as int),\n"
                        + " cast(animation_type as bigint),\n"
                        + " cast(allow_offline as int),\n"
                        + " cast(g_p2_c_d_n as int),\n"
                        + " detail_page_imp_track_urls,\n"
                        + " detail_page_click_track_urls,\n"
                        + " cast(auto_download as bigint),\n"
                        + " om_sdk_monitor,\n"
                        + " advertiser_action_tracker,\n"
                        + " cast(need_second_track as bigint),\n"
                        + " second_imp_monitor_url,\n"
                        + " second_click_monitor_url,\n"
                        + " cast(ad_animation_type_video as bigint),\n"
                        + " video_source_list,\n"
                        + " cast(downloader as bigint),\n"
                        + " cast(is_mini_site as bigint),\n"
                        + " cast(reservation_download as bigint),\n"
                        + " cast(reservation_download_condition as bigint),\n"
                        + " reservation_download_period,\n"
                        + " cast(reservation_app_launch_time as bigint),\n"
                        + " reminder_email_sending_time,\n"
                        + " reminder_email_address,\n"
                        + " cast(silently_install as bigint),\n"
                        + " cast(ad_in_keep_popup as bigint),\n"
                        + " cast(video_jump_time as bigint),\n"
                        + " vast_tag_url,\n"
                        + " cast(support_jump_vast_video as int),\n"
                        + " cast(vast_video_jump_time as bigint),\n"
                        + " cast(advertiser_id as bigint),\n"
                        + " cast(associate_agency_id as bigint),\n"
                        + " cast(format_type as bigint),\n"
                        + " format_sub_type,\n"
                        + " tag_ids,\n"
                        + " cast(ad_style as bigint),\n"
                        + " cast(page_id as bigint),\n"
                        + " cast(creator_id as bigint),\n"
                        + " creative_md5,\n"
                        + " js_tag_template_name,\n"
                        + " cast(major_image_id as bigint),\n"
                        + " cast(major_video_id as bigint)\n"
                        + "from\n"
                        + " ads_dmp.dim_ads_midas_t_creative_info_part_v3\n"
                        + "WHERE\n"
                        + " dt = '20220101'";
        String sql10 = "create table qz_test.test_risk_03 like qz_test.test_risk_02";
        String sql11 = "alter table qz_test.test_risk_02 RENAME TO qz_test.test_risk_022";
        String sql12 = "create table test_risk_33 as select * from  qz_test.test_risk_02";
        String sql13 = "CREATE TABLE student (id INT, name STRING, age INT)";
        String sql14 =
                "LOAD DATA LOCAL INPATH '/user/hive/warehouse/students' OVERWRITE INTO TABLE qz_test.test_risk_02;";
        String sql15 = "SHOW create table qz_test.test_risk_02 AS SERDE;";
        String sql16 = "ALTER TABLE qz_test.test_risk_02 SET TBLPROPERTIES ( 'a1'='b1')";
        String sql17 =
                "ALTER TABLE qz_test.test_risk_02 ALTER COLUMN risk_params COMMENT \"new comment\"";
        String sql18 =
                "alter table ads_dmp.dim_ads_midas_t_creative_info_part_v3 add partition (dt=2022061507) \n"
                        + "location 's3://xxxxxxxx.ads.us-east-1/dw/table/dim/dim_ads_midas_t_creative_info_part_v3/dt=2022061507'";

        String sql19 =
                "\n"
                        + "select * from (\n"
                        + "\tselect sku_id,group_id from (\n"
                        + "\tselect a1.sku_id, a2.group_id \n"
                        + "\t\tfrom  ads_dmp.dim_ads_midas_t_sku_group_relation_part_v3  a1\n"
                        + "\t\tjoin   ads_dmp.dim_ads_midas_t_sku_group_relation_part_v3  a2\n"
                        + "\t\ton a1.id = a2.id\n"
                        + ") tmp1\n"
                        + ") tmp2\n";

        String sql20 =
                "insert into ads_dmp.dim_ads_midas_t_sku_group_relation_part_v3 "
                        + "  select * from ads_dmp.dim_ads_midas_t_sku_group_relation_part_v3  limit 1 ";

        String sql22 =
                "insert into ads_dmp.dim_ads_midas_t_sku_group_relation_part_v3 "
                        + "  select v3.* from ads_dmp.dim_ads_midas_t_sku_group_relation_part_v3  v3 limit 1";
        String sql21 =
                "\n"
                        + "create table  test123.rjx_test01 "
                        + "select * from (\n"
                        + "\tselect length(sku_id) as sid,group_id from (\n"
                        + "\tselect a1.sku_id, a2.group_id \n"
                        + "\t\tfrom  ads_dmp.dim_ads_midas_t_sku_group_relation_part_v3  a1\n"
                        + "\t\tjoin   ads_dmp.dim_ads_midas_t_sku_group_relation_part_v3  a2\n"
                        + "\t\ton a1.id = a2.id\n"
                        + ") tmp1\n"
                        + ") tmp2\n";

        String sql212 =
                "\n"
                        + "create table  test123.rjx_test01 "
                        + "select * from (\n"
                        + "\tselect length(sku_id + group_id ) as sid,group_id from (\n"
                        + "\tselect a1.sku_id, a2.group_id \n"
                        + "\t\tfrom  ads_dmp.dim_ads_midas_t_sku_group_relation_part_v3  a1\n"
                        + "\t\tjoin   ads_dmp.dim_ads_midas_t_sku_group_relation_part_v3  a2\n"
                        + "\t\ton a1.id = a2.id\n"
                        + ") tmp1\n"
                        + ") tmp2\n";

        String sql23 =
                "\n"
                        + "create table  test123.rjx_test01 "
                        + "select "
                        + "   sku_id as sid,"
                        + "   group_id as gid "
                        + "from  "
                        + "   ads_dmp.dim_ads_midas_t_sku_group_relation_part_v3 v3";
        String sql24 =
                "create table xxxx as select tmp.sid from   ( select  (v3.sku_id + group_id ) as sid, group_id as gid  from   ads_dmp.dim_ads_midas_t_sku_group_relation_part_v3  v3 ) tmp";

        String sql25 =
                "create table xxxx as "
                        + "select  length(v3.sku_id + group_id ), group_id as gid  "
                        + "from   ads_dmp.dim_ads_midas_t_sku_group_relation_part_v3  v3";

        String sql26 =
                "insert into  ads_dmp.dim_ads_midas_t_sku_group_relation_part_v3   "
                        + "  select "
                        + "  id, "
                        + "  length(id) as xxxx, "
                        + "  group_id gid,  "
                        + "  status,  "
                        + "  create_time,  "
                        + "  modify_time,"
                        + " noUserCol, "
                        + " dt "
                        + " from ads_dmp.dim_ads_midas_t_sku_group_relation_part_v3  limit 1 ";

        String sql27 =
                "WITH tempView AS (\n"
                        + "  select id, length(sku_id + group_id), group_id, status,create_time, modify_time,noUserCol,dt"
                        + " from ads_dmp.dim_ads_midas_t_sku_group_relation_part_v3 limit 1\n"
                        + ")\n"
                        + "insert into ads_dmp.dim_ads_midas_t_sku_group_relation_part_v3 SELECT * FROM tempView\n";

        String sql28 =
                "INSERT INTO TABLE wanglltestdb.tbl_1435\n" + "SELECT * FROM wanglltestdb.tbl_1437";

        String sql29 =
                " insert into table analyst.dws_ecom_pos_creative_all_daily partition(dt='20210310')\n"
                        + " select\n"
                        + "  pkgname,\n"
                        + "  first_channel,\n"
                        + "  sub_channel,\n"
                        + "  nation,\n"
                        + "  pos_id,\n"
                        + "  campaign_id,\n"
                        + "  a.cid,\n"
                        + "  img_url,\n"
                        + "  width,\n"
                        + "  height,\n"
                        + "  request_pv,\n"
                        + "  win_pv,\n"
                        + "  imp_pv,\n"
                        + "  click_pv,\n"
                        + "  revenue,\n"
                        + "  costs\n"
                        + "from\n"
                        + "  (\n"
                        + "    select\n"
                        + "      pkgname,\n"
                        + "      'External' as first_channel,\n"
                        + "      adnetwork as sub_channel,\n"
                        + "      upper(nation) as nation,\n"
                        + "      cast(cid as string) as cid,\n"
                        + "      cast(adpositionid as string) as pos_id,\n"
                        + "      cast(campaignid as string) as campaign_id,\n"
                        + "      sum(requestcount) as request_pv,\n"
                        + "      sum(wincount) as win_pv,\n"
                        + "      sum(impressions) as imp_pv,\n"
                        + "      sum(clicks) as click_pv,\n"
                        + "      sum(revenue) as revenue,\n"
                        + "      sum(loopmecost) as costs\n"
                        + "    from\n"
                        + "      ecom_dws.dws_midas_openrtb_ad_sum_day_union\n"
                        + "    where\n"
                        + "      dt = '20240310'\n"
                        + "    group by\n"
                        + "      1,\n"
                        + "      2,\n"
                        + "      3,\n"
                        + "      4,\n"
                        + "      5,\n"
                        + "      6,\n"
                        + "      7\n"
                        + "    union all\n"
                        + "    select\n"
                        + "      ad_package_name as pkgname,\n"
                        + "      'Internal' as first_channel,\n"
                        + "      second_channel_id as sub_channel,\n"
                        + "      upper(nation) as nation,\n"
                        + "      cid,\n"
                        + "      pos_id,\n"
                        + "      campaign_id,\n"
                        + "      count(if(event = 'request', gaid, null)) as request_pv,\n"
                        + "      count(if(event = 'win_notice', gaid, null)) as win_pv,\n"
                        + "      count(if(event = 'show', gaid, null)) as imp_pv,\n"
                        + "      count(if(event = 'click', gaid, null)) as click_pv,\n"
                        + "      cast(sum(if(event = 'click', bid_price, 0)) as double) / 1000000 as revenue,\n"
                        + "      sum(if(event = 'show', ecpm, 0)) / 1000 as costs\n"
                        + "    from\n"
                        + "      ecom_dws.dws_dsp_prectcvr_abtest_event_detail_hour_union\n"
                        + "    where\n"
                        + "      dt = '20240310'\n"
                        + "    group by\n"
                        + "      1,\n"
                        + "      2,\n"
                        + "      3,\n"
                        + "      4,\n"
                        + "      5,\n"
                        + "      6,\n"
                        + "      7\n"
                        + "  ) a\n"
                        + "  left join (\n"
                        + "    select\n"
                        + "      cast(creative_id as string) as cid,\n"
                        + "      if(img_url = '' or img_url is null,video_source,img_url) as img_url,\n"
                        + "      width,\n"
                        + "      height\n"
                        + "    from\n"
                        + "      ads_dmp.dim_ads_midas_t_creative_info\n"
                        + "    where\n"
                        + "      (coalesce(img_url, '') != '' or coalesce(video_source, '') != '')\n"
                        + "    group by\n"
                        + "      1,\n"
                        + "      2,\n"
                        + "      3,\n"
                        + "      4\n"
                        + "    union all\n"
                        + "    select\n"
                        + "      cast(creative_id as string) as cid,\n"
                        + "      if(img_url = '' or img_url is null,video_source,img_url) as img_url,\n"
                        + "      width,\n"
                        + "      height\n"
                        + "    from\n"
                        + "      ecom_rtb_adx_source.aigc_t_creative_info_all_new\n"
                        + "    where\n"
                        + "      (coalesce(img_url, '') != '' or coalesce(video_source, '') != '')\n"
                        + "    group by\n"
                        + "      1,\n"
                        + "      2,\n"
                        + "      3,\n"
                        + "      4\n"
                        + "  ) b on a.cid = b.cid";

        String sql30 =
                "insert into bd_dws.dws_beyla_device_active_all_daily_test01\n"
                        + " select\n"
                        + "  *\n"
                        + "from\n"
                        + "  bd_dws.dws_beyla_device_active_all_daily\n"
                        + "  where\n"
                        + " dt = '20240113'\n"
                        + "limit\n"
                        + "   10";

        SparkConf sparkConf = new SparkConf();
        // build QueryExecution
        sparkConf.set("spark.hadoop.hive.metastore.uris", "thrift://hms-sg1.uxxxxxxxx.org:9083");
        //        sparkConf.set(
        //                "spark.sql.extensions",
        // "io.polycat.probe.spark.SparkTablePermissionExtensions");
        sparkConf.set(
                "spark.sql.queryExecutionListeners", "io.polycat.probe.spark.SparkLineageListener");
        //                "spark.sql.queryExecutionListeners",
        //               "io.polycat.probe.spark.PolyCatSparkListener");
        sparkConf.set("spark.kubernetes.driverEnv.HADOOP_USER_NAME", "xxxxxxxx#renjianxu");
        sparkConf.set("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        sparkConf.set(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");

        sparkConf.set("spark.hadoop.polycat.catalog.name", "xxxxxxxx_sg1");
        sparkConf.set("spark.hadoop.polycat.client.host", "polycat-catalog.datacake.cloud");
        //        sparkConf.set("spark.hadoop.polycat.client.host",
        // "polycat-catalog.datacake.cloud1");
        sparkConf.set("spark.hadoop.polycat.client.port", "80");
        sparkConf.set("spark.sql.authorization.enabled", "true");
        sparkConf.set("spark.ui.enabled", "false");
        sparkConf.set("spark.hadoop.polycat.usageprofiles.taskId", "sparkTest0001");
        sparkConf.set("spark.network.timeout", "3000000");
        sparkConf.set(
                "spark.hadoop.polycat.client.token",
                "rO0ABXNyADJpby5sYWtlY2F0LmNhdGFsb2cuYXV0aGVudGljYXRpb24ubW9kZWwuTG9jYWxUb2tlbjcmbiYxJvNvAgADTAAJYWNjb3VudElkdAASTGphdmEvbGFuZy9TdHJpbmc7TAAGcGFzc3dkcQB+AAFMAAZ1c2VySWRxAH4AAXhwdAAHdGVuYW50QXQAA2JkcHQAA2JkcA==");
        sparkConf.set("spark.hadoop.polycat.client.projectId", "xxxxxxxx");
        sparkConf.set("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog");
        sparkConf.set("spark.sql.catalog.iceberg.type", "hive");
        sparkConf.set("spark.sql.catalog.iceberg.uri", "thrift://hms-sg1.uxxxxxxxx.org:9083");

        // -------------------------------------------------------
        SparkSession sparkSession =
                SparkSession.builder()
                        .master("local")
                        .config(sparkConf)
                        .enableHiveSupport()
                        .getOrCreate();
        //        QueryPlan plan = sparkSession.sessionState().sqlParser().parsePlan(sql1);
        //        QueryExecution queryExecution =
        // sparkSession.sessionState().executePlan((LogicalPlan) plan);
        //
        //        // build tableUsageProfiles
        //        SparkUsageProfileExtracter sparkUsageProfileExtracter =
        //                new SparkUsageProfileExtracter("xxxxxxxx_ue1");
        //        List<TableUsageProfile> tableUsageProfiles =
        //                sparkUsageProfileExtracter.extractTableUsageProfile(queryExecution);
        //        System.out.println(tableUsageProfiles);
        sparkSession.sql(sql29).show();
        sparkSession.close();
    }
}
