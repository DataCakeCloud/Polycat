--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

CREATE DATABASE migration;

USE migration;

-- 迁移表
CREATE TABLE `migration` (
  `id` int NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `name` varchar(64) COLLATE utf8_bin NOT NULL COMMENT '迁移名称，migration-自动生成的8个字符串',
  `source_uri` varchar(128) COLLATE utf8_bin NOT NULL COMMENT '源端地址信息',
  `source_user` varchar(32) COLLATE utf8_bin COMMENT '源端认证信息',
  `source_password` varchar(64) COLLATE utf8_bin,
  `dest_uri` varchar(128) COLLATE utf8_bin NOT NULL COMMENT '目的端地址',
  `dest_user` varchar(32) COLLATE utf8_bin NOT NULL COMMENT '目的端认证信息',
  `dest_password` varchar(64) COLLATE utf8_bin NOT NULL,
  `project_id` varchar(64) COLLATE utf8_bin NOT NULL,
  `tenant_name` varchar(128) COLLATE utf8_bin NOT NULL,
  `status` int DEFAULT NULL COMMENT '状态',
  `create_time` datetime DEFAULT NULL COMMENT '任务创建时间',
  `sync_time` datetime DEFAULT NULL COMMENT '任务的同步时间',
  `extra` varchar(512) COLLATE utf8_bin DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb3 COLLATE=utf8_bin;



-- 迁移对象表
CREATE TABLE `migration_item` (
  `id` BIGINT(128) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `migration_id` INT NOT NULL COMMENT 'migration表的外键',
  `type` INT NOT NULL COMMENT '迁移类型',
  `item_name` VARCHAR(256) NOT NULL COMMENT '迁移对象的名称',
  `start_time` DATETIME NULL COMMENT '迁移开始时间',
  `end_time` DATETIME NULL COMMENT '迁移结束时间',
  `status` INT NULL COMMENT '迁移状态',
  `properties` VARCHAR(1024) NULL COMMENT '迁移对象的属性',
  `extra` VARCHAR(1024) NULL,
  PRIMARY KEY (`id`));
