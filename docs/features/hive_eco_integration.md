# LMS与Hive数仓生态对接


## 需求背景

LMS目标是构建新一代云原生的元数据管理平台，为SQL数仓和AI应用提供在线元数据支持，同时也要能兼容原有基于HiveMetaStore的大数据生态，并支持用户从HMS迁移到LMS。

需要支持如下场景：

1. 新建表：支持在Spark、Trino等Hadoop生态引擎中创表，元数据存储在LMS中，其他引擎可使用。
2. 新建表：支持通过CLI或WebApp直接在LMS中创表，Hadoop生态引擎可使用。
3. 表迁移：支持将Spark、Trino中已有表格迁移到LMS，迁移后用户SQL业务脚本不变。



由于业界存在两类引擎，不同用户可能使用不同引擎创建表格，所以对于这两类引擎都需要支持新建表和表迁移场景。
1. Spark3, Trino：支持三段式的Name Space，表格的全称为catalogName.dbName.tableName
2. Spark2, Hive：支持两段式Name Space，表格的全称为dbName.tableName




## 如何使用


对原有基于两段式NameSpace的计算引擎，使用LMS前需做如下部署：
1. 在HMS的classpath中加入HMSBridge Jar包
2. 修改hive-site.xml，加入如下内容，让HMS使用HMSBridge
```
<property>
    <name>hive.metastore.rawstore.impl</name>
    <value>io.polycat.catalog.hms.hive2.HMSBridgeStore</value>
</property>
<property>
    <name>polycat.host</name>
    <value>host name or IP of LMS</value>
</property>
<property>
    <name>polycat.port</name>
    <value>port of LMS</value>
</property>
```

3. 启动LMS



对于Spark3，使用LMS Castalog Plugin for Spark3 Jar包实现与LMS对接。

对于Trino，使用LMS Connector for Trino Jar包实现与LMS对接。



部署图如下：                                                                                               
                                                                                                                                      
                         Analyst                                                      Administrator                                   
                                                                                                                                      
          //创建Hive表                 //创建LMS表                                    create table prod.db.t4                               
          create table db.t1        create table lms.db.t3                      tblproperties(hive_name='db.t4')                      
                                    tblp (lms_name='prod.db.t3')                                                                      
          //创建LMS表                    |             |                               |                 |                             
          create table db.t2            |             |                               |                 |                             
          tblp(lms_name='prod.db.t2')   |             |                               v                 v                             
                    |                   |             |                         +----------+  +-------------------+                   
                    |                   |             |                         |    CLI   |  |      Web App      |                   
                    |                   |             v                         +----------+  +-------------------+                   
                    |                   |        +-----------+                        |                 |                             
                    v                   v        |   Trino   |                        |                 |                             
               +--------+       +------------+   |+---------+|                        v                 v                             
               | Spark2 |       |  Spark3    |   ||Connector||                  +---------------------------------+                   
               | Hive   |       | +--------+ |   |+---------+|<---------------> |          Lake Meta Store        |                   
               |        |       | |Catalog | |   +-----------+                  |                                 |                   
               |        |       | |Plugin  | |                                  |   +----------------------------+|                   
               |        |       | |Jar     |<---------------------------------> |   |       Table Name Map       ||                   
               |        |       | +--------+ |                                  |   |                            ||                   
               |        |       +------------+    +-----------------+           |   |    hive_name <-> lms_name  ||                   
               |        |                         | Hive Meta Store |           |   |                            ||                   
               |        |                         |                 | get name  |   | Ex. db.t2 <-> prod.db.t2   ||                   
               |        |                         |  +------------+ <-------------> |     db.t3 <-> prod.db.t3   ||                   
               |        |        hive_name        |  |            | |           |   |     db.t4 <-> prod.db.t4   ||                   
               |        |  <--------------------> |  |            | |           |   +----------------------------+|                   
               |        |                         |  | HMSBridge  | | lms_name  |   +----------------------------+|                   
               +--------+                         |  |    Jar     | <---------> |   |         KV Store           ||                   
                    ^                             |  |            | |           |   |                            ||                   
                    |                             |  |            | |           |   +----------------------------+|                   
                    |                             |  |            | |           +---------------------------------+                   
                    v                             |  |            | |                                                                 
             +------------------------+           |  |            | | hive_name +---------------------------------+                   
             |                        |           |  |            | <---------> |              MySQL              |                   
             |         Storage        |           |  +------------+ |           |                                 |                   
             |                        |           |                 |           +---------------------------------+                   
             +------------------------+           +-----------------+                                                                 


​             



所以当新建表时需要建立Hive表格和LMS表格的映射关系，需引入两个表属性：

- lms_name: 表示该表格在LMS中name space
- hive_name：表示该表格在Hive中的name space

例子：

```
// 在hive或spark2中创建t1表，并射影到LMS表 catalog1.db1.t1
CREATE TABLE db1.t1 (...) tblproperties ('lms_name'='catalog1.db1.t1')


// 使用CLI在LMS直接创表，并映射到Hive表 db2.t1
CREATE TABLE catalog2.db2.t1 (...) tblproperties ('hive_name'='db2.t1')

// 在spark2中入库和查询该表
INSERT INTO db2.t1 ...
SELECT * FROM db2.t1

```



## 设计和实现



在FoundationDB中新增Subspace：

|            | type   |
| ---------- | ------ |
| projectId  | tring  |
| objectName | string |
| hiveName   | String |



## 如何测试



