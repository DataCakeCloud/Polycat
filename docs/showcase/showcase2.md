# 阶段二演示

演示如下特性：
1. 免编程流式入库，推断Schema建表
2. CarbonSQL和Hive访问一份数据，边流入边查询，ACID
3. 跨用户使用共享数据，实现Live Sharing
4. 跨用户共享数据分支，协同开发
5. 创建共享，跨租户使用共享数据
6. 使用CatalogUI查看版本历史
7. Metrics查看性能统计数据

### 环境准备

1). 提前安装并启动hadoop和kafka

    参考single-node-showcase-env-setup.md

2). 启动hive：
```bash
    cp /home/fangxieyun/workspace/dash/catalog/hmsbridge/target/polycat-catalog-hmsbridge-0.1-SNAPSHOT.jar /root/software/apache-hive-3.1.2-bin/lib/
    cp /home/fangxieyun/workspace/dash/catalog/hmsbridge/target/lib/polycat-catalog-* /root/software/apache-hive-3.1.2-bin/lib/
    hiveserver2
```

3). 启动dash server：
```bash
    ./bin/start.sh server
```


## 免编程流式入库

1). 用户wukong登录并创建表：
```sql
./bin/start-carbon.sh cli
!connect 127.0.0.1 9001 wukong pwd
show catalogs;
create catalog c1;
use catalog c1;
create database db1 location 'hdfs://localhost:9000/user/db1';
show databases;
use database db1;
show tables;
create table target (L_ORDERKEY int, L_PARTKEY int, L_SUPPKEY int, L_LINENUMBER int,
            L_QUANTITY double, L_EXTENDEDPRICE double, L_DISCOUNT double, L_TAX double,
            L_RETURNFLAG string, L_LINESTATUS string, L_SHIPDATE string, L_COMMITDATE string,
            L_RECEIPTDATE string, L_SHIPINSTRUCT string, L_SHIPMODE string, L_COMMENT string)
            stored as parquet
show tables;
desc table target;
show streams;
```

2). 向kafka消息队列定时发送数据:
cd /home/fangxieyun/workspace/dash/bin/streamer
kafka-topics.sh --zookeeper 127.0.0.1:2181 --list
./datagen.sh
```
<b><font color="#660000">注</font></b>：datagen.sh脚本使用kafka-console-producer.sh向kafka发送csv格式的数据，默认的kafka sever地址为localhost:9092，
topic为streamer_test，如果kafka中不存在该topic，kafka-console-producer.sh会自动创建一个.
```

3). 用户wukong启动流
```sql
create stream s1 into table target from KAFKA 
       stmproperties ('topic'='streamer_test', 'bootstrap'='127.0.0.1:9092', 'format'='csv', 'separator'='|')
start stream s1 options ('interval'='2sec')
select count(*) from target;
```
<b><font color="#660000">注</font></b>：topic填写上面datagen.sh发送的topic信息，boostrap为datagen.sh对应的kafka-server地址

预览：未来支持json数据自动推断schema和schema自适应变更:
```
create table source stored as kakfa (topic='my_topic', format='json')
create table target (id string) like source
create stream st into table target from source
start stream st options (interval='2sec')
```

## CarbonSQL和Hive访问一份数据，边流入边查询，ACID
1). 用户wukong使用CarbonSQL查询
```sql
select count(*) from target;   数据不断增加
```

2). 用户wukong使用beeline查询
```sql
beeline
!connect jdbc:hive2://127.0.0.1:10000
wukong
pwd
set metaconf:metastore.catalog.default;
set metaconf:metastore.catalog.default=c1;
show databases;
desc database db1;
use db1;
show tables;
select count(*) from target;    数据不断增加
```

3). 停止向kafak入数据

## 跨用户使用共享数据，实现Live Sharing
1). 用户bajie登录：
```sql
./bin/start-carbon.sh cli
!connect 127.0.0.1 9001 bajie pwd
show catalogs;
use catalog c1;  显示无权限
select count(*) from c1.db1.target;  显示无权限
```

2). 用户wukong给bajie增加权限：
```sql
create role role1;  创建角色role1
grant select on table c1.db1.target to role role1;  授予角色role1某个对象操作权限
show grants to role role1;   查看角色role1有那些权限
grant role role1 to user bajie;  把角色role1授予用户bajie
show roles;  查看所有角色,并能显示每个role授予了那些用户
```

3). 用户bajie查看：
```sql
select count(*) from c1.db1.target;  可以正常显示数据
desc table c1.db1.target;  无权限
insert into c1.db1.target values (12,32,4,5,5.1,6.1,2.3,7.1,'ag','fag','ag','fag','ag','fag','ag','fag'); 无权限
```

4). 启动datagen,向kafka消息队列定时发送数据：
./datagen

5). 用户wukong、bajie分别查询
```sql
select count(*) from c1.db1.target;   数据实时刷新
```

6). 停止向kafak入数据


## 跨用户共享数据分支，协同开发

1). 用户wukong创建分支:
```sql
create branch b1 on c1;
show catalogs;
show branches on c1;
```

2). 用户wujing登录：
```sql
./bin/start-carbon.sh cli
!connect 127.0.0.1 9001 wujing pwd
show catalogs;
select count(*) from b1.db1.target;   无权限
```

3). 用户wukong给wujing增加权限：
```sql
create role role2;            创建角色role2
grant all on table b1.db1.target to role role2;  授予角色role2某个对象所有操作权限
show grants to role role2;        查看角色role2有那些权限
grant role role2 to user wujing;   把角色role1授予用户wujing
show roles;
```

4). 用户wujing登录操作：
```sql
select count(*) from b1.db1.target;
desc table b1.db1.target;
select count(*) from c1.db1.target;     无权限
```

5). 启动datagen,向kafka消息队列定时发送数据：
./datagen

6). 用户wukong登录查询
select count(*) from target;   数据量不断增加

7). 用户wujing登录操作：
```sql
select count(*) from b1.db1.target;  查询表的数据未增加,符合预期
```

8). 停止向kafak入数据

9). 用户wujing登录操作：
```sql
insert into b1.db1.target values (12,32,4,5,5.1,6.1,2.3,7.1,'ag','fag','ag','fag','ag','fag','ag','fag');
select count(*) from b1.db1.target;  增加一条数据
```

10). 用户wukong登录查询
select count(*) from target;   数据量还是10000整数倍


## 创建共享，跨租户使用共享数据

1). 用户guanyin登录：
```sql
./bin/start-carbon.sh cli
!connect 127.0.0.1 9001 guanyin pwd
show catalogs;
show shares;
```

2). wukong给guanyin增加权限：
```sql
create share share1;
grant select on table c1.db1.target to share share1;  给share添加对象权限
alter share share1 add accounts='tenantB';   给share添加租户
show shares;     查看share信息
```

3). 用户guanyin：
```sql
show shares;
select count(*) from project1.share1.db1.target;
```

4). 启动datagen,向kafka消息队列定时发送数据：
./datagen

5). 用户guanyin：
```sql
select count(*) from project1.share1.db1.target;   数据量实时更新
```

6). 停止向kafak入数据


7). 用户guanyin创建自己的表，并于share表join：
```sql
show catalogs;
create catalog c2;
use catalog c2;
create database db2 location 'hdfs://localhost:9000/user/db2';
use database db2;
create table part (p_partkey int, p_name string);
insert into part values(155190, 'book'),(67310, 'computer'),(63700, 'fruit'),(2132, 'milk');
select * from part;
select l_orderkey, p_partkey, p_name from project1.share1.db1.target, part where l_orderkey = 0 and l_partkey = p_partkey;
```

## 演示CatalogUI
8. Catalog页面、Database页面、Table页面
