# IMC场景阶段一演示

## 环境准备

湖仓项目对接IMC场景，所使用的资源都是从华为云购买的资源，详细资源清单：

| 产品            | 规格                                                         | 用途                                            |
| --------------- | ------------------------------------------------------------ | ----------------------------------------------- |
| 云数据库RDS     | 单机 8 vCPUs 16 GB                                           | 用于HMS存储                                     |
| 云容器引擎CCE   | 4个ECS节点，每节点：16 vCPUs、 32GB 、40GB 系统盘、100GB 超高IO盘 | 用于spark、lake catalog、gateway、HMS容器化部署 |
| 对象存储服务OBS | 并行文件系统、单AZ存储                                       | 存储数据                                        |
| 弹性云服务器ECS | 3个节点，每节点：通用计算增强型 、c6.4xlarge.2、16vCPUs、32 GiB内存 、40GB 系统盘、200GB SSD | FDB分布式部署                                   |
| 弹性云服务器ESC | 1个节点：通用计算增强型 、c6.4xlarge.2、16vCPUs、32 GiB内存 、40GB 系统盘、200GB SSD | spark driver节点                                |

## 使用约束

datatype当前只支持类型：INT、INTEGER、CHAR、VARCHAR、STRING、FLOAT、DOUBLE、DATA，TIMESTAMP

## demo

### 创建数据库

1、使用beeline直连spark，创建database

```
beeline -u jdbc:hive2://124.71.231.53:10000   #124.71.231.53为spark driver的弹性IP
create database db1                                            #db1为db名称
```

2、使用polycat cli连接gateway，创建catalog、database

```
tar -zxvf polycat-gateway-client-0.1-SNAPSHOT.tar.gz
cd polycat-gateway-client/bin/
./cli.sh
!connect 124.71.239.204 9001 wukong pwd   #124.71.239.204 为gateway服务地址
create catalog c1;                #c1名称是后续创建table时，lms_name的名称
use catalog c1;
create database db1;          #该db1名字需要和beeline连接时创建database名字一样
show databases;   # 该命令确认db1是否创建成功；
```

## 入库

当前批量入库需要以创建外表的形式导入数据，导入TPC-H中nation表数据相关流程包括：

1、数据以csv文件格式上操到OBS文件系统中

```
./obsutil ../tpch_2_14_3/dbgen/nation.csv obs://obs-imcshujuhucang-wuqiang/source/nation/nation.csv   #nation.csv是nation表的文件，obs-imcshujuhucang-wuqiang是文件系统名称
```

2、创建外表，外表location是csv文件所在目录，如果csv文件分隔符不是‘,’，需要显示添加delimiter参数。例如，分隔符是'|'，创建外表需要加参数options(delimiter='|')

```
beeline -u jdbc:hive2://124.71.231.53:10000
use db1;
CREATE TABLE NATION_S  ( N_NATIONKEY  INTEGER NOT NULL,
                            N_NAME       CHAR(25) NOT NULL,
                            N_REGIONKEY  INTEGER NOT NULL,
                            N_COMMENT    VARCHAR(152)) using csv location 'obs://obs-imcshujuhucang-wuqiang/source/nation' options(delimiter='|');
```

3、创建目标表

```
CREATE TABLE NATION  ( N_NATIONKEY  INTEGER NOT NULL,
                            N_NAME       CHAR(25) NOT NULL,
                            N_REGIONKEY  INTEGER NOT NULL,
                            N_COMMENT    VARCHAR(152)) using parquet tblproperties('lms_name'='c1');    #c1名称是通过polycat cli创建的catalog名称
```

4、导入数据到目标表

```
INSERT INTO NATION SELECT * FROM NATION_S;
```

5、TPC-H中其他表入库相关命令：

```
上传csv文件到不同目录，注意，一定要各自在不同的目录下：
./obsutil ../tpch_2_14_3/dbgen/region.csv obs://obs-imcshujuhucang-wuqiang/source/nation/region.csv
./obsutil ../tpch_2_14_3/dbgen/part.csv obs://obs-imcshujuhucang-wuqiang/source/part/part.csv
./obsutil ../tpch_2_14_3/dbgen/supplier.csv obs://obs-imcshujuhucang-wuqiang/source/supplier/supplier.csv
./obsutil ../tpch_2_14_3/dbgen/partsupp.csv obs://obs-imcshujuhucang-wuqiang/source/partsuppn/partsupp.csv
./obsutil ../tpch_2_14_3/dbgen/customer.csv obs://obs-imcshujuhucang-wuqiang/source/customer/customer.csv
./obsutil ../tpch_2_14_3/dbgen/orders.csv obs://obs-imcshujuhucang-wuqiang/source/orders/orders.csv
./obsutil ../tpch_2_14_3/dbgen/lineitem.csv obs://obs-imcshujuhucang-wuqiang/source/lineitem/lineitem.csv

在beeline客户端执行命令：
CREATE TABLE REGION_S  ( R_REGIONKEY  INTEGER NOT NULL,
                            R_NAME       CHAR(25) NOT NULL,
                            R_COMMENT    VARCHAR(152)) using csv location 'obs://obs-imcshujuhucang-wuqiang/source/region' options(delimiter='|');
CREATE TABLE REGION  ( R_REGIONKEY  INTEGER NOT NULL,
                            R_NAME       CHAR(25) NOT NULL,
                            R_COMMENT    VARCHAR(152)) using parquet tblproperties('lms_name'='c1');
INSERT INTO REGION SELECT * FROM REGION_S;

part表入库：
CREATE TABLE PART_S  ( P_PARTKEY     INTEGER NOT NULL,
                          P_NAME        VARCHAR(55) NOT NULL,
                          P_MFGR        CHAR(25) NOT NULL,
                          P_BRAND       CHAR(10) NOT NULL,
                          P_TYPE        VARCHAR(25) NOT NULL,
                          P_SIZE        INTEGER NOT NULL,
                          P_CONTAINER   CHAR(10) NOT NULL,
                          P_RETAILPRICE STRING NOT NULL,
                          P_COMMENT     VARCHAR(23) NOT NULL ) using csv location 'obs://obs-imcshujuhucang-wuqiang/source/part' options(delimiter='|');
CREATE TABLE PART  ( P_PARTKEY     INTEGER NOT NULL,
                          P_NAME        VARCHAR(55) NOT NULL,
                          P_MFGR        CHAR(25) NOT NULL,
                          P_BRAND       CHAR(10) NOT NULL,
                          P_TYPE        VARCHAR(25) NOT NULL,
                          P_SIZE        INTEGER NOT NULL,
                          P_CONTAINER   CHAR(10) NOT NULL,
                          P_RETAILPRICE STRING NOT NULL,
                          P_COMMENT     VARCHAR(23) NOT NULL ) using parquet tblproperties('lms_name'='c1');
INSERT INTO PART SELECT * FROM PART_S;

supplier表入库：
CREATE TABLE SUPPLIER_S ( S_SUPPKEY     INTEGER NOT NULL,
                             S_NAME        CHAR(25) NOT NULL,
                             S_ADDRESS     VARCHAR(40) NOT NULL,
                             S_NATIONKEY   INTEGER NOT NULL,
                             S_PHONE       CHAR(15) NOT NULL,
                             S_ACCTBAL     STRING NOT NULL,
                             S_COMMENT     VARCHAR(101) NOT NULL) using csv location 'obs://obs-imcshujuhucang-wuqiang/source/supplier' options(delimiter='|');
CREATE TABLE SUPPLIER ( S_SUPPKEY     INTEGER NOT NULL,
                             S_NAME        CHAR(25) NOT NULL,
                             S_ADDRESS     VARCHAR(40) NOT NULL,
                             S_NATIONKEY   INTEGER NOT NULL,
                             S_PHONE       CHAR(15) NOT NULL,
                             S_ACCTBAL     STRING NOT NULL,
                             S_COMMENT     VARCHAR(101) NOT NULL) using parquet tblproperties('lms_name'='c1');
INSERT INTO SUPPLIER SELECT * FROM SUPPLIER_S;

partsupp表入库：
CREATE TABLE PARTSUPP_S ( PS_PARTKEY     INTEGER NOT NULL,
                             PS_SUPPKEY     INTEGER NOT NULL,
                             PS_AVAILQTY    INTEGER NOT NULL,
                             PS_SUPPLYCOST  STRING  NOT NULL,
                             PS_COMMENT     VARCHAR(199) NOT NULL ) using csv location 'obs://obs-imcshujuhucang-wuqiang/source/partsupp' options(delimiter='|');
CREATE TABLE PARTSUPP ( PS_PARTKEY     INTEGER NOT NULL,
                             PS_SUPPKEY     INTEGER NOT NULL,
                             PS_AVAILQTY    INTEGER NOT NULL,
                             PS_SUPPLYCOST  STRING  NOT NULL,
                             PS_COMMENT     VARCHAR(199) NOT NULL ) using parquet tblproperties('lms_name'='c1');
INSERT INTO PARTSUPP SELECT * FROM PARTSUPP_S;

customer表入库：
CREATE TABLE CUSTOMER_S ( C_CUSTKEY     INTEGER NOT NULL,
                             C_NAME        VARCHAR(25) NOT NULL,
                             C_ADDRESS     VARCHAR(40) NOT NULL,
                             C_NATIONKEY   INTEGER NOT NULL,
                             C_PHONE       CHAR(15) NOT NULL,
                             C_ACCTBAL     STRING   NOT NULL,
                             C_MKTSEGMENT  CHAR(10) NOT NULL,
                             C_COMMENT     VARCHAR(117) NOT NULL) using csv location 'obs://obs-imcshujuhucang-wuqiang/source/customer' options(delimiter='|');
CREATE TABLE CUSTOMER ( C_CUSTKEY     INTEGER NOT NULL,
                             C_NAME        VARCHAR(25) NOT NULL,
                             C_ADDRESS     VARCHAR(40) NOT NULL,
                             C_NATIONKEY   INTEGER NOT NULL,
                             C_PHONE       CHAR(15) NOT NULL,
                             C_ACCTBAL     STRING   NOT NULL,
                             C_MKTSEGMENT  CHAR(10) NOT NULL,
                             C_COMMENT     VARCHAR(117) NOT NULL) using parquet tblproperties('lms_name'='c1');
INSERT INTO CUSTOMER SELECT * FROM CUSTOMER_S;

orders表入库：
CREATE TABLE ORDERS_S  ( O_ORDERKEY       INTEGER NOT NULL,
                           O_CUSTKEY        INTEGER NOT NULL,
                           O_ORDERSTATUS    CHAR(1) NOT NULL,
                           O_TOTALPRICE     STRING NOT NULL,
                           O_ORDERDATE      DATE NOT NULL,
                           O_ORDERPRIORITY  CHAR(15) NOT NULL,  
                           O_CLERK          CHAR(15) NOT NULL, 
                           O_SHIPPRIORITY   INTEGER NOT NULL,
                           O_COMMENT        VARCHAR(79) NOT NULL) using csv location 'obs://obs-imcshujuhucang-wuqiang/source/orders' options(delimiter='|');
CREATE TABLE ORDERS  ( O_ORDERKEY       INTEGER NOT NULL,
                           O_CUSTKEY        INTEGER NOT NULL,
                           O_ORDERSTATUS    CHAR(1) NOT NULL,
                           O_TOTALPRICE     STRING NOT NULL,
                           O_ORDERDATE      DATE NOT NULL,
                           O_ORDERPRIORITY  CHAR(15) NOT NULL,  
                           O_CLERK          CHAR(15) NOT NULL, 
                           O_SHIPPRIORITY   INTEGER NOT NULL,
                           O_COMMENT        VARCHAR(79) NOT NULL) using parquet tblproperties('lms_name'='c1');
INSERT INTO ORDERS SELECT * FROM ORDERS_S;

lineitem表入库：
CREATE TABLE LINEITEM_S ( L_ORDERKEY    INTEGER NOT NULL,
                             L_PARTKEY     INTEGER NOT NULL,
                             L_SUPPKEY     INTEGER NOT NULL,
                             L_LINENUMBER  INTEGER NOT NULL,
                             L_QUANTITY    STRING NOT NULL,
                             L_EXTENDEDPRICE  STRING NOT NULL,
                             L_DISCOUNT    STRING NOT NULL,
                             L_TAX         STRING NOT NULL,
                             L_RETURNFLAG  CHAR(1) NOT NULL,
                             L_LINESTATUS  CHAR(1) NOT NULL,
                             L_SHIPDATE    DATE NOT NULL,
                             L_COMMITDATE  DATE NOT NULL,
                             L_RECEIPTDATE DATE NOT NULL,
                             L_SHIPINSTRUCT CHAR(25) NOT NULL,
                             L_SHIPMODE     CHAR(10) NOT NULL,
                             L_COMMENT      VARCHAR(44) NOT NULL) using csv location 'obs://obs-imcshujuhucang-wuqiang/source/lineitem' options(delimiter='|');
CREATE TABLE LINEITEM ( L_ORDERKEY    INTEGER NOT NULL,
                             L_PARTKEY     INTEGER NOT NULL,
                             L_SUPPKEY     INTEGER NOT NULL,
                             L_LINENUMBER  INTEGER NOT NULL,
                             L_QUANTITY    STRING NOT NULL,
                             L_EXTENDEDPRICE  STRING NOT NULL,
                             L_DISCOUNT    STRING NOT NULL,
                             L_TAX         STRING NOT NULL,
                             L_RETURNFLAG  CHAR(1) NOT NULL,
                             L_LINESTATUS  CHAR(1) NOT NULL,
                             L_SHIPDATE    DATE NOT NULL,
                             L_COMMITDATE  DATE NOT NULL,
                             L_RECEIPTDATE DATE NOT NULL,
                             L_SHIPINSTRUCT CHAR(25) NOT NULL,
                             L_SHIPMODE     CHAR(10) NOT NULL,
                             L_COMMENT      VARCHAR(44) NOT NULL) using parquet tblproperties('lms_name'='c1');
INSERT INTO LINEITEM SELECT * FROM LINEITEM_S;
```



## 查询

1、价格统计报告查询

```
select 
    l_returnflag,
    l_linestatus, 
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, 
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, 
    avg(l_quantity) as avg_qty, 
    avg(l_extendedprice) as avg_price, 
    avg(l_discount) as avg_disc, 
    count(*) as count_order
from 
    lineitem
where 
    l_shipdate <= date'1998-12-01' - interval '90' day 
group by
    l_returnflag, 
    l_linestatus
order by
    l_returnflag, 
    l_linestatus;
```

2、最小代价供货商查询

```
select
    s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
from
    part, supplier, partsupp, nation, region
where
    p_partkey = ps_partkey
    and s_suppkey = ps_suppkey
    and p_size = 10
    and p_type like 'STANDARD%'
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = 'AFRICA'
    and ps_supplycost = (
        select
            min(ps_supplycost)
        from
            partsupp, supplier, nation, region
        where
            p_partkey = ps_partkey
            and s_suppkey = ps_suppkey
            and s_nationkey = n_nationkey
            and n_regionkey = r_regionkey
            and r_name = 'AFRICA'
    )
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey;
```

3、运送优先级查询

```
select
l_orderkey,
sum(l_extendedprice*(1-l_discount)) as revenue,
o_orderdate,
o_shippriority
from
customer, orders, lineitem
where
c_mktsegment = 'MACHINERY'
and c_custkey = o_custkey
and l_orderkey = o_orderkey
and o_orderdate < date '1995-03-15'
and l_shipdate > date '1995-03-10'
group by
l_orderkey,
o_orderdate,
o_shippriority
order by
revenue desc,
o_orderdate;
```

4、订单优先级查询

```
select
o_orderpriority, 
count(*) as order_count
from orders
where
o_orderdate >= date '1993-03-15'
and o_orderdate < date '1993-3-7' + interval '3' month
and exists (
select
*
from
lineitem
where
l_orderkey = o_orderkey
and l_commitdate < l_receiptdate
)
group by
o_orderpriority
order by
o_orderpriority;
```

5、消费者订单数量查询

```
select
c_count, count(*) as custdist
from
(select
c_custkey,
count(o_orderkey)
from
customer left outer join orders on
c_custkey = o_custkey
and o_comment not like '%special%requests%'
group by
c_custkey
)as c_orders (c_custkey, c_count)
group by
c_count
order by
custdist desc,
c_count desc;
```
