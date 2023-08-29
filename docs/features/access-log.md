# Access Log and its application

## 需求背景

大数据平台中表格众多，为了提升数据分析和管理效率，管理员需要了解数据的使用模式，例如基于表格的使用频度确定有针对性的管理，基于血缘分析了解数据之间的使用关系等。

数据的访问频度统计、血缘分析等功能其实都依赖于用户查询日志，所以此特性首先需要获得大数据引擎的查询日志，在日志基础上实现 可以扩展出元数据的应用。


## 如何使用


### SQL语法

查询表格使用次数并排序
```sql
SHOW { READ | WRITE } ACCESS STATS
     FOR CATALOG <catalog_name>
     [ LIMIT = <limit_num> ] [ DESC | ASC ]

{ READ | WRITE } 表示用户对表的操作是读或写, 必选参数
[ DESC | ASC ] 表示结果升序或者降序排列, 可选参数, 默认为ASC

1. SHOW READ ACCESS STATS FOR CATALOG c1 LIMIT = 100 DESC 
查询CATALOG c1内表格读操作最多的前100

2. SHOW WRITE ACCESS STATS FOR CATALOG c1 LIMIT = 1000 ASC 
查询CATALOG c1内表格写操作最少的前1000
```

查询表格使用次数
```sql
DESC ACCESS STATS FOR TABLE <table_name>
```

查询表格血缘关系
```sql
SHOW DATA LINEAGE FOR TABLE <table_name>
```

## 设计和实现

### FDB表结构

FoundationDB中新增如下2个Subspace

#### 血缘关系Subspace

| PROJECT_ID | TARGET_CATALOG_ID | TARGET_DATABASE_ID | TARGET_TABLE_ID | SOURCE_CATALOG_ID | SOURCE_DATABASE_ID | SOURCE_TABLE_ID |
| :----: | :----: | :----: | :----: | :----: | :----: | :----: |
| PROJECT_ID | TARGET_CATALOG_ID | TARGET_DATABASE_ID | TARGET_TABLE_ID | SOURCE_CATALOG_ID | SOURCE_DATABASE_ID | SOURCE_TABLE_ID |



#### 数据画像Subspace

数据湖中存储了用户大量的数据，系统需要帮助用户判断不同数据的使用频率，实现冷热表分离和数据管理，更好地优化数据或进行ETL。

| START_TIMESTAMP | PROJECT_ID | CATALOG_ID | DATABASE_ID | TABLE_ID | OP_TYPE | COUNT |
| :----: | :----: | :----: | :----: | :----: | :----: | :----: | 
| 开始时间 | PROJECT ID | CATALOG ID | DATABASE ID | TABLE ID | 操作类型 | 操作计数 |

开始时间：按照周期规整后的时间戳，当前为1min

操作类型：READ, WRITE


## 如何测试

向大数据引擎发起查询，然后查询LMS的数据画像、血缘、表格使用次数等做验证
