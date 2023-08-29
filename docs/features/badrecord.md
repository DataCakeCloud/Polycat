# Bad Record in Streamer

## 需求背景

在Streamer流数据入库过程中，可能出现数据和Schema对不齐的情况，系统应将这种异常数据记录下来，并给用户提供查询手段分析数据异常原因。


## 如何使用

客户查询 BadRecord 
```
SHOW BADRECORDS FOR Table <tablename>
```

输出展示：

| Time                | Table         | Bad Record Type（string）  | RecordFile(string)                          | Exception Message(String)                         |
| ------------------- | ------------- | ------------------------- | ------------------------------------------- | ------------------------------------------------- |
| 2021-09-10-10:55:10 | tablename     | SCHEMA NOT MATCH          | $table/.badrecord/JSON/20210724T101153.json | source type 'STRING' not mache table schema 'INT' |
|                     |               |                           |                                             |                                                   |


## 设计和实现

### 文件路径（存储格式采用AVRO）

#### Badrecord files(记录原始Kafka数据):  
$table/.badrecord/avro/$timestamp.avro

### Log file(记录badrecord生成事件，用于统一查询):
$table/.log/badrecordlog.avro


### 生成badrecord规则
- 单个bache同时出现5个schema的情况（导致5个writer）
- Json的type无法和table schema的type 兼容
- table的schema columns 缺少 >=50% 。 例如: 10个 column ,  Json 只匹配了5个
注： 其他场景逐渐完善


## 如何测试
构造异常格式的JSON数据，发送到Kafka，通过Streamer启动流入库，验证：
1. 验证存储上是否生成bad record
2. 使用SQL查询bad record，得到正确输出
