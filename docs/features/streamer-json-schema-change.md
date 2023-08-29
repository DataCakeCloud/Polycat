# JSON schema auto change in Streamer

## 需要背景

流入库场景中，Kafka中的JSON数据Schema经常发生变化，目前大多数用户使用人工改Schema的方式非常麻烦，希望系统提供Schema自动变更能力。

## 如何使用

Streamer提供一个针对Stream对象的新配置项，用户在创建Stream时可配置是否打开此特性。

## 设计和实现

在Streamer中按如下步骤完成Schema自动变更：
1. 初始化：当前的writer个数为1(最多支持5个)
2. 如果JSON的SCHEMA不足TABLE SCHEMA预定义的50%，则将这条记录计入bad record. 如果JSON数据和表的SCHEMA匹配，则正常写入这个Partitions在这个批次的处理逻辑结束转5，如果有新增字段转3
3. 将之前的数据按照变更前的schema信息写入parquet文件，根据JSON数据值推断类型，并更新ParquetRowBatchWriter的schema信息，转4
4. writer个数加1，如果超过5个，则将后续数据写入bad record，这个Partitions在这个批次的处理逻辑结束. 转5
5. mapPartitions的collect后，查看写文件的stats的SCHEMA是否发生变更，如果没有变更，转7。如果有变更，转6 
6. 取最新的SCHEMA，并调用carbon的ALTER TABLE的接口来更新表的SCHEMA，转7
7. 调用carbon的insert segment接口，转8
8. 等待下一个微批的执行，进入流程1循环


## 如何测试

构造不断增加新字段的JSON，发送到kafka，启动Streamer，验证目标表的schema是否同步完成更新。
