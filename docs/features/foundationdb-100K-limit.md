# 解决foundationdb 单次事务 value不能超过100K， 限制segments提交数量的问题

## 需求背景

FDB有如下使用约束
1. Transaction size cannot exceed 10,000,000 bytes of affected data。事务不能超过10MB。
2. 单条KV记录大小限制，Keys cannot exceed 10,000 bytes in size. Values cannot exceed 100,000 bytes in size。key不能超过10K，value不能超过100K。
3. https://apple.github.io/foundationdb/known-limitations.html#known-limitations
4. record layer 可以解决单个KV过大的问题，但是不能解决单次事务内提交数据量的限制。

为了解决foundationdb 单次事务 value不能超过100K的问题，需要此特性

## 如何使用

本特性为了解决FDB的约束，不提供新的使用接口


## 设计和实现

 ##支持场景
 1. 流式入库，小批量写入，一段时间内会产生大量的segment文件
 2. 元数据迁移，迁移到其他的FDB集群，迁移到其他的KV数据库
 3. 支持COW，MOR等写入方式
 4. 分支合并，子分支基于原表插入大量segment文件后，批量回合到父分支
 
 ##设计约束
 1. 采用KV存储segment记录
 2. 支持动态扩展，每次写入数据量要满足KV数据库的使用约束
 3. 保持之前的Segment结构不变，在外面在扩展一层SegmentSet的结构封装树形逻辑。
 4. 目前我们采用了查询性能优先方案，也就是每次写入后，都会采用COW方式修改segment数据，每次COW修改，都会合并历史segment和最新的segment。
    在当前版本中保存完整的数据快照副本的索引。查询时可以在当前版本快速索引到对应的文件，避免通过version多次点查询；为了查询性能优先才需要解决当前事务内，提交数据内容过多的问题。
	
 ##segment存储结构状态变迁：
                    [datasegmentset0,    datasegmentset1,   ...datasegmentsetn]                        [indexsegmentset0,  indexsegment1, ...indexsegmentn]
 null   ----->          /      \              /     \                             ----->                     /    \                                      
                 [segment0, ...segmentn]  [segment(n+1), ...segmentm]                       [datasegmentset0,  datasegmentset1, ...datasegmentsetn]
				                                                                                 /    \
																						[segment0, ...segmentn]	 