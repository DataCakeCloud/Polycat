# FDB性能测试配置

## 影响FDB性能的几个基本规则

1. 多并发写入，可以提升性能。

   采用单核的配置，（KV 大小：1 million rows with 16-byte keys and random 8-100 byte values ）。

   | 测试方法                          | 性能结果       |
   | --------------------------------- | -------------- |
   | 单并发，每次写入1个key            | 917/sec        |
   | 单并发，每次写入10个key           | 8 650/sec      |
   | 100并发，每次写入10个key          | 46 000/sec     |
   | 100并发读，每次读10个key          | 305 000/sec    |
   | 100并发，每次读9个key，写入1个key | 107 000/sec    |
   | 100并发，每次range 读1000个key    | 3 6000 000/sec |

   12个server集群，每个server有4核，以及1块SATA SSD。（100,000,000 key-value pairs，2副本）

   | 测试方法                           | 性能结果      |
   | ---------------------------------- | ------------- |
   | 3200并发，每次写入10个key          | 720 000/sec   |
   | 3200并发，每次读取10个key          | 5 540 000/sec |
   | 3200并发，每次读9个key，写入1个key | 2 390 000/sec |

   

   原因：

   （1）FDB会调用fsync将写入数据刷盘后才能返回事务成功，所以事务单并发，会串行等待数据刷盘。单并发，很难给够磁盘压力，cpu表现是很空闲的，5%作用的cpu占用。

   （2）FDB的存储采用有序的存储，顺序读取，可以进一步获取性能提升。

   数据来源：

   https://apple.github.io/foundationdb/benchmarking.html?highlight=perf

2. 多个client 并发测试

   FDB client端只有一个线程在后台处理FDB网络流量，这很容易因为持续的基准测试工作负载达到饱和。  

   运行多个客户端进程是更好的选择。  

3. 设置进程的分类，通常可以指定class=storage的进程来作为执行存储服务器，class=transaction用于日志服务器进程，class=stateless用于proxy、resolver等无状态进程。

   事务类型的进程最好和存储服务的进程使用不同的SSD 盘，因为在同一个磁盘上运行事务日志和存储节点，这样会导致顺序事务日志工作负载对磁盘来说不再是顺序的；它将与其他存储服务进程一起竞争sync。这两个角色有非常不同的磁盘使用模式，日志执行频繁的小fsync，而存储服务器执行大量读取和较不频繁的fsync。 当两者都使用同一个磁盘时，您可能会看到由于两者的干扰而增加的延迟。  

   proxy负责读写事务的处理，如果proxy数量很少（假设只有1个proxy），某个很热的数据的读取，会消耗过多的proxy资源，导致其他事务受到影响。
   
   存储服务和其他服务在一个进程，这意味着您的一些存储服务器将把它们的一些CPU资源用于其他任务，如果CPU成为瓶颈，导致存储服务时间减少。
   
    class=transaction((log server)进程的建议最小数量为8(活动)+ 2(备用)，class=stateless 无状态进程的建议最小数量为4(proxy)+ 1(resolver)+ 1(cluster controller)+ 1(master)+ 2(备用)。 最好将事务和无状态进程分散到尽可能多的机器上。

## 其他性能配置项

   1. FoundationDB 推荐使用 ext4 filesystem.  mount 需要设置以下几个参数`defaults,noatime,discard`.

      （1）noatime选项在读取文件时禁用更新访问时间，这是FoundationDB不需要的特性，它增加了磁盘上的写活动。 

      （2）discard选项，选择支持TRIM，允许操作系统有效地将擦除的块通知SSD，保持高写速度和增加驱动器生命周期。  

2. 出于性能原因，Linux ssd上的分区必须与磁盘的EBS (Erase Block Size)对齐。 EBS的值是特定于供应商的，但1024kib的值大于或等于任何当前EBS的倍数，因此可以安全地用于任何SSD。 (默认为1024 KiB是Windows 7和最近的Linux发行版保证SSD高效运行的方式。) 要验证分区的开始与驱动器的EBS对齐，请使用fdisk实用程序。

## 性能配置

1. 根据可靠性要求和成本，计算服务器，SSD数量和容量。
2. 配置进程的class类型，log server 和 storage server使用不同的SSD。并且都是独立的进程。
3. 剩余的core配配置为stateless 类型的进程，并按照比例设置不同类型进程的数量。



## 性能配置操作

已经配置fdb集群的环境执行这个操作，否则按照fdb安装配置指导进行安装，**以下操作在所有集群节点执行相同的操作**：

1. 停止fdb的服务，service foundationdb stop

2. 选择本地盘或者EVS SSD盘，格式化 ext4文件系统。例如:/dev/vdb, mkfs.ext4 /dev/vdb

3. mount ssd盘到某个目录下，例如mount /dev/vdb /home/data1/  mount 需要设置以下几个参数 defaults,noatime,discard

4. 修改/home/data1的用户组和拥有者为foundationdb。chown foundationdb /home/data1; chgrp foundationdb /home/data1

   如果在fdb写入目录是/home/data1 的子目录，也需要修改子目录的用户组和拥有者。

   例如：创建子目录fdb，配置fdb的写入数据目录在 /home/data1/fdb。

   mkdir  /home/data1/fdb 

   chown foundationdb /home/data1/fdb; chgrp foundationdb /home/data1/fdb

5. 修改 /etc/foundationdb/foundationdb.conf 文件：

   （1）datadir =  /home/data1/fdb/$ID

     (2)   配置进程数量和类型（单盘写入的目录相同，这里可以不用设置目录了，都是使用公共配置的datadir）

   以下是示例，实际配置数量，以实际环境为准。

   [fdbserver.4500]

   class=stateless

   [fdbserver.4501]

   class=stateless

   [fdbserver.4502]

   class=stateless

   [fdbserver.4503]

   class=transaction

   [fdbserver.4504]

   class=transaction

   [fdbserver.4505]

   class=storage

   [fdbserver.4506]

   class=storage

**选择集群内的一个节点执行**：重新执行一次集群的配置，先还原到单节点模式，成功拉起后，再加入其他节点。

例如选择：192.168.0.145节点，先恢复单节点模式。

1. 删除/etc/foundationdb/fdb.cluster文件中其他节点的coordinate信息。只保留192.168.0.145 的IP和端口信息。

2. 启动fdb 服务， service foundationdb start

3. 进入fdbcli ，这时status看状态是异常的，重新创建database信息。

   fdb> configure new signle ssd

   查询status状态，显示正常。

4. 执行安装配置操作指导里面其他步骤，同步/etc/foundationdb/fdb.cluster 文件到其他节点。启动fdb的服务。配置fdb为3副本模式。

   添加其他的coordinate节点。

5. 修改stateless，log进程的角色数量：

   fdb> configure proxies=3

   fdb> configure logs=2 (对应系统里面 transaction进程的数量)

   fdb> configure resolvers=2

## fdbserver性能测试工具

fdbserver是fdb自带的banchmark性能测试工具，通过修改  /etc/foundationdb/foundationdb.conf 添加一个进程类型是test的进程，这些test进程将作为运行基准测试工作负载的client端。 

在运行大型的FoundationDB集群之前，建议运行基准测试，同时更改proxy、resolvers和logs进程的数量，以大致了解应该运行的比例。 越多并不总是越好，特别是对于proxy和resolvers。

```shell
fdbserver -r multitest -f /home/RandomReadWrite.txt --num_testers 1
```

通过执行这个命令，就可以启动测试，需要注意的是，这个测试工具会产生额外的测试数据写入db，但是在测试结束后不会清理这些测试数据，所以不要再生产环境运行这个测试工具。这个工具只是在测试环境进行模拟测试。

RandomReadWrite.txt 文件就是可以编辑的测试负载的描述文件。

```shell
; This file is used by the Atlas Load Generator
    testTitle=RandomReadWriteTest
    testName=ReadWrite
    testDuration=600.0
    transactionsPerSecond=10000
    writesPerTransactionA=0
    readsPerTransactionA=10
    writesPerTransactionB=10
    readsPerTransactionB=1
    ; Fraction of transactions that will be of type B
    alpha=0.1
    ; Produces 12TB
    nodeCount=20000000000
    valueBytes=1000
    ; average 600
    minValueBytes=200
    discardEdgeMeasurements=false
    warmingDelay=20.0
    timeout=300000.0
    databasePingDelay=300000.0
```

1.  testTitle : 测试文件名称

2.  testName ： 测试内容名称

3. testDuration： 测试执行时间（秒）

4. transactionsPerSecond: 每秒钟产生多少个读写事务

5. writesPerTransactionA: 事务A，每次事务产生多少个写请求

6. readsPerTransactionA: 事务A，每次事务产生多少个读请求

7. writesPerTransactionB: 事务B,   每次事务产生多少个写请求

8. readsPerTransactionB: 事务B，每次事务产生多少个读请求

9.    alpha=0.1： A事务占的比重

10.   nodeCount：节点数量

11.  valueBytes： value值的大小

12. minValueBytes：最小value值大小

13. discardEdgeMeasurements：丢弃一些毛刺数据，极大或者极小值

14. warmingDelay：预热时间

15. timeout：超时时间

16. databasePingDelay：网络ping延迟

测试运行结束后会出一个详细的统计结果清单：

```shell
root@LAPTOP-C9QCDJ8O:~# fdbserver -r multitest -f /home/RandomReadWrite.txt --num_testers 1
startingConfiguration: start
Run test:RandomReadWriteTest start
setting up test (RandomReadWriteTest)...
running test (RandomReadWriteTest)...
RandomReadWriteTest complete
checking test (RandomReadWriteTest)...
fetching metrics (RandomReadWriteTest)...
Metric (0, 0): Measured Duration, 600.000000, 600
Metric (0, 1): Transactions/sec, 2162.323333, 2.16e+03
Metric (0, 2): Operations/sec, 21835.796667, 2.18e+04
Metric (0, 3): A Transactions, 1169856.000000, 1169856
Metric (0, 4): B Transactions, 127538.000000, 127538
Metric (0, 5): Retries, 3612762.000000, 3612762
Metric (0, 6): Mean load time (seconds), 0.149393, 0.149
Metric (0, 7): Read rows, 11826098.000000, 1.18e+07
Metric (0, 8): Write rows, 1275380.000000, 1.28e+06
Metric (0, 9): Mean Latency (ms), 1069.958857, 1.07e+03
Metric (0, 10): Median Latency (ms, averaged), 1.538992, 1.54
Metric (0, 11): 90% Latency (ms, averaged), 30.286312, 30.3
Metric (0, 12): 98% Latency (ms, averaged), 18270.177841, 1.83e+04
Metric (0, 13): Max Latency (ms, averaged), 144614.749193, 1.45e+05
Metric (0, 14): Mean Row Read Latency (ms), 0.751362, 0.751
Metric (0, 15): Median Row Read Latency (ms, averaged), 0.673771, 0.674
Metric (0, 16): Max Row Read Latency (ms, averaged), 99.530697, 99.5
Metric (0, 17): Mean Total Read Latency (ms), 0.701582, 0.702
Metric (0, 18): Median Total Read Latency (ms, averaged), 0.618458, 0.618
Metric (0, 19): Max Total Latency (ms, averaged), 99.530697, 99.5
Metric (0, 20): Mean GRV Latency (ms), 1.112681, 1.11
Metric (0, 21): Median GRV Latency (ms, averaged), 0.756025, 0.756
Metric (0, 22): Max GRV Latency (ms, averaged), 148.500919, 149
Metric (0, 23): Mean Commit Latency (ms), 3.794311, 3.79
Metric (0, 24): Median Commit Latency (ms, averaged), 3.671408, 3.67
Metric (0, 25): Max Commit Latency (ms, averaged), 311.218500, 311
Metric (0, 26): Read rows/sec, 19710.163333, 1.97e+04
Metric (0, 27): Write rows/sec, 2125.633333, 2.13e+03
Metric (0, 28): Bytes read/sec, 12141460.613333, 1.21e+07
Metric (0, 29): Bytes written/sec, 1309390.133333, 1.31e+06
1 test clients passed; 0 test clients failed
Run test:RandomReadWriteTest Done.

1 tests passed; 0 tests failed.
Waiting for DD to end...

```

