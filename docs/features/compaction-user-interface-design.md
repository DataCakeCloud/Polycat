# Compaction的用户使用方式

## 1. 设置Compaction和清理策略

```sql
ALTER TABLE target TBLPROPERTY ('compaction_threshold' = '1GB');

ALTER TABLE target TBLPROPERTY ('block_size' = '128MB');

ALTER TABLE target TBLPROPERTY ('sort_columns' = 'c1,c2');

ALTER TABLE target TBLPROPERTY ('auto_gc' = '1day');

ALTER TABLE target TBLPROPERTY ('auto_retention' = '90day');

ALTER TABLE target TBLPROPERTY ('version_keep_policy' = 'TIME_PERIOD');

ALTER TABLE target TBLPROPERTY ('version_keep_period' = '1hour');
```

策略说明：

| 参数名               | 参数说明                                                     | 缺省取值           |
| -------------------- | ------------------------------------------------------------ | ------------------ |
| compaction_threshold | 被合并文件的判断上限，小于这个值的文件才会被合并，大于的不参与合并                 | 1GB                |
| block_size           | 写入文件的数据块的大小，数据入库和写入（不止是合并）都遵循这个块大小                                     | 120MB              |
| sort_columns         | 表数据排序的列，按照顺序自左到右进行排序判断；合并和入库建索引都按照这些列进行排序       | 无，说明不进行排序 |
| auto_gc              | 自动进行无效数据清理的时间间隔                               | 无，说明不进行清理 |
| auto_retention       | 自动保留数据版本的时间，超过该时间的版本数据会被清理，无法进行按时间回退 | 180day             |


 **注：上述参数中，blocksize和sort_columns这两个参数是系统参数，Compaction的时候也会使用。**

## 2. 执行Compaction

1）短期：提供命令用户手动触发

```sql
MAINTAIN TABLE target COMPACT;
```

2）未来：系统自动触发，支持自动检测和触发的时机，并支持并发的合并。

## 3. 查看冗余数据

```sql
PURGE TABLE target BEFORE version/time DRYRUN;
```

输出：返回一个列表

- 最新的完整数据的版本是哪个版本
- 历史版本里每个版本有多少冗余数据，MB/GB

## 4. 清理冗余数据

```sql
PURGE TABLE target BEFORE version/time;

例如：
PURGE TABLE target BEFORE '2021-09-03';
```

一旦Purge后的版本(Version History)，会被标记为Purged。Purged的版本不能做Restore和TimeTravel。

## 5. 例子

Compaction前：

```bash
    v0(s0) -> v1(s0,s1) -> v2(s0,s1,s2) -> v3(s0,s1,s2,s3)

    s0是2GB, s1是10MB，s2是2GB，s3是50MB
```

假设同时有一个并发入库在进行s4

compaction后：

```bash
    v0(s0) -> v1(s0,s1) -> v2(s0,s1,s2) -> v3(s0,s1,s2,s3) -> v4(s0,s1,s2,s3,s4 = s0,s2,s4,s5)
       \ --------------------------------> v5(s0,s2,s5)-----------/

    s5=s1+s3
```

自动按照version_keep_period周期去Disable版本（假设V1，V2都被标记为Disabled）。
```bash
    v0(s0) ----------------------->  v5(s0,s2,s5) ----> v4(s0,s2,s4,s5)

    (v1,v2,v3标记状态为disabled)
```


purge动作扫描无版本引用的segment，然后删除对应的文件及segment。

purge之后:

```bash
    (s1,s3对应的文件被删除)
```

此时，如果Restore到v2，则报错，因为是v2被标记为Purged Version

## 6. Compaction说明

Compaction类型：

- segment内部合并 （segment大小>1GB，但内部有很多小文件）
- segment间合并: 小于1GB的segment都要和其他segment合并，直到大于1GB

Schema变更对Compaction的影响：

1）短期先不支持跨Schema变更点的Compaction

2）长期支持跨Schema变更点的Compaction

## 7. 对其他模块的需求

1）需要LMS新API: 
- listSegment：列出某Table当前最新版本的Segment列表
- commitCompactionSegment：要标记老Segment为冗余Segment，并记录新的Segment为新版本

2）对LMS新需求：拉分支时不要拷贝Segment，只做父分支Segment列表的引用
