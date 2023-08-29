# FDB状态监控

## 集群配置

https://apple.github.io/foundationdb/administration.html

https://apple.github.io/foundationdb/configuration.html

https://apple.github.io/foundationdb/building-cluster.html

2个配置文件的功能：

fdb.cluster 包含的是coordination servers 的服务器IP和端口，负责接入集群。

foundationdb.conf 负责单个节点的fdb配置，进程数量，存储位置，副本配置，进程类型等。 （影响性能和可靠性）

### 集群配置文件

FoundationDB服务器和客户端使用集群文件(通常命名为db.cluster)连接到集群。通常，我们将复制fdb.cluster，从FoundationDB服务器上的默认位置到每个客户机上的默认位置的集群文件。  

```shell
Linux: `/etc/foundationdb/fdb.cluster`
```



**连接多个集群的场景：**

所有FoundationDB组件都可以配置指定的集群文件:  

1. fdbcli工具允许使用-C选项在命令行上传递集群文件。  
2. 客户端api允许在连接到集群时传递集群文件，通常通过openFDB -- > fdb.open(clusterFile)可以传入集群的配置文件 。  
3. FoundationDB服务器或备份代理允许在FoundationDB .conf中指定集群文件。  

此外，FoundationDB允许您使用环境变量FDB_CLUSTER_FILE来指定一个集群文件。 如果您操作或访问多个集群，这种方法很有帮助。  

集群文件包含一组IP地址的列表和标识符，用于指定协调服务器（coordination servers）。

```shell
description:ID@IP:PORT,IP:PORT,...
```

### 选择 coordination servers

任何FoundationDB进程都可以用作一组集群的协调器。 作为协调者对性能的影响是微不足道的。 协调器完全不参与提交事务。 

coordinators的介绍资料：

https://apple.github.io/foundationdb/kv-architecture.html?highlight=coordinators

管理员Administrators，应该选择coordinators的数量和物理位置，以最大限度地提高容错能力。 大多数配置应该遵循以下准则:  

1.  选择奇数个coordinators。  

2. 使用足够的coordinators来补充集群的冗余模式，通常是3或5个。  

3. 将coordinators放置在不同的机架、电源或数据中心，且不受故障影响。  

4. 将coordinators放置在距离较远远的数据中心是可以的; 在正常操作中，coordinators的延迟时间不影响系统延迟时间。

   **一句话总结：coordinators 选择要注重容错，考虑高可靠性。**

### 修改coordination servers

有时需要更改coordinators服务器集。 您可能因为网络条件变化、机器故障或只是计划调整需要变更coordinators 服务器。 可以使用自动的fdbcli命令更改协调器。 在更改期间，FoundationDB将维护ACID保证。  

满足以下这些条件可以进行coordinators 服务器变更。

1. 目前的大多数coordinators都是可用的

2. 所有新的coordinators都是可用的

3. 客户端和服务器集群文件及其父目录是可写的

使用cli命令行进行coordinators 服务器变更

```shell
user@host$ fdbcli
Using cluster file `/etc/foundationdb/fdb.cluster'.

The database is available.

Welcome to the fdbcli. For help, type `help'.
fdb> coordinators 10.0.4.1:4500 10.0.4.2:4500 10.0.4.3:4500
Coordinators changed
```

```
fdb> status details
Coordination servers:
  10.0.4.1:4500
  10.0.4.2:4500
  10.0.4.3:4500
```

coordinators命令还支持一个方便的选项，coordinators auto，它自动选择一组适合于冗余模式的协调服务器:  如果当前的协调器都是可用的并且支持当前冗余级别，那么协调器自动将不会进行任何更改。

```
Welcome to the fdbcli. For help, type `help'.
fdb> coordinators auto
Coordinators changed
```

说明：变更期间可能出现服务器或者网络异常，处理策略：https://apple.github.io/foundationdb/configuration.html

### foundationdb.conf 配置文件

fdbmonitor守护进程在每个服务器上运行，负责fdbserver服务器进程的运行和监控。 Fdbmonitor和fdbserver本身由位于以下位置的foundationdb.conf文件控制:  

/etc/foundationdb/foundationdb.conf  

当foundationdb.conf文件发生变化时，fdbmonitor守护进程自动检测这些变化，并根据需要启动、停止或重新启动子进程。 注意，对配置文件内容的更改必须是atomically。 建议将修改后的文件保存到一个临时文件名中，然后将其 move/rename 到合适的位置，取代原来的文件。 有些文本编辑器在保存时自动执行此操作。  

```Shell
## foundationdb.conf
##
## Configuration file for FoundationDB server processes
## Full documentation is available at
## https://apple.github.io/foundationdb/configuration.html#the-configuration-file

[fdbmonitor]
user = foundationdb
group = foundationdb

[general]
restart_delay = 60
## by default, restart_backoff = restart_delay_reset_interval = restart_delay
# initial_restart_delay = 0
# restart_backoff = 60
# restart_delay_reset_interval = 60
cluster_file = /etc/foundationdb/fdb.cluster
# delete_envvars =
# kill_on_configuration_change = true

## Default parameters for individual fdbserver processes
[fdbserver]
command = /usr/sbin/fdbserver
public_address = auto:$ID
listen_address = public
datadir = /var/lib/foundationdb/data/$ID
logdir = /var/log/foundationdb
# logsize = 10MiB
# maxlogssize = 100MiB
# machine_id =
# datacenter_id =
# class =
# memory = 8GiB
# storage_memory = 1GiB
# cache_memory = 2GiB
# metrics_cluster =
# metrics_prefix =

## An individual fdbserver process with id 4500
## Parameters set here override defaults from the [fdbserver] section
[fdbserver.4500]

[backup_agent]
command = /usr/lib/foundationdb/backup_agent/backup_agent
logdir = /var/log/foundationdb

[backup_agent.1]

```

1. [fdbmonitor]

   包含fdbmonitor进程的基本配置参数。 user和group在Linux系统中用于控制子进程的权限级别。
   
2. [general]  

   包含适用于所有进程的设置(例如fdbserver, backup_agent)。 

   `cluster_file`: cluster_file:指定集群文件的位置。 这个文件和包含它的目录必须能被所有进程写入(即由[fdbmonitor]部分中的用户或组设置)。

   `delete_envvars`: 要从子进程的环境中删除的环境变量列表（以空格分隔）。 如果fdbmonitor进程需要运行其子进程中不需要的环境变量，则可以使用此命令。  

   `kill_on_configuration_change`: 如果为true，则当配置文件更改时，受影响的进程将重新启动。 默认值为true。 

   `disable_lifecycle_logging`: 如果为true, fdbmonitor将**不会**在进程启动或终止时写入日志事件。 默认值为false。

   这个部分还包含一些参数来控制进程在死亡时如何重新启动。 Fdb monitor使用backoff logic来防止重复死亡的进程循环过快，它还在延迟中引入高达±10%的随机抖动，以避免多个进程同时重启。 Fdb monitor跟踪每个进程的独立退避状态，因此一个进程的重新启动不会影响另一个进程的退避行为。  

   `restart_delay`: fdbmonitor在重新启动失败进程之前将延迟的最大秒数(取决于抖动)。

   `initial_restart_delay`: 进程第一次死亡时，fdbmonitor等待重新启动进程的秒数。 默认为0(即进程立即重新启动)。

   `restart_backoff`: 控制进程重复死亡时fdbmonitor退出的速度。 前一个延迟(或者1，如果前一个延迟是0)乘以restart_backoff得到下一个延迟，在restart_delay的值处达到最大值。 默认值为restart_delay，这意味着第二次和随后的失败都将延迟重启之间的restart_delay。  

   `restart_delay_reset_interval`:将回退设置为initial_restart_delay的值之前，进程必须运行的秒数。 默认值为restart_delay。  

   例子：

   ```shell
   restart_delay = 60
   initial_restart_delay = 0
   restart_backoff = 2.0
   restart_delay_reset_interval = 180
   ```

   一个重复死亡的进程的延迟将是0,2,4,8,16,32,60,60，… ，每个延迟都会有10%的随机抖动。 在进程存活180秒后，backoff将重置，下一次失败将立即重新启动进程。  使用默认参数，如果失败，进程将立即重启，如果在restart_delay秒内再次失败，进程将延迟restart_delay秒。  

3. [fdbserver]

   包含此机器上所有fdbserver进程的默认参数。 这些相同的选项可以在各自的[fdbserver.<ID>]中覆盖各个进程。在本节中，可以使用值中的$ID变量替换单个fdbserver的ID。 例如，public_address = auto:$ID使每个fdbserver监听与其ID相等的端口。 
   
   `command`:   fdbserver二进制文件的位置。
   
   **`public_address`: 公开可见的IP:进程的端口。 如果是auto，则该地址将是用于与coordination server通信的地址。**（管理面的通信地址）
   
   **`listen_address`: server socket 应该绑定到的IP:端口。 如果是public，则与public_address相同。**（数据面的通信地址）
   
   **`datadir`: 一个可写目录(root用户或[fdbmonitor]中设置的用户)，用于存储持久数据文件。**
   
   `logdir`: 一个可写目录(root用户或[fdbmonitor]节中设置的用户)，FoundationDB将在这里存储日志文件。  

​       `logsize`: 在当前日志文件达到指定大小后，切换到一个新的日志文件。 缺省值为10MiB。 

​       `maxlogssize`: 当所有日志文件的总大小超过指定大小时，删除最老的日志文件。 设置为0B时，不删除旧的日志文件。 缺省值为100MiB。 

​       **`class`: 进程类型，指定进程将在集群中扮演的角色。 选项有：存储、事务、无状态（storage`, `transaction`, `stateless）。**

​       `memory`:进程使用的最大内存。 默认值为8GiB。 如果没有指定单元，则假定MiB。 这个参数不会改变程序的内存分配。 **相反，它设置了一个硬限制，超过这   个限制，进程将自行终止并重新启动。** 默认值8GiB是默认配置中预期内存使用量的两倍(提供一个紧急缓冲区来处理内存泄漏或类似问题)。 **不建议将该参数的值降低到其默认值以下。**

 如果您希望分配大量的存储引擎内存或缓存，它可能会增加。 **特别是，当storage_memory或cache_memory参数增加时，memory参数应该增加相同的数量。**  

​       `storage_memory`: 数据存储使用的最大内存。 该参数仅适用于内存存储引擎，ssd存储引擎不适用。 缺省值为1GiB。 如果没有指定单位，则假定为MB。 集群将被限制为每个进程使用此数量的内存用于数据存储。 与存储数据相关的内存开销将计算在这个总数中。 如果您增加了storage_memory参数，您还应该增加相同数量的内存参数。

​     `cache_memory`: 用于从磁盘缓存页面的最大内存。 默认值为2GiB。 如果没有指定单元，则假定MiB。 如果你增加cache_memory参数，你也应该增加相同数量的memory参数。  

​    `locality_machineid`: Machine identifier key。 一台机器上的所有进程应该共享一个唯一的id。 默认情况下，机器上的进程决定共享的唯一id。 这通常不需要设置。  

​    `locality_dcid`: Datacenter identifier key。位于数据中心的所有进程都应该共享这个id。 没有默认值。 如果依赖于基于数据中心的复制，则必须在所有进程上设置此选项。  

​     *`locality_zoneid`:Zone identifier key.。 出于数据复制的目的，共享一个zone id的进程被认为是non-unique。 如果未设置，默认为机器id。（当前版本配置文件里面没有这个配置项，FDB主页的介绍文档里面有这个选项）*

​    *`locality_data_hall`:  Data hall identifier key。所有物理上位于data hall 的进程都应该共享id。 没有默认值。 如果您依赖于基于data hall 的复制，则必须在所有进程上设置此参数。（当前版本配置文件里面没有这个配置项，FDB主页的介绍文档里面有这个选项）*

​    *`io_trust_seconds`: 允许读或写操作在允许的超时时间。 如果某个操作超时，那么该文件上的future operations 都将失败。 只有在Linux中使用AsyncFileKAIO时才有效果。 如果未设置，默认为0，这意味着禁用超时。  （当前版本配置文件里面没有这个配置项，FDB主页的介绍文档里面有这个选项）*

4. `[fdbserver.<ID>]`

   这种类型的每个部分表示将运行的一个fdbserver进程。 id不能重复。 通常，管理员会选择为每个CPU内核运行一个fdbserver。 这个部分设置的参数仅适用于单个fdbserver进程，并覆盖[fdbserver]节中设置的默认值。 注意，在默认情况下，本节中指定的ID也用作网口和数据目录。  

```shell
## An individual fdbserver process with id 4500
## Parameters set here override defaults from the [fdbserver] section
[fdbserver.4500]
datadir=/ssd1/foundationdb/4500

[fdbserver.4501]
datadir=/ssd2/foundationdb/4501
```

### 数据冗余模式

这部分的冗余模式配置，都是针对单个集群的。多集群之间的冗余属于容灾，在下一个章节有介绍。

#### Single datacenter modes

|                                                    | single       | **double**   | **triple**  |
| -------------------------------------------------- | ------------ | ------------ | ----------- |
| Best for                                           | 1-2 machines | 3-4 machines | 5+ machines |
| Total Replicas                                     | 1 copy       | 2 copies     | 3 copies    |
| Live machines required to make progress            | 1            | 2            | 3           |
| Required machines for fault tolerance              | impossible   | 3            | 4           |
| Ideal number of coordination servers               | 1            | 3            | 5           |
| Simultaneous failures after which data may be lost | any process  | 2+ machines  | 3+ machines |

FoundationDB不会在同一台机器上复制任何单个数据。 由于这个原因，在上面的汇总表中引用的“机器”数量是物理机器的数量，而不是fdbserver进程的数量。  

***

Three data hall mode（机房）

FoundationDB将数据存储为三份，每三个data hall（机房）的存储服务器上都有一份。 事务日志被复制了四次，两个data hall各包含两个副本。 因此，采取这种模式，需要四台可用的机器(两个data hall各有两台)。 这种配置使集群在丢失单个data hall和另一个data hall中的一台机器后仍然可用。  

```
|<------------------ DC 1------------------------>|
|                 |             |                 | 
|                 |             |                 |
|                 |             |                 |
|   LS            |   LS        |                 | 
|                 |             |                 |  
|   SS            |   SS        |      SS         | 
|                 |             |                 |
|  data hall 1    | data hall 2 |  data hall 3    |
```

#### Datacenter-aware mode

这个版本的FoundationDB还支持跨多个数据中心的冗余。  当使用数据中心感知模式时，应该在命令行上向所有fdbserver进程传递一个有效的数据中心标识符。

three_datacenter mode is **not compatible** with region configuration.

`three_datacenter` mode *(for 5+ machines in 3 datacenters)*

FoundationDB尝试跨三个数据中心复制数据，每个数据中心有2个副本， 数据复制6次。 事务日志存储在与three_data_hall模式相同的配置中，因此提交延迟与数据中心之间的延迟绑定。 为了获得最大可用性，您应该使用5个coordination servers:两个在两个数据中心中，一个在第三个数据中心中。

在这种模式下，提交延迟将包括数据中心之间的往返延迟。 如果放弃因果一致性，只读事务仍然可以非常快。

####  Two regions mode

Regions configuration 支持在两个数据中心之间自动故障转移，而**不增加WAN提交延迟，同时仍然保持FoundationDB提供的所有一致性属性**。

这是通过结合两个特点而实现的。 第一种是two regions之间的异步复制。 在返回提交成功之前，不等待远程区域的提交持久化成功，这意味着 remote region将略微落后于主区域。 这与fdbdr（fdb disaster recovery）类似，不同之处在于异步复制是在单个集群中完成的，而不是在不同的FoundationDB集群之间。  第二个特性是能够在不同的数据中心中添加一个或多个变化日志的同步副本。 由于该数据中心只持有提交到数据库变化的瞬态副本，因此只需要少数几个FoundationDB进程就可以完成这个角色。 如果主数据中心失败，外部变化日志副本仍然允许访问最近的提交。 这允许滞后的远程副本赶上主副本。 一旦远程副本应用了所有的更改，它就可以开始接受新的提交，而不会造成任何数据丢失。

Region故障 + Region内多个AZ同时故障的概率并不高。

```
|<----------- Primary Region 1 ---------->|<--------------- Standby Region 2 ---------->|
|          |            |                 | LogRouters    |            |                |  
|   TS     |            |                 |               |            |                |
|          |            |                 |               |            |                |
|   LS     |   LS       |      LS         |    LS         |            |                |
|          |            |                 |               |            |                |
|   SS     |            |                 |    SS         |            |                |
|          |            |                 |               |            |                |
|  DC1     | Satellite1 |    Satellite2   |    DC2        | Satellite3 |  Satellite4    |
```

这两个区域都有一个数据中心(DC)  以及 一个或者多个satellite sites。satellite sites和DC(在同一区域)，但有独立的故障域。 satellite sites只需要存储日志副本(即重做日志)，  DC需要包含，LS、SS和TS。 

读操作在主数据中心和从数据中心进行处理（primary and secondary），如果需要一致性读，需要从主数据中获取读版本。 

所有客户端写都转发到primary region和由DC1中的代理处理，然后同步地持久化到DC1中的logserver和主服务器中的一个或两个satellite sites区域(视配置而定)，避免跨区域WAN延迟。 然后将更新异步复制到DC2，它们被存储在多个LS服务器上，最终分散到多个storageserver。 LogRouters实现一个特殊类型的FDB角色，方便跨区域的数据传输。  它们是为了避免相同的信息跨地区的重复传输。 LogRouters传输每个log entry只会传输一次，将其传输到在DC2本地所有相关的LS服务器。 

Region1将保留所有已提交变化日志的副本，直到它们被region2数据中心应用。 如果region2已经失败了足够长的时间，region1的日志不再有足够的磁盘空间来继续存储，可以请求FoundationDB完全删除region2的副本。 这个操作不是自动的，需要对配置进行手动更改。 region1数据库将作为一个单一的数据中心数据库，直到region2恢复联机。 因为region2数据中心已经完全从配置中删除，所以FoundationDB必须复制区域之间的所有数据才能使其恢复在线。  

```json
fdb> fileconfigure regions.json
Configuration changed.

"regions":[{
    "datacenters":[{
        "id":"WC1",
        "priority":1,
        "satellite":1,
        "satellite_logs":2
    }],
    "satellite_redundancy_mode":"one_satellite_double",
    "satellite_logs":2
}]
```

json文档中的regions对象可以是一个数组。 数组的每个元素描述了单个region的配置。每个region使用一个包含数据中心数组的对象进行描述。 每个region还可以选择性地提供satellite_redundancy_mode 和 satellite_logs。每个数据中心都用一个对象来描述，该对象包含该数据中心的id和priority。 id可以是任何唯一的字母数字字符串。 拥有完整数据副本的数据中心称为主数据中心 primary datacenters。 仅存储事务日志的数据中心称为satellite datacenters。 要指定一个数据中心是一个satellite，它需要包括"satellite": 1。 同一区域内的卫星数据中心可以设置不同的优先级，不同的优先级在副本写入故障接管时有对应的处理策略，卫星数据中心的优先级仅与同一区域内的其他卫星数据中心进行比较。 主数据中心的优先级只与其他主数据中心进行比较。  

每个区域都配置了satellite_redundancy_mode，并指定每个变化需要复制到satellite datacenters。

one_satellite_single模式  

在卫星数据中心保留一份变化记录的副本，优先级最高。 如果最高优先级的卫星不可用，它将把事务日志放在次高优先级的卫星数据中心。  

one_satellite_double模式  

在卫星数据中心保留两份变化记录的副本，优先级最高。  

one_satellite_triple模式  

在卫星数据中心保留三份变化记录的副本，优先级最高。  

 two_satellite_safe模式  

 在两个优先级最高的卫星数据中心各保留两份变化日志，每个变化总共有四份副本。 这种模式可以防止主数据中心和其中一个卫星数据中心同时丢失。 如果只有一颗卫星可用，它将退回到只在剩余的数据中心存储两份变化日志副本的状态。  

two_satellite_fast模式  

 在两个优先级最高的卫星数据中心各保留两份变化日志，每个变化总共有四份副本。 FoundationDB只会同步等待两个卫星数据中心中的一个变化日志持久，然后才会考虑提交成功。 这将减少数据中心之间的网络问题引起的尾部延迟。 如果只有一颗卫星可用，它将退回到只在剩余的数据中心存储两份变化日志副本的状态。

每个区域还配置了satellite_logs的数量。 它表示应在卫星数据中心中事务日志的期望数量。 卫星事务日志所做的工作比主数据中心事务日志略少。 因此，虽然在主数据中心和卫星数据中心中日志与副本的比率应该保持大致相等，但稍微减少卫星事务日志的数量可能是性能的最佳平衡。

### 存储配置

（容量不能全部使用完，需要预留一部分容量做节点故障场景的数据重构，这部分容量是怎么预留是否需要我们自己预留？）*

为了提高性能和容量，需要将单个fdbserver进程的datadir配置为指向不同的磁盘，如下所示:  

```shell
[fdbserver.4500]
datadir=/ssd1/foundationdb/4500

[fdbserver.4501]
datadir=/ssd2/foundationdb/4501
```

**ssd不同于hdd，它们在磨损一定程度后会失效。 FoundationDB将以大致相同的速率使用集群中的所有ssd。 因此，如果一个集群在全新的ssd上启动，所有ssd都将由于磨损而几乎同时失效。 为了防止这种情况的发生，需要轮转地将新驱动器旋转到集群中，并监视其S.M.A.R.T.属性以检查磨损程度**。



### 容灾和备份 Backup vs DR

https://apple.github.io/foundationdb/backups.html?highlight=redundancy%20mode

FoundationDB可以将数据库备份到本地磁盘、对象存储(如Amazon S3)或另一个FoundationDB数据库。

将一个数据库备份到另一个数据库是一种特殊的备份形式，称为DR备份或简称DR。 DR代表Disaster Recovery，因为可以使用它使两个地理上分离的数据库保持密切同步，以从灾难性灾难中恢复。  

**备份数据在磁盘或blob存储帐户上不加密。**  

有5个用于备份和容灾操作的命令行工具:  

**fdbbackup**  

该命令行工具用于控制(不执行)备份任务和管理备份数据。 它可以启动、修改或中止备份，停止连续备份，获取正在进行的备份的状态，或等待备份完成。 它还可以描述、删除、过期备份中的数据，或在目标文件夹URL中列出备份。  

**fdbrestore**  

此命令行工具用于控制(而不是执行)恢复作业。 它可以启动或中止恢复，获取当前和最近的恢复任务的状态，或者在打印正在进行的进度详细信息的同时等待恢复任务完成。  

**backup_agent**  

备份代理是实际执行备份和恢复作业工作的守护进程。 指向同一个数据库的任意数量的备份代理都将合作执行备份和恢复。 所有backup_agent进程都必须可以访问为备份或恢复指定的备份URL。  

**fdbdr**  

这个命令行工具用于控制(而不是执行)灾备任务——从一个数据库备份到另一个数据库。 它可以启动、终止容灾作业或切换容灾方向。 它还可以获取正在运行的容灾作业的状态。  

**dr_agent**  

数据库备份代理是一个守护进程，它实际执行DR作业的工作，将快照和日志数据写入目标数据库。 指向相同数据库的任何数量的代理都将合作执行备份。  

默认情况下，FoundationDB包被配置为在每个FoundationDB服务器上启动单个backup_agent进程。 如果您想要对每个服务器都可以访问的网络驱动器或blob存储实例执行备份，那么您可以从任何可以访问集群的机器上立即使用fdbbackup start命令来启动备份  

### Disk snapshot backup and Restore

https://apple.github.io/foundationdb/disk-snapshot-backup.html?highlight=redundancy%20mode

FoundationDB的磁盘快照备份工具通过对所有具有持久数据的磁盘存储进行 crash consistent snapshot ，在没有停机时间的情况下对FoundationDB数据库进行 crash consistent snapshot 时间点的备份。  

 此特性的先决条件是在运行FoundationDB的文件系统(或磁盘)上具有崩溃一致性快照支持。  

 磁盘快照备份工具对所有磁盘映像进行编排快照，确保它们可以恢复到一致的时间点。  

### 设置 process class

在FoundationDB集群中，每个fdbserver进程执行不同的任务。 每个流程被招募来执行基于其流程类的特定任务。 例如，具有class=storage的进程优先被招募来执行存储服务器任务，class=transaction用于日志服务器进程，class=stateless用于代理、解析器等无状态进程，  

 class=事务(日志服务器)进程的建议最小数量为8(活动)+ 2(备用)，class=无状态进程的建议最小数量为4(代理)+ 1(解析器)+ 1(集群控制器)+ 1(主)+ 2(备用)。 最好将事务和无状态进程分散到尽可能多的机器上。

FoundationDB使用的冲突解决算法是保守的:它保证不提交冲突事务，但它可能无法提交一些理论上可以提交的事务。 这种保守性的影响可能会随着解析器数量的增加而增加。

```shell
fdb> configure proxies=5
fdb> configure logs=8
```

### 性能配置

默认安装在每台机器上只运行一个FoundationDB服务器进程(只使用一个CPU核)。 大多数多机器配置的用户都希望通过每个核心运行一个FoundationDB服务器进程来最大化性能。 这是通过修改配置文件(位于/etc/foundationdb/foundationdb.conf)来实现的。[fdbserver.<ID>]部分为每个核心。

 注意，每个FoundationDB服务器进程都需要4GiB ECC RAM(参见系统需求)。  （内存资源和核数量有比例关系，每个进程需要4G的内存）

为了提高性能和容量，需要将单个fdbserver进程的datadir配置为指向不同的磁盘。

FoundationDB 推荐使用 ext4 filesystem. Ext4 filesystems 需要指定mount参数  `defaults,noatime,discard`. noatime选项在读取文件时禁用更新访问时间，这是FoundationDB不需要的特性，它增加了磁盘上的写活动。 丢弃选项支持TRIM，允许操作系统有效地将擦除的块通知SSD，保持高写速度和增加驱动器生命周期。  

出于性能原因，Linux ssd上的分区必须与磁盘的EBS (Erase Block Size)对齐。 EBS的值是特定于供应商的，但1024kib的值大于或等于任何当前EBS的倍数，因此可以安全地用于任何SSD。 (默认为1024 KiB是Windows 7和最近的Linux发行版保证SSD高效运行的方式。) 要验证分区的开始与驱动器的EBS对齐，请使用fdisk实用程序。



### 集群扩容

1. 在新机器上安装FoundationDB。  
2. 从集群中的服务器复制现有的集群文件到新机器，覆盖现有的fdb。 集群文件。  
3. 在新机器上重启FoundationDB，以便它使用新的集群文件:  



### 集群缩容

1. 确认缩容后冗余级别是否满足要求
2. 使用fdbcli中的exclude命令:

```shell
Welcome to the fdbcli. For help, type `help'.
fdb> exclude 1.2.3.4 1.2.3.5 1.2.3.6
Waiting for state to be removed from all excluded servers.  This may take a while.
It is now safe to remove these machines or processes from the cluster.
```

exclude可用于移除，机器(通过指定IP地址)或单个进程(通过指定IP:PORT)。  

## 集群状态监控

### Cli命令行方式

**使用fdbcli的status命令**，判断集群是否处于正常运行状态。  

```shell
user@host$ fdbcli
Using cluster file `/etc/foundationdb/fdb.cluster'.

The database is available.

Welcome to the fdbcli. For help, type `help'.
fdb> status

Configuration:
  Redundancy mode        - triple
  Storage engine         - ssd-2
  Coordinators           - 5
  Desired Proxies        - 5
  Desired Logs           - 8

Cluster:
  FoundationDB processes - 272
  Machines               - 16
  Memory availability    - 14.5 GB per process on machine with least available
  Retransmissions rate   - 20 Hz
  Fault Tolerance        - 2 machines
  Server time            - 03/19/18 08:51:52

Data:
  Replication health     - Healthy
  Moving data            - 0.000 GB
  Sum of key-value sizes - 3.298 TB
  Disk space used        - 15.243 TB

Operating space:
  Storage server         - 1656.2 GB free on most full server
  Log server             - 1794.7 GB free on most full server

Workload:
  Read rate              - 55990 Hz
  Write rate             - 14946 Hz
  Transactions started   - 6321 Hz
  Transactions committed - 1132 Hz
  Conflict rate          - 0 Hz

Backup and DR:
  Running backups        - 1
  Running DRs            - 1 as primary

Client time: 03/19/18 08:51:51
```

1. **Data: 数据状态**： Replication health 健康状态，Sum of key-value sizes，不包含冗余副本的数据容量， Disk space used包含冗余副本的数据容量
2. **Workload访问状态**：Read rate ，Write rate 读写，每秒次数统计，Transactions started，Transactions committed，Conflict rate事务状态的每秒次数统计

**使用status details** 显示每个进程的状态。

```shell
Welcome to the fdbcli. For help, type `help'.
fdb> status details

Configuration:
  Redundancy mode        - triple
  Storage engine         - ssd-2
  Coordinators           - 5

Cluster:
  FoundationDB processes - 85
  Machines               - 5
  Memory availability    - 7.4 GB per process on machine with least available
  Retransmissions rate   - 5 Hz
  Fault Tolerance        - 2 machines
  Server time            - 03/19/18 08:59:37

Data:
  Replication health     - Healthy
  Moving data            - 0.000 GB
  Sum of key-value sizes - 87.068 GB
  Disk space used        - 327.819 GB

Operating space:
  Storage server         - 888.2 GB free on most full server
  Log server             - 897.3 GB free on most full server

Workload:
  Read rate              - 117 Hz
  Write rate             - 0 Hz
  Transactions started   - 43 Hz
  Transactions committed - 1 Hz
  Conflict rate          - 0 Hz

Process performance details:
  10.0.4.1:4500     (  2% cpu;  2% machine; 0.010 Gbps;  0% disk IO; 3.2 GB / 7.4 GB RAM  )
  10.0.4.1:4501     (  1% cpu;  2% machine; 0.010 Gbps;  3% disk IO; 2.6 GB / 7.4 GB RAM  )
  10.0.4.1:4502     (  2% cpu;  2% machine; 0.010 Gbps;  0% disk IO; 2.6 GB / 7.4 GB RAM  )
  10.0.4.1:4503     (  0% cpu;  2% machine; 0.010 Gbps;  0% disk IO; 2.7 GB / 7.4 GB RAM  )
  10.0.4.1:4504     (  0% cpu;  2% machine; 0.010 Gbps;  0% disk IO; 2.6 GB / 7.4 GB RAM  )
  10.0.4.1:4505     (  2% cpu;  2% machine; 0.010 Gbps;  0% disk IO; 2.7 GB / 7.4 GB RAM  )
  10.0.4.1:4506     (  2% cpu;  2% machine; 0.010 Gbps;  0% disk IO; 2.7 GB / 7.4 GB RAM  )
  10.0.4.1:4507     (  2% cpu;  2% machine; 0.010 Gbps;  0% disk IO; 2.7 GB / 7.4 GB RAM  )
  10.0.4.1:4508     (  2% cpu;  2% machine; 0.010 Gbps;  1% disk IO; 2.6 GB / 7.4 GB RAM  )
  10.0.4.1:4509     (  2% cpu;  2% machine; 0.010 Gbps;  1% disk IO; 2.6 GB / 7.4 GB RAM  )
  10.0.4.1:4510     (  1% cpu;  2% machine; 0.010 Gbps;  1% disk IO; 2.7 GB / 7.4 GB RAM  )
  
Coordination servers:
  10.0.4.1:4500  (reachable)
  10.0.4.2:4500  (reachable)
  10.0.4.3:4500  (reachable)
  10.0.4.4:4500  (reachable)
  10.0.4.5:4500  (reachable)

Client time: 03/19/18 08:59:37
```

If a process has had more than 10 TCP segments retransmitted in the last 5 seconds, the warning message `REXMIT!` is displayed。

```shell
10.0.4.1:4500       ( 3% cpu;  2% machine; 0.004 Gbps;  0% disk; REXMIT! 2.5 GB / 4.1 GB RAM  )
```

### 输出json 文件方式

status命令可以使用json参数提供关于集群和数据库的统计信息的完整摘要。可以通过程序化的方式监控集群状态。

https://apple.github.io/foundationdb/mr-status.html

FoundationDB以机器可读的JSON形式提供状态信息(除了cli提供的可读形式)。

提供了三种方式访问JSON形式的文本。

1. 在fdbcli中，使用命令status json。 该命令将以JSON格式输出状态信息。 更多信息请参见status命令。

   https://apple.github.io/foundationdb/command-line-interface.html#cli-status

2.   通过shell执行status命令：use fdbcli by running `fdbcli --exec "status json"`

3. 从任何客户端，读取key ' \xFF\xFF/status/json '。 该key的value值是一个JSON对象，序列化为UTF-8编码的 byte string类型。 在Python中，给定一个开放的数据库' db '， JSON对象可以通过以下方式读取和反序列化:  

   ```
   import json
   status = json.loads(db['\xff\xff/status/json'])
   ```

**FDB软件版本升级可能导致Json格式的变更，读取程序需要自己考虑格式的变化。**

```json
"client":{
   "coordinators":{
      "coordinators":[
         {
            "reachable":true,
            "address":"127.0.0.1:4701"
         }
      ],
      "quorum_reachable":true
   },
   "database_status":{
      "available":true,
      "healthy":true
   },
   "messages":[
      {
         "name":{ // when not limiting
            "$enum":[
               "inconsistent_cluster_file",
               "unreachable_cluster_controller",
               "no_cluster_controller",
               "status_incomplete_client",
               "status_incomplete_coordinators",
               "status_incomplete_error",
               "status_incomplete_timeout",
               "status_incomplete_cluster",
               "quorum_not_reachable"
            ]
         },
         "description":"The cluster file is not up to date."
      }
   ],
   "timestamp":1415650089,
   "cluster_file":{
      "path":"/etc/foundationdb/fdb.cluster",
      "up_to_date":true
   }
}
```

| JSON Path                            | Name                               | Description                                                  |
| ------------------------------------ | ---------------------------------- | ------------------------------------------------------------ |
| client.messages                      | inconsistent_cluster_file          | Cluster file is not up to date. It contains the connection string ‘<value>’. The current connection string is ‘<value>’. This must mean that file permissions or other platform issues have prevented the file from being updated. To change coordinators without manual intervention, the cluster file and its containing folder must be writable by all servers and clients. If a majority of the coordinators referenced by the old connection string are lost, the database will stop working until the correct cluster file is distributed to all processes. |
| client.messages                      | no_cluster_controller              | Unable to locate a cluster controller within 2 seconds. Check that there are server processes running. |
| client.messages                      | quorum_not_reachable               | Unable to reach a quorum of coordinators.                    |
| client.messages                      | server_overloaded                  | Server is under too much load and cannot respond.            |
| client.messages                      | status_incomplete_client           | Could not retrieve client status information.                |
| client.messages                      | status_incomplete_cluster          | Could not retrieve cluster status information.               |
| client.messages                      | status_incomplete_coordinators     | Could not fetch coordinator info.                            |
| client.messages                      | status_incomplete_error            | Cluster encountered an error fetching status.                |
| client.messages                      | status_incomplete_timeout          | Timed out fetching cluster status.                           |
| client.messages                      | unreachable_cluster_controller     | No response received from the cluster controller.            |
| cluster.messages                     | client_issues                      | Some clients of this cluster have issues.                    |
| cluster.messages                     | commit_timeout                     | Unable to commit after __ seconds.                           |
| cluster.messages                     | read_timeout                       | Unable to read after __ seconds.                             |
| cluster.messages                     | status_incomplete                  | Unable to retrieve all status information.                   |
| cluster.messages                     | storage_servers_error              | Timed out trying to retrieve storage servers.                |
| cluster.messages                     | log_servers_error                  | Timed out trying to retrieve log servers.                    |
| cluster.messages                     | transaction_start_timeout          | Unable to start transaction after __ seconds.                |
| cluster.messages                     | unreachable_master_worker          | Unable to locate the master worker.                          |
| cluster.messages                     | unreachable_dataDistributor_worker | Unable to locate the data distributor worker.                |
| cluster.messages                     | unreachable_ratekeeper_worker      | Unable to locate the ratekeeper worker.                      |
| cluster.messages                     | unreachable_processes              | The cluster has some unreachable processes.                  |
| cluster.messages                     | unreadable_configuration           | Unable to read database configuration.                       |
| cluster.messages                     | layer_status_incomplete            | Some or all of the layers subdocument could not be read.     |
| cluster.messages                     | primary_dc_missing                 | Unable to determine primary datacenter.                      |
| cluster.messages                     | fetch_primary_dc_timeout           | Fetching primary DC timed out.                               |
| cluster.processes.<process>.messages | file_open_error                    | Unable to open ‘<file>’ (<os_error>).                        |
| cluster.processes.<process>.messages | incorrect_cluster_file_contents    | Cluster file contents do not match current cluster connection string. Verify cluster file is writable and has not been overwritten externally. |
| cluster.processes.<process>.messages | io_error                           | <error> occured in <subsystem>                               |
| cluster.processes.<process>.messages | platform_error                     | <error> occured in <subsystem>                               |

## 性能监控

服务侧的时延统计：Server-side latency band tracking

FoundationDB还提供了可选的功能来测量所有传入的 get read version (GRV), read, 和 commit 的延迟，并报告关于这些请求的一些基本细节。 **延迟是从服务器接收请求到响应请求的时间度量的，因此不包括客户机和服务器之间传输所花费的时间或客户机进程本身的延迟。**  

设置 `\xff\x02/latencyBandConfig` key to a string encoding the following JSON document:

```json
  "get_read_version" : {
    "bands" : [ 0.01, 0.1]
  },
  "read" : {
    "bands" : [ 0.01, 0.1],
    "max_key_selector_offset" : 1000,
    "max_read_bytes" : 1000000
  },
  "commit" : {
    "bands" : [ 0.01, 0.1],
    "max_commit_bytes" : 1000000
  }
```



- `bands` - a list of thresholds (**in seconds)** to be measured for the given request type (`get_read_version`, `read`, or `commit`)
- `max_key_selector_offset` - an integer specifying the maximum key selector offset a read request can have and still be counted
- `max_read_bytes` - an integer specifying the maximum size in bytes of a read response that will be counted
- `max_commit_bytes` - an integer specifying the maximum size in bytes of a commit request that will be counted

配置后通过 `status json` 输出统计的信息 `cluster.processes.<ID>.roles[N].*_latency_bands`:

```json
"grv_latency_bands" : {
  0.01: 10,
  0.1: 0,
  inf: 1,
  filtered: 0
},
"read_latency_bands" : {
  0.01: 12,
  0.1: 1,
  inf: 0,
  filtered: 0
},
"commit_latency_bands" : {
  0.01: 5,
  0.1: 5,
  inf: 2,
  filtered: 1
}
```

For example, `0.1: 1` in `read_latency_bands` indicates that there has been 1 read request with a latency in the range `[0.01, 0.1)`. For the smallest specified threshold, the lower bound is 0 (e.g. `[0, 0.01)` in the example above). Requests that took longer than any defined latency band will be reported in the `inf` (infinity) band. Requests that were filtered by the configuration (e.g. using `max_read_bytes`) are reported in the `filtered` category.
