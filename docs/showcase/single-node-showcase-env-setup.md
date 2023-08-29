# 单节点showcase环境的搭建

showcase2开始，需要大家hadoop，hive，kafka，spark等环境进行演示，本文档描述环境搭建过程。
具体的showcase操作，请参照showcase2.md的描述。

## 1. 前提假设

1）假设在WSL上进行安装环境，操作系统选择Ubuntu1804。

2）假设WSL2环境已经安装完毕，如果没安装，请到微软网站上按照参考文档安装。

3）Ubuntu的源配置华为源。

4）JDK安装完毕（当前是1.8版本）。

## 2. 重新安装ssh并配置免密登录

### 2.1 WSL原带的openssh有问题，需要卸载并重新安装一次

```
sudo apt-get remove openssh-server
sudo apt-get install openssh-server
sudo service ssh restart
```

### 2.2 localhost免密登录

```bash
ssh-keygen -t rsa
ssh-copy-id -i localhost
ssh localhost
```

## 3. Hadoop安装

Hadoop安装的是单节点伪分布式环境，版本选择和后继的Spark选择有关联。

例如：Hadoop 3.2.2

### 3.1 下载并解压

1）下载hadoop二进制包 https://hadoop.apache.org/releases.html

2）解压缩到自己方便的安装目录

### 3.2 Hadoop配置并运行 HDFS

1）添加环境变量 ~/.bashrc：

```bash
export HADOOP_HOME=<YOUR_HADOOP_DECOMPRESSD_PATH>
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
export HADOOP_OPTS="-Djava.library.path=${HADOOP_HOME}/lib/native"

source ~/.bashrc
```

$HADOOP_HOME/etc/hadoop/hadoop-env.sh
```bash
export JAVA_HOME=<YOUR_JAVA_HOME_PATH>
export HADOOP_OPTS="-Djava.library.path=${HADOOP_HOME}/lib/native"

此处很奇怪，不知道为什么系统的JAVA_HOME没生效，在这个hadoop-env.sh中还要再设置一次，不然后面启动dfs的时候报错。
```


运行下hadoop version看下是否能执行


2）创建几个hadoop存储文件的本地目录：

```bash
$ tree dfs
dfs
├── data
├── name
└── temp
```


3）修改配置文件

$HADOOP_HOME/etc/hadoop/core-site.xml

```xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>file:/opt/dfs/temp</value>      <---- 你自己的临时文件目录
    <description>Abase for other temporary directories.</description>
  </property>
</configuration>

```

$HADOOP_HOME/etc/hadoop/hdfs-site.xml

```xml
<configuration>
  <!--指定hdfs中namenode的存储位置-->
  <property>
    <name>dfs.namenode.name.dir</name> 
    <value>/opt/dfs/name</value>         <---- 你自己的HDFS本地Namenode存储目录
  </property>
  <!--指定hdfs中datanode的存储位置-->
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>/opt/dfs/data</value>         <---- 你自己的HDFS本地数据存储目录
  </property>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.permissions</name>
    <value>false</value>
  </property>
</configuration>
```

4）格式化namenode

```bash
hdfs namenode -format
```

5）启动HDFS

```bash
start-dfs.sh

$ jps
5264 Jps
4645 NameNode
4855 DataNode
5116 SecondaryNameNode
```

http://localhost:9870   <---- 或者你的WSL地址

就可以看到HDFS的管理页面。


## 4. Spark安装

### 4.1 Scala安装

下载，解压，并配置环境变量。

下载的时候需要注意一下Scala的版本和Spark的匹配。当前Spark 3.0+是使用Scala2.12进行的预编译。
https://www.scala-lang.org/download/scala2.html

```bash
export SCALA_HOME=/home/redstar/setup/scala-2.12.14
export PATH=$SCALA_HOME/bin:$PATH

source ~/.bashrc
```

运行scala查看是否安装正常。


### 4.2 Spark下载并配置

1）下载 http://spark.apache.org/downloads.html

下载的时候需要看下Hadoop的支持版本。例如：spark-3.1.2-bin-hadoop3.2.tgz是指支持的Hadoop是3.2以上。

2）添加环境变量： ~/.bashrc：

```bash
export SPARK_HOME=/home/redstar/setup/spark-3.1.2-bin-hadoop3.2
export PATH=$SPARK_HOME/bin:$PATH

source ~/.bashrc
```

3）修改Spark的配置
   
初始下载的是$SPARK_HOME/conf/spark-env.sh.template，更名为spark-env.sh

```bash
export JAVA_HOME=/home/redstar/setup/jdk1.8.0_301
export SPARK_MASTER_IP=localhost
export SPARK_WORKER_MEMORY=4g
```

4）启动spark

```bash
./setup/spark-3.1.2-bin-hadoop3.2/sbin/start-all.sh
starting org.apache.spark.deploy.master.Master, logging to /home/redstar/setup/spark-3.1.2-bin-hadoop3.2/logs/spark-redstar-org.apache.spark.deploy.master.Master-1-LAPTOP-B8VU4RHN.out
localhost: starting org.apache.spark.deploy.worker.Worker, logging to /home/redstar/setup/spark-3.1.2-bin-hadoop3.2/logs/spark-redstar-org.apache.spark.deploy.worker.Worker-1-LAPTOP-B8VU4RHN.out

jps
6306 Jps
6036 Master
4645 NameNode
4855 DataNode
5116 SecondaryNameNode
6223 Worker
```

http://localhost<或者你的WSL地址>:8080

就可以看到Spark的管理页面。

## 5. Kafka安装

1）下载，解压，配置

https://kafka.apache.org/downloads

kafka和scala的版本也有关系，下载的scala版本是2.12的kafka是2.8版本kafka_2.12-2.8.0.tgz。

```bash
export KAFKA_HOME=/home/redstar/setup/kafka_2.12-2.8.0
export PATH=$KAFKA_HOME/bin:$PATH

source ~/.bashrc
```

2）启动

```bash
$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties

再启动一个窗口
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties
```


## 6. Hive安装

1）下载，解压

从 http://hive.apache.org/downloads.html 下载了最新的版本3.1.2。

下载后解压，准备开始配置。

2）配置Hive，缺省连接hadoop和derby元数据。

缺省hive的配置在$HIVE_HOME/conf中，以template结尾。需要改为使用的名字。

```bash
mv hive-default.xml.template hive-site.xml
mv hive-env.sh.template hive-env.sh
```

然后进行修改：

添加：
```xml
  <property>
    <name>system:java.io.tmpdir</name>
    <value>/home/redstar/hive</value>   <--- 建立一个临时文件目录
    <description></description>
  </property>
  <property>
    <name>system:user.name</name>
    <value>redstar</value>              <--- 访问HDFS用户名
    <description></description>
  </property>
```
如果运行失败，需要将hadoop中的guava-27.0-jre.jar拷贝到$HIVE_HOME/lib中，并将guava-19.0.jar删除。

针对hive-env.sh的修改：

```bash
# Set HADOOP_HOME to point to a specific hadoop install directory
HADOOP_HOME=/home/redstar/setup/hadoop-3.2.2

# Hive Configuration Directory can be controlled by:
export HIVE_CONF_DIR=/home/redstar/setup/apache-hive-3.1.2-bin/conf
```

修改hadoop的core-site.xml配置文件，此处修改为hadoop文件的用户名。
```xml
  <property>
    <name>hadoop.proxyuser.redstar.hosts</name>
    <value>*</value>
  </property>
  <property>
    <name>hadoop.proxyuser.redstar.groups</name>
    <value>*</value>
  </property> 
```

初始化Hive的Metastore。使用缺省的derby存储。

```bash
schematool -initSchema -dbType derby
```

3）验证hive的运行。

```bash
启动hadoop
cd $HADOOP_HOME/sbin
$ ./hadoop-daemon.sh start datanode
$ ./hadoop-daemon.sh start namenode
$ yarn-daemon.sh start nodemanager
$ yarn-daemon.sh start resourcemanager

启动hive
cd $HIVE_HOME/bin
./hiveserver2
查看日志，确保启动成功。

启动hive客户端beeline
./beeline
!connect jdbc:hive2://127.0.0.1:10000
```

如果提示连接成功，可以进行基本的hive建表及查询操作。

如果有错误，检查hadoop的磁盘文件权限。

4）配置连接carbon catalog server。 

修改hive-site.xml，将这两个变量修改为Carbon Data的类：

```xml
  <property>
    <name>hive.metastore.init.hooks</name>
    <value>io.polycat.catalog.hms.DashMetastoreInitListener</value>
    <description>
      A comma separated list of hooks to be invoked at the beginning of HMSHandler initialization. 
      An init hook is specified as the name of Java class which extends org.apache.hadoop.hive.metastore.MetaStoreInitListener.
    </description>
  </property>

  <property>
    <name>hive.metastore.rawstore.impl</name>
    <value>io.polycat.catalog.hms.DashRawStore</value>
    <description>
      Name of the class that implements org.apache.hadoop.hive.metastore.rawstore interface.
      This class is used to store and retrieval of raw metadata objects such as table, database
    </description>
  </property>
```

参照showcase2.md，将编译好的hms bridge文件polycat-catalog-hmsbridge-0.1-SNAPSHOT.jar，以及Catalog客户端SDK文件拷贝到$HIVE_HOME/lib目录下。

5）重新启动catalog server，hiveserver，并用beeline进行测试。

```bash
cd $DASH_HOME/bin
./start.sh all

$ jps
5249 Jps
3509 DataNode
3783 NodeManager
745 polycat-catalog-server-0.1-SNAPSHOT-exec.jar
1642 polycat-streamer-server-0.1-SNAPSHOT.jar
2522 ResourceManager
3629 NameNode

cd $HIVE_HOME/bin
./hiveserver2

./beeline
```

此时可以在hive的beeline客户端中创建表，并能再dash client中查看到hive catalog和相应的表。
