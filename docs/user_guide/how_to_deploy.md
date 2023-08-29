#  部署指导

## 单节点部署



## 多节点部署

### 前置条件

|                | 节点数量                                           | 必要软件 |
| -------------- | -------------------------------------------------- | -------- |
| foundationDB   | 3（foundationdb-server）+1（foundationdb-clients） |          |
| catalog server | 1（与foundationdb-clients共节点）                  | java8    |
| gateway server | 1                                                  | java8    |
| carbon server  | 1                                                  | java8
| carbpm worker | 1                                                  | java8    |
| carbon cli     | 1                                                  | java8    |



### foundationDB部署

foundationDB建议部署2副本，部署参见文件《foundationdb-distributed-deploy.md》文档。

### catalog server部署

1、下载安装包polycat-catalog-0.1-SNAPSHOT.tar.gz

2、解压：tar -zxvf polycat-catalog-0.1-SNAPSHOT.tar.gz

3、进入catalog启动目录：cd polycat-catalog/bin

4、后台启动：nohup ./catalog.sh &  或者 nohup ./catalog.sh >/dev/null 2>/dev/null &

5、确认进程启动成功：使用jps命令可查看到polycat-catalog-server-0.1-SNAPSHOT-exec.jar命名的java进程

### gateway server部署

1、下载安装包polycat-gateway-server-0.1-SNAPSHOT.tar.gz

2、解压：tar -zxvf polycat-gateway-server-0.1-SNAPSHOT.tar.gz

3、进入gateway server目录：cd polycat-gateway-server

4、修改conf/controller.conf文件配置参数：1）controller.catalog.host设置为catalog server节点IP；

5、后台启动：nohup ./bin/sql-server.sh & 或者 nohup ./bin/sql-server.sh  >/dev/null 2>/dev/null &

6、确认进程启动成功：使用jps命令可查看到java进程

### gateway worker部署

1、拷贝polycat-gateway-server-0.1-SNAPSHOT.tar.gz到gateway worker节点

2、解压：tar -zxvf polycat-gateway-server-0.1-SNAPSHOT.tar.gz

3、进入gateway server目录：cd polycat-gateway-server

4、修改conf/worker.conf文件配置参数：1）controller.registry.host设置为gateway server节点IP；2）worker.stage.host设置为本机IP（与skeyline server互通）；

5、后台启动：nohup ./bin/carbon-worker.sh & 或者 nohup ./bin/carbon-worker.sh  >/dev/null 2>/dev/null &

6、确认进程启动成功：使用jps命令可查看到java进程

### cli部署

1、下载安装包polycat-gateway-client-0.1-SNAPSHOT.tar.gz

2、解压：tar -zxvf polycat-gateway-client-0.1-SNAPSHOT.tar.gz

3、进入gateway cli启动目录：cd polycat-gateway-client/bin/

4、启动cli：./cli.sh

5、与gateway server建立连接：!connect [gateway server ip] 9001 [user name] [passwd]，如!connect 192.168.0.220 9001 wukong pwd

## 基于CCE容器化部署

### 前置条件

1、在华为云上购买CCE集群，参见操作指导：https://support.huaweicloud.com/usermanual-cce/cce_01_0028.html

2、购买一定数量的弹性云服务器，比如3台，用于foundationdb多节点部署；

### foundationdb部署

foundationDB建议部署2副本，部署参见文件《foundationdb-distributed-deploy.md》文档。

### catalog server部署

1、创建配置项，操作指导：https://support.huaweicloud.com/usermanual-cce/cce_01_0152.html，配置名称可设置为catalog-configmap，增加配置数据，键为fdb.cluster，值为foundationdb启动服务器上/etc/foundationdb/fdb.cluster文件的内容；

2、创建无状态工作负载，操作指导见：https://support.huaweicloud.com/usermanual-cce/cce_01_0047.html；

3、在创建无状态工作负载过程中，在”容器设置“步骤中，使用catalog镜像；

4、在创建无状态工作负载过程中，在”容器设置“步骤中，展开”数据存储“，单击”添加本地磁盘“，选择”配置项“存储类型，配置项选择前面创建的配置项catalog-configmap，挂载路径设置为/etc/foundationdb；其他事宜默认值即可。

5、在创建无状态工作负载过程中，在”添加服务“步骤中，设置访问类型为”集群内访问（ClusterIP）“，容器端口设置为8082，访问端口设置为8082；

6、其他使用默认方式配置即可。

### gateway server部署

1、（可选）如果需要在gateway中对接obs，则需要额外创建配置项（“配置中心”-“配置项configmap”-""创建配置项），配置名称可设置为gateway-configmap，增加配置数据，键为gateway.conf，值为代码仓conf目录下的gateway.conf文件中的内容；

并新增下列几行配置：

```
# warehouse
catalog.sql.warehouse.dir = obs://xxxx/  #obs的桶名或者填本地目录名

# ak
fs.obs.access.key = xxxxx   #obs用户ak

# sk
fs.obs.secret.key = xxxxx   #obs用户sk

# endpoint
fs.obs.endpoint = obs.xxxxx.myhuaweicloud.com  #obs桶所在域名地址
```

注意替换catalog.sql.warehouse.dir、obs.ak、obs.sk、obs.endpoint的值；

2、创建无状态工作服务，操作指导见：https://support.huaweicloud.com/usermanual-cce/cce_01_0047.html；

3、在创建无状态工作负载过程中，在”容器设置“步骤中，使用gateway镜像；

4、在创建无状态工作负载过程中，在”容器设置“步骤中，展开”环境变量“，单击”添加环境变量“，类型选择”手动添加“，变量名称为”CATALOG_HOST“,变量值为catalog或者catalog.default.svc.cluster.local，即上一节catalog部署完成之后的访问地址；

5、(可选)在创建无状态工作负载过程中，在”容器设置“步骤中，展开”数据存储“，单击”添加本地磁盘“，选择”配置项“存储类型，配置项选择前面创建的配置项gateway-configmap，挂载路径设置为/opt/dash/polycat-gateway-server/conf/gateway.conf，子路径设置为 gateway.conf ；其他事宜默认值即可。

6、在创建无状态工作负载过程中，在”添加服务“步骤中，设置访问类型为”负载均衡（LoadBalancer）“，添加两个端口配置，一个设置端口为9001，一个设置端口为9002；

7、其他事宜默认方式配置即可；

### Spark环境准备

#### 1.安装kubectl

选中一个与CCE集群相同VPC相同安全组的另外一个节点作为kubectl节点。

在华为云CCE界面控制台，操作“资源管理”-“集群管理”-点击“集群名称”-右侧界面下拉-点击“kubectl”；

参考安装步骤，安装kubectl工具；详细参考 [Install Tools | Kubernetes](https://kubernetes.io/docs/tasks/tools/) ；

安装完成后，可以在本节点使用kubectl命令；

#### 2.安装spark环境

在kucectl节点环境上下载java、hadoop和spark,以及hdfs-obsa的jar包；

```shell
$  apt install openjdk-8-jre-headless
$  cd /opt
$  wget http://archive.apache.org/dist/hadoop/common/hadoop-3.2.2/hadoop-3.2.2.tar.gz
$  mkdir /opt/hadoop
$  tar -zxvf hadoop-3.2.2.tar.gz -C /opt/hadoop

$  wget http://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz
$  mkdir /opt/spark
$  tar xvf spark-3.1.2-bin-hadoop3.2.tgz -C /opt/spark

$  wget https://github.com/huaweicloud/obsa-hdfs/raw/master/release/hadoop-huaweicloud-3.1.1-hw-45.jar  #这一步骤会比较慢，要等待；
$  cp hadoop-huaweicloud-3.1.1-hw-45.jar /opt/spark/spark-3.1.2-bin-hadoop3.2/jars

$  vim /etc/profile
# ...
export HADOOP_HOME=/opt/hadoop/hadoop-3.2.2
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
export SPARK_HOME=/opt/spark/spark-3.1.2-bin-hadoop3.2
export PATH=$SPARK_HOME/bin:$PATH
```

#### 3.创建权限用户

此时如果直接执行spark-submit向CCE提交任务，会出现无法pull到私有镜像的问题，所以还需创建一个权限用户:

在kubectl安装节点执行如下命令：

```yaml
$  kubectl create serviceaccount spark   #用户名是spark

# 修改spark用户的配置,增加imagePullSecrets参数赋予spark用户拉取镜像权限
$  kubectl edit sa spark

apiVersion: v1
automountServiceAccountToken: false
imagePullSecrets:         #增加这一行
- name: default-secret    #增加这一行
kind: ServiceAccount

# 保存退出
```

### Spark2部署

```
         user input  user input                                       user input                       
         +--------+  +---------+                                     +-----------+       
         |        |  |         |                                     |           |       
         |   CLI  |  | BEELINE |                                     |  BEELINE  |       
         |        |  |         |                                     |           |       
         +----^---+  +--^------+                                     +-----^-----+       
              |         |                                                  |             
              |         |                                                  |             
    +---------|---------|------------------------------------+             |             
    |         |         |                                    |    +--------v--------+     
    |         |         |                                    |    |                 |     
    |    +----v---------v----+     +-------------------+     |    |  spark-driver   |     
    |    |                   |     |                   <---------->                 |     
    |    |     gateway       |     |  spark-exectuor   |     |    +-----------------+     
    |    |                   |     |                   |     |                           
    |    +----^--------------+     +---------^---------<-------------+                   
    |         |                              |               |       |                   
    |         |                              |               |       |                   
    |         |                              |               |       |                   
    |    +----v--------------+     +---------v---------+     |       |                   
    |    |                   |     |   hive            |     |       |                   
    |    | catalog           <----->                   |     |       |                   
    |    |                   |     |         hms-bridge|     |       |                   
    |    +----^--------------+     +----^--------------+     |       |                   
    |         |                         |                    |       |                   
    |         |                         |     CCE-CLUSTER    |       |         
    +---------|-------------------------|--------------------+       |                   
              |                         |                            |                   
              |                         |                            |                   
          +---v---------+            +--v--------+          +--------v-------+           
          |FDB CLUSTER  |            | Mysql     |          |       OBS      |           
          |             |            |           |          |                |           
          +-------------+            +-----------+          +----------------+                          
```

#### 1.hivemetastore部署

1、创建配置项（“配置中心”-“配置项configmap”-""创建配置项），配置名称可设置为hive-configmap，增加配置数据:

第一个键为 core-site.xml ，值如下：

```xml
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
    <configuration>
      <property>
        <name>fs.obs.impl</name>
        <value>org.apache.hadoop.fs.obs.OBSFileSystem</value>
      </property>
      <property>
        <name>fs.obs.access.key</name>
        <value>xxxxx</value>
        <description>the access key for obs</description>
      </property>
      <property>
        <name>fs.obs.secret.key</name>
        <value>xxxxx</value>
        <description>the secret key for obs</description>
      </property>
      <property>
        <name>fs.obs.endpoint</name>
        <value>obs.xxxxx.myhuaweicloud.com</value>
        <description>the region endpoint for obs</description>
      </property>
    </configuration>
```

第二个键为hive-site.xml，值如下;并注意替换文件内容中下述键的值：

```xml
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
  <configuration>
    <property>
      <name>javax.jdo.option.ConnectionDriverName</name>
      <value>com.mysql.cj.jdbc.Driver</value>
    </property>
     <property>
      <name>javax.jdo.option.ConnectionURL</name>
      <value>jdbc:mysql://${mysqlip}:port/${mysql_db}?createDatabaseIfNotExist=true</value>
      <description>the connection url for mysql</description>
    </property>
    <property>
      <name>javax.jdo.option.ConnectionUserName</name>
      <value>xxxxxx</value>
      <description>the connection username for mysql</description>
    </property>
    <property>
      <name>javax.jdo.option.ConnectionPassword</name>
      <value>xxxxxx</value>
      <description>the connection passed for mysql</description>
    </property>
    <property>
      <name>hive.metastore.warehouse.dir</name>
      <value>obs://$(bucketname)/</value>
    </property>
    <property>
      <name>hive.metastore.port</name>
      <value>9083</value>
    </property>
    <property>
      <name>hive.metastore.schema.verification</name>
      <value>false</value>
      <description>version check</description>
    </property>
    <property>
      <name>datanucleus.schema.autoCreateAll</name>
      <value>true</value>
      <description>creates necessary schema on a startup if one doesn't exist. set this to false, after creating it once</description>
    </property>
    <property>
      <name>hive.metastore.rawstore.impl</name>
      <value>io.polycat.catalog.hms.hive2.HMSBridgeStore</value>
    </property>
    <property>
      <name>polycat.user.name</name>
      <value>user</value>
      <description>the user for auth, for example 'test'</description>
    </property>
    <property>
      <name>polycat.user.password</name>
      <value>passwd</value>
      <description>the pass for auth, for example 'dash'</description>
    </property>
  </configuration>
```

javax.jdo.option.ConnectionURL（mysql数据库的地址、端口以及数据库名）

javax.jdo.option.ConnectionUserName（mysql数据库的用户名）

javax.jdo.option.ConnectionPassword（mysql数据库的登录密码）

hive.metastore.warehouse.dir（obs桶名或者本地目录）

2、创建无状态工作服务，操作指导见：https://support.huaweicloud.com/usermanual-cce/cce_01_0047.html；

3、在创建无状态工作负载过程中，在”容器设置“步骤中，使用hive镜像；

4、在创建无状态工作负载过程中，在”容器设置“步骤中，展开”环境变量“，单击”添加环境变量“，类型选择”手动添加“，变量名称为”CATALOG_HOST“,变量值为catalog或者catalog.default.svc.cluster.local，即上一节catalog部署完成之后的访问地址；

5、(可选)在创建无状态工作负载过程中，在”容器设置“步骤中，展开”数据存储“，单击”添加本地磁盘“，选择”配置项“存储类型，配置项选择前面创建的配置项hive-configmap，挂载路径设置为/opt/apache-hive-2.3.9-bin/conf；其他事宜默认值即可。

6、在创建无状态工作负载过程中，在”添加服务“步骤中，设置访问类型为”节点访问“，添加端口配置，一个设置端口为9083；

7、其他事宜默认方式配置即可；

8、如果是第一次访问mysql数据库，还修改hivemetastore-deployment的yaml文件（“工作负载”-无状态工作负载“-”hivemetastore“-"编译yaml"）,增加初始化命令；

```yaml
 /opt/apache-hive-2.3.9-bin/bin/schematool -dbType mysql -initSchema; 
```

#### 2.spark-thriftserver部署

在spark安装节点下配置core-site.xml和hive-site.xml

core-site参照工作负载hivemetastore中的configmap；

hive-site则如下设置：

```xml
<configuration>
<property>
  <name>hive.metastore.uris</name>
  <value>thrift://${hmsip}:${port}</value>  #即hivemetastore部署时通过service对外放出的ip：port
</property>
<property>
  <name>hive.metastore.warehouse.dir</name>
  <value>obs://${bucketname}/</value>  #obs桶名
</property>
</configuration>    
```

配置文件完成后，后台执行spark-submit命令如下:

其中master的value通过在CCE集群执行kubectl cluster-info获取；

spark.kubernetes.container.image通过在华为云容器镜像服务SWR-“我的镜像”-“自有镜像”-“spark”-"下载指令"中进行获取；

spark.driver.host是执行机的节点ip；

--jars 参数需要指定exector执行所需的jar包；通过wget https://github.com/huaweicloud/obsa-hdfs/raw/master/release/hadoop-huaweicloud-3.1.1-hw-45.jar命令获取，并把jar包放到spark的jars目录即（spark-3.1.2-bin-hadoop3.2/jars）；

可以通过--hiveconf hive.server2.thrift.port=xxxxx指定对外暴露的端口（beeline连接使用该端口），如果不指定则默认为10000；

```
nohup spark-submit \
--master k8s://https://ip:port \
--deploy-mode client \
--name spark-thriftserver  \
--class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2  \
--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
--conf spark.kubernetes.container.image=swr.${region}.myhuaweicloud.com/${repo}/spark:v3.1.2 \
--conf spark.driver.host=192.168.0.191  \
--driver-memory 8g --num-executors 3 --executor-cores 4 --executor-memory 8g \
--jars /opt/spark/spark-3.1.2-bin-hadoop3.2/jars/hadoop-huaweicloud-3.1.1-hw-45.jar \
local:///opt/spark-3.1.2-bin-hadoop3.2/jars/spark-hive-thriftserver_2.11-2.4.5.jar &
```

### Spark3部署

```
                              user input                                               
                              +------+                                                 
                              | CLI  |                                                 
                              +------+              +--------------+                   
                                |                   |              |                   
                               connect     +------->| spark-driver |                   
                                |          |        |              |                   
                                |      attch engine +---^----------+                   
                         +------|----------|------------|---------------+               
                         |      |          |            |               |               
                         |   +--v----------v---+    +---v-----------+   |               
                         |   |                 |    |               |   |               
                         |   |    gateway      |    | spark-exectuor|   |               
                         |   |                 |    |               |   |               
                         |   +-----------------+    +----^-----^----+   |               
                         |                               |     |        |               
                         |                               |     |        |               
                         |     +----------+              |     |        |               
                         |     |          |              |     |        |               
                         |     | catalog  <--------------+     |        |               
                         |     |          |                    |        |               
                         |     +-----^----+     CCE CLUSTER    |        |               
                         +-----------|-------------------------|--------+               
                                     |                         |                       
                                     |                         |                       
                                     |                         |                       
                              +------v-----+                +--v---------+               
                              |FDB CLUSTER |                |   OBS      |               
                              |            |                |            |               
                              +------------+                +------------+               
```



#### 1. 新增catalog负载均衡服务

进入cce华为云控制台，“资源管理”-“网络管理”-“添加service”-"负载均衡访问"-’“关联工作负载catalog”;

映射端口为8082，监听端口号为8082，其余选项默认，创建之后会暴露一个catalog的服务公网ip,可以供cce集群外的节点进行访问；

#### 2. spark-thriftserver部署

先拷贝spark依赖的jar包到saprk/jar目录下：

其中包括代码编译生成的polycat包：

```
polycat-catalog-api-*.jar           polycat-catalog-authentication-*.jar   
polycat-catalog-client-*.jar        polycat-spark-catalog-*.jar
```

还有开源maven仓库中的carbondata包：

```
carbondata-common-2.2.0.jar      carbondata-core-2.2.0.jar         
carbondata-format-2.2.0.jar      carbondata-hadoop-2.2.0.jar       
carbondata-processing-2.2.0.jar  carbondata-spark_3.1-2.2.0.jar
carbondata-streaming_3.1-2.2.0.jar
```

当然也可以直接从代码编译节点的PolyCat/catalog/spark/target/lib中获取；

在spark安装节点下配置core-site.xml和gateway.conf（均放在$spark/conf目录下）

core-site参照工作负载hivemetastore中的configmap；

gateway.conf用于给spark-driver提供catalog的连接地址； CATALOG_HOST和CATALOG_PORT的值应设置为catalog的负载均衡服务对外暴露的ip:port;

```shell
catalog.host = 49.4.22.80
catalog.port = 8082
```

以上修改完成后，后台执行spark-submit命令如下:

```
nohup spark-submit \
--master k8s://https://ip:port \
--deploy-mode client \
--name spark-thriftserver  \
--class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2  \
--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
--conf spark.kubernetes.container.image=swr.${region}.myhuaweicloud.com/${repo}/spark:v3.1.2 \
--conf spark.driver.host=192.168.0.191  \
--driver-memory 8g --num-executors 3 --executor-cores 4 --executor-memory 8g \
--jars /opt/spark/spark-3.1.2-bin-hadoop3.2/jars/hadoop-huaweicloud-3.1.1-hw-45.jar \
local:///opt/spark-3.1.2-bin-hadoop3.2/jars/spark-hive-thriftserver_2.11-2.4.5.jar &
```

### carbon cli部署

参见”多节点“部署。

(可选) 如果要在cli使用spark3方式连接，需要执行命令:

其中ip和port的值应该设置为spark-driver节点的ip和port;

```shell
polycat(default:default)>  attach engine spark ('type'='spark', 'ip'='192.168.0.184', 'port'='10000');
```




##  基于k8s进行容器化部署

### 前置条件

拥有一套部署好的k8s集群，以及三台用于部署数据库的节点；

### foundationdb分布式部署

foundationDB建议部署2副本，部署参见文件《foundationdb-distributed-deploy.md》文档 。

### 镜像准备

如果在外网环境下，我们的catalog:v1和gateway:v1镜像已经推到开源镜像仓库；

可以使用docker search catalog 查询到catalog的远端镜像；

通过docker pull xxxxx/catalog:v1 或者 docker pull xxxxx/gateway-server:v1下载到镜像；

xxxx是指docker远端仓库的dockerid；

### catalog server部署

#### 1.创建配置项configMap;

```
$  kubectl create configmap CFGNAME --from-literal=KEY=VALUE 
```

CFGNAME 为要创建的配置名称，可以设置为catalog-configmap 

KEY为fdb.cluster，VALUE 为foundationdb启动服务器上/etc/foundationdb/fdb.cluster文件中的内容； 

或者先从远端获取到配置文件，修改后进行创建；

```
$  wget https://raw.githubusercontent.com/carbonlake/dash/ossdev/conf/catalog-configmap.yaml
```

修改文件中fdb.cluster的值：

```
$  vim  catalog-configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
	name:  DataLakeOS-configmap
	namespace: default
data:
	fdb.cluster:  xxxxxx    #将值修改成/etc/foundationdb/fdb.cluster文件的内容

$  kubectl create -f DataLakeOS-configmap.yaml
```

查看当前所有的配置文件

```
$   kubectl get configmap --all-namespaces 

NAMESPACE     NAME                                 DATA   AGE  
default       DataLakeOS-configmap                       1      2s 

$   kubectl describe configmap DataLakeOS-configmap    #用于查看配置内容是否正确
```

#### 2. 创建工作负载deployment

下载yaml文件

```
$ wget https://raw.githubusercontent.com/carbonlake/dash/ossdev/conf/catalog-deployment.yaml
```

修改该文件中的configmap名称以及image镜像地址

```
$ vim catalog-deployment.yaml

        spec:
            volumes:
              - name: vol-15927317327    #使用字母-数字的格式进行命名
                configMap:
                    name: DataLakeOS-configmap    # 前面创建的configMap的名称
                    items:
                      - key: fdb.cluster
                        path: fdb.cluster
            containers:
              - name: containers-catalog-demo     #给要启动的容器命名，中间不能出现下划线
                image: xxxxx/catalog:v1   
                           #如果镜像地址已经知道，这里要填完整的镜像路径，否则要保证各节点能找到该镜像
                volumeMounts:
                  - name: vol-15927317327     #前面spec.template.spec.volume.name设置的值
                    readOnly: true
                    mountPath: /etc/foundationdb
              
 保存退出；
 $  kubectl create  -f catalog-deployment.yaml
```

 查看工作负载

```
$ kubectl get depoyments  --all-namespaces
$ kubectl get pods -o wide
```

备注：

> 如果出现异常，可以通过下述命令进行删除
>
> ```
> $ kubectl delete depoyments/xxxxx    # xxxx代表创建的工作负载的名称
> ```

#### 3.创建服务Service

```
$   kubectl create service clusterip catalog --tcp 8082:8082
```

clusterip表示服务访问类型-集群内访问；

除此之外，还有loadbalancer(负载均衡)， nodeport(节点端口)；

对于catalog服务要选用集群内的访问；

--tcp port1:port2 是指容器端口设置为8082，访问端口设置为 8082



### gateway server部署

#### 1.创建配置项configMap;

从远端获取到配置文件，修改后进行创建；

```
$  wget https://raw.githubusercontent.com/carbonlake/dash/ossdev/conf/gateway-configmap.yaml
```

要修改其中的预设值之后

```
 $  kubectl create  -f gateway-configmap.yaml
```

#### 2.创建工作负载deployments

下载yaml文件：

```
$  wget https://raw.githubusercontent.com/carbonlake/dash/ossdev/conf/gateway-deployment.yaml
```

修改yaml文件中的镜像仓库地址；

```
$ vim gateway-deployment.yaml

        spec:
            containers:
              - name: containers-gateway-demo     #给要启动的容器命名，中间不能出现下划线
                image: xxxxx/gateway-server:v1   
                           #如果镜像地址已经知道，这里要填完整的镜像路径，否则要保证各节点能找到该镜像
                    
保存退出；
$  kubectl create  -f gateway-deployment.yaml
```

#### 3.创建服务service

如果使用节点访问方式，可以下载yaml文件;

```
$  wget https://raw.githubusercontent.com/carbonlake/dash/ossdev/conf/gateway-service.yaml
$  kubectl create  -f gateway-service.yaml
```

对于gateway服务要选用负载均衡访问方式，建议使用界面操作

--tcp port1:port2 是指两个端口分别为9001和9002

### hivemetastore部署

#### 1.创建配置项configMap;

从远端获取到配置文件，修改后进行创建；

```
$  wget https://raw.githubusercontent.com/carbonlake/dash/ossdev/conf/hive-configmap.yaml
```

要修改其中的预设值之后

```
 $  kubectl create  -f hive-configmap.yaml
```

#### 2.创建工作负载deployments

下载yaml文件：

```
$  wget https://raw.githubusercontent.com/carbonlake/dash/ossdev/conf/hivemetastore-deployment.yaml
```

修改yaml文件中的镜像仓库地址；

```
$ vim gateway-deployment.yaml

        spec:
            containers:
              - name: containers-gateway-demo     #给要启动的容器命名，中间不能出现下划线
                image: xxxxx/gateway-server:v1   
                           #如果镜像地址已经知道，这里要填完整的镜像路径，否则要保证各节点能找到该镜像
                    
保存退出；
$  kubectl create  -f hivemetastore-deployment.yaml
```

#### 3.创建服务service

如果使用节点访问方式，可以下载yaml文件;

```
$  wget https://raw.githubusercontent.com/carbonlake/dash/ossdev/conf/hivemetastore-service.yaml
$  kubectl create  -f hivemetastore-service.yaml
```

如果要选用负载均衡访问方式，建议使用界面操作；

