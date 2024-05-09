# 1 需求说明
对于存量用户，之前的元数据服务很多是使用的大数据生态下的Hive MetaStore，
为了让这部分用户使用PolyCat，需要将Hive Metastore里的元数据信息迁移到
PolyCat服务上。

# 2 使用指导
## 2.1 解压安装包
版本包的路径在代码编译后的目录为migration/target/polycat-migration-0.1-SNAPSHOT.tar.gz
拷贝版本包到执行环境上，执行环境需要和Hive Metestore和PolyCat 服务所在的服务
器能够正常连接。

解压版本包
```shell
➜  target git:(main) ✗ tar -zxf polycat-migration-0.1-SNAPSHOT.tar.gz 
➜  target git:(main) ✗ cd polycat-migration-0.1-SNAPSHOT 
➜  polycat-migration-0.1-SNAPSHOT git:(main) ✗ ls -lt
total 144
drwxr-xr-x    4 liulm  staff    128  3 26 10:03 config
drwxr-xr-x  229 liulm  staff   7328  3 26 10:03 lib
-rwxr-xr-x    1 liulm  staff  64103  3 26 08:48 polycat-migration-0.1-SNAPSHOT.jar
-rwxr-xr-x    1 liulm  staff    268  3 26 08:23 run.sh
-rwxr-xr-x    1 liulm  staff    196  3 26 08:23 spark-submit-demo.sh
➜  polycat-migration-0.1-SNAPSHOT git:(main) ✗ 

```

## 2.2 修改配置
### 2.2.1 数据库配置
需要事先安装mysql，在mysql中导入初始化sql脚本（脚本存放路径在config/init.sql）

### 2.2.2 修改配置文件
配置文件路径（config/config.properties），文件内容为
```shell
➜  config git:(main) ✗ cat config.properties 
url=127.0.0.1:3306
username=root
password=*****
```
按照mysql数据库的信息修改url、username和passord

## 2.3 执行迁移
### 2.3.1 命令帮助
```shell
➜  polycat-migration-0.1-SNAPSHOT git:(main) ✗ ./run.sh       
Missing required subcommand
Usage: migration [COMMAND]
Commands:
  create   create migration
  prepare  prepare for migration
  migrate  run migration
  list     list migration

```

### 2.3.2 创建迁移命令
创建迁移，需要指定hive metastore 和 polycat 相关信息
```shell
➜  polycat-migration-0.1-SNAPSHOT git:(main) ✗ ./run.sh create 
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/Users/liulm/workspace/PolyCat/migration/target/polycat-migration-0.1-SNAPSHOT/lib/slf4j-log4j12-1.7.25.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/Users/liulm/workspace/PolyCat/migration/target/polycat-migration-0.1-SNAPSHOT/lib/log4j-slf4j-impl-2.17.1.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
Missing required options: '--s_url=<sourceURI>', '--d_url=<destURI>', '--d_user=<destUser>', '--d_password=<destPassword>', '--project=<projectId>', '--tenant=<tenantName>'
Usage: migration create [-hV] --d_password=<destPassword> --d_url=<destURI>
                        --d_user=<destUser> --project=<projectId>
                        [--s_password=<sourcePassword>] --s_url=<sourceURI>
                        [--s_user=<sourceUser>] --tenant=<tenantName>
create migration
      --s_url=<sourceURI>   源地址信息, eg: thrift://*.*.*.*:9083.
      --s_user=<sourceUser> 源端认证用户名
      --s_password=<sourcePassword>
                            源端认证密码
      --d_url=<destURI>     目的端PolyCat地址信息, eg: 127.0.0.1:8082.
      --d_user=<destUser>   PolyCat端认证用户名
      --d_password=<destPassword>
                            PolyCat端认证密码
      --project=<projectId> PolyCat端Project ID
      --tenant=<tenantName> PolyCat端Tenant Name
  -h, --help                Show this help message and exit.
  -V, --version             Print version information and exit.
```

### 2.3.3 查看迁移信息
```shell
➜  polycat-migration-0.1-SNAPSHOT git:(main) ✗ ./run.sh list   
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/Users/liulm/workspace/PolyCat/migration/target/polycat-migration-0.1-SNAPSHOT/lib/slf4j-log4j12-1.7.25.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/Users/liulm/workspace/PolyCat/migration/target/polycat-migration-0.1-SNAPSHOT/lib/log4j-slf4j-impl-2.17.1.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
log4j:WARN No appenders could be found for logger (io.polycat.tool.migration.command.MigrationList).
log4j:WARN Please initialize the log4j system properly.
log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.
Sat Mar 26 10:20:44 CST 2022 WARN: Establishing SSL connection without server's identity verification is not recommended. According to MySQL 5.5.45+, 5.6.26+ and 5.7.6+ requirements SSL connection must be established by default if explicit option isn't set. For compliance with existing applications not using SSL the verifyServerCertificate property is set to 'false'. You need either to explicitly disable SSL by setting useSSL=false, or set useSSL=true and provide truststore for server certificate verification.

迁移列表信息
ID   名称    HMS地址   PolyCat地址  状态
2 migration-wzzPVyYE thrift://192.168.173.100:9083 192.168.173.1:8082 PREPARED
```

### 2.3.4 准备迁移
这个过程是将hive metastore当前的元数据信息（数据库和表的名称）导出到
迁移工具的数据库中
```shell
➜  polycat-migration-0.1-SNAPSHOT git:(main) ✗ ./run.sh prepare
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/Users/liulm/workspace/PolyCat/migration/target/polycat-migration-0.1-SNAPSHOT/lib/slf4j-log4j12-1.7.25.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/Users/liulm/workspace/PolyCat/migration/target/polycat-migration-0.1-SNAPSHOT/lib/log4j-slf4j-impl-2.17.1.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
Missing required option: '--name=<migrationName>'
Usage: migration prepare [-hV] --name=<migrationName>
prepare for migration
      --name=<migrationName>
                  migration名称, eg: migration-****.
  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.
```

### 2.3.5 迁移
- 单机执行
```shell
➜  polycat-migration-0.1-SNAPSHOT git:(main) ✗ ./run.sh migrate
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/Users/liulm/workspace/PolyCat/migration/target/polycat-migration-0.1-SNAPSHOT/lib/slf4j-log4j12-1.7.25.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/Users/liulm/workspace/PolyCat/migration/target/polycat-migration-0.1-SNAPSHOT/lib/log4j-slf4j-impl-2.17.1.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
Missing required option: '--name=<migrationName>'
Usage: migration migrate [-hV] [--with-partition] --name=<migrationName>
run migration
      --name=<migrationName>
                         migration名称, eg: migration-****.
      --with-partition   是否迁移表的partition
  -h, --help             Show this help message and exit.
  -V, --version          Print version information and exit.
```
- 提交spark集群执行
```shell
spark-submit --master **** --name test --class io.polycat.tool.migration.Migrate --jars $(echo lib/*.jar | tr ' ' ',') polycat-migration-0.1-SNAPSHOT.jar migration-****
```
备注
- --master 指定spark集群的地址
- 最后的参数migration-**** 可以通过 ./run.sh list 查看迁移的名称
