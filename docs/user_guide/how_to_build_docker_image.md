#docker镜像部署指南
##1. 基础软件安装

###1）JDK(11):必装，请安装好后在/etc/profile下配置JAVA_HOME及PATH变量

###2）docker：必装

##2. 制作docker镜像

在dash代码工程目录执行命令：mvn clean -DskipTests package -Pdocker

该命令执行成功之后，执行查看镜像命令（sudo docker images）可以看到新生成的镜像
![docker-images.png](images/docker-images.png)

##3. 启动镜像

linux宿主机启动polycat镜像，执行命令：
sudo docker run -t -i -p 9001:9001 polycat:v1
![docker-run.png](images/docker-run.png)

##4. 启动sql-cli

linux宿主机启动sql-cli，在代码工程目录执行命令：./bin/start-carboncli.sh
![start-sql-cli.png](images/start-sql-cli.png)

