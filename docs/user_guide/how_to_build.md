#开发指南
##1. 安装git

从iDesk下载git
安装参考
http://3ms.huawei.com/km/groups/3943816/blogs/details/9292535

配置参考
http://3ms.huawei.com/hi/group/3947196/wiki_6166976.html

安装验证：在目录下，右键菜单可以看到“git bash here”




##2. 下载dash代码

在本地某个存放代码的目录下，打开右键菜单，选择“git bash here”

###1) 首次使用git，需要配置user.name和user.email

git config --global user.name "xxx账号"

git config --global user.email "邮件地址"

###2) codehub添加ssh密钥
生成密钥，一路回车完成
ssh-keygen -t rsa -C 邮件地址

查看密钥内容
cat ~/.ssh/id_rsa.pub


打开https://codehub-g.huawei.com/profile/keys
点击“添加SSH密钥”按钮，添加密钥内容

###3）fork dash到自己的账号下
https://codehub-g.huawei.com/CTO_Technical_Innovation/Storage/lakehouse/dash/files?ref=master

点击由上角"Fork", 然后选择自己的账户，点击“确定”完成fork



###4）clone dash
查看codehub下自己的工程, 应该可以看到新的dash工程
https://codehub-g.huawei.com/workspace/projects?tab=personal
选择dash
https://codehub-g.huawei.com/c00318382/dash/files?ref=master
点击“克隆/下载”，选择“用SSH克隆”，复制ssh地址

执行如下命令，clone代码到本地
git clone ssh://git@codehub-dg-g.huawei.com:2222/xxx账号/dash.git



##2. 安装huawei jdk 11
https://cmc-szv.clouddragon.huawei.com/cmcversion/index/releaseView?deltaId=2597662893010944

windows选择下载jdk-11.0.8-windows-x64.msi （注意是jdk,不是jre)

linux选择下载jdk-11.0.8-linux-x64.tar.gz

安装参考：
http://3ms.huawei.com/km/blogs/details/8223117 （参考中使用的是jdk 8, 此工程需要安装jdk11）

安装校验：java -version
输出：
openjdk version "11.0.8" 2020-07-14
OpenJDK Runtime Environment Cloud_Compiler_JDK_V100R002C00SPC080B003 (build 11.0.8+10)
OpenJDK 64-Bit Server VM Cloud_Compiler_JDK_V100R002C00SPC080B003 (build 11.0.8+10, mixed mode)



##3. 安装IDEA

安装参考：
http://3ms.huawei.com/hi/group/3273071/wiki_5542372.html

激活IDEA

1. 申请使用
http://toolcloud.huawei.com/toolmall/tooldetails/98ed3f4ff84f47d0981d25b162d89588
点击“申请使用”，填写申请信息, 相关信息可以通过命令获取ipconfig /all

申请完成后，点击“查看我的申请”，查看license详情，复制license server链接

2. 启动IDEA
  输入license server，点击active激活idea



##4. 安装Maven

###1)下载apache-maven解压到安装目录下，注意不能有中文路径
例如：D:\apache-maven-3.8.1
https://mirrors.bfsu.edu.cn/apache/maven/maven-3/3.8.1/binaries/apache-maven-3.8.1-bin.zip

系统变量修改：
增加M2_HOME,配置为maven安装目录
path添加：%M2_HOME%\bin

安装验证：
mvn -version
输出:
Apache Maven 3.8.1 (05c21c65bdfed0f71a2f2ada8b84da59348c4c5d)
Maven home: D:\app\apache-maven-3.8.1\bin\..
Java version: 11.0.8, vendor: Huawei Technologies Co., LTD, runtime: D:\app\huawei\jdk-11.0.8
Default locale: zh_CN, platform encoding: GBK
OS name: "windows 10", version: "10.0", arch: "amd64", family: "windows"

###2)把.cid/settings.xml放在apache-maven-3.8.1\conf\目录下，并删除默认的settings.xml，否则会编译失败


###3) 修改settings.xml文件，配置localRepository
<localRepository>D:\Program files\mvnrepo</localRepository>



##5. IDEA配置MAVEN

maven安装及idea配置参考：
http://3ms.huawei.com/km/blogs/details/6423163

启动idea, 选择Customize/All settings
settings/build,execution,deployment/Build Tools/Maven

配置：
maven home path 设置为MAVEN安装目录
User setting files 设置为MAVEN安装目录\conf\settings.xml



##6. MAVEN编译工程

清理编译结果

cd dash
mvn clean package

mvn clean compile -DskipTests



##7. IDEA导入dash工程

启动 idea

点击"open", 选择代码路径 dash，选择trust project, 完成导入

菜单File/Project Structure/Project Settings/Project, 设置Project SDK 为jdk 11, 点击apply完成











