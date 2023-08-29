 #foundationdb集群分布式部署步骤
1. foundationdb集群的每台服务器，安装sudo dpkg -i foundationdb-clients_6.3.15-1_amd64.deb/foundationdb-server_6.3.15-1_amd64.deb;
2. 选择一台foundation机器作为集群的启动服务器, 执行命令：python3 /usr/lib/foundationdb/make_public.py -a ${ipaddr}，ipaddr为本机ip地址；
3. 拷贝启动服务器下面的fdb.cluster到其余节点并重启foundationdb进程， copy /etc/foundationdb/fdb.cluster to the new machine， sudo service foundationdb restart;
4. 配置三副本和SSD存储，fdb> configure triple ssd;
5. 配置多coordinators，防止单点故障问题，fdb> coordinators ipaddr1:4500 ipaddr2:4500 ipaddr3:4500;
6. 使用命令查看foundationdb集群状态， fdb> status details;
7. 只安装fdb 客户端的服务器，需要手动拷贝 /etc/foundationdb/fdb.cluster 文件到本地;

 #添加一个节点到foundationdb集群
 拷贝集群服务器下面存在的fdb.cluster文件到新节点，并重启新节点foundationdb进程
 
 #从foundationdb集群移除一个节点
 fdb> exclude ipaddr
 从foundationdb移除节点，如果该节点为coordinators节点，需要先从coordinators中移除，待生效后，再从集群移除
 
 
 #单数据中心模式配置推荐
---------------------------------------------------------------------------------------------------------- 
 	 	                                             |      single	    |     double	 |    triple      |
----------------------------------------------------------------------------------------------------------
Best for	 	                                     |   1-2 machines	|  3-4 machines	 |  5+ machines   |
----------------------------------------------------------------------------------------------------------
Total Replicas	 	                                 |     1 copy	    |   2 copies	 |  3 copies      |
----------------------------------------------------------------------------------------------------------
Live machines required to make progress	 	         |        1	        |      2	     |      3         |
----------------------------------------------------------------------------------------------------------
Required machines for fault tolerance	 	         |   impossible	    |      3	     |      4         |
----------------------------------------------------------------------------------------------------------
Ideal number of coordination servers	 	         |        1	        |      3	     |      5         |
----------------------------------------------------------------------------------------------------------
Simultaneous failures after which data may be lost	 |	 any process	|  2+ machines	 |  3+ machines   |
----------------------------------------------------------------------------------------------------------
