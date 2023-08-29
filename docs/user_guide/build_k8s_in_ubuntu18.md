# Ubuntu-18.04.1安装kubernetes-1.12.0

【简述】：该指导文档分为三个步骤“物理环境准备”、“软件环境准备”、“k8s集群搭建”。

## 一. 物理环境准备

### 1.1 安装规划

3台主机， 操作系统：Ubuntu 18.04.1 LTS

其中etcd为K8S数据库，其他组件为k8s组件。

| 点IP地址      | 角色                    | 安装组件名称                                                 |
| ------------- | ----------------------- | ------------------------------------------------------------ |
| 192.168.0.204 | kube-master（管理节点） | docker、kubelet、kubectl、kubeadm、etcd、kube-apiserver、kube-controller-manager、kube-scheduler |
| 192.168.0.191 | kube-node1（计算节点）  | docker 、kubelet、kubectl、kubeadm、kube-proxy               |
| 192.168.0.192 | kube-node2（计算节点）  | docker 、kubelet、kubectl、kubeadm、kube-proxy               |

### 1.2 关闭防火墙（所有节点执行）

关闭ufw防火墙,Ubuntu默认未启用,无需设置 

```
$  sudo ufw disable
```

备注：

> ufw的底层是使用iptables进行数据过滤，建立在iptables之上，这可能会与 Docker 产生冲突。
>
> 为避免不必要的麻烦，这里把firewalld关掉。

### 1.3 禁用SELINUX（所有节点执行）

ubuntu默认不安装selinux，假如安装了的话，按如下步骤禁用selinux 

临时禁用（重启后失效） 

`$  sudo setenforce 0   # 0代表permissive 1代表enforcing`

永久禁用

```
$  sudo vi /etc/selinux/config
SELINUX=permisssive
```

备注：

> kubelet目前对selinux的支持还不好，需要禁用掉。
> 不禁用selinux的话有时还会出现明明容器里是root身份运行，操作挂载进来的主机文件夹时没有权限的情况，这时候要么配置selinux的权限，要么就禁掉selinux
> 另外，当docker的storage-driver使用overlay2的时候，低版本内核的selinux不支持overlay2文件驱动，docker启动时设置为--selinux-enabled会失败报错:“Error starting daemon: SELinux is not supported with the overlay2 graph driver on this kernel”，需设置--selinux-enabled=false

### 1.4 开启数据包转发（所有节点执行）

#### 1.4.1 内核开启ipv4转发

1. 修改/etc/sysctl.conf，开启ipv4转发：

   ```
   $   sudo vim /etc/sysctl.conf
   net.ipv4.ip_forward = 1   #开启ipv4转发，允许内置路由
   ```

2. 写入后执行如下命令生效：

   ```
   $  sudo sysctl -p
   ```

备注：

> 什么是ipv4转发：出于安全考虑，Linux系统默认是禁止数据包转发的。转发即当主机拥有多于一块的网卡时，其中一块收到数据包，根据数据包的目的ip地址将数据包发往本机另一块网卡，该网卡根据路由表继续发送数据包。这通常是路由器所要实现的功能。
> kube-proxy的ipvs模式和calico(都涉及路由转发)都需要主机开启ipv4转发。
> 另外，不使用k8s，即使只使用docker的时候，以下两种情况也依赖ipv4转发：
> <1>当同一主机上的两个跨bridge(跨bridge相当于跨网段，跨网络需要路由)的容器互访
> <2>从容器内访问外部
>
> 参考：<https://docs.docker.com/v17.09/engine/userguide/networking/default_network/container-communication/#communicating-to-the-outside-world> 

#### 1.4.2 防火墙修改FORWARD链默认策略

数据包经过路由后，假如不是发往本机的流量，下一步会走iptables的FORWARD链，而docker从1.13版本开始，将FORWARD链的默认策略设置为DROP，会导致出现一些例如跨主机的两个pod使用podIP互访失败等问题。解决方案有2个：

1. 在所有节点上开机启动时执行iptables -P FORWARD ACCEPT （不建议）

   临时生效：

   ```
   $  sudo iptables -P  FORWARD ACCEPT
   ```

   iptables的配置重启后会丢失，可以将配置写入到`/etc/rc.local`中，重启后自动执行

   ```
   /usr/sbin/iptables -P FORWARD ACCEPT
   ```

2. 让docker不操作iptables （建议）

   设置docker启动参数添加`--iptables=false`选项，使docker不再操作iptables，比如1.10版以上可编辑docker daemon默认配置文件`/etc/docker/daemon.json`: 

   ```
   {
        "iptables" : false
   } 
   ```

 备注：

> 建议方案二
> kubernetes官网建议和k8s结合的时候docker的启动参数设置--iptables=false使得docker不再操作iptables，完全由kube-proxy来操作iptables。
> 参考：
> <1>https://docs.docker.com/v17.09/engine/userguide/networking/default_network/container-communication/#container-communication-between-hosts
> <2>https://github.com/kubernetes/kubernetes/issues/40182
> <3>https://kubernetes.io/docs/setup/scratch/#docker
> <4>https://github.com/moby/moby/pull/28257

### 1.5 禁用SWAP（所有节点执行）

1. 禁用所有的swap分区

   `$   sudo swapoff -a`

2. 同时还需要修改`/etc/fstab`文件，注释掉SWAP的自动挂载，防止重启后配置丢失

备注：

> Kubernetes 1.8开始要求关闭系统的Swap，如果不关闭，默认配置下kubelet将无法启动，虽然可以通过kubelet的启动参数--fail-swap-on=false更改这个限制，但不建议，最好还是不要开启swap。
> 一些为什么要关闭swap的讨论：
> <1>https://github.com/kubernetes/kubernetes/issues/7294
> <2>https://github.com/kubernetes/kubernetes/issues/53533



### 1.6 配置iptables参数，配置网桥（所有节点执行）

```
$  sudo tee /etc/sysctl.d/k8s.conf <<EOF
net.bridge.bridge-nf-call-iptables  = 1
net.bridge.bridge-nf-call-ip6tables  = 1
EOF

$   sudo sysctl --system
```

备注：

> 网络插件需要为kube-proxy提供一些特定的支持，比如kube-proxy的iptables模式基于iptables，网络插件就需要确保容器的流量可以流过iptables。比如一些网络插件会用到网桥，而网桥工作在数据链路层，iptables/netfilter防火墙工作在网络层，以上配置则可以使得通过网桥的流量也进入iptables/netfilter防火墙中，确保iptables模式的kube-proxy可以正常工作。
> 默认没有指定kubelet网络插件的情况下，会使用noop插件，它也会设置net/bridge/bridge-nf-call-iptables=1来确保iptables模式的kube-proxy可以正常工作。
> 参考：
> <1>https://kubernetes.io/docs/concepts/extend-kubernetes/compute-storage-net/network-plugins/#network-plugin-requirements
> <2>https://kubernetes.io/docs/setup/independent/install-kubeadm/



## 二、软件环境准备

### 2.1 安装docker工具(所有节点执行)

#### 安装docker-18.03.1

1.卸载旧docker

```
$  sudo apt-get remove docker docker-engine docker.io
```
2.安装依赖，使apt可以使用https

```
$  sudo apt-get install apt-transport-https ca-certificates curl software-properties-common
```
3.添加docker的GPG key

由于国内访问download.docker.com不稳定，可以使用阿里云

```
$  curl -fsSL https://mirrors.aliyun.com/docker-ce/linux/ubuntu/gpg | sudo apt-key add -
```
4.设置docker镜像源

使用阿里云镜像仓库

```
$  sudo add-apt-repository  "deb [arch=amd64] https://mirrors.aliyun.com/docker-ce/linux/ubuntu  $(lsb_release -cs) stable"
```
5.安装指定版本docker-ce

查看源中有哪些版本

```
$  apt-cache madison docker-ce
docker-ce | 18.03.1~ce~3-0~ubuntu | https://mirrors.aliyun.com/docker-ce/linux/ubuntu bionic/sta....
```

安装18.03.1版本

```
$  sudo apt-get install -y docker-ce=18.03.1~ce~3-0~ubuntu
```
6.启动并设置开机启动

```
$  sudo  systemctl enable docker && sudo systemctl start docker
```
7.如果当前用户非root,需要将当前用户加入到docker用户组中’

```
$  sudo usermod -aG docker username
```

然后退出，重新登录，使用docker命令不需要加sudo了

备注：

> `docker部署参考：<https://docs.docker.com/install/linux/docker-ce/ubuntu>` 

#### docker启动参数配置

为docker做如下配置：

```
$ sudo  tee /etc/docker/daemon.json <<EOF
{
    "registry-mirrors": ["https://registry.aliyuncs.com"],
    "iptables": false,
    "ip-masq": false,
    "storage-driver": "overlay2",
    "graph": "/var/lib/docker"
}
EOF

$  sudo systemctl restart docker
```

上述行配置功能：

设置阿里云镜像库加速dockerhub的镜像。国内访问dockerhub不稳定，将对dockerhub的镜像拉取代理到阿里云镜像库
配上1.3.2的禁用iptables的设置
如果想让podIP可路由的话，设置docker不再对podIP做MASQUERADE，否则docker会将podIP这个源地址SNAT成nodeIP
设置docker存储驱动为overlay2（需要linux kernel版本在4.0以上，docker版本大于1.12）
根据业务规划修改容器实例存储根路径（默认路径是/var/lib/docker）

备注：

> docker的所有启动参数可见：<https://docs.docker.com/engine/reference/commandline/dockerd/>



### 2.2 安装kubeadm,kubectl,kubelet(所有节点执行)

#### 创建kubernetes的repo

创建kubernetes的source文件：

```
$ sudo  curl -s  https://mirrors.aliyun.com/kubernetes/apt/doc/apt-key.gpg | sudo apt-key add -

$  sudo tee /etc/apt/sources.list.d/kubernetes.list <<EOF
deb https://mirrors.aliyun.com/kubernetes/apt kubernetes-xenial main
EOF

$  sudo apt-get update
```

备注：

> 虽然ubuntu版本是18.04.1（bionic），但k8s的apt包目前还没支持到这么高，使用xenial。 

#### 安装kubeadm/kubelet/kubectl

查看可用版本：

$ apt-cache madison kubelet

安装指定版本：

```
$  sudo apt-get install -y kubelet=1.12.0-00
$  sudo apt-mark hold kubelet=1.12.0-00
```

同理安装kubeadm/kubectl, 中间有依赖的包（kubernetes-cni?），也按照提示版本进行安装;

设置开机自启动

```
$  sudo systemctl enable kubelet && sudo systemctl start kubelet
```

备注：

> 此时kubelet的服务运行状态是异常的(因为缺少主配置文件kubelet.conf等，可以暂不处理，因为在完成Master节点的初始化后才会生成这个配置文件）
>
> 本章节参考: https://kubernetes.io/docs/setup/independent/install-kubeadm/

### 2.3  提前下载k8s镜像（master和slave分别执行）

#### 查看目标版本kubernetes需要哪些镜像

```
$ sudo kubeadm config images list --kubernetes-version=v1.12.0
k8s.gcr.io/kube-apiserver:v1.12.0
k8s.gcr.io/kube-controller-manager:v1.12.0
k8s.gcr.io/kube-scheduler:v1.12.0
k8s.gcr.io/kube-proxy:v1.12.0
k8s.gcr.io/pause:3.1
k8s.gcr.io/etcd:3.2.24
k8s.gcr.io/kcoredns:1.2.2
```

<u>因为gcr.io被墙，所以通过kubeadm引导安装的时候无法正确下载到需要的镜像，所以要提前下载；</u>

#### master节点从其他镜像源下载k8s组件：

使用阿里云的镜像源下载，版本使用之前查到的版本

```
$  docker pull registry.aliyuncs.com/google_containers/kube-apiserver:v1.12.0
$  docker pull registry.aliyuncs.com/google_containers/kube-controller-manager:v1.12.0
$  docker pull registry.aliyuncs.com/google_containers/kube-scheduler:v1.12.0
$  docker pull registry.aliyuncs.com/google_containers/kube-proxy:v1.12.0
$  docker pull registry.aliyuncs.com/google_containers/pause:3.1
$  docker pull registry.aliyuncs.com/google_containers/etcd:3.2.24
$  docker pull registry.aliyuncs.com/google_containers/coredns:1.2.2
```

重新打成k8s.gcr.io的tag

```
$  docker tag registry.aliyuncs.com/google_containers/kube-apiserver:v1.12.0 k8s.gcr.io/kube-apiserver:v1.12.0
$  docker tag registry.aliyuncs.com/google_containers/kube-controller-manager:v1.12.0 k8s.gcr.io/kube-controller-manager:v1.12.0
$  docker tag registry.aliyuncs.com/google_containers/kube-scheduler:v1.12.0 k8s.gcr.io/kube-scheduler:v1.12.0
$  docker tag registry.aliyuncs.com/google_containers/kube-proxy:v1.12.0 k8s.gcr.io/kube-proxy:v1.12.0
$  docker tag registry.aliyuncs.com/google_containers/pause:3.1 k8s.gcr.io/pause:3.1
$  docker tag registry.aliyuncs.com/google_containers/etcd:3.2.24 k8s.gcr.io/etcd:3.2.24
$  docker tag registry.aliyuncs.com/google_containers/coredns:1.2.2 k8s.gcr.io/coredns:1.2.2
```

删除原镜像

```
$  docker rmi registry.aliyuncs.com/google_containers/kube-apiserver:v1.12.0
$  docker rmi registry.aliyuncs.com/google_containers/kube-controller-manager:v1.12.0
$  docker rmi registry.aliyuncs.com/google_containers/kube-scheduler:v1.12.0
$  docker rmi registry.aliyuncs.com/google_containers/kube-proxy:v1.12.0
$  docker rmi registry.aliyuncs.com/google_containers/pause:3.1
$  docker rmi registry.aliyuncs.com/google_containers/etcd:3.2.24
$  docker rmi registry.aliyuncs.com/google_containers/coredns:1.2.2
```

或者通过脚本方式完成上述步骤

```
!/bin/bash

ARCH=amd64
version=v1.12.0
username=registry.aliyuncs.com/google_containers
targername=k8s.gcr.io

kubeadm config images list --kubernetes-version=$version
sed -i "s@${targetname}@${username}@g"  images.txt
cat images.txt | while read line
do
	docker pull $line
done

sed -i "s@${username}@@" images.txt
do 
	docker tag ${username}${line} ${targetname}${line}
	docker rmi -f ${username}${line}
done

docker images
rm -rf images.txt
```

#### slave节点从master节点拷贝镜像

slave需要从master拷贝如下镜像

```
k8s.gcr.io/kube-proxy:v1.12.0
k8s.gcr.io/kube-scheduler:v1.12.0
k8s.gcr.io/pause:3.1
k8s.gcr.io/coredns:1.2.2
```

先在master节点将镜像打包成文件：

```
$  docker save -o kube-proxy.tar  k8s.gcr.io/kube-proxy:v1.12.0
$  docker save -o kube-scheduler.tar  k8s.gcr.io/kube-scheduler:v1.12.0
$  docker save -o pause.tar  k8s.gcr.io/pause:3.1
$  docker save -o coredns.tar  k8s.gcr.io/coredns:v1.2.2
```

然后通过scp命令将save到的tar文件传输到slave节点;

最后在slave节点加载这些镜像：

```
$  docker load < kube-proxy.tar
$  docker load < kube-scheduler.tar
$  docker load < pause.tar
$  docker load < coredns.tar
```

可以通过`docker images`查看有没有正确加载。

## 三、k8s集群搭建

### 3.1 集群初始化以及配置参数（master上执行）

#### 命令行方式初始化集群

```
$  kubeadm init  --apiserver-advertise-address=192.168.0.204 --pod-network-cidr=10.16.0.0/16 --service-cidr=10.233.0.0/16 --kubernetes-version=1.12.0
```

--apiserver-advertise-address参数，要使用master本机网卡IP；

--pod-network-cidr参数，要求与网卡ip网段不相同，即网卡ip是192.169.0.0网段，该参数就不能使用同网段，该参数在后续安装calico组件时会使用到；

--service-cidr参数，用于在k8s上启动服务时的网段；

--kubernetes-version参数，指定版本编号；

> 配置文件写法参考： <https://unofficial-kubernetes.readthedocs.io/en/latest/admin/kubeadm/> <http://pwittrock.github.io/docs/admin/kubeadm/> 

部署成功会输出下述内容：

```
Your Kubernetes master has initialized successfully!

To start using your cluster, you need to run the following as a regular user:

  mkdir -p $HOME/.kube
  sudo cp -i /etc/kubernetes/admin.conf $HOME/.kube/config
  sudo chown $(id -u):$(id -g) $HOME/.kube/config

You should now deploy a pod network to the cluster.
Run "kubectl apply -f [podnetwork].yaml" with one of the options listed at:
  https://kubernetes.io/docs/concepts/cluster-administration/addons/

You can now join any number of machines by running the following on each node
as root:

  kubeadm join 192.168.0.204:6443 --token v1nj22.l30dctzysf2jynly --discovery-token-ca-cert-hash sha256:0170607e7e069ffde2f2b6b440e7982f066887e59db49e9a62ac9518924af690
```

记下最后一行输出的token和token-hash, 在slave加入集群时使用；

备注：

> 集群初始化如果遇到问题，可以使用下面的命令进行清理再重新初始化：
> sudo kubeadm reset

#### 创建kubectl使用的kubeconfig文件

```
$  mkdir -p $HOME/.kube
$  sudo cp -i /etc/kubernetes/admin.conf $HOME/.kube/config
$  sudo chown $(id -u):$(id -g) $HOME/.kube/config
```

创建完成即可使用kubectl操作集群。 

#### 设置master参与工作负载

使用kubeadm初始化的集群，将master节点做了taint（污点），使得默认情况下（不设置容忍）Pod不会被调度到master上。

这里搭建的是测试环境可以使用下面的命令去掉master的taint，使master参与工作负载：

```
$ kubectl taint nodes --all node-role.kubernetes.io/master-
```

#### 设置kubelet使用的cgroup driver

kubelet启动时指定的cgroup driver需要和docker所使用的保持一致。

查看 Docker 使用的 cgroup driver:

```
$ docker info | grep -i cgroup
-> Cgroup Driver: cgroupfs 
```

可以看出docker 默认使用的Cgroup Driver为cgroupfs。

查看kubelet指定的cgroup driver

Kubernetes文档中kubelet的启动参数--cgroup-driver string Driver that the kubelet uses to manipulate cgroups on the host. Possible values: 'cgroupfs', 'systemd' (default "cgroupfs")。默认值为cgroupfs。yum安装kubelet、kubeadm时生成10-kubeadm.conf文件中可能将这个参数值改成了systemd。

查看kubelet的配置文件（1.12.0版本的封装在/var/lib/kubelet/kubeadm-flags.env文件中），如果是默认的cgroupfs，不需要修改。否则需要修改/etc/default/kubelet（或者/var/lib/kubelet/kubeadm-flags.env）文件：

```
$ sudo vim /etc/default/kubelet
    KUBELET_KUBEADM_EXTRA_ARGS=--cgroup-driver=<value>    
$ sudo systemctl daemon-reload
$ sudo systemctl restart kubelet
```

> 参考：https://kubernetes.io/docs/setup/independent/install-kubeadm/#configure-cgroup-driver-used-by-kubelet-on-master-node

#### 网络部署，安装POD网络插件（calico）

将kubectl配置成RBAC的方式

```
$  wget https://docs.projectcalico.org/v3.1/getting-started/kubernetes/installation/hosted/rbac-kdd.yaml
$  kubectl apply -f rbac-kdd.yaml
```

安装calico,先下载初始配置文件，将文件中的pod网段设置成kubeadm引导时设置的pod-network-cidr参数指定的网段；

```
$  wegt https://docs.projectcalico.org/v3.1/getting-started/kubernetes/installation/hosted/kubernetes-datastore/calico-networking/1.7/calico.yaml

$  vim calico.yaml
     - name: CALICO_IPV4POOL_CIDR
       value:  10.16.0.0/16  #默认值为192.168.0.0/16，需要修改成pod指定网段；

$  kubectl apply -f calico.yaml
```

重点关注`calico-node`和`coredns`,如果都处于running状态，说明正常，至此master初始化集群完成。

```
root@SZX1000471670:/home/tools/k8s# kubectl get pods --all-namespaces
NAMESPACE     NAME                                    READY     STATUS    RESTARTS   AGE
kube-system   calico-node-qs94t                       2/2       Running   0          2m
kube-system   etcd-szx1000471670                      1/1       Running   0          4h
kube-system   kube-apiserver-szx1000471670            1/1       Running   0          4h
kube-system   kube-controller-manager-szx1000471670   1/1       Running   0          4h
kube-system   coredns-6f4fd4bdf-6dkkl                 1/1       Running   0          1m
kube-system   kube-proxy-9t4jx                        1/1       Running   0          4h
kube-system   kube-scheduler-szx1000471670            1/1       Running   0          4h
root@SZX1000471670:/home/chenhq/tools/k8s#
```

如果发现coredns一直在CrashBackoff,running,error中变化，说明coredns启动失败；

通过下述两条命令查看信息

```
$  kubectl get pods --all-namespaces –o wide 
$  kubectl logs  -n kube-system  $corens_name 
```

最终解决流程：

```
$  kubectl -n kube-system edit configmap coredns
#将其中的loop注释掉

$  kubectl -n kube-system delete pod -l k8s-app=kube-dns  #重启pod内dns
```

备注：

> 解决办法参考：
>
>  https://johng.cn/minikube-fatal-pluginloop-seen/
>
> <https://stackoverflow.com/questions/53075796/coredns-pods-have-crashloopbackoff-or-error-state>

### 3.2 slave节点加入集群

确保slave上已经下载到kube-proxy等k8s镜像，以及kubeclt、kubeadm、kubelet等工具。

将master节点上的/etc/kubernetes/pki/etcd拷贝到slave相同位置，用于calico-node连接etcd使用，然后在slave节点上执行以下命令即可加入集群

```
$  Kubeadm join masterip:6443 --token=xxxxxx –-discovery-token-ca-cert-hash=sha256:xxxx
```

如果忘记token，可以到master节点上执行命令进行查看

```
$ kubeadm  token list   
```

如果需要重新生成密钥token:

(默认token的有效期为24小时，当过期之后，该token就不可用了, 设置–ttl=0代表永不过期)

```
Token=$(Kubeadm token generate)
$  Kubeadm token create $token --print-join-command  --ttl=0
```

【注意】此命令生成的join命令不能直接执行，要“--token+空格+value”改成--token=value”的格式，其它参数也要修改；

 备注：

> 如果添加失败，或者状态异常，需要踢出节点
>
> Master上操作：
>
> ```
> $  Kubectl get node
> $  Kuectl drain xxxname --delete-local-data --force --ignore-daemonsets
> $  Kubectl delete node xxxname
> ```
>
> 然后在被踢出的slave上操作：
>
> ```
> $  Kubeadm reset
> ```

#### 3.3 查看状态

master上执行：所有节点都ready状态

```
$  kubectl get node
```

