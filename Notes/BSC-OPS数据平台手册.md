BSC-OPS数据平台手册



[TOC]





## 1 测试环境

IP:**10.226.98.58**

内存信息

> MemTotal:       24522644 kB (24G)   

| 内存 | 物理CPU | CPU内核  | 逻辑CPU | /var （/dev/mapper/rootvg-var） | /apps （/dev/mapper/appvg-applv） |      |
| :--: | ------- | -------- | ------- | ------------------------------- | --------------------------------- | ---- |
| 24G  | 3       | 6 （2x3) | 6       | 25G                             | 100G （(正在用的)）               |      |



## 2. OS version

-bash-4.2$ lsb_release -a
LSB Version:    :core-4.1-amd64:core-4.1-noarch:cxx-4.1-amd64:cxx-4.1-noarch:desktop-4.1-amd64:desktop-4.1-noarch:languages-4.1-amd64:languages-4.1-noarch:printing-4.1-amd64:printing-4.1-noarch
Distributor ID: CentOS
Description:    CentOS Linux release 7.9.2009 (Core)
Release:        7.9.2009
Codename:       Core

![image-20211130135601008](BSC-OPS%E6%95%B0%E6%8D%AE%E5%B9%B3%E5%8F%B0%E6%89%8B%E5%86%8C.assets/image-20211130135601008.png)

```
docker-ce-20.10.7

sudo yum install docker-ce-20.10.7 docker-ce-cli-20.10.7 containerd.io



```

## 3. Tableau Desktop 

Tableau Desktop is a visual exploration and analysis application, where users connect to data, create dashboards, and publish content to Tableau Server.You have been assigned a license key for Tableau Desktop. Below are resources to get started with Tableau Desktop.Visit [Tableau @BSC](https://bostonscientific.sharepoint.com/sites/TableauBSC) for [learning](https://bostonscientific.sharepoint.com/sites/TableauBSC/SitePages/Learning.aspx), [support](https://bostonscientific.sharepoint.com/sites/TableauBSC/SitePages/Support.aspx), and [community](https://bostonscientific.sharepoint.com/sites/TableauBSC/SitePages/Community.aspx) resources for Tableau.

| **Install**  | Complete these steps to install Tableau Desktop:Please make sure that your laptop is connected to VPNGo to **Windows button->All Apps->Microsoft Endpoint Manager ->Software Center** or search **Software Center** in your laptopSearch for **tableau** (upper right corner) under **Application** tabSelect **Tableau Desktop 2020.2.4** and click **Install** |
| ------------ | ------------------------------------------------------------ |
| **Activate** | Complete these steps to activate Tableau Desktop:Open **Tableau Desktop**Select **Help > Manage Product Keys** and then click **Activate****Assigned User:** Jia, Wen [Wen.Jia@bsci.com](mailto:Wen.Jia@bsci.com)     **Assigned Key:** TDQU-B21B-95F0-0377-422B**Do not share or activate this license on more than one machine.** |
| **Learning** | The following training resources are available for Tableau Desktop:· [Online Help](https://help.tableau.com/current/pro/desktop/en-us/default.htm)· [Tableau @BSC](https://bostonscientific.sharepoint.com/sites/TableauBSC/SitePages/Learning-Tableau-Desktop.aspx)· [Training Videos](https://www.tableau.com/learn/training)·  [Live Training](https://www.tableau.com/learn/live-training) |
| **Support**  | You've got questions? We've got answers!Visit [Tableau Support](https://bostonscientific.sharepoint.com/sites/TableauBSC/SitePages/Support.aspx) for details on how to submit a ticket |

## 4 Docker 配置信息

### 4.1 Create CONTAINER

```dockerfile
docker run -itd --net=bsc-br -p 8081:8081 -p 8080:8080 -p 8089:8089 -p 8443:8443 -p 9870:9870 -p 9868:9868 -p 9864:9864 -p 8088:8088 -p 10000:10000  --name bsc-ops-01 --hostname hadoop-master -p 10028:22 bsc-ops:3

docker run -itd --net=bsc-br --name hadoop-slave1 --hostname hadoop-slave1 -p 10027:22 bsc-ops:3
docker run -itd --net=bsc-br --name hadoop-slave2 --hostname hadoop-slave2 -p 10026:22 bsc-ops:3

scp -r /opt/module/azkaban/azkaban-exec hadoop-slave1:/opt/module/azkaban/
scp -r /opt/module/azkaban/azkaban-exec hadoop-slave2:/opt/module/azkaban/

```

### 4.2 Configure container

```shell
#1 删除旧数据
cd $HADOOP_HOME/data
rm -rf dfs  #删除旧数据
#2 初始化namenode
hdfs namenode -format 

#3 启动所有的节点
start-all.sh 
#4 查看进程
jps 
3108 SecondaryNameNode
3752 Jps
2699 NameNode
3563 NodeManager
2847 DataNode

#5 启动HIVE Service2
hiveservices.sh start
hiveservices.sh status

#6 q
```

### 4.3 Hadoop 空间不足

```
检查总体情况
 hadoop dfsadmin -report
检查每个目录
 hdfs dfs -du -h /
[root@hadoop-master hadoop]#  hdfs dfs -du -h /
2.4 G  2.4 G  /bsc
597    597    /system
1.2 G  6.7 G  /tmp
0      0      /user
如果tmp文件过大，可以删除用下面命令：
hdfs dfs -rm -f -r /tmp
hdfs dfs -mkdir /tmp
  
还可以给用户设置空间配额，避免每个用户占用的空间过大 
  #设置配额
  hdfs dfsadmin -setSpaceQuota 2G /user/tom
  #清除配额
  hdfs dfsadmin -clrSpaceQuota /user/tom
  #察看配额
  hdfs dfs -count -q -v  /user/tom
  

 # 罗列出当前目录文件所占磁盘大小
   du -sh *
 docker system df
 
 du -hsx * | sort -rh | head -10
 
```

## 5 Hadoop 平台常用命令

- ### start-all.sh

  启动hadoop 平台的name,data,yarn 节点服务

  ```sh
  [root@hadoop-master hadoop]# start-all.sh
  WARNING: HADOOP_SECURE_DN_USER has been replaced by HDFS_DATANODE_SECURE_USER. Using value of HADOOP_SECURE_DN_USER.
  Starting namenodes on [hadoop-master]
  Last login: Thu Jul  8 12:48:29 CST 2021 on pts/1
  Starting datanodes
  Last login: Thu Jul  8 12:49:17 CST 2021 on pts/1
  Starting secondary namenodes [hadoop-master]
  Last login: Thu Jul  8 12:49:19 CST 2021 on pts/1
  Starting resourcemanager
  Last login: Thu Jul  8 12:49:22 CST 2021 on pts/1
  Starting nodemanagers
  Last login: Thu Jul  8 12:49:26 CST 2021 on pts/1
  ```

- ### stop-all.sh

  停止namenodes，datanodes，secondary namenodes，resourcemanager，nodemanagers等节点服务

- ### hiveservices.sh start

  启动 Metastore 服务和HiveServer2 服务

- ### hiveservices.sh stop

  停止Metastore 服务和HiveServer2 服务

- ### hiveservices.sh status

  查看Metastore 服务和HiveServer2 服务状态

  ```sh
  [root@hadoop-master hadoop]# hiveservices.sh start
  Metastroe 服务已启动
  HiveServer2 服务已启动
  ```

- ### Azkaban 启动与关闭

  启动顺序不能改变：Executor服务---->web服务

  - 启动与关闭Executor服务

    ```sh
    #启动并激活Executor 服务
    cd /opt/module/azkaban/azkaban-exec
    bin/start-exec.sh
    #激活
    curl -G "hadoop-master:12321/executor?action=activate" && echo
    下面的命令在spark环境执行
    curl -G "spark-master:12321/executor?action=activate" && echo
    #停止Executor 服务
    bin/shutdown-exec.sh 
    ```
  
  - 启动与关闭web服务
  
    ```sh
    cd /opt/module/azkaban/azkaban-web
    bin/start-web.sh #启动
    bin/shutdown-web.sh #关闭
    ```



   - ### **Dolphinscheduler** 启动与关闭

~~~sh
cd /opt/module/dolphin

# 启动 Standalone Server 服务
sh ./bin/dolphinscheduler-daemon.sh start standalone-server
# 停止 Standalone Server 服务
sh ./bin/dolphinscheduler-daemon.sh stop standalone-server
```
~~~



## 6 日常监控

### 6.1 Cluster运行状态

访问地址：http://10.226.98.58:8088/

![image-20210708170541938](BSC-OPS%E6%95%B0%E6%8D%AE%E5%B9%B3%E5%8F%B0%E6%89%8B%E5%86%8C.assets/image-20210708170541938.png)

**Applications**

![image-20210708171134179](BSC-OPS%E6%95%B0%E6%8D%AE%E5%B9%B3%E5%8F%B0%E6%89%8B%E5%86%8C.assets/image-20210708171134179.png)



### 6.2 Namenode 状态

访问地址：http://10.226.98.58:9870/

![image-20210708171421328](BSC-OPS%E6%95%B0%E6%8D%AE%E5%B9%B3%E5%8F%B0%E6%89%8B%E5%86%8C.assets/image-20210708171421328.png)

**Datanodes**

![image-20210708171550121](BSC-OPS%E6%95%B0%E6%8D%AE%E5%B9%B3%E5%8F%B0%E6%89%8B%E5%86%8C.assets/image-20210708171550121.png)

### 6.3 库文件状态

地址：[Browsing HDFS](http://10.226.98.58:9870/explorer.html#/bsc/opsdw)

![image-20210708172053191](BSC-OPS%E6%95%B0%E6%8D%AE%E5%B9%B3%E5%8F%B0%E6%89%8B%E5%86%8C.assets/image-20210708172053191.png)

### 6.4 Azkaban 状态

访问地址：[Azkaban Web Client](http://10.226.98.58:8081/index)

**历史执行记录**

![image-20210708173307143](BSC-OPS%E6%95%B0%E6%8D%AE%E5%B9%B3%E5%8F%B0%E6%89%8B%E5%86%8C.assets/image-20210708173307143.png)

**Job 状态**

![image-20210708173448572](BSC-OPS%E6%95%B0%E6%8D%AE%E5%B9%B3%E5%8F%B0%E6%89%8B%E5%86%8C.assets/image-20210708173448572.png)[Data Life Cyle](http://10.226.98.58:8081/manager?project=Data_Life_Cycle&flow=data_life_cycle)

![image-20210708173740113](BSC-OPS%E6%95%B0%E6%8D%AE%E5%B9%B3%E5%8F%B0%E6%89%8B%E5%86%8C.assets/image-20210708173740113.png)

**扩展后查看各个节点运行情况**

![image-20210708173825429](BSC-OPS%E6%95%B0%E6%8D%AE%E5%B9%B3%E5%8F%B0%E6%89%8B%E5%86%8C.assets/image-20210708173825429.png)

## 7 环境配置过程

环境配置基于Docker + Hadoop + Hive平台

### 7.1 Build SSH image

**Dockerfile**

```
FROM centos:7
MAINTAINER donnychen(donnych@wicrenet.com)

RUN yum install -y openssh-server sudo
RUN sed -i 's/UsePAM yes/UsePAM no/g' /etc/ssh/sshd_config
RUN yum  install -y openssh-clients

RUN echo "root:1qazxsw2" | chpasswd 
RUN echo "root   ALL=(ALL)       ALL" >> /etc/sudoers
RUN ssh-keygen -t dsa -f /etc/ssh/ssh_host_dsa_key
RUN ssh-keygen -t rsa -f /etc/ssh/ssh_host_rsa_key

RUN mkdir /var/run/sshd
EXPOSE 22
CMD ["/usr/sbin/sshd", "-D"]
```

Docker build

```bash
docker build -t="bsc-ssh" . #镜像名
```

### 7.2 Install JDK & Hadoop & Hive

**Dockerfile**

```
# container 
docker run -d -p 10023:22 bsc-ssh:latest /usr/sbin/sshd -D
```

**Install JDK**

```
tar -zxvf jdk-8u281-linux-x64.tar.gz -C /opt/module/

vim /etc/profile.d/bsc_env.sh

#JAVA_HOME
export JAVA_HOME=/opt/module/jdk18
export PATH=$PATH:$JAVA_HOME/bin

source /etc/profile.d/bsc_env.sh
```

**Install Hadoop**

```
tar -zxvf hadoop-3.1.4.tar.gz -C /opt/module
mv hadoop-3.1.4/ hadoop3

vim /etc/profile.d/bsc_env.sh

#HADOOP_HOME 
export HADOOP_HOME=/opt/module/hadoop3
export PATH=$PATH:$HADOOP_HOME/bin
export PATH=$PATH:$HADOOP_HOME/sbin

source /etc/profile.d/bsc_env.sh
```

**Install Hive**

```
tar -zxvf apache-hive-3.1.2-bin.tar.gz -C /opt/module
mv apache-hive-3.1.2-bin/ hive3

vim /etc/profile.d/bsc_env.sh

#HIVE_HOME 
export HIVE_HOME=/opt/module/hive3
export PATH=$PATH:$HIVE_HOME/bin

source /etc/profile.d/bsc_env.sh

```

**install Sqoop**

```
tar -zxvf sqoop-1.4.7.tar.gz -C /opt/module
mv sqoop-1.4.7/ sqoop
vim /etc/profile.d/bsc_env.sh
#SQOOP_HOME 
export SQOOP_HOME=/opt/module/sqoop
export PATH=$PATH:$SQOOP_HOME/bin

source /etc/profile.d/bsc_env.sh

sqoop list-databases --connect jdbc:mysql://172.23.128.1:3306/ --username root --password 1234567
```

### 7.3 Configure cluster

- **core-site.xml**

  ```xml
  cd /opt/module/hadoop3/etc/hadoop/
   vim core-site.xml
   
   <property>
          <name>fs.defaultFS</name>
          <value>hdfs://hadoop-master:8020</value>
      </property>
      <property>
          <name>hadoop.tmp.dir</name>
          <value>/opt/module/hadoop3/data</value>
      </property>
      <property>
          <name>io.file.buffer.size</name>
          <value>131702</value>
      </property>
  	<property>
          <name>hadoop.proxyuser.root.hosts</name>
          <value>*</value>
      </property>
      <property>
          <name>hadoop.proxyuser.root.groups</name>
          <value>*</value>
      </property>
      <property>
   <name>io.compression.codecs</name>
   <value>
   org.apache.hadoop.io.compress.GzipCodec,
   org.apache.hadoop.io.compress.DefaultCodec,
   org.apache.hadoop.io.compress.BZip2Codec,
   org.apache.hadoop.io.compress.SnappyCodec,
   com.hadoop.compression.lzo.LzoCodec,
   com.hadoop.compression.lzo.LzopCodec
   </value>
   </property>
   <property>
   <name>io.compression.codec.lzo.class</name>
   <value>com.hadoop.compression.lzo.LzoCodec</value>
   </property>
  ```

  **hdfs-site.xml**

  ```xml
  cd /opt/module/hadoop3/etc/hadoop
  vim hdfs-site.xml
  <property>
   <name>dfs.namenode.http-address</name>
   <value>hadoop-master:9870</value>
   </property>
   
   <property>
   <name>dfs.namenode.secondary.http-address</name>
   <value>hadoop-slave2:9868</value>
   </property>
   
   <property>
   <name>dfs.replication</name>
   <value>2</value>
   </property>
   
   <property>
     <name>dfs.webhdfs.enabled</name>
     <value>true</value>
   </property>
   
   <property>
      <name>dfs.client.use.datanode.hostname</name>
      <value>true</value>
      <description>Whether clients should use datanode hostnames when
        connecting to datanodes.
      </description>
  </property>
  ```

  **yarn-site.xml**

```xml
vim yarn-site.xml
 <property>
 <name>yarn.nodemanager.aux-services</name>
 <value>mapreduce_shuffle</value>
 </property>
 
 <property>
 <name>yarn.resourcemanager.hostname</name>
 <value>hadoop-slave1</value>
 </property>
 
 <property>
 <name>yarn.nodemanager.env-whitelist</name>
<value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CO
NF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_MAPRED_HOME</value>
 </property>
 
 <property>
 <name>yarn.scheduler.minimum-allocation-mb</name>
 <value>1024</value>
 </property>
 <property>
 <name>yarn.scheduler.maximum-allocation-mb</name>
 <value>8192</value>
 </property>
 
 <property>
 <name>yarn.nodemanager.resource.memory-mb</name>
 <value>10240</value>
 </property>
 
 <property>
 <name>yarn.nodemanager.pmem-check-enabled</name>
 <value>false</value>
 </property>
 <property>
 <name>yarn.nodemanager.vmem-check-enabled</name>
 <value>false</value>
 </property>
```

内存分配时注意可以分配内存的80%：

![image-20210708175316897](BSC-OPS%E6%95%B0%E6%8D%AE%E5%B9%B3%E5%8F%B0%E6%89%8B%E5%86%8C.assets/image-20210708175316897.png)

**mapred-site.xml**

```
vim mapred-site.xml
  <property>
 <name>mapreduce.framework.name</name>
 <value>yarn</value>
 </property>

 <property>
 <name>mapreduce.jobhistory.address</name>
 <value>hadoop-master:10020</value>
</property>

 <property>
 <name>mapreduce.jobhistory.webapp.address</name>
 <value>hadoop-master:19888</value>
</property>
```

**hive-site.xml**

```
vim $HIVE_HOME/conf/hive-site.xml
<property>

<name>javax.jdo.option.ConnectionURL</name>

<value>jdbc:mysql://localhost:3306/hive_hdp?characterEncoding=UTF-8&createDatabaseIfNotExist=true</value>

<description>JDBC connect string for a JDBC metastore</description>

</property>
bscopsdw?characterEncoding=UTF-8&createDatabaseIfNotExist=true

<!-- 指定 hiveserver2 连接的 host -->
 <property>
 <name>hive.server2.thrift.bind.host</name> 
 <value>hadoop-master</value>
 </property>
 <!-- 指定 hiveserver2 连接的端口号 -->
 <property>
 <name>hive.server2.thrift.port</name>
 <value>10000</value>
 </property
```

### 7.4 配置 workers

```
vim $HADOOP_HOME/etc/hadoop/workers
hadoop-master
```

###  7.5 NameNode格式化

```
hdfs namenode -format
```

### 7.6 启动HDFS

```
vi start-dfs.sh#第二行添加如下4句
vi stop-dfs.sh#第二行添加如下4句
HDFS_DATANODE_USER=root
HADOOP_SECURE_DN_USER=hdfs
HDFS_NAMENODE_USER=root
HDFS_SECONDARYNAMENODE_USER=root

vi start-yarn.sh#第二行添加如下3句
vi stop-yarn.sh#第二行添加如下3句
YARN_RESOURCEMANAGER_USER=root
HADOOP_SECURE_DN_USER=yarn
YARN_NODEMANAGER_USER=root

 start-dfs.sh
 [root@hadoop-master ~]# start-dfs.sh
WARNING: HADOOP_SECURE_DN_USER has been replaced by HDFS_DATANODE_SECURE_USER. Using value of HADOOP_SECURE_DN_USER.
Starting namenodes on [hadoop-master]
Last login: Wed Apr 14 14:52:31 UTC 2021 from gateway on pts/7
Starting datanodes
Last login: Wed Apr 14 14:53:04 UTC 2021 on pts/7
hadoop-slave1: WARNING: /opt/module/hadoop3/logs does not exist. Creating.
hadoop-slave2: WARNING: /opt/module/hadoop3/logs does not exist. Creating.
Starting secondary namenodes [hadoop-slave2]
Last login: Wed Apr 14 14:53:06 UTC 2021 on pts/7
[root@hadoop-master ~]# 

[root@hadoop-slave1 sbin]# start-yarn.sh
Starting resourcemanager
Last login: Wed Apr 14 14:36:27 UTC 2021 from gateway on pts/7
Starting nodemanagers
Last login: Wed Apr 14 14:56:49 UTC 2021 on pts/7
hadoop-slave1: Warning: Permanently added 'hadoop-slave1,172.18.0.2' (RSA) to the list of known hosts.
hadoop-slave1: Permission denied (publickey,gssapi-keyex,gssapi-with-mic,password).
[root@hadoop-slave1 sbin]#

```

### 7.7  Hive 初始化

```
vim $HIVE_HOME/conf/hive-site.xml
schematool -initSchema -dbType mysql --verbose
jdbc:mysql://172.23.128.1:3306/bscopsdw?characterEncoding=UTF-8&amp;createDatabaseIfNotExist=true&amp;useSSL=false


schematool -initSchema -dbType postg --verbose
```

### 7.8 **hiveservices shell script**

```
vim $HIVE_HOME/bin/hiveservices.sh

#!/bin/bash
HIVE_LOG_DIR=$HIVE_HOME/logs
if [ ! -d $HIVE_LOG_DIR ]
then
mkdir -p $HIVE_LOG_DIR
fi
#检查进程是否运行正常，参数 1 为进程名，参数 2 为进程端口
function check_process()
{
 pid=$(ps -ef 2>/dev/null | grep -v grep | grep -i $1 | awk '{print 
$2}')
 ppid=$(netstat -nltp 2>/dev/null | grep $2 | awk '{print $7}' | cut -
d '/' -f 1)
 echo $pid
 [[ "$pid" =~ "$ppid" ]] && [ "$ppid" ] && return 0 || return 1
}
function hive_start()
{
 metapid=$(check_process HiveMetastore 9083)
 cmd="nohup hive --service metastore >$HIVE_LOG_DIR/metastore.log 2>&1 
&"
 [ -z "$metapid" ] && eval $cmd || echo "Metastroe 服务已启动"
 server2pid=$(check_process HiveServer2 10000)
 cmd="nohup hiveserver2 >$HIVE_LOG_DIR/hiveServer2.log 2>&1 &"
 [ -z "$server2pid" ] && eval $cmd || echo "HiveServer2 服务已启动"
}
function hive_stop()
{
metapid=$(check_process HiveMetastore 9083)
 [ "$metapid" ] && kill $metapid || echo "Metastore 服务未启动"
 server2pid=$(check_process HiveServer2 10000)
 [ "$server2pid" ] && kill $server2pid || echo "HiveServer2 服务未启动"
}
case $1 in
"start")
 hive_start
 ;;
"stop")
 hive_stop
 ;;
"restart")
 hive_stop
 sleep 2
 hive_start
 ;;
"status")
 check_process HiveMetastore 9083 >/dev/null && echo "Metastore 服务运行
正常" || echo "Metastore 服务运行异常"
 check_process HiveServer2 10000 >/dev/null && echo "HiveServer2 服务运
行正常" || echo "HiveServer2 服务运行异常"
 ;;
*)
 echo Invalid Args!
 echo 'Usage: '$(basename $0)' start|stop|restart|status'
 ;;
esac
```

**beeline测试连接：**

```sh
chmod 777 $HIVE_HOME/bin/hiveservices.sh
hiveservices.sh start

beeline -u jdbc:hive2://hadoop-master:10000 -n root -p 1qazxsw2
172.18.0.2
```

### **7.9 相关软件列表**

存放服务器地址： /opt/software

```sh
[root@hadoop-master software]# pwd
/opt/software
[root@hadoop-master software]# ll
total 1344448
-rw-r--r--. 1 root root 312850286 Nov 23  2020 apache-hive-3.1.2-bin.tar.gz
-rw-r--r--. 1 root root  62945274 Apr 20 12:57 apache-tez-0.9.2-bin.tar.gz
-rw-r--r--. 1 root root      6433 Mar 31 10:10 azkaban-db-3.84.4.tar.gz
-rw-r--r--. 1 root root  16175002 Apr  1 09:19 azkaban-exec-server-3.84.4.tar.gz
-rw-r--r--. 1 root root  20239974 Apr  1 09:19 azkaban-web-server-3.84.4.tar.gz
-rw-r--r--. 1 root root 348326890 Apr  7 23:10 hadoop-3.1.4.tar.gz
-rw-r--r--. 1 root root 143722924 Apr  7 22:31 jdk-8u281-linux-x64.tar.gz
-rw-r--r--. 1 root root   1006904 Apr 20  2020 mysql-connector-java-5.1.49.jar
-rw-r--r--. 1 root root 224453229 Apr  9 22:45 spark-3.0.0-bin-hadoop3.2.tgz
-rw-r--r--. 1 root root 156791324 Apr  9 22:45 spark-3.0.0-bin-without-hadoop.tgz
-rw-r--r--. 1 root root  17953604 Apr 19 14:00 sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz
-rw-r--r--. 1 root root   1152112 Apr 19 13:18 sqoop-1.4.7.tar.gz
-rw-r--r--. 1 root root  18214958 Nov 23  2020 tez-0.10.1-SNAPSHOT-minimal.tar.gz
-rw-r--r--. 1 root root  52846364 Nov 23  2020 tez-0.10.1-SNAPSHOT.tar.gz
```

| Apache框架版本 |        |
| -------------- | ------ |
| 软件           | 版本   |
| Hadoop         | 3.1.4  |
| Hive           | 3.1.2  |
| Sqoop          | 1.4.7  |
| Java           | 1.8    |
| azkaban        | 3.84.4 |

## 8 环境迁移流程

### 8.1  安装Docker & MySQL

安装过程请参考官方文档 [Get Started with Docker | Docker](https://www.docker.com/get-started)

### 8.2 初始化Docker网络环境

```shell
docker network create --driver bridge bsc-br
docker network inspect bridge bsc-br
```

### 8.3.1导出镜像

```sh
#配置调度
#每月1号1点执行

crontab -e 0 1 1 * * sh /opt/corntab_wj/hive_corntab_backup.sh >>/opt/corntab_wj/tb.log 2>&1
#导出hive
nohup docker export -o /home/xul12/image_backup/bsc-dsr.tar 54739da1eefc   > /dev/null 2>&1 &
#再次导出hive
nohup docker export -o /home/xul12/image_backup/bsc-dsr-0603.tar 54739da1eefc   > /dev/null 2>&1 &
jobs
#导出pg镜像
nohup docker export -o /home/xul12/image_backup/postgres.tar 27decf81c34b   > /dev/null 2>&1 &
#导出mysql
nohup docker export -o /home/xul12/image_backup/mysql-dsr.tar 7ee128f90d4c    > /dev/null 2>&1 &
jobs
#mysql导出方法2
docker save -o /home/xul12/image_backup/mysql-dsr64.tar mysql:6.4	
```



### 8.3.2 导入镜像

```shell
#导入mysql镜像
docker import /home/xul12/image_backup/mysql-dsr.tar mysql-dsr:v1
#导入mysql镜像方法2
docker load -i /home/xul12/image_backup/mysql-dsr64.tar
#导入hive镜像
docker import /home/xul12/image_backup/bsc-dsr.tar bsc-dsr:v1
#导入pg镜像
docker import /home/xul12/image_backup/postgres.tar 
#确认导入成功
docker images


endport


```

### 8.4 启动容器及初始化

```sh
#hive容器
docker run -itd --net=bsc-br -p 28081:8081 -p 28080:8080 -p 28089:8089 -p 28443:8443 -p 29870:9870 -p 29868:9868 -p 29864:9864 -p 28088:8088 -p 20000:10000 --name bsc-ops-02 --hostname hadoop-master -p 10029:22 bsc-dsr:v1 /usr/sbin/sshd -D

#启动mysql的容器
docker run -itd --net=bsc-br --name mysql-dsr -p 3306:3306 -e MYSQL_ROOT_PASSWORD=1qazXSW@ mysql-dsr:v1 
docker-entrypoint.sh

#启动pg的容器
docker run -itd --name postgres  -p 55433:5432 -e POSTGRES_PASSWORD=1qazxsw2 postgres
#启动save方式的容器
docker run -itd --net=bsc-br --name mysql-dsr -p 3306:3306 -e MYSQL_ROOT_PASSWORD=1qazXSW@ mysql:6.4

#删除容器
docker stop 41681ff15a3d
docker rm 41681ff15a3d
#登陆mysqlr
 docker exec -it 88c2622e3a4b    /bin/bash 
  mysql -h localhost -u root -p
  
  
#发现数据不完整，找到源数据位置
 docker inspect mysql |grep Mounts -A 20
 sudo -i
cd /apps/docker/lib/docker/volumes/d1922a2c326283f1d545c1641647ee089d0abea7b71d1acae54f337ccb6af46d/_data
#复制数据到公盘
cp -r *  /home/xul12/image_backup/_date/
#找到目标数据位置
docker inspect 88c2622e3a4b
 sudo -i
 cd /apps/docker/lib/volumes/a9d3dfcd1c29df0d7a0d2a419292888cd10d4ede266f7db01d84c3cff041b19d/_data
 cp /home/xul12/image_backup/_date//apps/docker/lib/volumes/a9d3dfcd1c29df0d7a0d2a419292888cd10d4ede266f7db01d84c3cff041b19d/_data/
#hive数据不完整
#登陆新服务器
#检测有问题块
hdfs fsck / | egrep -v '^\.+$' | grep -v replica | grep -v Replica
#删除tmp文件
hdfs dfs -rm -skipTrash /tmp/hadoop-yarn/staging/history/done_intermediate/root/job_1653146332201_3723.summary
#还是不行
cp -r /opt/module/hadoop3/data/
#wangluo联通
hadoop distcp -skipcrccheck -update hdfs://10.226.98.58:8020/bsc hdfs://10.226.98.59:8020/bsc
hadoop distcp -skipcrccheck -update hdfs://linux121:9000/test hdfs://linux122:9000:/test
#网络不通
hdfs sdf -get /bsc
```

### 8.4.1hive元数据迁移

通过一个简单的shell脚本取出所有的表名和建表语句分别放入tables.txt和tablesDDL.txt

```sh
#!/bin/bash
#取出所有的表名和建表语句分别放入tables.txt和tablesDDL.txt
# History:
# 2022-06-03    slc   v1.0    init
target_db_name='opsdw' # 目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径
$hive -e "use opsdw;show tables;" > tables.txt 
cat tables.txt |while read eachline
do
$hive -e "use opsdw ;show create table $eachline" >>tablesDDL.txt
echo ";" >> tablesDDL.txt
done　
```

### 8.4.2hive 数据迁移

1.登陆老服务器58，先做hive登陆配置

```sql
#在~目录下创建一个.hiverc文件：目的是进入hive的时候直接使用数据库opsdw
vim ~/.hiverc
use opsdw;
#想要在命令行中显示当前数据库名称可以加上下面一句
set hive.cli.print.current.db=true;
#想要查询结果显示列的名称
set hive.cli.print.header=true;

```

2.做hdfs数据导出准备工作，在tmp创建一个目录

```sh
hdfs dfs -mkdir -p /tmp/opsdw_export
```

3.创建导出数据的hql

```sh
hive -e "show tables;" | awk  '{printf "export table %s to |/tmp/opsdw_export/%s|;\n",$1,$1}' | sed "s/|/'/g" | grep -v tab_name > ~/export.hql
```

4.执行刚刚的hql文件把数据都导入/tmp/opsdw_export

```sql
hive -f ~/export.hql
```

5.把数据拿到本地~/export_db 目录下，没有要先创建，且后台执行

```sh
nohup hdfs dfs -get -f  /tmp/opsdw_export/ ~/export_db > /dev/null 2>&1 & 
#如果想看到日志进度就最后的> /dev/null 2>&1 就不要加只保留&
jobs
#jobs 可以看后台运行的程序
```

6.把数据目录打包

```sh
nohup tar -zcvf opsdw_export.tar.gz /root/export_db/opsdw_export/* > /dev/null 2>&1 & 
#下面是打包和解压的语句
解压：tar zxvf FileName.tar.gz
压缩：tar zcvf FileName.tar.gz DirName
```

7.打包完成查看打包内容确认打包成功

```sh
#查看内容
tar -ztvf opsdw_export.tar.gz
```

8.把压缩文件拿到新服务器

```sh
nohup rsync -avz -e 'ssh -p 10029' /root/opsdw_export.tar.gz root@10.226.98.59:~/export_db/ > /dev/null 2>&1 & 
#rsync 这个命令是把数据从本机推到新服务器，所以本机要安装这个命令的yum install rsync
#之前走了几个弯路备注下
#尝试直接拿到服务器的公盘下通过公盘中转。失败因为本质来说hive是在容器里面的所以下面的mv是移动不过去的
mv opsdw_export.tar.gz /home/xul12/date_bakcup
#下面是正确的移动方式，但是超级慢,还不如用rsync一步到位
docker cp 54739da1eefc:/root/export_db/opsdw_export.tar.gz  /home/xul12/date_bakcup
```

9.到新服务器解压文件

```sh
tar -zxvf  opsdw_export.tar.gz -C ~/export_db
```

10.上传新服务器上的hdfs的临时目录，没有就创建

```sh
hdfs dfs -put /root/export_db/root/export_db/opsdw_export/* /tmp/opsdw_export
```

11.导入之前，因为之前是docker镜像迁移过来的所以先要删除所有表和hdfs上的数据，然后用8.4.1拿到的DDL语句创建所有的表。

12.此时所有的表已经创建好，数据也是空的，因为导入的时候不能有数据否则会报错，需要把azkaban的调度全部停止

13.准备导入hql脚本，把之前的export.hql里面的export 改为import ，to 改为from 替换的时候选择全词匹配

14.所有准备工作完成开始导入

```sh
#nohup hive -f ~/import.hql > /dev/null 2>&1 & 
#这里建议不要后台执行，因为这里有些表是有点问题会导入失败需要人为干预，猜测是我之前没有关调度导致有些表里面有数据了
hive -f ~/import.hql
#然后查询一下就会发现全部导入完成
```

15.设置每月1号凌晨1点备份hive

```sh
0 1 1 * * sh /opt/corntab_wj/hive_corntab_backup.sh >>/opt/corntab_wj/tb.log 2>&1
#内容
#!/bin/bash
if [ -n "$1" ] ;then
    sync_date=$1
else
    sync_date=$(date  +%F)
fi
docker export -o /home/xul12/image_backup/bsc-dsr-$sync_date.tar 111fe0275802
```



### 8.4.3 pg迁移

1.首先创建一个pg镜像，直接下载一个，下面命令失败的话可以多执行几次，之前第一次失败第三次成功了有点奇怪

```sql
docker pull postgres
```

2.下载完成启动镜像

```sh
docker run -itd --name tableaudb  -p 55433:5432 -e POSTGRES_PASSWORD=1qazxsw2 postgres
#-name 是要的数据库名称 -p是端口号映射，前面可以改为自己想要的后面不变 -e就是登陆密码
```

3.1dbeaver的工具备份老pg的数据发现server和dump的版本不一致失败

3.2登陆老的服务器58的主机进入pg镜像

```sh
 docker exec -it 27decf81c34b    /bin/bash 
```

3.3找到dump文件用和server版本一致的文件替换bin目录下的dump文件

```sh
#找到dump文件
find / -name pg_dump
#出现两个文件
/usr/bin/pg_dump
/usr/lib/postgresql/14/bin/pg_dump
#用14版本的替换/usr/bin/pg_dump文件，替换前先备份
#要是存的版本还是不对的话还可以下载一个
sudo apt-get install postgresql-client-9.6
```

4.创建一个备份文件的存放路径

```sh
 backup_data=$(date  +%F) #当天
 backup_path="/root/dump_backup/$backup_data" #为文件备份准备的路径+今天的日期
if [ ! -x $backup_path ]; #判断上面的备份路径是否存在
then
      mkdir -p $backup_path  #不存在就创建-p创建多层目录
fi
```

5.开始备份

```sh
 #需要在文件存放位置执行下面命令
 pg_dump -U postgres -d tableaudb -f /root/dump_backup/dump.sql
```

6.从容器里面把数据拿到公盘

```sh
docker cp 27decf81c34b:/root/dump_backup/dump.sql /home/xul12/image_backup/
```

7.从公盘把数据拿到新容器里面去,先在新容器建一个目录/dump_import

```sh
docker cp /home/xul12/image_backup/dump.sql 570892731bf2:/dump_import
```

8.进入新容器

```sh
  docker exec -it 570892731bf2    /bin/bash 
```

9.登陆pg

```sh
psql -h 127.0.0.1 -p 5432 -U postgres  
```

10.创建一个和原来名字一样的库

```sql
create database tableaudb;
```

11.确认数据库创建成功

```sql
  \l #列出所有数据库  
  #一些扩展命令
\c database_name 切换数据库
\d 列出当前数据库下的表
\d tablename 列出指定表的所有字段
\d+ tablename 查看指定表的基本情况
\dn 展示当前数据库下所有schema信息
SHOW search_path; 显示当前使用的schema
SET search_path TO myschema; 切换当前schema
\q 退出登录
```

12.开始导入备份文件

```sh
 #需要在文件存放位置执行下面命令，在容器里面
 psql -U postgres -h localhost -p 5432 -d tableaudb -f /dump_import/dump.sql
```

13.验证是否导入成功

```sql
  \l #列出所有数据库  
  \c tableaudb #切换数据库
  \d #列出当前数据库下的表
#到此pg迁移成功
```

### 8.4.3 .1pg每月自动备份

用crontab定时备份到公盘

```shell
 *  *  *  *  * user-name command to be executed
```

| 字段         | 说明                                                         |
| ------------ | ------------------------------------------------------------ |
| minute       | 分钟，取值范围 0-59                                          |
| hour         | 小时，取值范围 0-23                                          |
| day of month | 日，取值范围 1-31                                            |
| month        | 月，取值范围 1-12，或者使用英文缩写jan, feb, mar, apr ...    |
| day of week  | 星期，取值范围 0-6，0 或 7 表示星期日，或者使用英文缩写 sun, mon, tue, wed, thu, fri, sat |
| user-name    | 执行该定时任务的用户                                         |
| command      | 具体执行的命令，可以是一个简单的命令，也可以是一个脚本，或者是一个目录，如果是一个目录，则表示执行目录中的所有脚本，目录的前面必须加上 run-parts |

- 星号(*)表示取值范围内的所有值。例如，* 在hour的位置，表示每小时执行一次。
- 连字符(-)表示一个范围。例如，8-12表示8、9、10、11、12。
- 逗号(,)表示分割指定的数值。例如：3,5-7,9表示3,5,6,7,9。
- 正斜杠(/)表示步进值。例如，分钟的位置为*/5，表示每五分钟执行一次。

除了 root 用户以外的所有用户定义的 crontab 计划任务都存放在/var/spool/cron目录，通过 `crontab -e` 命令编辑，格式与 `/etc/crontab` 相同，可以不用指定 user-name。

cron 服务会每分钟检查一次 `/etc/crontab`、`/etc/cron.d/` 和 `/var/spool/cron/` 中的所有文件，并依此执行。

- 每隔 3 分钟执行 `*/3 * * * * /opt/test.sh`

- 每隔 3 小时执行 `0 */3 * * * /opt/test.sh > /dev/null 2>&1`

- 每天凌晨 1 点执行 `0 1 * * * /opt/test.sh`

- 每个星期天凌晨执行 `0 0 * * SUN /opt/test.sh`

- 每月 1 号凌晨执行 `0 0 1 * * /opt/test.sh > /dev/null 2>&1`

  #### 正式脚本

  ```sh
  #!/bin/bash
  if [ -n "$1" ] ;then
      backup_data=$1
  else
      backup_data=$(date  +%F)
  fi
  backup_path="/home/xul12/image_backup/dump_backup" #为文件备份准备的路径+今天的日期
  if [ ! -x $backup_path ]; #判断上面的备份路径是否存在
  then
        mkdir -p $backup_path  #不存在就创建-p创建多层目录
  fi
  docker exec -it 570892731bf2    /bin/bash -c "pg_dump -U postgres -d tableaudb -f /dump_backup/dump_$backup_data.sql;exit;"
  docker cp 570892731bf2:/dump_backup/dump_$backup_data.sql $backup_path
  docker exec -it 570892731bf2    /bin/bash -c "rm -rf /dump_backup/dump_$backup_data.sql;exit;"
  #文件存放位置：/opt/corntab_wj/pg_dump.sh
  ```

  添加定时脚本sudo crontab -e

  ```sh
  0 1 1 * * sh /opt/corntab_wj/pg_dump.sh >>/opt/corntab_wj/tb.log 2>&1
  ```

  

### 8.5 修改MySql 连接地址

```
vim $HIVE_HOME/conf/hive-site.xml
# 修改mysql地址  原来是172.18.0.2
jdbc:mysql://10.226.98.59:3306/bscopsdw?characterEncoding=UTF-8&amp;createDatabaseIfNotExist=true&amp;useSSL=false  
```

### 8.6 初始化Hive 元数据库--不执行

```
schematool -initSchema -dbType mysql --verbose
```

###  8.7  启动 元数据服务和HiveServer2 服务

```sh


start-all.sh #启动所有的节点
jps
[root@hadoop-master data]# jps
3108 SecondaryNameNode
3752 Jps
2699 NameNode
3563 NodeManager
2847 DataNode

## 启动hive service
hiveservices.sh start
hiveservices.sh status


```

### 8.7.1Azkaban 启动与关闭

![image-20220604002135360](BSC-OPS%E6%95%B0%E6%8D%AE%E5%B9%B3%E5%8F%B0%E6%89%8B%E5%86%8C.assets/image-20220604002135360.png)

启动顺序不能改变：Executor服务---->web服务

- 启动与关闭Executor服务

  ```sh
  #启动并激活Executor 服务
  cd /opt/module/azkaban/azkaban-exec
  bin/start-exec.sh
  #激活
  curl -G "hadoop-master:12321/executor?action=activate" && echo
  下面的命令在spark环境执行
  curl -G "spark-master:12321/executor?action=activate" && echo
  #停止Executor 服务
  bin/shutdown-exec.sh 
  ```

- 启动与关闭web服务

  ```sh
  cd /opt/module/azkaban/azkaban-web
  bin/start-web.sh #启动
  bin/shutdown-web.sh #关闭
  ```

### 8.8 配置用户免密及权限--不执行

创建部署用户，并且一定要配置 `sudo` 免密。以创建 dolphinscheduler 用户为例

```shell
# 创建用户需使用 root 登录
useradd donny

# 添加密码
echo "Boston01" | passwd --stdin donny

# 配置 sudo 免密
echo 'donny  ALL=(ALL)  NOPASSWD: ALL' >> /etc/sudoers
echo 'Defaults    requirett'  >> /etc/sudoers
sed -i 's/Defaults    requirett/#Defaults    requirett/g' /etc/sudoers

# 修改目录权限，使得部署用户对 dolphinscheduler-bin 目录有操作权限
chown -R dolphinscheduler:dolphinscheduler dolphinscheduler-bin
```

> ***注意:\***
>
> - 因为任务执行服务是以 `sudo -u {linux-user}` 切换不同 linux 用户的方式来实现多租户运行作业，所以部署用户需要有 sudo 权限，而且是免密的。初学习者不理解的话，完全可以暂时忽略这一点
> - 如果发现 `/etc/sudoers` 文件中有 "Defaults requirett" 这行，也请注释掉



### 8.9 配置机器SSH免密登陆 --不执行

由于安装的时候需要向不同机器发送资源，所以要求各台机器间能实现SSH免密登陆。配置免密登陆的步骤如下

```shell
su dolphinscheduler

ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys

 ssh-keygen

ssh-copy-id -i /root/.ssh/id_rsa.pub spark-master

ssh-copy-id -i /root/.ssh/id_rsa.pub spark-worker1

ssh-copy-id -i /root/.ssh/id_rsa.pub spark-worker2

```

> ***注意:\*** 配置完成后，可以通过运行命令 `ssh localhost` 判断是否成功，如果不需要输入密码就能ssh登陆则证明成功\\

## 9 Shell Script

### 9.1 HDFS

| ID   | Name                  | Server Path                         | Comments             |
| ---- | --------------------- | ----------------------------------- | -------------------- |
| #1   | bsc_ops_db_to_hdfs.sh | /bscflow/hdfs/bsc_ops_db_to_hdfs.sh | 同步主数据和交易记录 |

### 9.2 ODS

| ID   | Name                  | Server Path   | ODS Table                                                    | Comments                                   |
| ---- | --------------------- | ------------- | ------------------------------------------------------------ | ------------------------------------------ |
| #1   | hdfs_to_ods_master.sh | /bscflow/ods/ | exchange_rate,IDD, calendar,location, plant,material,batch,customer,division | 同步主数据相关ODS layer                    |
| #2   | hdfs_to_ods_trans.sh  | /bscflow/ods/ | so,po, import sto, domatic sto, wo                           | 同步相关业务数据至ODS layer, depends on:#1 |

### 9.3 DWD

**Server Path:   /bscflow/dwd/**

| ID   | Name                                         | DWD Table                                                    | Comments                     |
| ---- | -------------------------------------------- | ------------------------------------------------------------ | ---------------------------- |
| #1   | ods_to_dwd_master.sh                         | dwd_dim_plant, dwd_dim_locaiton, dwd_dim_batch, dwd_dim_calendar, dwd_dim_exchange_rate, dwd_dim_division, dwd_dim_customer | sync dimision table          |
| #2   | ods_to_dwd_dim_material.sh                   | dwd_dim_material                                             | sync sku info, depends on #1 |
| #3   | ods_to_dwd_import_export_sto.sh              | dwd_fact_import_export_sto                                   | depends on #2                |
| #4   | ods_to_dwd_import_export_sto_dn.sh           | dwd_fact_import_export_dn_detail                             | depends on #3                |
| #5   | ods_to_dwd_fact_import_export_declaration.sh | dwd_fact_import_export_declaration_info                      | depends on #4                |
| #6   | ods_to_dwd_fact_domestic_sto_info.sh         | dwd_fact_domestic_sto_info                                   | depends on #2                |
| #7   | ods_to_dwd_fact_domestic_sto_dn.sh           | dwd_fact_domestic_sto_dn_info                                | depends on #6                |
| #8   | ods_to_dwd_fact_so.sh                        | dwd_fact_sales_order_info                                    | depends on #2                |
| #9   | ods_to_dwd_fact_so_dn.sh                     | dwd_fact_sales_order_dn_info, dwd_fact_sales_order_dn_detail | depends on #8                |
| #10  | ods_to_dwd_fact_sales_order_invoice.sh       | dwd_fact_sales_order_invoice                                 | depends on #9                |
| #11  | ods_to_dwd_fact_work_order.sh                | dwd_fact_work_order                                          | depends on #2                |
| #12  | ods_to_dwd_fact_dealer_purchase_quotation.sh | dwd_fact_dealer_purchase_quotation                           | depends on #2                |
| #13  | ods_to_dwd_fact_inventory_movement.sh        | dwd_fact_inventory_movement_trans                            | depends on #2                |
| #14  | ods_to_dwd_fact_inventory_onhand.sh          | dwd_fact_inventory_onhand                                    | depends on #2                |
| #15  | ods_to_dwd_fact_purchase_order.sh            | dwd_fact_purchase_order_info                                 | depends on #2                |

### 9.4 DWS

#### 9.4.1 DSR

> Server Path: /bscflow/dws/

| ID   | Name                                      | DWS Table                       | Comments           |
| ---- | ----------------------------------------- | ------------------------------- | ------------------ |
| #1   | dwd_to_dws_dsr_daily_trans.sh             | dws_dsr_daily_trans             | Depends On: #2, #3 |
| #2   | dwd_to_dws_dsr_fulfill_daily_trans.sh     | dws_dsr_fulfill_daily_trans     |                    |
| #3   | dwd_to_dws_dsr_ship_daily_trans.sh        | dws_dsr_ship_daily_trans        |                    |
| #4   | dwd_to_dws_dsr_dealer_daily_transation.sh | dws_dsr_dealer_daily_transation |                    |

#### 9.4.2 Lead Time

| ID   | Name                                                         | DWS Table                                    | Comments                  |
| ---- | ------------------------------------------------------------ | -------------------------------------------- | ------------------------- |
| #1   | dwd_to_dws_plc_so_sto_wo_daily_trans_d835, dwd_to_dws_plc_so_sto_wo_daily_trans_d838 | dws_so_sto_wo_daily_trans                    |                           |
| #2   | dwd_to_dws_plc_wo_daily_trans.sh                             | dws_plc_wo_daily_trans                       | Depends On:one:           |
| #3   | dwd_to_dws_plc_so_daily_trans.sh                             | dws_plc_so_daily_trans                       | Depends On:one:           |
| #4   | dwd_to_dws_plc_import_export_daily_trans.sh                  | dws_plc_import_export_daily_trans            | Depends On:one:           |
| #5   | dwd_to_dws_plc_domestic_sto_daily_trans.sh                   | dws_plc_domestic_sto_daily_trans             | Depends On:one:           |
| #6   | dwd_to_dws_import_export_daily_trans.sh                      | dws_import_export_daily_trans                | Depends on  #1,4          |
| #7   | dwd_to_dws_product_putaway_leadtime_slc_daily_trans.sh       | dws_product_putaway_leadtime_slc_daily_trans | Depends On:one:#1,2,3,4,5 |
| #8   | dwd_to_dws_product_putaway_leadtime_yh_daily_trans.sh        | dws_product_putaway_leadtime_yh_daily_trans  | Depends On:one:#1,2,3,4,5 |
| #9   | dwd_to_dws_order_proce_custlev3_daily_trans.sh               | dws_order_proce_custlev3_daily_trans         |                           |
| #10  | dwd_to_dws_order_proce_division_daily_trans.sh               | dws_order_proce_division_daily_trans         |                           |
| #11  | dwd_to_dws_order_proce_tob_daily_trans.sh                    | dws_order_proce_tob_daily_trans              |                           |
| #12  | dwd_to_dws_plant_delivery_processing_daily_trans.sh          | dws_plant_delivery_processing_daily_trans    |                           |
| #13  | dwd_to_dws_t1_plant_trans.sh                                 | dws_t1_plant_daily_transation                | Depends on: #3, #4        |
| #14  | dwd_to_dws_forwarder_daily_trans.sh                          | dws_forwarder_daily_trans                    | Depends on: #3, #4        |
| #15  | dwd_to_dws_lifecycle_leadtime_slc_daily_trans.sh             | dws_lifecycle_leadtime_SLC_daily_trans       | Depends on: #2,#3, #4     |
| #16  | dwd_to_dws_lifecycle_leadtime_yh_daily_trans.sh              | dws_lifecycle_leadtime_YH_daily_trans        | Depends on: #2,#3, #4,#5  |
| #17  | dwd_to_dws_sale_order_leadtime_daily_trans.sh                | dws_sale_order_leadtime_daily_trans          |                           |



### 9.5 DWT

> Server Path: /bscflow/dwt

#### 9.5.1 DSR

| ID   | Name                                   | DWT Table                    | Comments |
| ---- | -------------------------------------- | ---------------------------- | -------- |
| #1   | dws_to_dwt_dsr_dealer_quarter_trans.sh | dwt_dsr_dealer_quarter_trans |          |

#### 9.5.1 LeadTime

| ID   | Name                                            | DWT Table                                    | Comments            |
| ---- | ----------------------------------------------- | -------------------------------------------- | ------------------- |
| #1   | dws_to_dwt_forwarder_topic.sh                   | dwt_forwarder_topic                          |                     |
| #2   | dws_to_dwt_imported_topic.sh                    | dwt_imported_topic                           |                     |
| #3   | dws_to_dwt_order_proce_custlev3_topic.sh        | dwt_order_proce_custlev3_topic               |                     |
| #4   | dws_to_dwt_order_proce_division_topic.sh        | dwt_order_proce_division_topic               |                     |
| #5   | dws_to_dwt_order_proce_tob_topic.sh             | dwt_order_proce_tob_topic                    |                     |
| #6   | dws_to_dwt_plant_delivery_processing_topic.sh   | dwt_plant_delivery_processing_topic          |                     |
| #7   | dws_to_dwt_plant_topic.sh                       | dwt_plant_topic                              |                     |
| #8   | dws_dwt_product_putaway_leadtime_slc_topic.sh   | dwt_product_putaway_leadtime_slc_topic       |                     |
| #9   | dws_to_dwt_product_putaway_leadtime_yh_topic.sh | dwt_product_putaway_leadtime_yh_topic        |                     |
| #10  | dws_to_dwt_sale_order_leadtime_topic.sh         | dwt_sale_order_leadtime_topic                |                     |
| #11  | dws_to_dwt_lifecycle_slcyh_summarize_topic.sh   | dwt_lifecycle_leadtime_slcyh_summarize_topic | depends on #12, #13 |
| #12  | dws_to_dwt_lifecycle_slc_summarize_topic.sh     | dwt_lifecycle_leadtime_slc_summarize_topic   | depends on #15      |
| #13  | dws_to_dwt_lifecycle_yh_summarize_topic.sh      | dwt_lifecycle_leadtime_yh_summarize_topic    | depends on #14      |
| #14  | dws_to_dwt_lifecycle_leadtime_yh_topic.sh       | dwt_lifecycle_leadtime_YH_topic              |                     |
| #15  | dws_to_dwt_lifecycle_leadtime_slc_topic.sh      | dwt_lifecycle_leadtime_SLC_topic             |                     |
| #16  | dws_to_dwt_lifecycle_leadtime_division_slcyh.sh | dwt_lifecycle_leadtime_division_slcyh_topic  |                     |

### 9.6 ADS

> Server Path: /bscflow/ads

| ID   | Name                                      | ADS Table                              | DWT Table | DWS Table |
| ---- | ----------------------------------------- | -------------------------------------- | --------- | --------- |
| #1   | ads_imported_ratio.sh                     | ads_imported_ratio                     |           |           |
| #2   | ads_product_putaway_leadtime_slc_ratio.sh | ads_product_putaway_leadtime_slc_ratio |           |           |
| #3   | ads_product_putaway_leadtime_yh_ratio.sh  | ads_product_putaway_leadtime_yh_ratio  |           |           |
| #4   | ads_sale_order_leadtime_ratio.sh          | ads_sale_order_leadtime_ratio          |           |           |
| #5   | ads_lifecycle_leadtime_slcyh_ratio.sh     | ads_lifecycle_leadtime_slcyh_ratio     |           |           |

### 9.7 LocalData

> Server Path:/bscflow/ods/load_local_data_to_ods_dwd.sh 
>
> /bscflow/ods/wo_qr_local_to_ods.sh
>
> /bscflow/dwd/ods_to_dwd_wo_qr.sh

**Work Oder QR code:**

a) Upload data file to server

from bsc\ODS\LocalData  to /bscflow/data

![image-20210711160114896](BSC-OPS%E6%95%B0%E6%8D%AE%E5%B9%B3%E5%8F%B0%E6%89%8B%E5%86%8C.assets/image-20210711160114896.png)

b) 修改文件的编号，使其连续。



<img src="BSC-OPS%E6%95%B0%E6%8D%AE%E5%B9%B3%E5%8F%B0%E6%89%8B%E5%86%8C.assets/image-20210711160401946.png" alt="image-20210711160401946" style="zoom:50%;" />





c) 执行Job

选择 Load wo_qr_data flow, 点击Execute Flow

![image-20210711160541971](BSC-OPS%E6%95%B0%E6%8D%AE%E5%B9%B3%E5%8F%B0%E6%89%8B%E5%86%8C.assets/image-20210711160541971.png)

```
sh /bscflow/ods/wo_qr_local_to_ods.sh ${start} ${end}
```

![](BSC-OPS%E6%95%B0%E6%8D%AE%E5%B9%B3%E5%8F%B0%E6%89%8B%E5%86%8C.assets/image-20210711160915957.png)

d） 设置Flow parameters：start, end

单击Execute



**cust_level, cust_type, operation_type, rebate_rate**:

```
sh /bscflow/ods/load_local_data_to_ods_dwd.sh ${data_type} ${date_file}
```

需要设置两个参数：

data_type: 取值范围cust_level|cust_type|operation_type|rebate_rate

data_file: 文件路径 例如：/bscflow/data/customer_level.txt



## 10 Azkaban 

调度流程图如下：

![image-20210711180452615](BSC-OPS%E6%95%B0%E6%8D%AE%E5%B9%B3%E5%8F%B0%E6%89%8B%E5%86%8C.assets/image-20210711180452615.png)

### 10.1 手工调度过程

**Step 1:** Go to [Azkaban Web Client](http://10.226.98.58:8081/index)

**Step 2:** 选择对应的Project：

![image-20210711173004071](BSC-OPS%E6%95%B0%E6%8D%AE%E5%B9%B3%E5%8F%B0%E6%89%8B%E5%86%8C.assets/image-20210711173004071.png)

**Step 3:**根据Job特点设置参数

以参数job为例：

![image-20210709101524719](BSC-OPS%E6%95%B0%E6%8D%AE%E5%B9%B3%E5%8F%B0%E6%89%8B%E5%86%8C.assets/image-20210709101524719.png)

**Step 4: 点击执行**

**Step 5:  Go to Executing**

![image-20210711173413674](BSC-OPS%E6%95%B0%E6%8D%AE%E5%B9%B3%E5%8F%B0%E6%89%8B%E5%86%8C.assets/image-20210711173413674.png)

**Step 6: 点击 Execution Id -> Job List** 

查看子每个子job 状态

![image-20210711173551349](BSC-OPS%E6%95%B0%E6%8D%AE%E5%B9%B3%E5%8F%B0%E6%89%8B%E5%86%8C.assets/image-20210711173551349.png)

**Step 7： 点击 Log**

查看Job的 执行过程，是否存在异常情况



### 10.2 定时调度

**Step 1： 选择Project->Execute flow**

<img src="BSC-OPS%E6%95%B0%E6%8D%AE%E5%B9%B3%E5%8F%B0%E6%89%8B%E5%86%8C.assets/image-20210711175422589.png" alt="image-20210711175422589" style="zoom:80%;" />

**Step 2： 单击 Schedule**

<img src="BSC-OPS%E6%95%B0%E6%8D%AE%E5%B9%B3%E5%8F%B0%E6%89%8B%E5%86%8C.assets/image-20210711175545728.png" alt="image-20210711175545728" style="zoom:80%;" />



指定参数：

![image-20210711175719623](BSC-OPS%E6%95%B0%E6%8D%AE%E5%B9%B3%E5%8F%B0%E6%89%8B%E5%86%8C.assets/image-20210711175719623.png)

Demo:

0 0 9 ? * *               Fire at 9:00am every day



### 10.3 历史记录

Step１：Go to  [Job History](http://10.226.98.58:8081/history)



## 11 数据平台的技术架构

### 11.1 技术选型

| Apache框架版本 |                |
| -------------- | -------------- |
| 软件           | 版本           |
| Hadoop         | 3.1.4          |
| Hive           | 3.1.2          |
| Sqoop          | 1.4.7          |
| Java           | 1.8            |
| azkaban        | 3.84.4         |
| Docker         | Latest version |

### 11.2 **物理部署（本地）**

<img src="BSC-OPS%E6%95%B0%E6%8D%AE%E5%B9%B3%E5%8F%B0%E6%89%8B%E5%86%8C.assets/image-20210804154503685.png" alt="image-20210804154503685" style="zoom:80%;" />

### 11.3 **业务数据流（本地）**

<img src="BSC-OPS%E6%95%B0%E6%8D%AE%E5%B9%B3%E5%8F%B0%E6%89%8B%E5%86%8C.assets/image-20210804154546082.png" alt="image-20210804154546082" style="zoom:80%;" />

### 11.4 **物理部署（云）**

<img src="BSC-OPS%E6%95%B0%E6%8D%AE%E5%B9%B3%E5%8F%B0%E6%89%8B%E5%86%8C.assets/image-20210804154618103.png" alt="image-20210804154618103" style="zoom:80%;" />

### 11.5 **业务数据流（云）**

<img src="BSC-OPS%E6%95%B0%E6%8D%AE%E5%B9%B3%E5%8F%B0%E6%89%8B%E5%86%8C.assets/image-20210804154651316.png" alt="image-20210804154651316" style="zoom:80%;" />



## 12 服务启动过程

```json
-bash-4.2$ docker inspect bsc-br
[
    {
        "Name": "bsc-br",
        "Id": "ff4550a7cab3763ae51826a8d964da0905a044c10bda07c60b624054a0894fb0",
        "Created": "2021-06-15T15:40:48.896527235Z",
        "Scope": "local",
        "Driver": "bridge",
        "EnableIPv6": false,
        "IPAM": {
            "Driver": "default",
            "Options": {},
            "Config": [
                {
                    "Subnet": "172.18.0.0/16",
                    "Gateway": "172.18.0.1"
                }
            ]
        },
        "Internal": false,
        "Attachable": false,
        "Ingress": false,
        "ConfigFrom": {
            "Network": ""
        },
        "ConfigOnly": false,
        "Containers": {
            "20a2e52a57a7ec459a9f5d10d63a74de22a0ae16acd102fb1d138dc2e6a8192f": {
                "Name": "postgresql",
                "EndpointID": "d0540e099cd6018b321ec0abb250ac97b0256cc596040b30ec8c805cd598ac2c",
                "MacAddress": "02:42:ac:12:00:04",
                "IPv4Address": "172.18.0.4/16",
                "IPv6Address": ""
            },
            "2d0c9fd14d3fe0f9837a25afc77063c20bd34ec6f2445fade8ffb3f86daf505c": {
                "Name": "bsc-ops-01",
                "EndpointID": "02d431c2f4a256e29f80840a766faf6709c1cfb27e9558b0c639610afe63a0b5",
                "MacAddress": "02:42:ac:12:00:03",
                "IPv4Address": "172.18.0.3/16",
                "IPv6Address": ""
            },
            "6e9b8adf6fef79e65713211150c32af1554804689a47357f5427256bd04f0ceb": {
                "Name": "worker2",
                "EndpointID": "7ba8049a68782ac2f689e93af86aacd22aca10228c384b983e212db142f9746d",
                "MacAddress": "02:42:ac:12:00:07",
                "IPv4Address": "172.18.0.7/16",
                "IPv6Address": ""
            },
            "73b70b52b64147d45967433272eebc2a453620a3ef5d130c5153ff81c00fe8ef": {
                "Name": "worker1",
                "EndpointID": "81b90b1cad115d11f544c71e3eabd1cdd1dc57b84163153645b49b0b8261f2d2",
                "MacAddress": "02:42:ac:12:00:06",
                "IPv4Address": "172.18.0.6/16",
                "IPv6Address": ""
            },
            "7ee128f90d4c3d372d5490eec225aafae36cb4f16f7e0551b30ce6e95d80ae33": {
                "Name": "mysql",
                "EndpointID": "8ea2561a4735fdb5c6fdc40cf36a6601b98b6925201c62e70cee1f952741a9c5",
                "MacAddress": "02:42:ac:12:00:02",
                "IPv4Address": "172.18.0.2/16",
                "IPv6Address": ""
            },
            "8416b7758c8044f196406ca1ef5193715fa573254512cc433be4eb293646fdaa": {
                "Name": "SparkV2",
                "EndpointID": "03d1a939556a0124b9cbceae9eabcd2589be401108ff731744f7f1f59df1b5f6",
                "MacAddress": "02:42:ac:12:00:05",
                "IPv4Address": "172.18.0.5/16",
                "IPv6Address": ""
            }
        },
        "Options": {},
        "Labels": {}
    }
]
-bash-4.2$

```

### #备份

```
cd '镜像tar文件所在目录'
docker load -i bsc-ops-dw.tar

# 创建新的镜像docker commit [OPTIONS] CONTAINER [REPOSITORY[:TAG]]
docker commit -a "BSC DW" -m "backup on 20210808" 2d0c9fd14d3f  bsc-ops:5

docker commit -a "BSC DW Spark" -m "B" 2d0c9fd14d3f  bsc-ops:5

## Backup on 2021113
docker commit -a="DonnyChen" -m="BSC OPS V6.1 20220328 " 54739da1eefc bsc-ops:6.1

## Backup on 2021113
docker commit -a="DonnyChen" -m="BSC Spark v1.1" 3d51b2790bec bsc-spark:1.1

docker commit -a="DonnyChen" -m="BSC Spark v1.2" 8416b7758c80  bsc-spark:1.2
docker commit -a="DonnyChen" -m="BSC Spark v1.3" 8416b7758c80  bsc-spark:1.3

docker commit -a="DonnyChen" -m="BSC Spark v6.1 20220328" 8416b7758c80  bsc-spark:6.1 

###Backup on 20220401
docker commit -a="DonnyChen" -m="BSC OPS V6.1 	20220401  hive " 54739da1eefc bsc-ops:6.1
docker commit -a="DonnyChen" -m="BSC Spark v6.1 20220401  spark" 8416b7758c80  bsc-spark:6.1 
8416b7758c80 spark环境
54739da1eefc  hive环境

10029 hive
22 zhuji
10086 
```

### #备份2

```sh
docker run -itd --net=bsc-br -p 9870:9870 -p 9868:9868 -p 9864:9864 -p 8088:8088 -p 10000:10000 -p 50070:50070 -p 10024:22  --name bsc-dev --hostname hadoop-master  bsc-ops:3

docker run -itd --net=bsc-br --name spark --hostname spark-master --privileged -p 10086:22 -p 19870:9870  -p 18088:8088 -p 18080:8080 -p 18081:8081 -p 17077:7077 -p 14040:4040 bsc-spark:1 /usr/sbin/init 

## Spark-V2
docker run -itd --net=bsc-br --name SparkV2 --hostname spark-master -p 10086:22 -p 14040:4040 -p 17077:7077 -p 18080:8080 -p 19870:9870  -p 18088:8088 -p 18042:8042 -p 19888:19888 -p 18081:8081 -p 18082:8082 -p 10010:10000 -p 12345:12345 -p 11281:2181 -p 18887:8887 -p 12315:12315 -p 12306:12306 bsc-spark:1.1 /usr/sbin/sshd -D


# Spark-Worker1
docker run -itd --net=bsc-br --name worker1 --hostname spark-worker1 -p 10087:22 -p 29888:19888 -p 18083:8083 -p 18084:8084  bsc-spark:1.3 /usr/sbin/sshd -D
# Spark-Worker2
docker run -itd --net=bsc-br --name worker2 --hostname spark-worker2 -p 10088:22 -p 19868:9868 -p 18085:8085 -p 18086:8086  bsc-spark:1.3 /usr/sbin/sshd -D


cd $HADOOP_HOME/data
rm -rf $HADOOP_HOME/data/dfs  #删除旧数据
hdfs namenode -format #初始化namenode
hiveservices.sh start


## 启动Spark --不执行
/opt/module/spark32/sbin/start-master.sh
$SPARK_HOME/sbin/start-worker.sh spark://spark-master:7077


$SPARK_HOME/sbin/start-all.sh
$SPARK_HOME/sbin/stop-all.sh

# master
[root@spark-master sbin]# start-master.sh
starting org.apache.spark.deploy.master.Master, logging to /opt/module/spark32/logs/spark-root-org.apache.spark.deploy.master.Master-1-spark-master.out
[root@spark-master sbin]

# worker

[root@spark-master sbin]# $SPARK_HOME/sbin/start-worker.sh spark://spark-master:7077
starting org.apache.spark.deploy.worker.Worker, logging to /opt/module/spark32/logs/spark-root-org.apache.spark.deploy.worker.Worker-1-spark-master.out
[root@spark-master sbin]#

# 启动 Spark Thir
sparkservices.sh start
```

# 服务器实例全表挂掉重启步骤：

## 1.先到host  10.226.98.58 22

1.docker ps -a       确认是否全表挂掉

2.按ip地址从小到大对应的CONTAINER ID 启动所有实例

| docker start 7ee128f90d4c     -----------------------------MySQL：ip:172.18.0.2/16 |      |      |      |      |
| ------------------------------------------------------------ | ---- | ---- | ---- | ---- |
|                                                              |      |      |      |      |
|                                                              |      |      |      |      |
|                                                              |      |      |      |      |
|                                                              |      |      |      |      |
|                                                              |      |      |      |      |
|                                                              |      |      |      |      |

docker start 7ee128f90d4c     -----------------------------MySQL：ip:172.18.0.2/16

docker start  54739da1eefc    -----------------------------bsc-ops-02  :ip :172.18.0.3/16

docker start  20a2e52a57a7   -----------------------------postgresql  :ip :172.18.0.4/16

docker start  8416b7758c80   -----------------------------SparkV2:ip :172.18.0.5/16

docker start  73b70b52b641-----------------------------worker1:ip :172.18.0.6/16

docker start  6e9b8adf6fef   -----------------------------worker1:ip :172.18.0.7/16

docker inspect bsc-br      

最后还有两个没启动，再依次启动

docker start  27decf81c34b   -----------------------------postgres

docker start  2d0c9fd14d3f   -----------------------------bsc-ops-01

docker ps -a            -----------确认全部都启动了

## 2.再到hive 10.226.98.58 10029

#1.启动hadoop 平台的name,data,yarn 节点服务     

```shell
start-all.sh 
```

#去对应网页检查是否启动成功

[All Applications](http://10.226.98.58:28088/cluster)

[Namenode information](http://10.226.98.58:29870/dfshealth.html#tab-overview)

#2.----启动 Metastore 服务和HiveServer2 服务

```
hiveservices.sh start    
```

#3.----确认启动成功

```shell
hiveservices.sh status  
```

#4.启动azkaban

--------到azkanban目录下先起Executor服务

```sh
cd /opt/module/azkaban/azkaban-exec         
```

```sh
#启动并激活Executor 服务

bin/start-exec.sh          ---------如果要停止：#停止Executor 服务：bin/shutdown-exec.sh 

curl -G "hadoop-master:12321/executor?action=activate" && echo    #显示{"status":"success"} 代表成功

#启动web服务

cd /opt/module/azkaban/azkaban-web      --------先到对应目录
bin/start-web.sh #启动      -----------如果要停止：bin/shutdown-web.sh #关闭

#去对应网页再次确认
#如何让azkaban报错，只需要把它要执行的sh 文件删除就行6666
```

[Azkaban Web Client](http://10.226.98.58:28081/index)

## 3.再到spark 10.226.98.58 10086

#1.---启动Hadoop

start-all.sh    -------必须第一步，错了就先stop-all.sh 再启

#hadoop是否正常

[Namenode information](http://10.226.98.58:19870/dfshealth.html#tab-overview)

![image-20220520213831245](%E5%AE%9E%E4%BE%8B%E5%85%A8%E9%83%A8%E6%8C%82%E6%8E%89%E9%87%8D%E5%90%AF%E6%AD%A5%E9%AA%A4.assets/image-20220520213831245.png)



### 4.到spark work2 10.226.98.58 10088

#1.---启动

start-all.sh

#看yarn节点是否启动

[All Applications](http://10.226.98.58:18887/cluster)

![image-20220520213708309](%E5%AE%9E%E4%BE%8B%E5%85%A8%E9%83%A8%E6%8C%82%E6%8E%89%E9%87%8D%E5%90%AF%E6%AD%A5%E9%AA%A4.assets/image-20220520213708309.png)

### 5.再回到spark 10.226.98.58 10086

#1.---启动azkaban --------到azkanban目录下先起Executor服务

cd /opt/module/azkaban/azkaban-exec            

#启动并激活Executor 服务
bin/start-exec.sh          ---------如果要停止：#停止Executor 服务：bin/shutdown-exec.sh 

curl -G "spark-master:12321/executor?action=activate" && echo    #显示{"status":"success"} 代表成功

#启动web服务

cd /opt/module/azkaban/azkaban-web      --------先到对应目录
bin/start-web.sh #启动      -----------如果要停止：bin/shutdown-web.sh #关闭

#去对应网页再次确认

[Azkaban Web Client](http://10.226.98.58:12306/history)

#2.---启动服务

hiveservices.sh start

hiveservices.sh status

cd /opt/module/spark32/sbin

sh stop-all.sh

sh start-all.sh

sh sparkservices.sh start



#3.---------最后去网页确认是否都启动

#看3个worker是否都启动，

[Spark Master at spark://spark-master:7077](http://10.226.98.58:18080/)

http://10.226.98.58:18086/ yarn节点

![image-20220520213542047](%E5%AE%9E%E4%BE%8B%E5%85%A8%E9%83%A8%E6%8C%82%E6%8E%89%E9%87%8D%E5%90%AF%E6%AD%A5%E9%AA%A4.assets/image-20220520213542047.png)

![image-20220522164401096](%E5%AE%9E%E4%BE%8B%E5%85%A8%E9%83%A8%E6%8C%82%E6%8E%89%E9%87%8D%E5%90%AF%E6%AD%A5%E9%AA%A4.assets/image-20220522164401096.png)

这些东西都不能少，少了就得重启

# hive迁移到新机器

[docker的/var/lib/docker目录迁移 - ejiyuan - 博客园 (cnblogs.com)](https://www.cnblogs.com/ejiyuan/p/12241998.html)

从第二步开始

先创建目录

```sh
sudo mkdir -p /apps/docker/lib
```

开始迁移

```sh
sudo rsync -avz /var/lib/docker /apps/docker/lib/
```

迁移完成选用方法2

```sh
 cd /etc/docker/
```

```sh
sudo vim daemon.json
#发现没有这个文件
```

到主机上查看有这个文件，然后把文件复制过去

插入内容

```sh
{
  "registry-mirrors": ["http://hub-mirror.c.163.com"],
  "data-root": "/apps/docker/lib"
}
```

**5 重新加载 docker**

```sh
sudo systemctl daemon-reload
sudo systemctl restart docker
sudo systemctl enable docker
```

6 删除 /var/lib/docker

```sh
srm -rf /var/lib/docker
```

迁移数据之前首先需要备份hive和mysq的镜像

备份镜像之前需要先删除日志数据

```sh
hdfs dfs -du -h /   进入目录查看tmp文件大小
hdfs dfs -rm -f -r /tmp
hdfs dfs -mkdir /tmp
hadoop fs -chmod -R 777 /tmp

---查看大文件
du -hsx * | sort -rh | head -10
--清空tmp
cd /tmp
rm -rf *
/opt/module/hive3/logs/hiveServer2.log
```

然后执行备份操作：

```sh
docker commit -a="DonnyChen" -m="BSC OPS V6.4 	20220525  hive " 54739da1eefc bsc-ops:6.4
docker commit -a="DonnyChen" -m="mysql V6.4 	20220530  mysql " 7ee128f90d4c mysql:6.4
docker images

docker rmi 9ed9a3d65496  ---删除镜像image id 
#查看大文件
du -hsx * | sort -rh | head -10
```

-m: 提交的描述信息

-a: 指定镜像作者
54739da1eefc ：hive容器 ID
8416b7758c80  ：spark 容器id

7ee128f90d4c  : mysql容器id

备份完成后需要在hive环境下关闭安全模式否则无法把数据写入hdfs

```sh
hdfs dfsadmin -safemode leave
```

![image-20220525221112329](%E5%AE%9E%E4%BE%8B%E5%85%A8%E9%83%A8%E6%8C%82%E6%8E%89%E9%87%8D%E5%90%AF%E6%AD%A5%E9%AA%A4.assets/image-20220525221112329.png)

接下来进销镜像导出

```sh
#docker save -o bsc-ops.tar bsc-ops:6.2 
#为了小一点用export
nohup docker export -o /home/xul12/image_backup/bsc-ops.tar 54739da1eefc   > /dev/null 2>&1 &
docker save -o mysql.tar mysql:6.2	
#然后在新服务器导入
#docker load -i bsc-ops.tar
docker load -i mysql.tar
#启动mysql的容器
docker run -itd --net=bsc-br --name mysql-dsr -p 3306:3306 -e MYSQL_ROOT_PASSWORD=1qazXSW@ mysql:6.2
 docker exec -it 5eece8ea397e /bin/bash 

#运行下面这一个即可
docker run -itd --net=bsc-br -p 8081:8081 -p 8080:8080 -p 8089:8089 -p 8443:8443 -p 9870:9870 -p 9868:9868 -p 9864:9864 -p 8088:8088 -p 10000:10000  --name bsc-ops-01 --hostname hadoop-master -p 10028:22 bsc-ops:
```

### hdfs数据块损坏

```
hadoop  fsck -delete  #即可自动修复
```

### 登陆容器

hive

```shell
docker exec -it 54739da1eefc /bin/bash 
```

###在主机执行容器命令，hive为例

```shell
docker exec -it 54739da1eefc /bin/bash -c 'sh /root/test.sh'
```

### 58主机容器挂掉全部重启脚本

```
sh /home/xul12/docker_restart.sh
```

内容

```shell
#!/bin/bash
docker_id=(
	'7ee128f90d4c' #MySQL：		 ip :172.18.0.2/16
	'54739da1eefc' #bsc-ops-02  :ip :172.18.0.3/16
	'20a2e52a57a7' #postgresql  :ip :172.18.0.4/16 #spark的元数据存放
	'8416b7758c80' #SparkV2:	 ip :172.18.0.5/16
	'73b70b52b641' #worker1:	 ip :172.18.0.6/16
	'6e9b8adf6fef' #worker1:	 ip :172.18.0.7/16
	'27decf81c34b' #postgres
	'2d0c9fd14d3f' #bsc-ops-01   ip :172.18.0.8/16 这个不知道干啥的不要
    )

for docker_name in ${docker_id[*]};
do
    currTime=`date +"%Y-%m-%d %H:%M:%S"`
	#containerName=$docker_name
	# 查看进程是否存在
	#echo "$containerName"
	exist=`docker inspect --format '{{.State.Running}}' ${docker_name}`
	#如果存在就会返回true不存在就启动并写入日志文件
	if [ "${exist}" != "true" ]; then
		docker start ${docker_name}		
		# 记录
		#echo "${docker_name} 666"
		echo "${currTime} 重启docker容器，容器名称：${docker_name}" >> /home/xul12/log.txt
		#如果是hive挂了被重启
		#if [ "${docker_name}" = "54739da1eefc" ];then
			#docker exec -it 54739da1eefc /bin/bash -c 'sh /root/test.sh'
			#docker exec -it 54739da1eefc /bin/bash -c 'cd /opt/module/hadoop3/ && sbin/start-all.sh'
			#echo "jj"
			#sh /root/test.sh
		#fi
	fi
done
```

### 59主机容器挂掉重启脚本

```
sh /home/xul12/docker_new_hive_restart.sh
```

内容

```sh
#!/bin/bash
docker_id=(
	'88c2622e3a4b' #MySQL：		 ip :172.18.0.2/16
	'570892731bf2' #postgres
	'111fe0275802' #bsc-ops-02   ip :172.18.0.3/16
    )

for docker_name in ${docker_id[*]};
do
    currTime=`date +"%Y-%m-%d %H:%M:%S"`
	#containerName=$docker_name
	# 查看进程是否存在
	#echo "$containerName"
	exist=`docker inspect --format '{{.State.Running}}' ${docker_name}`
	#如果存在就会返回true不存在就启动并写入日志文件
	if [ "${exist}" != "true" ]; then
		docker start ${docker_name}		
		# 记录
		#echo "${docker_name} 666"
		echo "${currTime} 重启docker容器，容器名称：${docker_name}" >> /home/xul12/log.txt
		#如果是hive挂了被重启
		#if [ "${docker_name}" = "54739da1eefc" ];then
			#docker exec -it 54739da1eefc /bin/bash -c 'sh /root/test.sh'
			#docker exec -it 54739da1eefc /bin/bash -c 'cd /opt/module/hadoop3/ && sbin/start-all.sh'
			#echo "jj"
			#sh /root/test.sh
		#fi
	fi
done

```



### 58/59hive挂掉重启脚本

```
sh /root/hive_restart.sh
```

内容

```sh
#!/bin/bash
#启动hdfs
#pkill -15 java 
stop-all.sh
sleep 5
start-all.sh 
#启动服务
hiveservices.sh start 
sleep 5
#确认服务启动成功
hiveservices.sh status 
#去azkaban 目录启动Executor  
cd /opt/module/azkaban/azkaban-exec   
bin/start-exec.sh 
sleep 5
curl -G "hadoop-master:12321/executor?action=activate" && echo 
#去azkaban网页目录启动网页 
cd /opt/module/azkaban/azkaban-web
bin/start-web.sh
for i in {1..3}
do
	#取第3行
	a=`hiveservices.sh status | sed -n 3p`
	if [ "${a}" != "HiveServer2 服务运" ]; then
		hiveservices.sh start 
		echo "第 $i 次"
		sleep 40
	else
	   	echo "Thriftserver 服务运行正常"		
		break 
	fi
done

```

### spark 挂掉重启

步骤1：需要先启动hdfs然后去work2执行下面脚本

在BSC_SparkV2

```
start-all.sh 
```

在Worker2

```
start-all.sh
```

再回到BSC_SparkV2

```
sh /root/spark_restart.sh
```

内容

```shell
#!/bin/bash
cd /opt/module/azkaban/azkaban-exec    
bin/start-exec.sh  
curl -G "spark-master:12321/executor?action=activate" && echo
cd /opt/module/azkaban/azkaban-web 
bin/start-web.sh
hiveservices.sh start
hiveservices.sh status
cd /opt/module/spark32/sbin
sh stop-all.sh
sh start-all.sh
sh sparkservices.sh stop
for i in {1..10}
do
	sleep 2
	a=`sh sparkservices.sh status`
	if [ "${a}" != "Thriftserver 服务运行正常" ]; then
		sh $SPARK_HOME/sbin/start-thriftserver.sh --master spark://spark-master:7077 --executor-memory 2g --total-executor-cores 3 >$SPARK_LOG_DIR/spark-thriftserver.log 2>&1
		echo "第 $i 次"
	else
	    echo "Thriftserver 服务运行正常"
		break
	fi
done
```

### 配置定时任务

```sh
#在主机环境
sudo crontab -e
```

### 杀死所有java进程

```shell
pkill -9 java  #杀死掉所有进程
```

查看内存占用最大的5个线程

top  #然后按大写的M,看cpu就是大写的P

```sh
ps -ef |grep pid
```



```sh
#最后，总结下排查内存故障的方法和技巧有哪些：

1、top命令：Linux命令。可以查看实时的内存使用情况。  

2、jmap -histo:live [pid]，然后分析具体的对象数目和占用内存大小，从而定位代码。

3、jmap -dump:live,format=b,file=xxx.xxx [pid]，然后利用MAT工具分析是否存在内存泄漏等等。
```

### ssh远程登陆

```sh
ssh -p 10088 root@10.226.98.58 
```

查看剩余内存

free -h

### 发现小熊占用资源不释放，定时重启释放资源

sh /root/hiveservices_restart.sh

```sh
#!/bin/bash
#定时重启释放资源
hiveservices.sh stop
for i in {1..3}
do
	#取第3行
	a=`hiveservices.sh status | sed -n 3p`
	if [ "${a}" != "HiveServer2 服务运" ]; then
		hiveservices.sh start 
		echo "第 $i 次"
		sleep 40
	else
	   	echo "Thriftserver 服务运行正常"		
		break 
	fi
done
```

