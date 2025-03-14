#!/bin/bash
# Function:
#   sync up inventory movement transation data to dwd layer
# History:
# 2021-05-18    Donny   v1.0    init

# 参数
target_db_name='opsdw' # 目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径
#每周五晚上10.30执行
# 默认取当前时间的前一天 
if [ -n "$1" ] ;then 
    sync_date=$1
else
    sync_date=$(date  +%F)
fi

echo "start syncing dwd_t1_invoice into DWD layer on ${sync_date} .................."

# 1 清空tmp数据
echo "this day is 01 delete /tmp/*"	
    hdfs dfs -rm -f -r /tmp
    hdfs dfs -mkdir /tmp
    hadoop fs -chmod -R 777 /tmp
# 2. 删除快照备份
a=bsc_`date -d '-7 day' +%Y-%m-%d-%H`
echo "$a"
hadoop fs -deleteSnapshot /bsc/ $a
# 3. 创建新的快照备份
hadoop fs -createSnapshot /bsc/ bsc_`date +%Y-%m-%d-%H`
# 4.确认是否创建成功
hadoop fs -ls /bsc/.snapshot/bsc_`date +%Y-%m-%d-%H`
if [ $? -eq 0 ];then
    echo "/bsc/.snapshot/bsc_`date +%Y-%m-%d-%H` success"
else 
    rm -rf /bscflow/dwd/check.sh
fi
hadoop fs -ls /bsc/.snapshot/$a
if [ $? -eq 0 ];then
    rm -rf /bscflow/dwd/check.sh
else 
    echo "$a delete success"
fi
echo "End syncing dwd_t1_invoice data into DWD layer on ${sync_date} .................."