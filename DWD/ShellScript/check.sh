#!/bin/bash
# Function:
#   sync up inventory movement transation data to dwd layer
# History:
# 2021-05-18    Donny   v1.0    init

# 参数
target_db_name='opsdw' # 目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

# 默认取当前时间的前一天 
if [ -n "$1" ] ;then 
    sync_date=$1
else
    sync_date=$(date  +%F)
fi
#sh /root/ttt.sh
echo "succes"
#该脚本作用就是其他脚本执行成功就调用这个，如果执行失败就把这个文件删除，azkaban找不到这个文件就会报错
