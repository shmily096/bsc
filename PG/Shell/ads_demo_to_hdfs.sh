#!/bin/bash
# Function:
#   将查询的结果导出至HDFS上
# History:
# 2021-11-08    Donny   v1.0    init

# 参数
target_db_name='opsdw' # 目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

# 默认取当前时间 
if [ -n "$2" ] ;then 
    sync_date=$2
else
    sync_date=$(date  +%F)
fi

# 2 设置导出的 HDFS路径
# export_dir='/bsc/opsdw/export/ads_demo'

echo "start exporting xxxx data into HDFS on $sync_date .................."

# 1 Hive SQL string
# str_sql="

# use ${target_db_name};

# insert OVERWRITE directory '${export_dir}'
# row format delimited fields terminated by '\t' 
# stored as textfile
# select * 
# from ads_demo 
# where name is not null;

# "
dwd_trans_lpgreport="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dwd_trans_lpgreport'
row format delimited fields terminated by '\001' 
stored as textfile
SELECT 
	distinct upn,coo,coo_status,dt
    FROM opsdw.dwd_trans_lpgreport
where dt =(select max(dt) from opsdw.dwd_trans_lpgreport)
;
"

echo "$dwd_trans_lpgreport"
# 2. 执行加载数据SQL
$hive -e "$dwd_trans_lpgreport"

exitCode=$?
if [ $exitCode -ne 0 ];then
    echo "[Error] hive execute failed!"
    exit $exitCode
fi

echo "End exporting xxx data into HDFS on $sync_date .................."