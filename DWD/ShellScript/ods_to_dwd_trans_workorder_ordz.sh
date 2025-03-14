#!/bin/bash
# Function:
#   sync up work order loclization  information data to dwd layer
# History:
# 2021-05-28    Donny   v1.0    init

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


echo "start syncing work order data into DWD layer on ${sync_date} .................."

# 1 Hive SQL string
sto_sql="
-- 参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
---用来计算kpi本地化数量的
-- sync up SQL string
insert overwrite table ${target_db_name}.dwd_trans_workorder_ordz partition(dt)
SELECT 
	plant ,
	workorderno,
	sapdeliveryno,
	material,
	batch,
	releasedqty,
	date(workordercreatedt) as dt
from  ${target_db_name}.ods_trans_workorder_ordz --源表 TRANS_WorkOrder_OrdZ
where dt='$sync_date'
and month(workordercreatedt)>=1;"
# 2. 执行加载数据SQL
$hive -e "$sto_sql"

echo "End syncing work order data into DWD layer on ${sync_date} .................."