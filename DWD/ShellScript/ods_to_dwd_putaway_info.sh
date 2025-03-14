#!/bin/bash
# Function:
#   sync up sales order data from ods to dwd layer
# History:
# 2021-11-16    Amanda   v1.0    init


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

echo "start syncing so into DWD layer on ${sync_date} .................."

# 1 Hive SQL string
so_sql="
use ${target_db_name};
-- 参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;

-- sync up SQL string
insert overwrite table dwd_putaway_info partition(dt)
select 
    replace(invoice, '\'', '') as invoice,
    delivery_no, 
    putaway_date, 
    upn,
    qty,
    batch,
    plant,
    sl,
    unit,
    from_slocation,
    to_location,
    update_dt,
    date_format(putaway_date,'yyyy-MM-dd') as dt
from ods_putaway_info
where dt='$sync_date' and putaway_date is not null
"
# 2. 执行SQL
$hive -e "$so_sql"

echo "End syncing Sales order data into DWD layer on ${sync_date} .................."