#!/bin/bash
# Function:
#   sync up master data from ods data to dwd layer
# History:
# 2021-05-12    Donny   v1.0    draft

# 设置必要的参数
target_db_name='opsdw'                # 数据加载目标数据库名称
hive=/opt/module/hive3/bin/hive       # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

# 如果是输入的日期按照取输入日期，否则取当前时间的前一天
# 时间格式都配置成 YYYY-MM-DD 格式，这是 Hive 默认支持的时间格式
if [ -n "$1" ]; then
       sync_date=$1
else
       sync_date=$(date  +%F)
fi

echo "start syncing master data into DWD layer on ${sync_date} .................."

# 1 Hive SQL string
master_sql="
-- 配置参数
--set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
--6 Exchange Rate
insert overwrite table ${target_db_name}.dwd_dim_exchange_rate partition(dt='$sync_date')
select  from_currency
       ,to_currency
       ,valid_from
       ,rate
       ,ratio_from
       ,ratio_to
from ${target_db_name}.ods_exchange_rate
where dt='$sync_date'; 
"
# 2. 执行加载数据SQL
$hive -e "$master_sql"

echo "End syncing master data into DWD layer on ${sync_date} .................."