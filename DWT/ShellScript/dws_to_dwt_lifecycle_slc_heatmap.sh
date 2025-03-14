#!/bin/bash
# Function:
#   sync up xxx data to dwt layer
# History:
# 2021-07-08    Donny   v1.0    init

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

declare -A sync_date=$(date +'([day]=%F [year]=%Y [month]=%m)')
yesterday=$(date  +%F)

echo "start syncing data into dws layer on ${sync_date[year]} :${sync_date[month]} .................."

dwt_sql="
use ${target_db_name};
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.reducers.max=8;
set mapred.reduce.tasks=8;
set hive.exec.parallel=false;


with a as
(select pgi_month, pgi_year, 
      product_line1, cust_level3,
      leadtime_section,
      avg(leadtime) AS leadtime,
      dt_year,
      dt_month
FROM dwt_lifecycle_slc_falls
group by
      pgi_month, pgi_year, 
      product_line1, cust_level3,
      leadtime_section,
      dt_year,
      dt_month)

insert overwrite table dwt_lifecycle_slc_heatmap partition(dt_year, dt_month)
SELECT 
      pgi_month, pgi_year, 
      product_line1, cust_level3,
      'e2e' as leadtime_section,
      sum(leadtime) AS leadtime,
      dt_year,
      dt_month
FROM a
      where dt_month='${sync_date[month]}'
      and dt_year= '${sync_date[year]}'
   GROUP BY 
      pgi_month, pgi_year, 
      product_line1, cust_level3,
      dt_year,
      dt_month

;
"
# 2. 执行加载数据SQL
$hive -e "$dwt_sql"

echo "End syncing xxx data into DWT layer on $sync_date .................."
