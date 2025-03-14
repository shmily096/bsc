#!/bin/bash
# Function:
#   sync up lifecycle_leadtime_slcyh_summarize data to dwt layer
# History:
# 2021-07-08    Donny   v1.0    init

# 参数
target_db_name='opsdw' # 目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

# 默认取当前时间的前一天
if [ -n "$1" ] ;then 
    sync_year=$1
else
    sync_year=$(date  +'%Y')
fi
if [ -n "$2" ] ;then 
    sync_month=$2
else
    sync_month=$(date  +'%m')
fi

echo "start syncing data into dws layer on $sync_year :$sync_month .................."

dwt_sql="
use ${target_db_name};
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;




insert overwrite table dwt_lifecycle_leadtime_slcyh_summarize_topic partition(dt_year, dt_month)
select  a.e2e_min
       ,a.e2e_weiavg -- 加权平均
       ,a.e2e_median --中位数
       ,a.e2e_max
       ,a.in_store_min
       ,a.in_store_weiavg -- 加权平均
       ,a.in_store_median --中位数
       ,a.in_store_max
       ,a.putaway_min
       ,a.putaway_weiavg -- 加权平均
       ,a.putaway_median --中位数
       ,a.putaway_max
       ,a.pgi_month
       ,a.pgi_year
       ,'slc' as putaway_location
       ,a.dt_year
       ,a.dt_month
from dwt_lifecycle_leadtime_slc_summarize_topic a
      where a.dt_month ='$sync_month'
      and a.dt_year = '$sync_year'

union all
select  d.e2e_min
       ,d.e2e_weiavg -- 加权平均
       ,d.e2e_median --中位数
       ,d.e2e_max
       ,d.in_store_min
       ,d.in_store_weiavg -- 加权平均
       ,d.in_store_median --中位数
       ,d.in_store_max
       ,d.putaway_min
       ,d.putaway_weiavg -- 加权平均
       ,d.putaway_median --中位数
       ,d.putaway_max
       ,d.pgi_month
       ,d.pgi_year
       ,'yh' as putaway_location
       ,d.dt_year
       ,d.dt_month
from dwt_lifecycle_leadtime_yh_summarize_topic d
      where d.dt_month='$sync_month'
      and d.dt_year= '$sync_year'

;
"
# 2. 执行加载数据SQL
$hive -e "$dwt_sql"

echo "End syncing lifecycle_leadtime_slcyh_summarize data into DWT layer on $sync_month .................."