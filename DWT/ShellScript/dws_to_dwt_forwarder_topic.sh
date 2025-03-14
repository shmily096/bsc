#!/bin/bash
# Function:
#   sync up forwarder data to dwt layer
# History:
# 2021-07-08    Donny   v1.0    init

# 参数
target_db_name='opsdw' # 目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

# 默认取当前时间的前一天 
# if [ -n "$1" ] ;then 
#     sync_date=$1
# else
#     sync_date=$(date  +%F)
# fi

if [ -n "$1" ] ;then
    sync_year=$1
else
    sync_year=$(date  +'%Y')
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
(select 
     max(pick_up_leadtime) as pick_up_leadtime
     ,forwarder
     ,sto_no
     ,delivery_no
     ,pgi_date
from dws_forwarder_daily_trans
     where pick_up_leadtime is not null
           and date_format(pgi_date, 'MM')='${sync_date[month]}'
           and date_format(pgi_date, 'yyyy')= '${sync_date[year]}'
     group by forwarder
     ,sto_no
     ,delivery_no
     ,pgi_date
)

insert overwrite table dwt_forwarder_topic partition(dt_year, dt_month)
select
    a.forwarder,
    round(percentile_approx(a.pick_up_leadtime,0.5),1) as pick_up_median,
    month(a.pgi_date),
    year(a.pgi_date),
    date_format(a.pgi_date, 'yyyy'),
    date_format(a.pgi_date, 'MM')
from
    a
group by
    a.forwarder,
    month(a.pgi_date), 
    year(a.pgi_date),
    date_format(a.pgi_date, 'MM'),
    date_format(a.pgi_date, 'yyyy')
"
# 2. 执行加载数据SQL
$hive -e "$dwt_sql"

echo "End syncing forwarder data into DWT layer on $sync_year .................."