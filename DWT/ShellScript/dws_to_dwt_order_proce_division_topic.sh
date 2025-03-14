#!/bin/bash
# Function:
#   sync up order_proce_division_topic data to dwt layer
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


insert overwrite table opsdw.dwt_order_proce_division_topic partition(dt_year, dt_month)
select  opdd.division_id
       ,percentile_approx(opdd.order_processing,0.5) as order_processing_median
       ,month(opdd.actual_gi_date)                   as pgi_month
       ,year(opdd.actual_gi_date)                    as pgi_year
       ,date_format(opdd.actual_gi_date, 'yyyy')
       ,date_format(opdd.actual_gi_date, 'MM')
from dws_order_proce_division_daily_trans opdd
            where opdd.order_processing is not null
            and opdd.order_processing <>0
            and date_format(opdd.actual_gi_date, 'MM')='${sync_date[month]}'
            and date_format(opdd.actual_gi_date, 'yyyy')= '${sync_date[year]}'
group by  month(opdd.actual_gi_date)
         ,year(opdd.actual_gi_date)
         ,opdd.division_id
         ,date_format(opdd.actual_gi_date, 'yyyy')
         ,date_format(opdd.actual_gi_date, 'MM')
;
"
# 2. 执行加载数据SQL
$hive -e "$dwt_sql"

echo "End syncing order_proce_division_topic data into DWT layer on $sync_date .................."