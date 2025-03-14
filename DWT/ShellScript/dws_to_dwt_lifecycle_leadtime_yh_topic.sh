#!/bin/bash
# Function:
#   sync up lifecycle_leadtime_yh_topic data to dwt layer
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

insert overwrite table dwt_lifecycle_leadtime_YH_topic partition(dt_year, dt_month)
select  month(pgi_date)                               as pgi_month
       ,year(pgi_date)                                as pgi_year
       ,product_line1
       ,product_line2
       ,product_line3
       ,product_line4
       ,product_line5
       ,cust_level1
       ,cust_level2
       ,cust_level3
       ,cust_level4
       ,percentile_approx(inter_trans,0.5)            as inter_trans
       ,percentile_approx(import_record_leadtime,0.5) as import_record_leadtime
       ,percentile_approx(t2_migo,0.5)                as t2_migo
       ,percentile_approx(localization,0.5)           as localization
       ,percentile_approx(domestic_trans,0.5)         as domestic_trans
       ,percentile_approx(yh_product_putaway,0.5)     as yh_product_putaway
       ,percentile_approx(in_store_so_create,0.5)     as in_store_so_create
       ,percentile_approx(so_order_processing,0.5)    as so_order_processing
       ,percentile_approx(so_pgi_processing,0.5)      as so_pgi_processing
       ,percentile_approx(so_delivery,0.5)            as so_delivery
       ,percentile_approx(in_store,0.5)               as in_store
       ,percentile_approx(e2e,0.5)                    as e2e
       ,date_format(pgi_date, 'yyyy')
       ,date_format(pgi_date, 'MM')
from dws_lifecycle_leadtime_yh_daily_trans
          where inter_trans is not null
          and import_record_leadtime is not null
          and t2_migo is not null
          and localization is not null
          and domestic_trans is not null
          and yh_product_putaway is not null
          and in_store_so_create is not null
          and so_order_processing is not null
          and so_pgi_processing is not null
--          and so_delivery is not null
          and in_store is not null
          and date_format(pgi_date, 'MM')='${sync_date[month]}'
          and date_format(pgi_date, 'yyyy')='${sync_date[year]}'
group by  month(pgi_date)
         ,year(pgi_date)
         ,product_line1
         ,product_line2
         ,product_line3
         ,product_line4
         ,product_line5
         ,cust_level1
         ,cust_level2
         ,cust_level3
         ,cust_level4 
         ,date_format(pgi_date, 'MM')
         ,date_format(pgi_date, 'yyyy')
;
"
# 2. 执行加载数据SQL
$hive -e "$dwt_sql"

echo "End syncing lifecycle_leadtime_yh_topic data into DWT layer on $sync_date .................."