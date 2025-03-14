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


insert overwrite table dwt_lifecycle_yh_falls partition(dt_year, dt_month)
SELECT 
      pgi_month, pgi_year, 
      product_line1, product_line2, product_line3, product_line4, product_line5, 
      cust_level1, cust_level2, cust_level3, cust_level4,
      'inter_trans' AS leadtime_section,
      inter_trans AS leadtime,
      dt_year, 
      dt_month
FROM dwt_lifecycle_leadtime_yh_topic
      where dt_month='${sync_date[month]}'
      and dt_year= '${sync_date[year]}'
   GROUP BY pgi_month, pgi_year, product_line1, product_line2, product_line3, product_line4, product_line5, 
   cust_level1, cust_level2, cust_level3, cust_level4, inter_trans,
    dt_year, dt_month
UNION all
SELECT 
      pgi_month, pgi_year, 
      product_line1, product_line2, product_line3, product_line4, product_line5, 
      cust_level1, cust_level2, cust_level3, cust_level4,
       'import_record_leadtime' AS leadtime_section,
      import_record_leadtime as leadtime,
      dt_year, 
      dt_month
FROM dwt_lifecycle_leadtime_yh_topic
      where dt_month='${sync_date[month]}'
      and dt_year= '${sync_date[year]}'
   GROUP BY pgi_month, pgi_year, product_line1, product_line2, product_line3, product_line4, product_line5, 
   cust_level1, cust_level2, cust_level3, cust_level4, import_record_leadtime,
   dt_year, dt_month
UNION all
SELECT 
      pgi_month, pgi_year, 
      product_line1, product_line2, product_line3, product_line4, product_line5, 
      cust_level1, cust_level2, cust_level3, cust_level4,
      't2_migo' AS leadtime_section,
      t2_migo as leadtime,
      dt_year, 
      dt_month
FROM dwt_lifecycle_leadtime_yh_topic
      where dt_month='${sync_date[month]}'
      and dt_year= '${sync_date[year]}'
   GROUP BY pgi_month, pgi_year, product_line1, product_line2, product_line3, product_line4, product_line5, 
   cust_level1, cust_level2, cust_level3, cust_level4, t2_migo,
   dt_year, dt_month
UNION all
SELECT 
      pgi_month, pgi_year, 
      product_line1, product_line2, product_line3, product_line4, product_line5, 
      cust_level1, cust_level2, cust_level3, cust_level4,
      'localization' AS leadtime_section,
      localization as leadtime,
      dt_year, 
      dt_month
FROM dwt_lifecycle_leadtime_yh_topic
      where dt_month='${sync_date[month]}'
      and dt_year= '${sync_date[year]}'
   GROUP BY pgi_month, pgi_year, product_line1, product_line2, product_line3, product_line4, product_line5, 
   cust_level1, cust_level2, cust_level3, cust_level4, localization,
   dt_year, dt_month
UNION all
SELECT 
      pgi_month, pgi_year, 
      product_line1, product_line2, product_line3, product_line4, product_line5, 
      cust_level1, cust_level2, cust_level3, cust_level4,
      'domestic_trans' AS leadtime_section,
      domestic_trans as leadtime,
      dt_year, 
      dt_month
FROM dwt_lifecycle_leadtime_yh_topic
      where dt_month='${sync_date[month]}'
      and dt_year= '${sync_date[year]}'
   GROUP BY pgi_month, pgi_year, product_line1, product_line2, product_line3, product_line4, product_line5, 
   cust_level1, cust_level2, cust_level3, cust_level4, domestic_trans,
   dt_year, dt_month
union all
SELECT 
      pgi_month, pgi_year, 
      product_line1, product_line2, product_line3, product_line4, product_line5, 
      cust_level1, cust_level2, cust_level3, cust_level4,
      'yh_product_putaway' AS leadtime_section,
      yh_product_putaway as leadtime,
      dt_year, 
      dt_month
FROM dwt_lifecycle_leadtime_yh_topic
      where dt_month='${sync_date[month]}'
      and dt_year= '${sync_date[year]}'
   GROUP BY pgi_month, pgi_year, product_line1, product_line2, product_line3, product_line4, product_line5, 
   cust_level1, cust_level2, cust_level3, cust_level4, yh_product_putaway,
   dt_year, dt_month
UNION all
SELECT 
      pgi_month, pgi_year, 
      product_line1, product_line2, product_line3, product_line4, product_line5, 
      cust_level1, cust_level2, cust_level3, cust_level4,
      'in_store_so_create' AS leadtime_section,
      in_store_so_create as leadtime,
      dt_year, 
      dt_month
FROM dwt_lifecycle_leadtime_yh_topic
      where dt_month='${sync_date[month]}'
      and dt_year= '${sync_date[year]}'
   GROUP BY pgi_month, pgi_year, product_line1, product_line2, product_line3, product_line4, product_line5, 
   cust_level1, cust_level2, cust_level3, cust_level4, in_store_so_create,
   dt_year, dt_month
UNION all
SELECT 
      pgi_month, pgi_year, 
      product_line1, product_line2, product_line3, product_line4, product_line5, 
      cust_level1, cust_level2, cust_level3, cust_level4,
      'so_order_processing' AS leadtime_section,
      so_order_processing as leadtime,
      dt_year, 
      dt_month
FROM dwt_lifecycle_leadtime_yh_topic
      where dt_month='${sync_date[month]}'
      and dt_year= '${sync_date[year]}'
   GROUP BY pgi_month, pgi_year, product_line1, product_line2, product_line3, product_line4, product_line5, 
   cust_level1, cust_level2, cust_level3, cust_level4, so_order_processing,
   dt_year, dt_month
UNION all
SELECT 
      pgi_month, pgi_year, 
      product_line1, product_line2, product_line3, product_line4, product_line5, 
      cust_level1, cust_level2, cust_level3, cust_level4,
      'so_pgi_processing' AS leadtime_section,
      so_pgi_processing as leadtime,
      dt_year, 
      dt_month
FROM dwt_lifecycle_leadtime_yh_topic
      where dt_month='${sync_date[month]}'
      and dt_year= '${sync_date[year]}'
   GROUP BY pgi_month, pgi_year, product_line1, product_line2, product_line3, product_line4, product_line5, 
   cust_level1, cust_level2, cust_level3, cust_level4, so_pgi_processing,
   dt_year, dt_month
UNION all
SELECT 
      pgi_month, pgi_year, product_line1, product_line2, product_line3, product_line4, product_line5, 
      cust_level1, cust_level2, cust_level3, cust_level4,
      'so_delivery' AS leadtime_section,
      so_delivery as leadtime,
      dt_year, 
      dt_month
FROM dwt_lifecycle_leadtime_yh_topic
      where dt_month='${sync_date[month]}'
      and dt_year= '${sync_date[year]}'
   GROUP BY pgi_month, pgi_year, product_line1, product_line2, product_line3, product_line4, product_line5, 
   cust_level1, cust_level2, cust_level3, cust_level4, so_delivery,
   dt_year, dt_month

;
"
# 2. 执行加载数据SQL
$hive -e "$dwt_sql"

echo "End syncing xxx data into DWT layer on $sync_date .................."