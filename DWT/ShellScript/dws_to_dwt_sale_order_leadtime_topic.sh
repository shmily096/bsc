#!/bin/bash
# Function:
#   sync up sale_order_leadtime_topic data to dwt layer
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




with d as (
    select  sum(c.product_sale)     as product_sale_total
           ,sum(c.order_processing) as order_proce_total
           ,sum(c.pgi_processing)   as pgi_proce_total
           ,sum(c.transport)        as transport_total
           ,month(c.actual_gi_date)   as pgi_month
           ,year(c.actual_gi_date)    as pgi_year
           ,c.item_type
    from dws_sale_order_leadtime_daily_trans c
               where date_format(c.actual_gi_date, 'MM')='${sync_date[month]}'
               and date_format(c.actual_gi_date, 'yyyy')= '${sync_date[year]}'
               and c.product_sale is not null
               and c.order_processing is not null
               and c.pgi_processing is not null
               and c.transport is not null
    group by  month(c.actual_gi_date)
             ,year(c.actual_gi_date)
             ,c.item_type
)
, a as (
select round(product_sale*product_sale,1) as product_sale_t
,round(order_processing*order_processing,1) as order_processing_t
,round(pgi_processing*pgi_processing,1) as pgi_processing_t
,round(transport*transport,1) as transport_t
,month(actual_gi_date)    as pgi_month
,year(actual_gi_date)     as pgi_year
,item_type
from dws_sale_order_leadtime_daily_trans 
    where  date_format(actual_gi_date, 'MM')='${sync_date[month]}'
    and date_format(actual_gi_date, 'yyyy')= '${sync_date[year]}'
    and product_sale is not null
    and order_processing is not null
    and pgi_processing is not null
    and transport is not null
)
,e as (
select
sum(round(product_sale_t/product_sale_total,1)) as product_sale_weiavg
,sum(round(order_processing_t/order_proce_total,1)) as order_proce_weiavg
,sum(round(pgi_processing_t/pgi_proce_total,1)) as pgi_proce_weiavg
,sum(round(transport_t/transport_total,1)) as transport_weiavg
, a.pgi_month
, a.pgi_year
, a.item_type
from a 
left join 
 d
on a.pgi_month = d.pgi_month 
and a.pgi_year = d.pgi_year 
and a.item_type = d.item_type
group by a.pgi_month, a.pgi_year, a.item_type)
,f as (
 select b.item_type
       ,round(min(b.product_sale),1)                                          as product_sale_min
       ,round(max(b.product_sale),1)                                          as product_sale_max
       ,round(percentile_approx(b.product_sale,0.5),1)                        as product_sale_median
       ,round(min(b.order_processing),1)                                      as order_proce_min
       ,round(max(b.order_processing),1)                                      as order_proce_max
       ,round(percentile_approx(b.order_processing,0.5),1)                    as order_proce_median
       ,round(min(b.pgi_processing),1)                                        as pgi_proce_min
       ,round(max(b.pgi_processing),1)                                        as pgi_proce_max
       ,round(percentile_approx(b.pgi_processing,0.5),1)                      as pgi_proce_median
       ,round(min(b.transport),1)                                             as transport_min
       ,round(max(b.transport),1)                                             as transport_max
       ,round(percentile_approx(b.transport,0.5),1)                           as transport_median
       ,month(b.actual_gi_date)                                        as pgi_month
       ,year(b.actual_gi_date)                                         as pgi_year
              ,date_format(b.actual_gi_date, 'yyyy')  as dt_year
       ,date_format(b.actual_gi_date, 'MM') as dt_month
from dws_sale_order_leadtime_daily_trans b
where b.product_sale is not null
               and b.order_processing is not null
               and b.pgi_processing is not null
               and b.transport is not null
               and date_format(b.actual_gi_date, 'MM')='${sync_date[month]}'
               and date_format(b.actual_gi_date, 'yyyy')= '${sync_date[year]}'
group by  month(b.actual_gi_date)
         ,year(b.actual_gi_date) 
         ,b.item_type
         ,date_format(b.actual_gi_date, 'yyyy')
         ,date_format(b.actual_gi_date, 'MM'))





insert overwrite table dwt_sale_order_leadtime_topic partition(dt_year, dt_month)
select  f.item_type
       ,f.product_sale_min
       ,round(e.product_sale_weiavg,1)        as product_sale_weiavg
       ,f.product_sale_max
       ,f.product_sale_median
       ,f.order_proce_min
       ,round(e.order_proce_weiavg,1) as order_proce_weiavg
       ,f.order_proce_max
       ,f.order_proce_median
       ,f.pgi_proce_min
       ,round(e.pgi_proce_weiavg,1)       as pgi_proce_weiavg
       ,f.pgi_proce_max
       ,f.pgi_proce_median
       ,f.transport_min
       ,round(e.transport_weiavg,1)                 as transport_weiavg
       ,f.transport_max
       ,f.transport_median
       ,f.pgi_month
       ,f.pgi_year
       ,f.dt_year
       ,f.dt_month
from f
left join 
 e
on f.pgi_month = e.pgi_month 
and f.pgi_year = e.pgi_year 
and f.item_type = e.item_type
;
"
# 2. 执行加载数据SQL
$hive -e "$dwt_sql"

echo "End syncing xxx data into DWT layer on $sync_date .................."