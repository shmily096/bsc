#!/bin/bash
# Function:
#   sync up dwt_product_putaway_leadtime_slc_topic data to dwt layer
# History:
# 2021-05-18    Donny   v1.0    init

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



insert overwrite table dwt_product_putaway_leadtime_slc_topic partition(dt_year, dt_month)
select  min(slcd.localization)                                        as localization_min
       ,sum(slcd.localization*slcd.localization / localization_total) as localization_weiavg
       ,percentile_approx(slcd.localization,0.5)                      as localization_median
       ,max(slcd.localization)                                        as localization_max
       ,min(slcd.putaway)                                             as putaway_min
       ,sum(slcd.putaway*slcd.putaway / putaway_total)                as putaway_weiavg
       ,percentile_approx(slcd.putaway,0.5)                           as putaway_median
       ,max(slcd.putaway)                                             as putaway_max
       ,min(slcd.slc_putaway)                                         as slc_putaway_min
       ,sum(slcd.slc_putaway*slcd.slc_putaway / slc_putaway_total)    as slc_putaway_weiavg
       ,percentile_approx(slcd.slc_putaway,0.5)                       as slc_putaway_median
       ,max(slcd.slc_putaway)                                         as slc_putaway_max
       ,b.slc_qty
       ,month(slcd.pgi_date)                                          as putaway_month
       ,year(slcd.pgi_date)                                           as putaway_year
       ,b.local_wo_no
       ,date_format(slcd.pgi_date, 'yyyy')
       ,date_format(slcd.pgi_date, 'MM')
from dws_product_putaway_leadtime_slc_daily_trans slcd
left join 
(
    select  sum(slc_putaway)  as slc_putaway_total
           ,sum(localization) as localization_total
           ,sum(putaway)      as putaway_total
           ,month(pgi_date)   as pgi_month
           ,year(pgi_date)    as pgi_year
    from dws_product_putaway_leadtime_slc_daily_trans
  where date_format(pgi_date, 'MM')='${sync_date[month]}'
  and date_format(pgi_date, 'yyyy')= '${sync_date[year]}'
    group by  month(pgi_date)
             ,year(pgi_date) 
) a
on month(slcd.pgi_date) = a.pgi_month and year(slcd.pgi_date)= a.pgi_year
left join
( 
    select 
          sum(d.work_order_num) as local_wo_no
         ,sum(c.qr_code_num) as slc_qty
         ,c.pgi_month
         ,c.pgi_year
    from
    (
        select  
            max(qr_code_num) as qr_code_num
            ,work_order_no
            ,month(pgi_date)   as pgi_month
            ,year(pgi_date)    as pgi_year
            ,pgi_date
    from dws_product_putaway_leadtime_slc_daily_trans
  where date_format(pgi_date, 'MM')='${sync_date[month]}'
  and date_format(pgi_date, 'yyyy')= '${sync_date[year]}'
    group by  month(pgi_date)
             ,year(pgi_date) 
             ,work_order_no
             ,pgi_date
    ) c
    left join
    (
        select  max(work_order_num) as work_order_num
            ,work_order_no
            ,month(pgi_date)   as pgi_month
            ,year(pgi_date)    as pgi_year
    from dws_product_putaway_leadtime_slc_daily_trans
  where date_format(pgi_date, 'MM')='${sync_date[month]}'
  and date_format(pgi_date, 'yyyy')= '${sync_date[year]}'
    group by  month(pgi_date)
             ,year(pgi_date) 
             ,work_order_no
    )d
    on c.pgi_month=d.pgi_month
       and c.pgi_year=d.pgi_year
       and c.work_order_no = d.work_order_no
             group by  c.pgi_month
                      ,c.pgi_year 
)b
on month(slcd.pgi_date) = b.pgi_month and year(slcd.pgi_date)= b.pgi_year
            where slcd.localization is not null
            and slcd.putaway is not null
            and slcd.slc_putaway is not null
            and date_format(slcd.pgi_date, 'MM')='${sync_date[month]}'
            and date_format(slcd.pgi_date, 'yyyy')= '${sync_date[year]}'
group by  month(slcd.pgi_date)
         ,year(slcd.pgi_date)
         ,b.local_wo_no
         ,b.slc_qty
         ,date_format(slcd.pgi_date, 'yyyy')
         ,date_format(slcd.pgi_date, 'MM')
;
"
# 2. 执行加载数据SQL
$hive -e "$dwt_sql"

echo "End syncing dwt_product_putaway_leadtime_slc_topic data into DWT layer on $sync_date .................."