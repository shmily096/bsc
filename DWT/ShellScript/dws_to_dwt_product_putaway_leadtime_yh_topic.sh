#!/bin/bash
# Function:
#   sync up dws_product_putaway_leadtime_yh_daily_trans data to dwt layer
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


insert overwrite table dwt_product_putaway_leadtime_yh_topic partition(dt_year, dt_month)
select  min(slyd.localization)                                              as localization_min
       ,sum(slyd.localization*slyd.localization / localization_total)       as localization_weiavg
       ,percentile_approx(slyd.localization,0.5)                            as localization_median
       ,max(slyd.localization)                                              as localization_max
       ,min(slyd.domestic_trans)                                            as domestic_trans_min
       ,sum(slyd.domestic_trans*slyd.domestic_trans / domestic_trans_total) as domestic_trans_weiavg
       ,percentile_approx(slyd.domestic_trans,0.5)                          as domestic_trans_median
       ,max(slyd.domestic_trans)                                            as domestic_trans_max
       ,min(slyd.putaway)                                                   as putaway_min
       ,sum(slyd.putaway*slyd.putaway / putaway_total)                      as putaway_weiavg
       ,percentile_approx(slyd.putaway,0.5)                                 as putaway_median
       ,max(slyd.putaway)                                                   as putaway_max
       ,min(slyd.yh_putaway)                                                as yh_putaway_min
       ,sum(slyd.yh_putaway*slyd.yh_putaway / yh_putaway_total)             as yh_putaway_weiavg
       ,percentile_approx(slyd.yh_putaway,0.5)                              as yh_putaway_median
       ,max(slyd.yh_putaway)                                                as yh_putaway_max
       ,b.yh_qty
       ,month(slyd.pgi_date)                                                as putaway_month
       ,year(slyd.pgi_date)                                                 as putaway_year
       ,b.dom_wo_no
       ,date_format(pgi_date, 'yyyy')
       ,date_format(pgi_date, 'MM')
from dws_product_putaway_leadtime_yh_daily_trans slyd
left join 
(
    select  sum(yh_putaway)     as yh_putaway_total
           ,sum(localization)   as localization_total
           ,sum(domestic_trans) as domestic_trans_total
           ,sum(putaway)        as putaway_total
           ,month(pgi_date)     as pgi_month
           ,year(pgi_date)      as pgi_year
    from dws_product_putaway_leadtime_yh_daily_trans
    where date_format(pgi_date, 'MM')='${sync_date[month]}'
    and date_format(pgi_date, 'yyyy')= '${sync_date[year]}'
    group by  month(pgi_date)
             ,year(pgi_date) 
) a
on month(slyd.pgi_date) = a.pgi_month and year(slyd.pgi_date)= a.pgi_year
left join
( 
    select 
          sum(d.work_order_num) as dom_wo_no
         ,sum(c.qr_code_num) as yh_qty
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
    from dws_product_putaway_leadtime_yh_daily_trans
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
    from dws_product_putaway_leadtime_yh_daily_trans
    where date_format(pgi_date, 'MM')='${sync_date[month]}'
    and date_format(pgi_date, 'yyyy')= '${sync_date[year]}'
    group by  month(pgi_date)
             ,year(pgi_date) 
             ,work_order_no
    )d
    on c.pgi_month =d.pgi_month
       and c.pgi_year=d.pgi_year
       and c.work_order_no = d.work_order_no
             group by  c.pgi_month
                      ,c.pgi_year 
)b
on month(slyd.pgi_date) = b.pgi_month and year(slyd.pgi_date)= b.pgi_year
            where slyd.localization is not null
            and slyd.domestic_trans is not null
            and slyd.putaway is not null
            and slyd.yh_putaway is not null
            and date_format(slyd.pgi_date, 'MM')='${sync_date[month]}'
            and date_format(slyd.pgi_date, 'yyyy')= '${sync_date[year]}'
group by  month(slyd.pgi_date)
         ,year(slyd.pgi_date)
         ,date_format(pgi_date, 'yyyy')
         ,date_format(pgi_date, 'MM')
         ,b.dom_wo_no
         ,b.yh_qty
;
"
# 2. 执行加载数据SQL
$hive -e "$dwt_sql"

echo "End syncing dws_product_putaway_leadtime_yh_daily_trans data into DWT layer on $sync_date .................."