#!/bin/bash
# Function:
#   sync up dwt_imported_topic data to dwd layer
# History:
# 2021-05-18    Donny   v1.0    init

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

with iedt as( 
    select inter_trans_leadtime, migo_leadtime, inbound_leadtime, import_record_leadtime, work_order_no, work_order_num
    ,inter_trans_leadtime*inter_trans_leadtime as inter_trans_s
    ,migo_leadtime*migo_leadtime as migo_s
    ,inbound_leadtime*inbound_leadtime as inbound_s
    ,import_record_leadtime*import_record_leadtime as import_record_s
           ,month(pgi_date) as declar_month
           ,year(pgi_date) as declar_year
           ,date_format(pgi_date, 'yyyy') as dt_year
           ,date_format(pgi_date, 'MM') as dt_month
from dws_import_export_daily_trans
where date_format(pgi_date, 'MM')='${sync_date[month]}'
      and date_format(pgi_date, 'yyyy')= '${sync_date[year]}'
      and inter_trans_leadtime is not null
      and migo_leadtime is not null
      and inbound_leadtime is not null
      and import_record_leadtime is not null
group BY inter_trans_leadtime, migo_leadtime, inbound_leadtime, import_record_leadtime, work_order_no, work_order_num
,inter_trans_leadtime*inter_trans_leadtime 
    ,migo_leadtime*migo_leadtime 
    ,inbound_leadtime*inbound_leadtime 
    ,import_record_leadtime*import_record_leadtime
           ,month(pgi_date) 
           ,year(pgi_date)
            ,date_format(pgi_date, 'yyyy')
           ,date_format(pgi_date, 'MM')
      
)
,b as (
    select  round(sum(inter_trans_leadtime),1)         as inter_trans_total
           ,round(sum(migo_leadtime),1)               as migo_total
           ,round(sum(inbound_leadtime),1)            as inbound_total
           ,round(sum(import_record_leadtime),1)      as import_record_total
           ,declar_month
           ,declar_year
    from iedt
    group by  declar_month
              ,declar_year
)
,c as (

    select sum(work_order_num) as import_record_wo_num, declar_month, declar_year
    from 
    (select work_order_no, max(work_order_num) as work_order_num,declar_month
           ,declar_year
from iedt
group by work_order_no, declar_month
           ,declar_year
           )d
    group by declar_month, declar_year
)
,e as (
select (iedt.inter_trans_s / b.inter_trans_total) as inter_trans_wei
,(iedt.migo_s / b.migo_total) as migo_wei
,(iedt.inbound_s / b.inbound_total) as inbound_wei
,(iedt.import_record_s / b.import_record_total) as import_record_wei
,iedt.declar_month
           ,iedt.declar_year
    from iedt left join b 
    on iedt.declar_month = b.declar_month 
   and iedt.declar_year = b.declar_year
              )
,f as(
select round(sum(inter_trans_wei),1) as  inter_trans_weiavg
,round(sum(migo_wei),1) as migo_weiavg
,round(sum(inbound_wei),1) as inbound_weiavg
,round(sum(import_record_wei),1) as import_record_weiavg
,declar_month
           ,declar_year
    from e 
    group by  declar_month
              ,declar_year
)
,g as (
select round(min(iedt.inter_trans_leadtime),1)         as inter_trans_min
       ,round(percentile_approx(iedt.inter_trans_leadtime,0.5),1) as inter_trans_median
       ,round(max(iedt.inter_trans_leadtime),1) as inter_trans_max
       ,round(min(iedt.migo_leadtime),1)                as migo_min
       ,round(percentile_approx(iedt.migo_leadtime,0.5),1) as migo_median
       ,round(max(iedt.migo_leadtime),1) as migo_max
       ,round(min(iedt.inbound_leadtime),1)             as inbound_min
       ,round(percentile_approx(iedt.inbound_leadtime,0.5),1) as inbound_median
       ,round(max(iedt.inbound_leadtime),1)             as inbound_max
       ,round(max(iedt.import_record_leadtime),1) as import_record_max
       ,round(min(iedt.import_record_leadtime),1)       as import_record_min
       ,round(percentile_approx(iedt.import_record_leadtime,0.5),1) as import_record_median
       ,declar_month
           ,declar_year
           ,dt_year
           ,dt_month
       from iedt
       group by  declar_month
              ,declar_year
                         ,dt_year
           ,dt_month
)


insert overwrite table dwt_imported_topic partition(dt_year, dt_month)
select g.inter_trans_min
       ,f.inter_trans_weiavg
       ,g.inter_trans_median
       ,g.inter_trans_max
       ,g.migo_min
       ,f.migo_weiavg
       ,g.migo_median
       ,g.migo_max
       ,g.inbound_min
       ,f.inbound_weiavg
       ,g.inbound_median
       ,g.inbound_max
       ,g.import_record_max
       ,g.import_record_min
       ,f.import_record_weiavg
       ,g.import_record_median
       ,c.import_record_wo_num
       ,f.declar_month
       ,f.declar_year
       ,g.dt_year
        ,g.dt_month
from g
left join f
on g.declar_month  = f.declar_month 
   and g.declar_year = f.declar_year
left join c
on g.declar_month  = c.declar_month 
   and  g.declar_year = c.declar_year  

"
# 2. 执行加载数据SQL
$hive -e "$dwt_sql"

echo "End syncing dwt_imported_topic data into DWT layer on $sync_year .................."