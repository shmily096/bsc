#!/bin/bash
# Function:
#   sync up lifecycle_leadtime_slc data to dwt layer
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
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.reducers.max=8;
set mapred.reduce.tasks=8;
set hive.exec.parallel=false;

drop table if exists tmp_dwt_lifecycle_leadtime_slc_summarize_topic_lslc;
create table tmp_dwt_lifecycle_leadtime_slc_summarize_topic_lslc stored as orc as 
select 
    if(e2e is not null, e2e, 0)  as e2e, 
    if(in_store is not null, in_store, 0)  as in_store,
    if(in_store is not null, round(in_store*in_store,1), 0)  as in_store_s,
    if(localization is not null, localization, 0)  as localization, 
    if(slc_product_putaway is not null, slc_product_putaway,0) as slc_product_putaway, 
    month(pgi_date)                         as pgi_month
    ,year(pgi_date)                          as pgi_year
    ,date_format(pgi_date, 'yyyy')  as dt_year
    ,date_format(pgi_date, 'MM')  as dt_month
from dws_lifecycle_leadtime_slc_daily_trans
       where date_format(dt, 'MM')='$sync_month'
       and date_format(dt, 'yyyy')= '$sync_year'
group by e2e, in_store, in_store*in_store, localization, slc_product_putaway, month(pgi_date)
             ,year(pgi_date), date_format(pgi_date, 'yyyy')
       ,date_format(pgi_date, 'MM');

drop table if exists tmp_dwt_lifecycle_leadtime_slc_summarize_topic_b;
create table tmp_dwt_lifecycle_leadtime_slc_summarize_topic_b stored as orc as 
    select  sum(e2e)                                as e2e_total
           ,sum(in_store)                           as in_store_total
           ,sum(localization + slc_product_putaway) as slc_putaway_total
           ,pgi_month
           ,pgi_year
    from tmp_dwt_lifecycle_leadtime_slc_summarize_topic_lslc lslc
    group by pgi_month
           ,pgi_year;
drop table if exists tmp_dwt_lifecycle_leadtime_slc_summarize_topic_e;
create table tmp_dwt_lifecycle_leadtime_slc_summarize_topic_e stored as orc as 
select
     if(b.in_store_total>0,round((lslc.in_store_s/ b.in_store_total), 1),0) as in_store_wei
     ,if(b.e2e_total >0,round((lslc.e2e*lslc.e2e / b.e2e_total), 1),0) as e2e_wei
     ,if(b.slc_putaway_total>0,round(((lslc.localization + lslc.slc_product_putaway)*(lslc.localization + lslc.slc_product_putaway) / b.slc_putaway_total),1),0) as slc_putaway_wei
     ,lslc.pgi_month
     ,lslc.pgi_year
from tmp_dwt_lifecycle_leadtime_slc_summarize_topic_lslc lslc 
left join tmp_dwt_lifecycle_leadtime_slc_summarize_topic_b b
     on lslc.pgi_month = b.pgi_month and lslc.pgi_year = b.pgi_year;

drop table tmp_dwt_lifecycle_leadtime_slc_summarize_topic_b;

drop table if exists tmp_dwt_lifecycle_leadtime_slc_summarize_topic_d;
create table tmp_dwt_lifecycle_leadtime_slc_summarize_topic_d stored as orc as 
select  sum(e.e2e_wei)  as e2e_weiavg
       ,sum(e.in_store_wei)    as in_store_weiavg -- 加权平均
       ,sum(e.slc_putaway_wei) as slc_putaway_weiavg
       ,e.pgi_month
       ,e.pgi_year
       from  tmp_dwt_lifecycle_leadtime_slc_summarize_topic_e e
       group BY e.pgi_month
       ,e.pgi_year ;
drop table tmp_dwt_lifecycle_leadtime_slc_summarize_topic_e;

drop table if exists tmp_dwt_lifecycle_leadtime_slc_summarize_topic_f;
create table tmp_dwt_lifecycle_leadtime_slc_summarize_topic_f stored as orc as 
select round(percentile_approx(lslc.e2e,0.5),1)      as e2e_median --中位数
       ,round(percentile_approx(lslc.in_store,0.5),1)          as in_store_median
       ,round(percentile_approx((lslc.localization + lslc.slc_product_putaway),0.5),1)      as slc_putaway_median
       ,min(lslc.e2e) as e2e_min
       ,max(lslc.e2e)  as e2e_max
       ,min(lslc.in_store)  as in_store_min
       ,max(lslc.in_store) as in_store_max
       ,min((lslc.localization +lslc.slc_product_putaway))  as slc_putaway_min
       ,max((lslc.localization + lslc.slc_product_putaway))  as slc_putaway_max
       ,lslc.pgi_month
       ,lslc.pgi_year
       ,lslc.dt_year
       ,lslc.dt_month
       from tmp_dwt_lifecycle_leadtime_slc_summarize_topic_lslc lslc 
       where lslc.e2e >0 and lslc.in_store>0 and lslc.localization > 0 
       and lslc.slc_product_putaway >0
       group BY lslc.pgi_month
       ,lslc.pgi_year      
       ,lslc.dt_year
       ,lslc.dt_month;

drop table tmp_dwt_lifecycle_leadtime_slc_summarize_topic_lslc;
       
       
insert overwrite table dwt_lifecycle_leadtime_slc_summarize_topic partition(dt_year, dt_month)       
select f.e2e_min
       ,d.e2e_weiavg -- 加权平均
       ,f.e2e_median --中位数
       ,f.e2e_max
       ,f.in_store_min
       ,d.in_store_weiavg -- 加权平均
       ,f.in_store_median --中位数
       ,f.in_store_max
       ,f.slc_putaway_min
       ,d.slc_putaway_weiavg -- 加权平均
       ,f.slc_putaway_median --中位数
       ,f.slc_putaway_max
       ,f.pgi_month
       ,f.pgi_year
       ,f.dt_year
       ,f.dt_month
from tmp_dwt_lifecycle_leadtime_slc_summarize_topic_f f
left join tmp_dwt_lifecycle_leadtime_slc_summarize_topic_d d
on f.pgi_month = d.pgi_month and f.pgi_year = d.pgi_year
group by  
        f.e2e_min
       ,d.e2e_weiavg
       ,f.e2e_median 
       ,f.e2e_max
       ,f.in_store_min
       ,d.in_store_weiavg
       ,f.in_store_median 
       ,f.in_store_max
       ,f.slc_putaway_min
       ,d.slc_putaway_weiavg 
       ,f.slc_putaway_median 
       ,f.slc_putaway_max
       ,f.pgi_month
       ,f.pgi_year
       ,f.dt_year
       ,f.dt_month;
---删除临时表

drop table tmp_dwt_lifecycle_leadtime_slc_summarize_topic_f;
drop table tmp_dwt_lifecycle_leadtime_slc_summarize_topic_d;
"
# 2. 执行加载数据SQL
echo "$dwt_sql"
$hive -e "$dwt_sql"

echo "End syncing lifecycle_leadtime_slc data into DWT layer on $sync_year .................."