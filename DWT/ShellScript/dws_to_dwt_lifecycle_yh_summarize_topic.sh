#!/bin/bash
# Function:
#   sync up lifecycle_leadtime_yh data to dwt layer
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
    sync_month=$(date +'%m')
fi

echo "start syncing data into dws layer on $sync_year :$sync_month .................."

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
drop table if exists tmp_dwt_lifecycle_leadtime_yh_summarize_topic_lyh;
create table tmp_dwt_lifecycle_leadtime_yh_summarize_topic_lyh stored as orc as 
       select 
              if(e2e is not null, e2e, 0) as e2e, 
              if(in_store is not null, in_store, 0) as in_store,
              if(in_store is not null, round(in_store*in_store,1), 0) as in_store_s,
              if(localization is not null, localization, 0) as localization, 
              if(domestic_trans is not null, domestic_trans, 0) as domestic_trans,
              if(yh_product_putaway is not null, yh_product_putaway,0) as yh_product_putaway, 
              month(pgi_date)                         as pgi_month
              ,year(pgi_date)                          as pgi_year
              ,date_format(pgi_date, 'yyyy')  as dt_year
              ,date_format(pgi_date, 'MM') as dt_month
       from dws_lifecycle_leadtime_yh_daily_trans
       where date_format(dt, 'MM')='$sync_month'
              and date_format(dt, 'yyyy')= '$sync_year'
       group by e2e, in_store, in_store*in_store, localization,domestic_trans, yh_product_putaway, month(pgi_date)
              ,year(pgi_date), date_format(pgi_date, 'yyyy')
              ,date_format(pgi_date, 'MM');
drop table if exists tmp_dwt_lifecycle_leadtime_yh_summarize_topic_b;
create table tmp_dwt_lifecycle_leadtime_yh_summarize_topic_b stored as orc as 
    select  sum(e2e)                                as e2e_total
           ,sum(in_store)                           as in_store_total
           ,sum(localization + domestic_trans + yh_product_putaway) as yh_putaway_total
           ,pgi_month
           ,pgi_year
    from tmp_dwt_lifecycle_leadtime_yh_summarize_topic_lyh lyh
    group by pgi_month
           ,pgi_year;
drop table if exists tmp_dwt_lifecycle_leadtime_yh_summarize_topic_e;
create table tmp_dwt_lifecycle_leadtime_yh_summarize_topic_e stored as orc as 
       select
              if(b.in_store_total>0,round((lyh.in_store_s/ b.in_store_total), 1),0) as in_store_wei
              ,if(b.e2e_total >0,round((lyh.e2e*lyh.e2e / b.e2e_total), 1),0) as e2e_wei
              ,if(b.yh_putaway_total>0,round(((lyh.localization + lyh.domestic_trans + lyh.yh_product_putaway)*(lyh.localization + lyh.domestic_trans + lyh.yh_product_putaway) / b.yh_putaway_total),1),0) as yh_putaway_wei
              ,lyh.pgi_month
              ,lyh.pgi_year
       from tmp_dwt_lifecycle_leadtime_yh_summarize_topic_lyh lyh 
       left join tmp_dwt_lifecycle_leadtime_yh_summarize_topic_b b
              on  lyh.pgi_month = b.pgi_month and lyh.pgi_year = b.pgi_year;

drop table tmp_dwt_lifecycle_leadtime_yh_summarize_topic_b;

drop table if exists tmp_dwt_lifecycle_leadtime_yh_summarize_topic_d;
create table tmp_dwt_lifecycle_leadtime_yh_summarize_topic_d stored as orc as 
              select  sum(e.e2e_wei)  as e2e_weiavg
                     ,sum(e.in_store_wei)    as in_store_weiavg -- 加权平均
                     ,sum(e.yh_putaway_wei) as yh_putaway_weiavg
                     ,e.pgi_month
                     ,e.pgi_year
              from tmp_dwt_lifecycle_leadtime_yh_summarize_topic_e e
              group BY e.pgi_month
              ,e.pgi_year;

drop table tmp_dwt_lifecycle_leadtime_yh_summarize_topic_e;


drop table if exists tmp_dwt_lifecycle_leadtime_yh_summarize_topic_f;
create table tmp_dwt_lifecycle_leadtime_yh_summarize_topic_f stored as orc as 
       select round(percentile_approx(lyh.e2e,0.5),1)      as e2e_median --中位数
              ,round(percentile_approx(lyh.in_store,0.5),1)          as in_store_median
              ,round(percentile_approx((lyh.localization + lyh.domestic_trans + lyh.yh_product_putaway),0.5),1)      as yh_putaway_median
              ,min(lyh.e2e) as e2e_min
              ,max(lyh.e2e)  as e2e_max
              ,min(lyh.in_store)  as in_store_min
              ,max(lyh.in_store) as in_store_max
              ,min((lyh.localization + lyh.domestic_trans + lyh.yh_product_putaway))  as yh_putaway_min
              ,max((lyh.localization + lyh.domestic_trans + lyh.yh_product_putaway))  as yh_putaway_max
              ,lyh.pgi_month
              ,lyh.pgi_year
              ,lyh.dt_year
              ,lyh.dt_month
       from tmp_dwt_lifecycle_leadtime_yh_summarize_topic_lyh lyh 
       where lyh.e2e >0 and lyh.in_store>0 and lyh.localization>0 
       and lyh.domestic_trans>0 and lyh.yh_product_putaway>0
       group BY lyh.pgi_month
       ,lyh.pgi_year      
       ,lyh.dt_year
       ,lyh.dt_month;       
       
drop table tmp_dwt_lifecycle_leadtime_yh_summarize_topic_lyh;

insert overwrite table dwt_lifecycle_leadtime_yh_summarize_topic partition(dt_year, dt_month)       
select f.e2e_min
       ,d.e2e_weiavg -- 加权平均
       ,f.e2e_median --中位数
       ,f.e2e_max
       ,f.in_store_min
       ,d.in_store_weiavg -- 加权平均
       ,f.in_store_median --中位数
       ,f.in_store_max
       ,f.yh_putaway_min
       ,d.yh_putaway_weiavg -- 加权平均
       ,f.yh_putaway_median --中位数
       ,f.yh_putaway_max
       ,f.pgi_month
       ,f.pgi_year
       ,f.dt_year
       ,f.dt_month
from tmp_dwt_lifecycle_leadtime_yh_summarize_topic_f f
left join tmp_dwt_lifecycle_leadtime_yh_summarize_topic_d d
on f.pgi_month = d.pgi_month and f.pgi_year = d.pgi_year
group by  
        f.e2e_min
       ,d.e2e_weiavg -- 加权平均
       ,f.e2e_median --中位数
       ,f.e2e_max
       ,f.in_store_min
       ,d.in_store_weiavg -- 加权平均
       ,f.in_store_median --中位数
       ,f.in_store_max
       ,f.yh_putaway_min
       ,d.yh_putaway_weiavg -- 加权平均
       ,f.yh_putaway_median --中位数
       ,f.yh_putaway_max
       ,f.pgi_month
       ,f.pgi_year
       ,f.dt_year
       ,f.dt_month;

---删除临时表

drop table tmp_dwt_lifecycle_leadtime_yh_summarize_topic_d;
drop table tmp_dwt_lifecycle_leadtime_yh_summarize_topic_f;
"
# 2. 执行加载数据SQL
echo "$dwt_sql"
$hive -e "$dwt_sql"

echo "End syncing lifecycle_leadtime_yh data into DWT layer on $sync_month .................."