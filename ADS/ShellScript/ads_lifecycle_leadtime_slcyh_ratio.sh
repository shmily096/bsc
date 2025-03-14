#!/bin/bash
# Function:
#   sync up lifecycle leadtime of slc and yh ratio data to ads layer
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

echo "start syncing lifecycle leadtime of slc and yh ratio data into ads layer on $sync_date .................."

# 1 Hive SQL string
str_sql="
use ${target_db_name};
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.reducers.max=8;
set mapred.reduce.tasks=8;
set hive.exec.parallel=false;


with a as (
    select  
        pgi_month
        ,pgi_year
        ,round(sum(e2e_median),1)      as now_e2e
        ,round(sum(in_store_median),1) as now_in_store
        ,round(sum(putaway_median),1)  as now_putaway
    from dwt_lifecycle_leadtime_slc_summarize_topic
    group by  
        pgi_year
        ,pgi_month 
)
, b as (
    select  
        pgi_month
        ,pgi_year
        ,round(sum(e2e_median),1)      as now_e2e
        ,round(sum(in_store_median),1) as now_in_store
        ,round(sum(putaway_median),1)  as now_putaway
    from dwt_lifecycle_leadtime_yh_summarize_topic
    group by  
        pgi_year
        ,pgi_month 
)
insert overwrite table ads_lifecycle_leadtime_slcyh_ratio
select  '$sync_date'
       ,'slc' as putaway_location
       ,pgi_year
       ,pgi_month
       ,round((a.now_e2e-lag(a.now_e2e,1) over(order by cast(a.pgi_month as int),a.pgi_year)) /lag(a.now_e2e,1) over(order by cast(a.pgi_month as int),a.pgi_year),2) as m_e2e_ratio
       ,round((a.now_in_store-lag(a.now_in_store,1) over(order by cast(a.pgi_month as int),a.pgi_year)) /lag(a.now_in_store,1) over(order by cast(a.pgi_month as int),a.pgi_year),2) as m_in_store_ratio
       ,round((a.now_putaway-lag(a.now_putaway,1) over(order by cast(a.pgi_month as int),a.pgi_year)) /lag(a.now_putaway,1) over(order by cast(a.pgi_month as int),a.pgi_year),2) as m_putaway_ratio
       ,round((a.now_e2e-lag(a.now_e2e,12) over(order by cast(a.pgi_month as int),a.pgi_year)) /lag(a.now_e2e,12) over(order by cast(a.pgi_month as int),a.pgi_year),2) as y_e2e_ratio
       ,round((a.now_in_store-lag(a.now_in_store,12) over(order by cast(a.pgi_month as int),a.pgi_year)) /lag(a.now_in_store,12) over(order by cast(a.pgi_month as int),a.pgi_year),2) as y_in_store_ratio
       ,round((a.now_putaway-lag(a.now_putaway,12) over(order by cast(a.pgi_month as int),a.pgi_year)) /lag(a.now_putaway,12) over(order by cast(a.pgi_month as int),a.pgi_year),2) as y_putaway_ratio
from a
union all 
select '$sync_date'
       ,'yh' as putaway_location
       ,pgi_year
       ,pgi_month
       ,round((b.now_e2e-lag(b.now_e2e,1) over(order by cast(b.pgi_month as int),b.pgi_year)) /lag(b.now_e2e,1) over(order by cast(b.pgi_month as int),b.pgi_year),2) as m_e2e_ratio
       ,round((b.now_in_store-lag(b.now_in_store,1) over(order by cast(b.pgi_month as int),b.pgi_year)) /lag(b.now_in_store,1) over(order by cast(b.pgi_month as int),b.pgi_year),2) as m_in_store_ratio
       ,round((b.now_putaway-lag(b.now_putaway,1) over(order by cast(b.pgi_month as int),b.pgi_year)) /lag(b.now_putaway,1) over(order by cast(b.pgi_month as int),b.pgi_year),2) as m_putaway_ratio
       ,round((b.now_e2e-lag(b.now_e2e,12) over(order by cast(b.pgi_month as int),b.pgi_year)) /lag(b.now_e2e,12) over(order by cast(b.pgi_month as int),b.pgi_year),2) as y_e2e_ratio
       ,round((b.now_in_store-lag(b.now_in_store,12) over(order by cast(b.pgi_month as int),b.pgi_year)) /lag(b.now_in_store,12) over(order by cast(b.pgi_month as int),b.pgi_year),2) as y_in_store_ratio
       ,round((b.now_putaway-lag(b.now_putaway,12) over(order by cast(b.pgi_month as int),b.pgi_year)) /lag(b.now_putaway,12) over(order by cast(b.pgi_month as int),b.pgi_year),2) as y_putaway_ratio
from b
;
"
# 2. 执行加载数据SQL
$hive -e "$str_sql"

echo "End syncing lifecycle leadtime of slc and yh ratio data data into ads layer on $sync_date .................."