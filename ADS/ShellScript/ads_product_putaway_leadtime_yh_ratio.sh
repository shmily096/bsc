#!/bin/bash
# Function:
#   sync up ads_product_putaway_leadtime_yh_ratio
# History:
# 2021-07-06    Donny   v1.0    init

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

echo "start syncing ads_product_putaway_leadtime_yh_ratio data into DWT layer on $sync_date .................."

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
    select  putaway_year
        ,putaway_month
        ,round(sum(localization_median),1)   as now_localization
        ,round(sum(domestic_trans_median),1) as now_domestic_trans
        ,round(sum(putaway_median),1)        as now_putaway
        ,round(sum(yh_putaway_median),1)     as now_yh_putaway
        ,round(sum(yh_qty),1)                as now_yh_qty
        ,round(sum(dom_wo_no),1)             as now_dom_wo_no
    from dwt_product_putaway_leadtime_yh_topic
    group by  putaway_year
            ,putaway_month order by putaway_year
            ,putaway_month
)

insert overwrite table ads_product_putaway_leadtime_yh_ratio
select  '$sync_date'
       ,a.putaway_month
       ,a.putaway_year
       ,round((a.now_localization-lag(a.now_localization,1) over(order by cast(a.putaway_month as int),a.putaway_year)) /lag(a.now_localization,1) over(order by cast(a.putaway_month as int),a.putaway_year),2) as m_localization_ratio
       ,round((a.now_domestic_trans-lag(a.now_domestic_trans,1) over(order by cast(a.putaway_month as int),a.putaway_year)) /lag(a.now_domestic_trans,1) over(order by cast(a.putaway_month as int),a.putaway_year),2) as m_domestic_trans_ratio
       ,round((a.now_putaway-lag(a.now_putaway,1) over(order by cast(a.putaway_month as int),a.putaway_year)) /lag(a.now_putaway,1) over(order by cast(a.putaway_month as int),a.putaway_year),2) as m_putaway_ratio
       ,round((a.now_yh_putaway-lag(a.now_yh_putaway,1) over(order by cast(a.putaway_month as int),a.putaway_year)) /lag(a.now_yh_putaway,1) over(order by cast(a.putaway_month as int),a.putaway_year),2) as m_yh_putaway_ratio
       ,round((a.now_yh_qty-lag(a.now_yh_qty,1) over(order by cast(a.putaway_month as int),a.putaway_year)) /lag(a.now_yh_qty,1) over(order by cast(a.putaway_month as int),a.putaway_year),2) as m_yh_qty_ratio
       ,round((a.now_dom_wo_no-lag(a.now_dom_wo_no,1) over(order by cast(a.putaway_month as int),a.putaway_year)) /lag(a.now_dom_wo_no,1) over(order by cast(a.putaway_month as int),a.putaway_year),2) as m_yh_dom_wo_no_ratio
       ,round((a.now_localization-lag(a.now_localization,12) over(order by cast(a.putaway_month as int),a.putaway_year)) /lag(a.now_localization,12) over(order by cast(a.putaway_month as int),a.putaway_year),2) as y_localization_ratio
       ,round((a.now_domestic_trans-lag(a.now_domestic_trans,12) over(order by cast(a.putaway_month as int),a.putaway_year)) /lag(a.now_domestic_trans,12) over(order by cast(a.putaway_month as int),a.putaway_year),2) as y_domestic_trans_ratio
       ,round((a.now_putaway-lag(a.now_putaway,12) over(order by cast(a.putaway_month as int),a.putaway_year)) /lag(a.now_putaway,12) over(order by cast(a.putaway_month as int),a.putaway_year),2) as y_putaway_ratio
       ,round((a.now_yh_putaway-lag(a.now_yh_putaway,12) over(order by cast(a.putaway_month as int),a.putaway_year)) /lag(a.now_yh_putaway,12) over(order by cast(a.putaway_month as int),a.putaway_year),2) as y_yh_putaway_ratio
       ,round((a.now_yh_qty-lag(a.now_yh_qty,12) over(order by cast(a.putaway_month as int),a.putaway_year)) /lag(a.now_yh_qty,12) over(order by cast(a.putaway_month as int),a.putaway_year),2) as y_yh_qty_ratio
       ,round((a.now_dom_wo_no-lag(a.now_dom_wo_no,12) over(order by cast(a.putaway_month as int),a.putaway_year)) /lag(a.now_dom_wo_no,12) over(order by cast(a.putaway_month as int),a.putaway_year),2) as y_yh_dom_wo_no_ratio
from a

"
# 2. 执行加载数据SQL
$hive -e "$str_sql"

echo "End syncing ads_product_putaway_leadtime_yh_ratio data into DWT layer on $sync_date .................."