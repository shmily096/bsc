#!/bin/bash
# Function:
#   sync up imported_ratio data to ads layer
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

echo "start syncing imported_ratio data into ads layer on $sync_date .................."

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
    select  declar_month
        ,declar_year
        ,round(sum(inter_trans_median),1)   as now_inter_trans
        ,round(sum(migo_median),1)          as now_migo
        ,round(sum(inbound_median),1)       as now_inbound
        ,round(sum(import_record_median),1) as now_import_record
        ,round(sum(import_record_wo_num),1) as now_import_qty
    from dwt_imported_topic
    group by  declar_month
            ,declar_year
)
-- load the data
insert overwrite table ads_imported_ratio
select  '$sync_date'
       ,declar_month
       ,declar_year
       ,round((a.now_inter_trans-lag(a.now_inter_trans,1) over(order by cast(a.declar_month as int),a.declar_year)) /lag(a.now_inter_trans,1) over(order by cast(a.declar_month as int),a.declar_year),2) as m_inter_trans_ratio
       ,round((a.now_migo-lag(a.now_migo,1) over(order by cast(a.declar_month as int),a.declar_year)) /lag(a.now_migo,1) over(order by cast(a.declar_month as int),a.declar_year),2) as m_migo_ratio
       ,round((a.now_inbound-lag(a.now_inbound,1) over(order by cast(a.declar_month as int),a.declar_year)) /lag(a.now_inbound,1) over(order by cast(a.declar_month as int),a.declar_year),2) as m_inbound_ratio
       ,round((a.now_import_record-lag(a.now_import_record,1) over(order by cast(a.declar_month as int),a.declar_year)) /lag(a.now_import_record,1) over(order by cast(a.declar_month as int),a.declar_year),2) as m_import_record_ratio
       ,round((a.now_import_qty-lag(a.now_import_qty,1) over(order by cast(a.declar_month as int),a.declar_year)) /lag(a.now_import_qty,1) over(order by cast(a.declar_month as int),a.declar_year),2) as m_import_qty_ratio
       ,round((a.now_inter_trans-lag(a.now_inter_trans,12) over(order by cast(a.declar_month as int),a.declar_year)) /lag(a.now_inter_trans,12) over(order by cast(a.declar_month as int),a.declar_year),2) as y_inter_trans_ratio
       ,round((a.now_migo-lag(a.now_migo,12) over(order by cast(a.declar_month as int),a.declar_year)) /lag(a.now_migo,12) over(order by cast(a.declar_month as int),a.declar_year),2) as y_migo_ratio
       ,round((a.now_inbound-lag(a.now_inbound,12) over(order by cast(a.declar_month as int),a.declar_year)) /lag(a.now_inbound,12) over(order by cast(a.declar_month as int),a.declar_year),2) as y_inbound_ratio
       ,round((a.now_import_record-lag(a.now_import_record,12) over(order by cast(a.declar_month as int),a.declar_year)) /lag(a.now_import_record,12) over(order by cast(a.declar_month as int),a.declar_year),2) as y_import_record_ratio
       ,round((a.now_import_qty-lag(a.now_import_qty,12) over(order by cast(a.declar_month as int),a.declar_year)) /lag(a.now_import_qty,12) over(order by cast(a.declar_month as int),a.declar_year),2) as y_import_qty_ratio
from a 
;
"
# 2. 执行加载数据SQL
$hive -e "$str_sql"

echo "End syncing imported_ratio data into ads layer on $sync_date .................."