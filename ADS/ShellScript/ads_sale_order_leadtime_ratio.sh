#!/bin/bash
# Function:
#   sync up ads_sale_order_leadtime_ratio data to ads layer
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

echo "start syncing ads_sale_order_leadtime_ratio data into ads layer on $sync_date .................."

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
    select  pgi_month
        ,pgi_year
        ,item_type
        ,round(sum(product_sale_median),1) as now_product_sale
        ,round(sum(order_proce_median),1)  as now_order_proce
        ,round(sum(pgi_proce_median),1)    as now_pgi_proce
        ,round(sum(transport_median),1)    as now_transport
    from dwt_sale_order_leadtime_topic
    group by  pgi_month
            ,pgi_year
            ,item_type
)


insert overwrite table ads_sale_order_leadtime_ratio
select  '$sync_date'
       ,pgi_month
       ,pgi_year
       ,item_type
       ,round((a.now_product_sale-lag(a.now_product_sale,1) over(order by cast(a.pgi_month as int),a.pgi_year,a.item_type)) /lag(a.now_product_sale,1) over(order by cast(a.pgi_month as int),a.pgi_year,a.item_type),2) as m_product_sale_ratio
       ,round((a.now_order_proce-lag(a.now_order_proce,1) over(order by cast(a.pgi_month as int),a.pgi_year,a.item_type)) /lag(a.now_order_proce,1) over(order by cast(a.pgi_month as int),a.pgi_year,a.item_type),2) as m_order_proce_ratio
       ,round((a.now_pgi_proce-lag(a.now_pgi_proce,1) over(order by cast(a.pgi_month as int),a.pgi_year,a.item_type)) /lag(a.now_pgi_proce,1) over(order by cast(a.pgi_month as int),a.pgi_year,a.item_type),2) as m_pgi_proce_ratio
       ,round((a.now_transport-lag(a.now_transport,1) over(order by cast(a.pgi_month as int),a.pgi_year,a.item_type)) /lag(a.now_transport,1) over(order by cast(a.pgi_month as int),a.pgi_year,a.item_type),2) as m_transport_ratio
       ,round((a.now_product_sale-lag(a.now_product_sale,12) over(order by cast(a.pgi_month as int),a.pgi_year,a.item_type)) /lag(a.now_product_sale,12) over(order by cast(a.pgi_month as int),a.pgi_year,a.item_type),2) as y_product_sale_ratio
       ,round((a.now_order_proce-lag(a.now_order_proce,12) over(order by cast(a.pgi_month as int),a.pgi_year,a.item_type)) /lag(a.now_order_proce,12) over(order by cast(a.pgi_month as int),a.pgi_year,a.item_type),2) as y_order_proce_ratio
       ,round((a.now_pgi_proce-lag(a.now_pgi_proce,12) over(order by cast(a.pgi_month as int),a.pgi_year,a.item_type)) /lag(a.now_pgi_proce,12) over(order by cast(a.pgi_month as int),a.pgi_year,a.item_type),2) as y_pgi_proce_ratio
       ,round((a.now_transport-lag(a.now_transport,12) over(order by cast(a.pgi_month as int),a.pgi_year,a.item_type)) /lag(a.now_transport,12) over(order by cast(a.pgi_month as int),a.pgi_year,a.item_type),2) as y_transport_ratio
from a
;
"
# 2. 执行加载数据SQL
$hive -e "$str_sql"

echo "End syncing ads_sale_order_leadtime_ratio data into ads layer on $sync_date .................."