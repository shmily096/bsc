#!/bin/bash
# Function:
#   sync up dwt_dsr_topic data to dwt layer
# History:
# 2021-07-08    Donny   v1.0    init

# 参数
target_db_name='opsdw' # 目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径
#取昨天对应的月份的1号大于这天
if [ -n "$1" ] ;then 
    sync_date=$1
else
    #默认取昨天的日期
    sync_date=$(date -d '-1 day' +%F)	
fi
this_month=${sync_date:0:7}-01
echo "start syncing dws_kpi_cc_so_delivery data into DWT layer on $sync_date : $this_month"

dws_sql="
use ${target_db_name};

set mapreduce.job.queuename = default;
set hive.exec.dynamic.partition = true;
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.exec.max.dynamic.partitions.pernode = 100000;
set hive.exec.max.dynamic.partitions = 100000;
set hive.exec.parallel=false;

insert overwrite table dws_kpi_cc_so_delivery partition(dt)
select
    b.material,
    b.batch,
    b.so_no,
    b.delivery_id,
    b.plant,
    b.pick_location_id,
    sum(case when trim(b.qr_code) <> ''and b.qr_code is not null then 1 else b.qty end) as qty,
    count(distinct b.line_number) as line,
    case when month(b.created_datetime) =month(b.actual_gi_date) 
    then 1 else 0 end as this_mon_flag,
    'delivery' as flag  ,
	b.dt	
from opsdw.dwd_fact_sales_order_dn_detail b
    where b.dt>= '$this_month' and dt<='$2'
group by    
    b.material,
    b.batch,
    b.so_no,
    b.delivery_id,
    b.plant,
    b.pick_location_id,
    case when month(b.created_datetime) =month(b.actual_gi_date) 
    then 1 else 0 end,
	b.dt
union all   
SELECT 
    material, 
    batch, 
    so_no,
    '' AS delivery_id,  
    pick_up_plant AS plant,
    '' AS pick_location_id,
    sum(qty)as qty,
    count(distinct line_number) as line,
    case when month(request_delivery_date) =month(created_datetime) 
    then 1 else 0 end as this_mon_flag,
    'so_no' as flag   ,
	dt
FROM opsdw.dwd_fact_sales_order_info
where dt>='$this_month' and dt<='$2' and order_status='C'
group by material, batch, so_no,pick_up_plant,
    case when month(request_delivery_date) =month(created_datetime) 
    then 1 else 0 end
	,dt
 ;
"
# 2. 执行加载数据SQL
echo "$dws_sql"
$hive -e "$dws_sql"

echo "End syncing dwt_dsr_topic data into DWT layer on $sync_date $this_month"   
    
    
    