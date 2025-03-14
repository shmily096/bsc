#!/bin/bash
# Function:
#   sync up work order loclization  information data to dwd layer
# History:
# 2021-05-28    Donny   v1.0    init

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


echo "start syncing work order data into DWD layer on ${sync_date} .................."

# 1 Hive SQL string
sto_sql="
-- 参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.reducers.max=8;
set mapred.reduce.tasks=8;
set hive.exec.parallel=false;

-- sync up SQL string
insert overwrite table ${target_db_name}.dwd_fact_work_order partition(dt)
select  
    wo.plant --plant_id
    ,wo.commercial_invoice_no
    ,wo.sap_delivery_no --delivery_no
    ,wo.work_order_no
    ,wo.create_dt --created_datetime
    ,wo.create_by --created_by
    ,wo.start_dt  --started_datetime
    ,wo.started_by --started_by
    ,wo.complete_dt --completed_datetime
    ,wo.release_dt --released_datetime
    ,wo.release_by --released_by
    ,wo.line_no 
    ,wo.material
    ,wo.batch
    ,wo.current_qty
    ,wo.processed_qty
    ,nvl(wo.release_qty, if(wo.release_dt is not null, wo.processed_qty, 0)) --release_qty
    ,wo.qr_code
    ,nvl(wo.work_order_status, (case when cast(wo.release_dt as bigint) > 0 then 'R'
                                    when cast(wo.release_dt as bigint) = 0 and cast(wo.complete_dt as bigint) > 0 then 'C'
                                    when cast(wo.release_dt as bigint) = 0 and cast(wo.complete_dt as bigint) = 0 and cast(wo.start_dt as bigint) > 0 then 'S'
                                else 'd'
                                end   )) --work_order_status
    ,date_format(wo.create_dt,'yyyy-MM-dd') as dt
from
(
    select distinct
        plant
       ,commercial_invoice_no
       ,sap_delivery_no
       ,work_order_no
       ,create_dt
       ,create_by
       ,work_order_status
       ,start_dt
       ,started_by
       ,complete_dt
       ,release_dt
       ,release_by
       ,line_no
       ,material
       ,batch
       ,current_qty
       ,processed_qty
       ,qr_code
       ,release_qty 
    from ${target_db_name}.ods_work_order --源表TRANS_Workorder
    where dt='$sync_date'
    and month(create_dt)>=1
) wo;"
# 2. 执行加载数据SQL
$hive -e "$sto_sql"

echo "End syncing work order data into DWD layer on ${sync_date} .................."