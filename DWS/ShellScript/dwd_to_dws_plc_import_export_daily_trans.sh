#!/bin/bash
# Function:
#   sync up import and export information 
# History:
# 2021-06-29    Donny   v1.0    init

# 设置必要的参数
target_db_name='opsdw' # 数据加载目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

if [ -n "$1" ] ;then 
    sync_date=$1
else
    sync_date=$(date  +%F)
fi

if [ -n "$2" ] ;then 
    sync_year=$2
else
    sync_year=$(date  +'%Y')
fi


echo "start syncing data into dws layer on $sync_year :$sync_date .................."



sql_str="
use ${target_db_name};
-- 配置参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
--set hive.exec.max.created.files=100000;
--set parquet.memory.min.chunk.size=100000;
--set hive.input.format=org.apache.hadoop.hive.ql.io.hiveinputformat;
--set hive.exec.reducers.max=8;
--set mapred.reduce.tasks=8;
--set hive.exec.parallel=false;



insert overwrite table dws_plc_import_export_daily_trans partition(dt)

select  distinct 
    so_sto_wo.material 
    ,so_sto_wo.batch 
    ,so_sto_wo.import_dn
    ,ixdni.import_dn_created_datetime
    ,ixdni.import_pgi
    ,ixdnic.import_pick_up_date
    ,ixdnic.import_invoice_receiving_date
    ,ixdnic.import_actual_arrival_time
    ,ixdnic.import_dock_warrant_date
    ,ixdnic.import_declaration_start_date
    ,ixdnic.import_declaration_completion_date
    ,ixdnic.import_into_inventory_date
    ,ixdni.import_migo
    ,date_format(nvl(ixdni.import_dn_created_datetime,'$sync_date'),'yyyy-MM-dd') as dt_date
from  (
    select
    material
    ,batch
    ,qr_code
    ,import_dn
    from dws_so_sto_wo_daily_trans
    where dt >=date_add('$sync_date',-7)
    group by
        material
        ,batch
        ,qr_code
        ,import_dn
) so_sto_wo
left join 
(
    select  delivery_no 
           ,created_datetime     as import_dn_created_datetime 
           ,actual_good_issue_datetime as import_pgi 
           ,actual_migo_datetime as import_migo
    from dwd_fact_import_export_dn_info  
    where dt >=date_add('$sync_date',-300)
    group by
           delivery_no 
           ,created_datetime
           ,actual_good_issue_datetime 
           ,actual_migo_datetime
) ixdni
on ixdni.delivery_no=so_sto_wo.import_dn
left join 
(
    select  related_delivery_no 
           ,t1_pick_up_date             as import_pick_up_date 
           ,invoice_receiving_date      as import_invoice_receiving_date 
           ,actual_arrival_time         as import_actual_arrival_time 
           ,dock_warrant_date           as import_dock_warrant_date 
           ,declaration_start_date      as import_declaration_start_date 
           ,declaration_completion_date as import_declaration_completion_date 
           ,into_inventory_date         as import_into_inventory_date
    from dwd_fact_import_export_declaration_info
    where related_delivery_no is not null  
    and related_delivery_no <>''
    and dt >=date_add('$sync_date',-300)
    group by
           related_delivery_no 
           ,t1_pick_up_date
           ,invoice_receiving_date
           ,actual_arrival_time
           ,dock_warrant_date
           ,declaration_start_date
           ,declaration_completion_date
           ,into_inventory_date
)ixdnic
on ixdnic.related_delivery_no=so_sto_wo.import_dn; 
"
# 2. 执行加载数据SQL
$hive -e "$sql_str"

echo "End syncing data into DWS layer on $sync_year : $sync_date .................."