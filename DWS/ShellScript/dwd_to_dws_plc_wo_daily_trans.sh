#!/bin/bash
# Function:
#   sync up work order of product life cycle 
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



--with parts

insert overwrite table dws_plc_wo_daily_trans partition(dt)
select  distinct 
        so_sto_wo.work_order_no
       ,so_sto_wo.material 
       ,so_sto_wo.batch
       ,so_sto_wo.qr_code
       ,wo.wo_created_dt 
       ,coalesce(wo.wo_completed_dt,wo.wo_released_dt) as wo_completed_dt
       ,wo.wo_released_dt
       ,coalesce(mov.mov_putaway_dt, ods_putawy.putawy_dt) --wo_internal_putway
       ,so_sto_wo.dt    ---原来是按wo_created_dt分区,现在按运单创建中国时间分区
from 
( 
    select  
        material
        ,batch
        ,qr_code
        ,domestic_sto_dn
        ,domestic_sto
        ,import_dn
        ,work_order_no
        ,dt
    from dws_so_sto_wo_daily_trans  --sals_dn 关联wo_qrcode通过qrcode关联取 import_dn,work_order_no
    where dt >=date_add('$sync_date',-7) ---最近7天运单创建的中国时间
    group by 
        material
        ,batch
        ,qr_code
        ,domestic_sto_dn
        ,domestic_sto
        ,import_dn
        ,work_order_no
        ,dt
) so_sto_wo
inner join 
(
    select  work_order_no 
           ,delivery_no 
           ,created_datetime   as wo_created_dt 
           ,completed_datetime as wo_completed_dt 
           ,released_datetime  as wo_released_dt
    from dwd_fact_work_order 
    where dt >=date_add('$sync_date',-300)
    group by
           work_order_no 
           ,delivery_no 
           ,created_datetime
           ,completed_datetime
           ,released_datetime
)wo
on wo.work_order_no=so_sto_wo.work_order_no 
and wo.delivery_no=so_sto_wo.import_dn
left join (
    select
        delivery_no,
        max(putaway_date) as putawy_dt
    from ods_putaway_info 
    where dt >=date_add('$sync_date',-300) and substr(putaway_date,1,1)='2'
    group by delivery_no
) ods_putawy on so_sto_wo.import_dn=ods_putawy.delivery_no
left join 
(
    select distinct
        concat_ws(' ', enter_date,mov_time) as mov_putaway_dt
        ,delivery_no
    from dwd_fact_inventory_movement_trans
    where movement_type='321'
    and dt >=date_add('$sync_date',-300)

) mov on mov.delivery_no=so_sto_wo.import_dn
; 
"
# 2. 执行加载数据SQL
echo "$sql_str"
$hive -e "$sql_str"

echo "End syncing data into DWS layer on $sync_year : $sync_date .................."