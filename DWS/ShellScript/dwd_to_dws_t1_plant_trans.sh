#!/bin/bash
# Function:
#   sync up dws_t1_plant_daily_transation
# History:
# 2021-06-09    Donny   v1.0    init
# 2021-07-08    Amanda   v1.1   update the logic

# 设置必要的参数
target_db_name='opsdw'                # 数据加载目标数据库名称
hive=/opt/module/hive3/bin/hive       # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

# if [ -n "$1" ]; then
#     sync_date=$1
# else
#     sync_date=$(date  +%F)
# fi

if [ -n "$1" ]; then
    sync_year=$1
else
    sync_year=$(date +'%Y')
fi

declare -A sync_date=$(date +'([day]=%F [year]=%Y [month]=%m)')
yesterday=$(date  +%F)

echo "start syncing data into dws layer on ${sync_date[year]} :${sync_date[month]} .................."


dn_sql="
-- 配置参数
use ${target_db_name};
--shipment_type ="inbound_进境"
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



with a as (
    select  iedd.sto_no
        ,pst.import_dn
        ,date_format(pst.so_dn_pgi,'yyyy-MM-dd') as pgi_date
        ,piet.import_actual_arrival_time
        ,piet.import_pgi
        ,round((unix_timestamp(piet.import_actual_arrival_time) - unix_timestamp(piet.import_pgi))/(60 * 60 * 24),1) as inter_trans_leadtime
    from 
     (
    select material
          ,batch
          ,so_dn_pgi
          ,import_dn
    from dws_plc_so_daily_trans
    where dt>=date_add('${sync_date[day]}',-7)
    group by
       material
          ,batch
          ,so_dn_pgi
          ,import_dn
    ) pst
    inner join (
        select import_actual_arrival_time
        ,import_pgi
        ,material
        ,batch
        ,import_dn
        from dws_plc_import_export_daily_trans 
        where dt>=date_add('${sync_date[day]}',-300)
        group by 
            import_actual_arrival_time
        ,import_pgi
        ,material
        ,batch
        ,import_dn
)piet
    on pst.material = piet.material and pst.batch = piet.batch and pst.import_dn= piet.import_dn
    inner join (
        select sto_no
        ,delivery_no
        ,material_code
        ,batch_number
        from dwd_fact_import_export_dn_detail 
        where dt>=date_add('${sync_date[day]}',-300)
        group by
            sto_no
        ,delivery_no
        ,material_code
        ,batch_number
)iedd
    on pst.material = iedd.material_code and pst.batch = iedd.batch_number and pst.import_dn = iedd.delivery_no
)
, ix as(
    select
       sto_no
       ,delivery_no
       ,ship_from_plant
    from dwd_fact_import_export_dn_info
    where dt>=date_add('${sync_date[day]}',-300)
    group by
       sto_no
       ,delivery_no
       ,ship_from_plant
)
insert overwrite table opsdw.dws_t1_plant_daily_transation partition(dt)
select  a.pgi_date
       ,ix.ship_from_plant
       ,a.sto_no
       ,a.import_dn
       ,a.import_actual_arrival_time
       ,a.import_pgi
       ,a.inter_trans_leadtime
       ,date_format(a.pgi_date,'yyyy-MM-dd') as dt
from a
inner join ix
on a.sto_no = ix.sto_no and a.import_dn = ix.delivery_no
group by
       a.pgi_date
       ,ix.ship_from_plant
       ,a.sto_no
       ,a.import_dn
       ,a.import_actual_arrival_time
       ,a.import_pgi
       ,a.inter_trans_leadtime
       ,date_format(a.pgi_date,'yyyy-MM-dd')
; 
"
# 2. 执行加载数据SQL
$hive -e "$dn_sql"

echo "End syncing dws_t1_plant_daily_transation data into DWS layer on $sync_year.................."
