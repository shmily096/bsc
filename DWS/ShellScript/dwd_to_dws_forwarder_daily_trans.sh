#!/bin/bash
# Function:
#   sync up xxxx 
# History:
# 2021-07-07    Donny   v1.0    init

# 设置必要的参数
target_db_name='opsdw' # 数据加载目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

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

declare -A sync_date=$(date +'([day]=%F [year]=%Y [month]=%m)')
yesterday=$(date  +%F)

echo "start syncing data into dws layer on ${sync_date[year]} :${sync_date[month]} .................."

sql_str="
use ${target_db_name};
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.reducers.max=8;
set mapred.reduce.tasks=8;
set hive.exec.parallel=false;
--set hive.exec.max.created.files=100000;
--set parquet.memory.min.chunk.size=100000;
--set hive.input.format=org.apache.hadoop.hive.ql.io.hiveinputformat; 
with a as (
    select  iedd.sto_no
        ,pst.import_dn
        ,date_format(pst.so_dn_pgi,'yyyy-MM-dd')                                                                                     as pgi_date
        ,piet.import_into_inventory_date
        ,piet.import_actual_arrival_time
        ,round((unix_timestamp(piet.import_into_inventory_date) - unix_timestamp(piet.import_actual_arrival_time))/(60 * 60 * 24),1) as pick_up_leadtime
    from (
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
left join (
        select import_into_inventory_date
              ,import_actual_arrival_time
              ,material
              ,batch
              ,import_dn
        from dws_plc_import_export_daily_trans 
        where dt >=date_add('${sync_date[day]}',-300)
        group by
            import_into_inventory_date
              ,import_actual_arrival_time
              ,material
              ,batch
              ,import_dn
)piet
    on pst.material = piet.material and pst.batch = piet.batch and pst.import_dn = piet.import_dn
left join (
    select sto_no
          ,delivery_no
          ,material_code
          ,batch_number
    from dwd_fact_import_export_dn_detail 
    where dt >=date_add('${sync_date[day]}',-300)
)iedd
    on pst.material = iedd.material_code and pst.batch = iedd.batch_number and pst.import_dn = iedd.delivery_no
) 
,iedi as
(
    select 
       related_delivery_no
       ,forwording
    from  dwd_fact_import_export_declaration_info 
    where dt >=date_add('${sync_date[day]}',-300)
    group by 
        related_delivery_no
       ,forwording
)
insert overwrite table dws_forwarder_daily_trans partition(dt)
select  a.pgi_date
       ,iedi.forwording                      as forwarder
       ,a.sto_no
       ,a.import_dn
       ,a.import_actual_arrival_time
       ,a.import_into_inventory_date
       ,a.pick_up_leadtime
       ,date_format(a.pgi_date,'yyyy-MM-dd') as dt
from a
inner join iedi
on a.import_dn = iedi.related_delivery_no
group by
       a.pgi_date
       ,iedi.forwording
       ,a.sto_no
       ,a.import_dn
       ,a.import_actual_arrival_time
       ,a.import_into_inventory_date
       ,a.pick_up_leadtime
       ,date_format(a.pgi_date,'yyyy-MM-dd')
;
"
# 2. 执行加载数据SQL
$hive -e "$sql_str"

echo "End syncing data into DWS layer on $sync_year : .................."