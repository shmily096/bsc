#!/bin/bash
# Function:
#   sync up dws_import_export_daily_trans 
# History:
# 2021-06-09    Donny   v1.1    init

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


dn_sql="
-- 配置参数
use ${target_db_name};
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.reducers.max=8;
set mapred.reduce.tasks=8;
set hive.exec.parallel=false;

insert overwrite table dws_import_export_daily_trans partition(dt)
select  date_format(pst.so_dn_pgi,'yyyy-MM-dd')                                                                                           as pgi_date
       ,piet.import_migo
       ,piet.import_declaration_completion_date
       ,piet.import_dock_warrant_date
       ,piet.import_actual_arrival_time
       ,piet.import_pgi
       ,pst.import_dn
       ,pst.work_order_no
       ,pst.work_order_num
       ,round((unix_timestamp(piet.import_migo) - unix_timestamp(piet.import_pgi))/(60 * 60 * 24),1)                                      as inbound_leadtime
       ,round((unix_timestamp(piet.import_actual_arrival_time) - unix_timestamp(piet.import_pgi))/(60 * 60 * 24),1)                       as inter_trans_leadtime
       ,round((unix_timestamp(piet.import_migo) - unix_timestamp(piet.import_actual_arrival_time))/(60 * 60 * 24),1)                      as migo_leadtime
       ,round((unix_timestamp(piet.import_declaration_completion_date) - unix_timestamp(piet.import_dock_warrant_date))/(60 * 60 * 24),1) as import_record_leadtime
       ,date_format(pst.so_dn_pgi,'yyyy-MM-dd')                                                                                           as dt
from (
    select material
          ,batch
          ,so_dn_pgi
          ,import_dn
          ,work_order_no
          ,count(distinct work_order_no) as work_order_num
    from dws_plc_so_daily_trans
    where dt>=date_add('${sync_date[day]}',-7)
    and so_dn_pgi <>''
    group by material
          ,batch
          ,so_dn_pgi
          ,import_dn
          ,work_order_no
) pst
left join(
    select import_migo
       ,import_declaration_completion_date
       ,import_dock_warrant_date
       ,import_actual_arrival_time
       ,import_pgi
       ,material
       ,batch
       ,import_dn
    from dws_plc_import_export_daily_trans 
    where dt >=date_add('${sync_date[day]}',-300)
    group by
     import_migo
       ,import_declaration_completion_date
       ,import_dock_warrant_date
       ,import_actual_arrival_time
       ,import_pgi
       ,material
       ,batch
       ,import_dn
)piet
on pst.material = piet.material and pst.batch = piet.batch and pst.import_dn = piet.import_dn
left join (
    select work_order_no
          ,material
          ,batch
    from dws_so_sto_wo_daily_trans 
    where dt >=date_add('${sync_date[day]}',-300)
    group by 
      work_order_no
          ,material
          ,batch
)swd
on pst.material = swd.material and pst.batch = swd.batch and pst.work_order_no =swd.work_order_no
group by  
        date_format(pst.so_dn_pgi,'yyyy-MM-dd')
       ,piet.import_migo
       ,piet.import_declaration_completion_date
       ,piet.import_dock_warrant_date
       ,piet.import_actual_arrival_time
       ,piet.import_pgi
       ,pst.import_dn
       ,pst.work_order_no
       ,pst.work_order_num
; 
"
# 2. 执行加载数据SQL
$hive -e "$dn_sql"

echo "End syncing dws_import_export_daily_trans data into DWS layer on $sync_year .................."