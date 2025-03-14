#!/bin/bash
# function:
#   sync up dws_product_putaway_leadtime_slc_daily_trans 
# history:
# 2021-07-07    donny   v1.0    init

# 设置必要的参数
target_db_name='opsdw' # 数据加载目标数据库名称
hive=/opt/module/hive3/bin/hive  # hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # hadoop的配置路径

if [ -n "$1" ] ;then 
    sync_date=$1
else
    sync_date=$(date  +%F)
fi

if [ -n "$2" ] ;then 
    sync_year=$2
else
    sync_year=$(date  +'%y')
fi


declare -A sync_date=$(date +'([day]=%F [year]=%Y [month]=%m)')
yesterday=$(date  +%F)

echo "start syncing data into dws layer on ${sync_date[year]} :${sync_date[month]} .................."


sql_str="
-- 配置参数
use ${target_db_name};
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
--set hive.exec.reducers.max=8;
--set mapred.reduce.tasks=8;
--set hive.exec.parallel=false;




with pstt as
(
select 
    count(pst.qr_code) as qr_code_num
    ,pst.material
    ,pst.batch
    ,pst.so_dn_pgi
    ,pst.import_dn
    ,pst.work_order_no
    from
(
    select 
    qr_code 
    ,material
    ,batch
    ,so_dn_pgi
    ,import_dn
    ,work_order_no
    from dws_plc_so_daily_trans
    where dt>=date_add('${sync_date[day]}',-7)
    and is_transfer_whs = 'false' 
    group by
    qr_code
    ,material
    ,batch
    ,so_dn_pgi
    ,work_order_no
    ,import_dn
)pst
group by
    pst.material
    ,pst.batch
    ,pst.so_dn_pgi
    ,pst.work_order_no
    ,pst.import_dn
)
, pwt as
(
    select  
       wo_internal_putway                                                                                 
       ,wo_release_dt                                                                                      
       ,wo_completed_dt
       ,wo_created_dt
       ,material
       ,batch
       ,work_order_no
    from dws_plc_wo_daily_trans 
    where dt>=date_add('${sync_date[day]}',-300)
    and wo_internal_putway is not null                                                                                 
    and wo_release_dt is not null                                                                                      
    and wo_completed_dt is not null 
    and wo_created_dt is not null 
    group by 
        wo_internal_putway                                                                                 
       ,wo_release_dt                                                                                      
       ,wo_completed_dt
       ,wo_created_dt
       ,material
       ,batch
       ,work_order_no
)
, piet as 
(
    select import_migo
          ,material
          ,batch
          ,import_dn
    from dws_plc_import_export_daily_trans
    where dt>=date_add('${sync_date[day]}',-300)
    and import_migo is not null
    group by 
           import_migo
          ,material
          ,batch
          ,import_dn
) 

insert overwrite table dws_product_putaway_leadtime_slc_daily_trans partition(dt)
select  date_format(pstt.so_dn_pgi,'yyyy-MM-dd')                      as pgi_date
       ,sum(pstt.qr_code_num)                                         as qr_code_num
       ,count(distinct pstt.work_order_no)                                 as work_order_num
       ,pstt.import_dn
       ,pstt.work_order_no
       ,pwt.wo_internal_putway                                        as wo_internal_putaway
       ,pwt.wo_release_dt                                             as wo_released_dt
       ,pwt.wo_completed_dt
       ,pwt.wo_created_dt
       ,piet.import_migo
       ,round((unix_timestamp(pwt.wo_internal_putway) - unix_timestamp(piet.import_migo))/(60 * 60 * 24),1)    as slc_putaway
       ,round((unix_timestamp(pwt.wo_completed_dt) - unix_timestamp(piet.import_migo))/(60 * 60 * 24),1)       as localization
       ,round((unix_timestamp(pwt.wo_internal_putway) - unix_timestamp(pwt.wo_completed_dt))/(60 * 60 * 24),1) as putaway
       ,date_format(pstt.so_dn_pgi,'yyyy-MM-dd')                                                                as dt
from 
pstt
left join pwt
on pstt.material = pwt.material 
and pstt.batch = pwt.batch
and pstt.work_order_no = pwt.work_order_no
left join piet
on pstt.material = piet.material 
and pstt.batch = piet.batch
and pstt.import_dn = piet.import_dn
group by
        date_format(pstt.so_dn_pgi,'yyyy-MM-dd')
       ,pstt.import_dn
       ,pstt.work_order_no
       ,pwt.wo_internal_putway
       ,pwt.wo_release_dt
       ,pwt.wo_completed_dt
       ,pwt.wo_created_dt
       ,piet.import_migo

; 
"
# 2. 执行加载数据sql
$hive -e "$sql_str"

echo "end syncing data into dws layer on $sync_year : $sync_date .................."