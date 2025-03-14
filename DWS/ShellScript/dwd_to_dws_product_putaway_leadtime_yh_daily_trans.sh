#!/bin/bash
# function:
#   sync up dws_product_putaway_leadtime_yh_daily_trans 
# history:
# 2021-06-29    donny   v1.0    init

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
--set hive.exec.max.created.files=100000;
--set parquet.memory.min.chunk.size=100000;
--set hive.input.format=org.apache.hadoop.hive.ql.io.hiveinputformat;
--set hive.exec.reducers.max=8;
--set mapred.reduce.tasks=8;
--set hive.exec.parallel=false;



with pst as 
(
    select qr_code
    ,so_dn_pgi
    ,material
    ,batch
    ,import_dn
    ,work_order_no
    from dws_plc_so_daily_trans
    where dt>=date_add('${sync_date[day]}',-7)
    and is_transfer_whs = 'true' 
    group by
       qr_code
    ,so_dn_pgi
    ,material
    ,batch
    ,import_dn
    ,work_order_no
) 
, pdst as
(
    select domestic_putaway
       ,domestic_migo
       ,domestic_sto_create_dt
       ,qr_code
    from dws_plc_domestic_sto_daily_trans 
    where dt>=date_add('${sync_date[day]}',-300)
    group by
        domestic_putaway
       ,domestic_migo
       ,domestic_sto_create_dt
       ,qr_code
)
,pstt as
(select count(pst.qr_code) as qr_code_num
    ,pst.so_dn_pgi
    ,pst.material
    ,pst.batch
    ,pst.import_dn
    ,pst.work_order_no
       ,pdst.domestic_putaway
       ,pdst.domestic_migo
       ,pdst.domestic_sto_create_dt
    from pst left join pdst
    on pst.qr_code = pdst.qr_code
    group by 
         pst.so_dn_pgi
        ,pst.material
        ,pst.batch
        ,pst.import_dn
        ,pst.work_order_no
        ,pdst.domestic_putaway
        ,pdst.domestic_migo
        ,pdst.domestic_sto_create_dt
)
, pwt as 
(
    select 
        work_order_no
        ,wo_completed_dt
        ,material
        ,batch
    from dws_plc_wo_daily_trans 
    where dt>=date_add('${sync_date[day]}',-300)
    group by
        wo_completed_dt
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
    group by 
          import_migo
         ,material
         ,batch
         ,import_dn
)

insert overwrite table dws_product_putaway_leadtime_yh_daily_trans partition(dt)
select  date_format(pstt.so_dn_pgi,'yyyy-MM-dd')                                                              as pgi_date
       ,sum(pstt.qr_code_num)                              as qr_code_num
       ,count( distinct pstt.work_order_no)                 as work_order_num
       ,pstt.import_dn
       ,pstt.work_order_no
       ,pstt.domestic_putaway
       ,pstt.domestic_migo
       ,pstt.domestic_sto_create_dt
       ,pwt.wo_completed_dt
       ,piet.import_migo 
       ,round((unix_timestamp(pstt.domestic_putaway) - unix_timestamp(piet.import_migo))/(60 * 60 * 24),1)   as yh_putaway
       ,round((unix_timestamp(pwt.wo_completed_dt) - unix_timestamp(piet.import_migo))/(60 * 60 * 24),1)     as localization
       ,round((unix_timestamp(pstt.domestic_migo) - unix_timestamp(pstt.domestic_sto_create_dt))/(60 * 60 * 24),1)as domestic_trans
       ,round((unix_timestamp(pstt.domestic_putaway) - unix_timestamp(pstt.domestic_migo))/(60 * 60 * 24),1) as putaway
       ,date_format(pstt.so_dn_pgi,'yyyy-MM-dd')                                                              as dt
from pstt
left join pwt
on pstt.material = pwt.material and pstt.batch = pwt.batch and pstt.work_order_no = pwt.work_order_no
left join piet
on pstt.material = piet.material and pstt.batch = piet.batch and pstt.import_dn=piet.import_dn
group by date_format(pstt.so_dn_pgi,'yyyy-MM-dd')
         ,pstt.domestic_putaway
         ,pstt.domestic_migo
         ,pstt.domestic_sto_create_dt
         ,pwt.wo_completed_dt
         ,piet.import_migo
         ,pstt.import_dn
         ,pstt.work_order_no


; 
"
# 2. 执行加载数据sql
$hive -e "$sql_str"

echo "end syncing data into dws layer on $sync_year : $sync_date .................."