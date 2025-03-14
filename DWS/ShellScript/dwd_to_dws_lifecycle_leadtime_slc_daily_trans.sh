#!/bin/bash
# Function:
#   sync up lifecycle_leadtime_SLC 
# History:
# 2021-07-09    Donny   v1.0    init

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
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
set hive.optimize.countdistinct=true;
--set hive.exec.reducers.max=8;
--set mapred.reduce.tasks=8;
--set hive.exec.parallel=false;

drop table if exists tmp_dws_lifecycle_leadtime_SLC_daily_trans_pst;
create table tmp_dws_lifecycle_leadtime_SLC_daily_trans_pst stored as orc as
 select
          material
          ,batch
          ,so_dn_pgi
          ,division
          ,product_line1
          ,product_line2
          ,product_line3
          ,product_line4
          ,product_line5
          ,cust_level1
          ,cust_level2
          ,cust_level3
          ,cust_level4
          ,so_customer_receive_dt
          ,so_dn_create_dt
          ,so_create_dt
          ,work_order_no
          ,import_dn
    from dws_plc_so_daily_trans
    where dt>=date_add('$sync_date',-7)
    and is_transfer_whs = 'false' 
    group by material
          ,batch
          ,so_dn_pgi
          ,division
          ,product_line1
          ,product_line2
          ,product_line3
          ,product_line4
          ,product_line5
          ,cust_level1
          ,cust_level2
          ,cust_level3
          ,cust_level4
          ,so_customer_receive_dt
          ,so_dn_create_dt
          ,so_create_dt
          ,work_order_no
          ,import_dn;
drop table if exists tmp_dws_lifecycle_leadtime_SLC_daily_trans_pwt;
create table tmp_dws_lifecycle_leadtime_SLC_daily_trans_pwt stored as orc as
select DISTINCT
	wo_internal_putway
       ,wo_completed_dt
       ,wo_created_dt
       ,material
       ,batch
       ,work_order_no
    from dws_plc_wo_daily_trans 
    where dt>=date_add('$sync_date',-300)
    ;
drop table if exists tmp_dws_lifecycle_leadtime_SLC_daily_trans_piet;
create table tmp_dws_lifecycle_leadtime_SLC_daily_trans_piet stored as orc as
    select import_migo
       ,import_declaration_completion_date
       ,import_actual_arrival_time
       ,import_pgi
       ,material
       ,batch
       ,import_dn
    from dws_plc_import_export_daily_trans 
    where dt>=date_add('$sync_date',-300)
    group by 
       import_migo
       ,import_declaration_completion_date
       ,import_actual_arrival_time
       ,import_pgi
       ,material
       ,batch
       ,import_dn;


insert overwrite table dws_lifecycle_leadtime_SLC_daily_trans partition(dt)
select  date_format(pst.so_dn_pgi,'yyyy-MM-dd')                                                                                             as pgi_date
       ,ucase(pst.division)                                                                                                                 as division
       ,pst.product_line1
       ,pst.product_line2
       ,pst.product_line3
       ,pst.product_line4
       ,pst.product_line5
       ,pst.cust_level1
       ,pst.cust_level2
       ,pst.cust_level3
       ,pst.cust_level4
       ,pst.work_order_no
       ,pst.import_dn
       ,pst.so_customer_receive_dt
       ,pst.so_dn_create_dt
       ,pst.so_create_dt
       ,pwt.wo_internal_putway
       ,pwt.wo_completed_dt
       ,pwt.wo_created_dt
       ,piet.import_migo
       ,piet.import_declaration_completion_date
       ,piet.import_actual_arrival_time
       ,piet.import_pgi
       ,round((unix_timestamp(piet.import_actual_arrival_time) - unix_timestamp(piet.import_pgi))/(60 * 60 * 24),1)                         as inter_trans
       ,round((unix_timestamp(piet.import_declaration_completion_date) - unix_timestamp(piet.import_actual_arrival_time))/(60 * 60 * 24),1) as import_record_leadtime
       ,round((unix_timestamp(piet.import_migo) - unix_timestamp(piet.import_declaration_completion_date))/(60 * 60 * 24),1)                as t2_migo
       ,round((unix_timestamp(pwt.wo_completed_dt) - unix_timestamp(piet.import_migo))/(60 * 60 * 24),1)                                    as localization
       ,round((unix_timestamp(pwt.wo_internal_putway) - unix_timestamp(pwt.wo_completed_dt))/(60 * 60 * 24),1)                              as slc_product_putaway
       ,round((unix_timestamp(pst.so_create_dt) - unix_timestamp(pwt.wo_internal_putway))/(60 * 60 * 24),1)                                 as in_store_so_create
       ,round((unix_timestamp(pst.so_dn_create_dt) - unix_timestamp(pst.so_create_dt))/(60 * 60 * 24),1)                                    as so_order_processing
       ,round((unix_timestamp(pst.so_dn_pgi) - unix_timestamp(pst.so_dn_create_dt))/(60 * 60 * 24),1)                                       as so_pgi_processing
       ,round((unix_timestamp(pst.so_customer_receive_dt) - unix_timestamp(pst.so_dn_pgi))/(60 * 60 * 24),1)                                as so_delivery
       ,round((unix_timestamp(pst.so_dn_pgi) - unix_timestamp(pwt.wo_internal_putway))/(60 * 60 * 24),1)                                    as in_store
       ,round((unix_timestamp(pst.so_customer_receive_dt) - unix_timestamp(piet.import_pgi))/(60 * 60 * 24),1)                              as e2e
       ,date_format(pst.so_dn_pgi,'yyyy-MM-dd')                                                                                             as dt
from  tmp_dws_lifecycle_leadtime_SLC_daily_trans_pst pst
inner join tmp_dws_lifecycle_leadtime_SLC_daily_trans_pwt pwt
	on pst.material = pwt.material and pst.batch = pwt.batch and pst.work_order_no=pwt.work_order_no
inner join tmp_dws_lifecycle_leadtime_SLC_daily_trans_piet piet
	on pst.material = piet.material and pst.batch = piet.batch and pst.import_dn=piet.import_dn
group by
date_format(pst.so_dn_pgi,'yyyy-MM-dd')
       ,pst.so_dn_pgi
       ,ucase(pst.division)
       ,pst.product_line1
       ,pst.product_line2
       ,pst.product_line3
       ,pst.product_line4
       ,pst.product_line5
       ,pst.cust_level1
       ,pst.cust_level2
       ,pst.cust_level3
       ,pst.cust_level4
       ,pst.work_order_no
       ,pst.import_dn
       ,pst.so_customer_receive_dt
       ,pst.so_dn_create_dt
       ,pst.so_create_dt
       ,pwt.wo_internal_putway
       ,pwt.wo_completed_dt
       ,pwt.wo_created_dt
       ,piet.import_migo
       ,piet.import_declaration_completion_date
       ,piet.import_actual_arrival_time
       ,piet.import_pgi; 
---删除临时表			
drop table tmp_dws_lifecycle_leadtime_SLC_daily_trans_pst;
drop table tmp_dws_lifecycle_leadtime_SLC_daily_trans_pwt;
drop table tmp_dws_lifecycle_leadtime_SLC_daily_trans_piet;
"
# 2. 执行加载数据SQL
$hive -e "$sql_str"

echo "End syncing data into DWS layer on $sync_year : $sync_date .................."