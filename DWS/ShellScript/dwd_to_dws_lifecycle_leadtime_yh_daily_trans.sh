#!/bin/bash
# Function:
#   sync up lifecycle_leadtime_YH 
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
--外仓leadtime life cycle

SET mapreduce.job.queuename=default;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET hive.exec.max.dynamic.partitions=100000;
--set hive.exec.reducers.max=8;
--set mapred.reduce.tasks=8;
--set hive.exec.parallel=false;
drop table if exists tmp_dws_lifecycle_leadtime_YH_daily_trans_pst;
create table tmp_dws_lifecycle_leadtime_YH_daily_trans_pst stored as orc as
 select material
          ,batch
          ,so_dn_pgi
          ,qr_code
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
          ,domestic_sto_dn
    from dws_plc_so_daily_trans
    where dt>=date_add('$sync_date',-7)
    and is_transfer_whs = 'true' 
    group by
          material
          ,batch
          ,so_dn_pgi
          ,qr_code
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
          ,domestic_sto_dn;
drop table if exists tmp_dws_lifecycle_leadtime_YH_daily_trans_pdst;
create table tmp_dws_lifecycle_leadtime_YH_daily_trans_pdst stored as orc as
  select domestic_putaway
       ,domestic_migo
       ,qr_code
       ,domestic_sto_dn
    from dws_plc_domestic_sto_daily_trans 
    where dt>=date_add('$sync_date',-300)
    group by
       domestic_putaway
       ,domestic_migo
       ,qr_code
       ,domestic_sto_dn;
drop table if exists tmp_dws_lifecycle_leadtime_YH_daily_trans_pstt;
create table tmp_dws_lifecycle_leadtime_YH_daily_trans_pstt stored as orc as
select pst.material
          ,pst.batch
          ,pst.so_dn_pgi
          ,pst.division
          ,pst.product_line1
          ,pst.product_line2
          ,pst.product_line3
          ,pst.product_line4
          ,pst.product_line5
          ,pst.cust_level1
          ,pst.cust_level2
          ,pst.cust_level3
          ,pst.cust_level4
          ,pst.so_customer_receive_dt
          ,pst.so_dn_create_dt
          ,pst.so_create_dt
          ,pdst.domestic_putaway
          ,pdst.domestic_migo
          ,pst.work_order_no
          ,pst.import_dn
    from tmp_dws_lifecycle_leadtime_YH_daily_trans_pst pst 
    inner join tmp_dws_lifecycle_leadtime_YH_daily_trans_pdst pdst
    ON pst.qr_code = pdst.qr_code
    group by pst.material
          ,pst.batch
          ,pst.so_dn_pgi
          ,pst.division
          ,pst.product_line1
          ,pst.product_line2
          ,pst.product_line3
          ,pst.product_line4
          ,pst.product_line5
          ,pst.cust_level1
          ,pst.cust_level2
          ,pst.cust_level3
          ,pst.cust_level4
          ,pst.so_customer_receive_dt
          ,pst.so_dn_create_dt
          ,pst.so_create_dt
          ,pdst.domestic_putaway
          ,pdst.domestic_migo
          ,pst.work_order_no
          ,pst.import_dn;
---删除临时表			
drop table tmp_dws_lifecycle_leadtime_YH_daily_trans_pst;
drop table tmp_dws_lifecycle_leadtime_YH_daily_trans_pdst;

drop table if exists tmp_dws_lifecycle_leadtime_YH_daily_trans_pwt;
create table tmp_dws_lifecycle_leadtime_YH_daily_trans_pwt stored as orc as
    select wo_completed_dt
          ,wo_created_dt
          ,material
          ,batch
          ,work_order_no
    from dws_plc_wo_daily_trans 
    where dt>=date_add('$sync_date',-300)
    group by
          wo_completed_dt
          ,wo_created_dt
          ,material
          ,batch
          ,work_order_no;
drop table if exists tmp_dws_lifecycle_leadtime_YH_daily_trans_piet;
create table tmp_dws_lifecycle_leadtime_YH_daily_trans_piet stored as orc as
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


INSERT OVERWRITE TABLE dws_lifecycle_leadtime_YH_daily_trans partition(dt)
SELECT  date_format(pstt.so_dn_pgi,'yyyy-MM-dd')                       AS pgi_date
       ,ucase(pstt.division)                                           AS division
       ,pstt.product_line1
       ,pstt.product_line2
       ,pstt.product_line3
       ,pstt.product_line4
       ,pstt.product_line5
       ,pstt.cust_level1
       ,pstt.cust_level2
       ,pstt.cust_level3
       ,pstt.cust_level4
       ,pstt.work_order_no
       ,pstt.import_dn
       ,pstt.so_customer_receive_dt
       ,pstt.so_dn_create_dt
       ,pstt.so_create_dt
       ,pstt.domestic_putaway
       ,pstt.domestic_migo
       ,pwt.wo_completed_dt
       ,pwt.wo_created_dt
       ,piet.import_migo
       ,piet.import_declaration_completion_date
       ,piet.import_actual_arrival_time
       ,piet.import_pgi
       ,round((unix_timestamp(piet.import_actual_arrival_time) - unix_timestamp(piet.import_pgi))/(60 * 60 * 24),1)                         AS inter_trans
       ,round((unix_timestamp(piet.import_declaration_completion_date) - unix_timestamp(piet.import_actual_arrival_time))/(60 * 60 * 24),1) AS import_record_leadtime
       ,round((unix_timestamp(piet.import_migo) - unix_timestamp(piet.import_declaration_completion_date))/(60 * 60 * 24),1)                AS T2_migo
       ,round((unix_timestamp(pwt.wo_completed_dt) - unix_timestamp(piet.import_migo))/(60 * 60 * 24),1)                                    AS localization
       ,round((unix_timestamp(pstt.domestic_migo) - unix_timestamp(pwt.wo_completed_dt))/(60 * 60 * 24),1)AS domestic_trans
       ,round((unix_timestamp(pstt.domestic_putaway) - unix_timestamp(pstt.domestic_migo))/(60 * 60 * 24),1)                                AS yh_product_putaway
       ,round((unix_timestamp(pstt.so_create_dt) - unix_timestamp(pstt.domestic_putaway))/(60 * 60 * 24),1)                                  AS in_store_so_create
       ,round((unix_timestamp(pstt.so_dn_create_dt) - unix_timestamp(pstt.so_create_dt))/(60 * 60 * 24),1)                                    AS so_order_processing
       ,round((unix_timestamp(pstt.so_dn_pgi) - unix_timestamp(pstt.so_dn_create_dt))/(60 * 60 * 24),1)                                       AS so_pgi_processing
       ,round((unix_timestamp(pstt.so_customer_receive_dt) - unix_timestamp(pstt.so_dn_pgi))/(60 * 60 * 24),1)                                AS so_delivery
       ,round((unix_timestamp(pstt.so_dn_pgi) - unix_timestamp(pstt.domestic_putaway))/(60 * 60 * 24),1)                                     AS in_store
       ,round((unix_timestamp(pstt.so_customer_receive_dt) - unix_timestamp(piet.import_pgi))/(60 * 60 * 24),1)                              AS E2E
       ,date_format(pstt.so_dn_pgi,'yyyy-MM-dd')                                                                                             AS dt
FROM tmp_dws_lifecycle_leadtime_YH_daily_trans_pstt pstt
inner JOIN tmp_dws_lifecycle_leadtime_YH_daily_trans_pwt pwt
	ON pstt.material = pwt.material AND pstt.batch = pwt.batch and pstt.work_order_no = pwt.work_order_no
inner JOIN tmp_dws_lifecycle_leadtime_YH_daily_trans_piet piet
	ON pstt.material = piet.material AND pstt.batch = piet.batch and pstt.import_dn = piet.import_dn
group by
date_format(pstt.so_dn_pgi,'yyyy-MM-dd')
       ,pstt.so_dn_pgi
       ,ucase(pstt.division)
       ,pstt.product_line1
       ,pstt.product_line2
       ,pstt.product_line3
       ,pstt.product_line4
       ,pstt.product_line5
       ,pstt.cust_level1
       ,pstt.cust_level2
       ,pstt.cust_level3
       ,pstt.cust_level4
       ,pstt.work_order_no
       ,pstt.import_dn
       ,pstt.so_customer_receive_dt
       ,pstt.so_dn_create_dt
       ,pstt.so_create_dt
       ,pstt.domestic_putaway
       ,pstt.domestic_migo
       ,pwt.wo_completed_dt
       ,pwt.wo_created_dt
       ,piet.import_migo
       ,piet.import_declaration_completion_date
       ,piet.import_actual_arrival_time
       ,piet.import_pgi
; 
---删除临时表			
drop table tmp_dws_lifecycle_leadtime_YH_daily_trans_pstt;
drop table tmp_dws_lifecycle_leadtime_YH_daily_trans_pwt;
drop table tmp_dws_lifecycle_leadtime_YH_daily_trans_piet;
"
# 2. 执行加载数据SQL
$hive -e "$sql_str"

echo "End syncing data into DWS layer on $sync_year : $sync_date .................."