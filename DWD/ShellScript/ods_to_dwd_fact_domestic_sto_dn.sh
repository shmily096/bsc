#!/bin/bash
# Function:
#   sync up domestic sto dn data from ODS to dwd layer
# History:
# 2021-05-18    Donny   v1.0    init

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

if [ -n "$2" ] ;then 
    sync_year=$2
else
    sync_year=$(date  +'%Y')
fi

echo "start syncing ${sync_year} domestic sto dn data into DWD layer on $sync_date .................."

# 1 Hive SQL string
dn_header_sql="
use ${target_db_name};
-- 参数
--set mapreduce.job.queuename=hive;
--set hive.exec.dynamic.partition.mode=nonstrict;
--set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;

drop table if exists tmp_dwd_fact_domestic_sto_dn_info;
create table tmp_dwd_fact_domestic_sto_dn_info stored as orc as
    select distinct
       sto_no
       ,sap_delivery_no_inbound
       ,sap_delivery_no_outbound
       ,dn_create_dt
       ,chinese_dncreatedt
       ,dn_create_by
       ,dn_update_dt
       ,dn_update_by
       ,dn_status
       ,ship_from_plant
       ,ship_from_location
       ,ship_to_plant
       ,ship_to_location
       ,delivery_mode
       ,carrier
       ,actual_migo_dt
       ,pgi_date       
       ,sum(case when trim(qr_code) <> '' and qr_code is not null then 1 else qty end) over(partition by sap_delivery_no_outbound, sto_no) as total_qty
    from ${target_db_name}.ods_domestic_delivery  --源表:TRANS_DomesticDelivery
    where dt='$sync_date'
        and month(dn_create_dt)>=1;

drop table if exists tmp_dwd_fact_domestic_sto_dn_info_ods_putawy;
create table tmp_dwd_fact_domestic_sto_dn_info_ods_putawy stored as orc as
    select
        delivery_no,
        max(putaway_date) as putawy_dt
    from ${target_db_name}.ods_putaway_info 
    where dt>=date_add('$sync_date',-365) and substr(putaway_date,1,1)='2'  ---剔除空值
    group by delivery_no;
	
drop table if exists tmp_dwd_fact_domestic_sto_dn_info_mov;
create table tmp_dwd_fact_domestic_sto_dn_info_mov stored as orc as
	    select 
        max(concat_ws(' ', enter_date,mov_time)) as mov_putaway_dt
        ,delivery_no
    from dwd_fact_inventory_movement_trans --源表:TRANS_InventoryTransactions
    where movement_type='321'
       and dt>=date_add('$sync_date',-365)
        group by delivery_no;
	
-- sync up SQL string
insert overwrite table ${target_db_name}.dwd_fact_domestic_sto_dn_info partition(dt)
select  ods_dn.sto_no
       ,ods_dn.sap_delivery_no_outbound --delivery_no
       ,ods_dn.sap_delivery_no_inbound --reference_dn_number
       ,ods_dn.dn_create_dt --create_datetime       
       ,ods_dn.dn_create_by --create_by
       ,ods_dn.dn_update_dt --update_datetime
       ,ods_dn.dn_status --update_by
       ,ods_dn.delivery_mode
       ,ods_dn.dn_status
       ,ods_dn.ship_from_location
       ,ods_dn.ship_from_plant
       ,ods_dn.ship_to_plant
       ,ods_dn.ship_to_location
       ,ods_dn.carrier
       ,ods_dn.actual_migo_dt
       ,null
       ,ods_dn.pgi_date  -- actual_good_issue_datetime
       ,ods_dn.total_qty
       ,coalesce(mov.mov_putaway_dt, ods_putawy.putawy_dt)  --ods_dn.actual_putaway_datetime
       ,ods_dn.pgi_date  --pgi
       ,ods_dn.chinese_dncreatedt
       ,date_format(ods_dn.dn_create_dt,'yyyy-MM-dd')
from tmp_dwd_fact_domestic_sto_dn_info ods_dn
left join tmp_dwd_fact_domestic_sto_dn_info_ods_putawy ods_putawy on ods_dn.sap_delivery_no_outbound=ods_putawy.delivery_no
left join tmp_dwd_fact_domestic_sto_dn_info_mov mov on mov.delivery_no=ods_dn.sap_delivery_no_outbound;
"

dn_lines_sql="
use ${target_db_name};
--set mapreduce.job.queuename=hive;
--set hive.exec.dynamic.partition.mode=nonstrict;
--set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;

insert overwrite table ${target_db_name}.dwd_fact_domestic_sto_dn_detail partition(dt)
select distinct
        sto_no
        ,sap_delivery_no_outbound
        ,sap_delivery_line_no        
        ,material 
        ,case when trim(qr_code) <> '' and qr_code is not null then 1 else qty end as qty 
		,batch
        ,qr_code        
		,chinese_dncreatedt
        ,date_format(dn_create_dt,'yyyy-MM-dd') 
    from ${target_db_name}.ods_domestic_delivery --源表 TRANS_DomesticDelivery
    where dt='$sync_date'
        and month(dn_create_dt)>=1

"
delete_tmp="
drop table tmp_dwd_fact_domestic_sto_dn_info;
drop table tmp_dwd_fact_domestic_sto_dn_info_ods_putawy;
drop table tmp_dwd_fact_domestic_sto_dn_info_mov;
"
# 2. 执行加载数据SQL
echo "$dn_header_sql""$dn_lines_sql"
$hive -e "$dn_header_sql""$dn_lines_sql"
#第二部分收尾删除所有临时表
echo "two $delete_tmp"
$hive -e "$delete_tmp"
echo "End syncing ${sync_year} domestic sto dn data into DWD layer on ${sync_date} .................."