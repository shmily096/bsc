#!/bin/bash
# Function:
#   sync up import and export declaration data to dwd layer
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

echo "start syncing import and export declaration into DWD layer on ${sync_date} .................."

# 1 Hive SQL string
sto_sql="
use ${target_db_name};
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.reducers.max=8;

drop table if exists tmp_dwd_fact_import_export_declaration_info_ixo;
create table tmp_dwd_fact_import_export_declaration_info_ixo stored as orc as
    select commercial_invoice
       ,bsc_inform_slc_date
       ,t1_pick_up_date
       ,actual_arrival_time
       ,dock_warrant_date
       ,forwording_inform_slc_pick
       ,forwording
       ,into_inventory_date
       ,update_date
       ,shipment_internal_number
       ,master_bill_no
       ,house_waybill_no
       ,import_export_flag
       ,shipment_type
       ,quantity
       ,gross_weight
       ,forwarder_service_level
       ,department
       ,country_area
       ,transportation_type
       ,etd
       ,eta
       ,revise_etd
       ,revise_eta 
       ,commodity_inspection
       ,customs_inspection
       ,declaration_completion_date 
       ,work_number
    from ${target_db_name}.ods_shipment_status_inbound_tracking  --源表TRANS_ShipmentStatusInbound从6-20开始未更新变更为TRANS_CTMShipmentStatus
    where dt='$sync_date' and month(update_date)>1 ;
	
drop table if exists tmp_dwd_fact_import_export_declaration_info_dn_map;
create table tmp_dwd_fact_import_export_declaration_info_dn_map stored as orc as	
	    select 
        delivery, 
        invoice, 
        mail_received
    from ${target_db_name}.ods_commercial_invoice_dn_mapping
    where dt = (select max(dt) from ${target_db_name}.ods_commercial_invoice_dn_mapping );
-- sync up SQL string
insert overwrite table ${target_db_name}.dwd_fact_import_export_declaration_info partition(dt)
select
    ixo.commercial_invoice
    ,ixo.update_date
    ,ixo.bsc_inform_slc_date
    ,ixo.t1_pick_up_date
    ,ixo.actual_arrival_time
    ,ixo.dock_warrant_date
    ,from_unixtime(unix_timestamp(dn_map.mail_received, 'MMM dd yyyy HH:mma'), 'yyyy-MM-dd HH:mm') --invoice_receiving_date
    ,ixo.forwording
    ,ixo.into_inventory_date
    ,ixo.shipment_internal_number
    ,ixo.master_bill_no
    ,ixo.house_waybill_no
    ,ixo.import_export_flag
    ,ixo.shipment_type
    ,ixo.quantity
    ,ixo.gross_weight
    ,ixo.department
    ,ixo.country_area
    ,ixo.transportation_type 
    ,ixo.etd
    ,ixo.eta
    ,ixo.revise_etd
    ,ixo.revise_eta
    ,ixo.commodity_inspection
    ,ixo.customs_inspection
    ,ixo.forwording_inform_slc_pick --declaration_start_date
    ,ixo.declaration_completion_date
    ,dn_map.delivery --related_delivery_no  
    ,ixo.work_number
	,ixo.update_date as dt
from tmp_dwd_fact_import_export_declaration_info_ixo ixo
left outer join tmp_dwd_fact_import_export_declaration_info_dn_map dn_map on ixo.commercial_invoice=dn_map.invoice;
"
###删除所有临时表
delete_tmp="
drop table tmp_dwd_fact_import_export_declaration_info_ixo;
drop table tmp_dwd_fact_import_export_declaration_info_dn_map;
"
# 2. 执行加载数据SQL
$hive -e "$sto_sql"
#第二部分收尾删除所有临时表
echo "four $delete_tmp"
$hive -e "$delete_tmp"
echo "End syncing import and export declaration data into DWD layer on ${sync_date} .................."