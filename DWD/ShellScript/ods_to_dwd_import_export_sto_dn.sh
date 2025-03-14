#!/bin/bash
# Function:
#   sync up import and export sto dn from ods data to dwd layer
# History:
# 2021-05-12    Donny   v1.0    init
# 2021-05-13    Donny   v1.1    50% completed the shell script
# 2021-05-14    Donny   v1.2    80% completed the shell script
# 2021-05-14    Donny   v1.3    add data clean rule

# 设置必要的参数
target_db_name='opsdw' # 目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

# 按指定的日期进行同步，默认取当前时间的前一天 
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

echo "start syncing import and export sto data into DWD layer on $sync_year $sync_date .................."

# 1 Hive SQL string
dn_sql="
-- 配置参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.parallel=false;

drop table if exists tmp_dwd_fact_import_export_dn_info_martiral;
create table tmp_dwd_fact_import_export_dn_info_martiral stored as orc as
 select distinct 
        material_code, 
        division_display_name,
        business_group
    from ${target_db_name}.dwd_dim_material
    where dt in ( select max(dt) from ${target_db_name}.dwd_dim_material where dt>=date_sub('$sync_date',10));

drop table if exists tmp_dwd_fact_import_export_dn_info_data;
create table tmp_dwd_fact_import_export_dn_info_data stored as orc as
    select 
       sto_no 
       ,sap_delivery_no_outbound
       ,sap_delivery_no_inbound
       ,min(dn_create_dt) over(partition by sap_delivery_no_outbound order by sto_no) as min_create_dt 
       ,dn_status 
       ,dn_update_dt 
       ,first_value(dn_create_by) over(partition by sap_delivery_no_outbound,sto_no order by dn_create_dt) as min_create_by
       ,dn_updated_by 
       ,receiver_customer_code 
       ,sap_delivery_line_no 
       ,material 
       ,sum(qty) over(partition by sto_no,sap_delivery_no_outbound,sap_delivery_no_inbound,sku.division_display_name,sku.business_group) as total_qty
       ,batch 
       ,ship_from_plant 
       ,ship_from_location 
       ,ship_to_plant 
       ,ship_to_location 
       ,delivery_mode 
       ,actual_migo_dt
       ,pgi_date
       ,sku.division_display_name
       ,sku.business_group
    from (select * from ${target_db_name}.ods_import_export_delivery where dt='$sync_date' and qty !=0 )xx--源表：TRANS_ImportExportDelivery
    left join tmp_dwd_fact_import_export_dn_info_martiral sku
        on xx.material=sku.material_code
    where dt='$sync_date' and qty !=0;


-- DN base information   分区还是按美国时间，其他时间改为中国时间
insert overwrite table ${target_db_name}.dwd_fact_import_export_dn_info partition(dt)
select  distinct
        ods_dn.sto_no  --sto_no
       ,ods_dn.sap_delivery_no_outbound --delivery_no 
       ,ods_dn.sap_delivery_no_inbound --reference_dn_no
       ,from_unixtime(unix_timestamp(ods_dn.min_create_dt)+12*60*60,'yyyy-MM-dd HH:mm:ss') as min_create_dt --created_datetime
       ,from_unixtime(unix_timestamp(ods_dn.dn_update_dt)+12*60*60,'yyyy-MM-dd HH:mm:ss') as dn_update_dt  --updated_datetime
       ,ods_dn.min_create_by --created_by
       ,ods_dn.dn_updated_by --updated_by
       ,ods_dn.receiver_customer_code  --receiver_customer_code
       ,ods_dn.delivery_mode  --delivery_mode
       ,ods_dn.dn_status --order_status
       ,ods_dn.ship_from_plant  --ship_from_plant
       ,ods_dn.ship_to_plant  --ship_to_plant
       ,ods_dn.total_qty --total_qty 
       ,null --planned_good_issue_datetime 
       ,from_unixtime(unix_timestamp(ods_dn.pgi_date)+12*60*60,'yyyy-MM-dd HH:mm:ss') as pgi_date --actual_good_issue_datetime
       ,from_unixtime(unix_timestamp(ods_dn.actual_migo_dt)+12*60*60,'yyyy-MM-dd HH:mm:ss') as actual_migo_dt
       ,from_unixtime(unix_timestamp(ods_dn.actual_migo_dt)+12*60*60,'yyyy-MM-dd HH:mm:ss') as actual_putaway_datetime
       ,division_display_name --fin_dim_id
       ,business_group --sku_item_group
       ,date_format(ods_dn.min_create_dt,'yyyy-MM-dd') as dt
from tmp_dwd_fact_import_export_dn_info_data ods_dn
where year(ods_dn.min_create_dt)='$sync_year'; 

drop table if exists tmp_dwd_fact_import_export_dn_detail_data;
create table tmp_dwd_fact_import_export_dn_detail_data stored as orc as
    select 
         sto_no
         ,sap_delivery_no_outbound
         ,sap_delivery_line_no
         ,material
         ,qty
         ,batch
         ,ship_from_location
         ,ship_to_location
            ,from_unixtime(unix_timestamp(pgi_date)+12*60*60,'yyyy-MM-dd HH:mm:ss') as pgi_date --actual_good_issue_datetime
       ,from_unixtime(unix_timestamp(actual_migo_dt)+12*60*60,'yyyy-MM-dd HH:mm:ss') as actual_migo_dt
         ,min(dn_create_dt) over(partition by sap_delivery_no_outbound order by sto_no) as min_create_dt 
    from ${target_db_name}.ods_import_export_delivery --源表 TRANS_ImportExportDelivery
    where dt='$sync_date'
        and qty !=0;

-- DN Detailed information
insert overwrite table ${target_db_name}.dwd_fact_import_export_dn_detail partition(dt)
select distinct
    ods_dn_detail.sto_no
    ,ods_dn_detail.sap_delivery_no_outbound
    ,ods_dn_detail.sap_delivery_line_no --line_number
    ,ods_dn_detail.material --material_code
    ,ods_dn_detail.qty
    ,ods_dn_detail.batch
    ,ods_dn_detail.ship_from_location
    ,ods_dn_detail.ship_to_location
    ,ods_dn_detail.pgi_date
    ,ods_dn_detail.actual_migo_dt
    ,from_unixtime(unix_timestamp(ods_dn_detail.min_create_dt)+12*60*60,'yyyy-MM-dd HH:mm:ss') as chinese_create_dt --created_datetime
    ,date_format(ods_dn_detail.min_create_dt,'yyyy-MM-dd') 
from tmp_dwd_fact_import_export_dn_detail_data ods_dn_detail
where year(ods_dn_detail.min_create_dt)='$sync_year';
drop table if exists tmp_dwd_fact_import_export_dn_info_martiral;
drop table if exists tmp_dwd_fact_import_export_dn_info_data;
drop table if exists tmp_dwd_fact_import_export_dn_detail_data;
"
# 2. 执行SQL，并判断查询结果是否为空
count=`$hive -e "select count(*) from ods_import_export_delivery where dt='$sync_date'and substr(dn_create_dt,1,1)='2'" | tail -n1`

if [ $count -eq 0 ]; then
  echo "Error: Failed to import data, count is zero."
  exit 1
fi
# 3. 执行SQL
$hive -e "$dn_sql"

echo "End syncing import and export sto data into DWD layer on ${sync_date} .................."