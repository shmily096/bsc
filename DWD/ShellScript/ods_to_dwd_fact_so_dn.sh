#!/bin/bash
# Function:
#   sync up so dn data from ods to dwd layer
# History:
# 2021-05-17    Donny   v1.0    init
# 2021-05-24    Donny   v1.1    add data clean rule
# 2021-06-08    Donny   v1.2    add new field receiving_confirmation_date

# 参数
target_db_name='opsdw' # 数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

# 默认取当前时间的前一天 
if [ -n "$2" ] ;then 
    sync_date=$2
else
    sync_date=$(date  +%F)
fi

echo "start syncing so dn data into DWD layer on ${sync_date} .................."

# 1 Hive SQL string
dn_header_sql="
use ${target_db_name};
-- 配置参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;


drop table if exists tmp_h_dn;
create table tmp_h_dn stored as orc as
     select 
        so_no
       ,sap_delivery_no
       ,dn_create_dt as min_create_dt
       ,chinese_dncreatedt as chinese_dncreatedt
       ,max(dn_update_dt) as dn_update_dt
       ,min(dn_create_by) as min_create_by
       ,max(dn_updated_by) as dn_updated_by
       ,ship_to
       ,real_ship_to_address
       ,max(planned_gi_date) as planned_gi_date
       ,max(actual_gi_date) as actual_gi_date
       ,delivery_mode
       ,carrier
       ,pick_location
		,plant
	   ,sum(case when trim(qr_code) <> ''  and qr_code is not null then 1 else qty end) as total_qty
    from ${target_db_name}.ods_sales_delivery  --源表:TRANS_SalesDelivery
    where dt='$sync_date'
	and substr(dn_create_dt,1,1)='2'
    and qty!=0
    group by so_no
            ,sap_delivery_no
			,dn_create_dt
			,chinese_dncreatedt
			,ship_to
            ,real_ship_to_address
			,delivery_mode
            ,carrier
            ,pick_location
	    ,plant;

drop table if exists tmp_cust_receipt;
create table tmp_cust_receipt stored as orc as
select delivery_no
    ,max(last_confirmation_date) as receiving_confirmation_date
from ${target_db_name}.ods_so_dn_receiving_confirmation --源表:TRANS_ReceivingConfirmation
where  dt=(select max(dt) from ${target_db_name}.ods_so_dn_receiving_confirmation) and last_confirmation_date is not null 
group by delivery_no ;

-- Sales order delivery note header
insert overwrite table dwd_fact_sales_order_dn_info partition(year_mon)
select distinct 
     h_dn.so_no
    ,h_dn.sap_delivery_no --delivery_id
    ,h_dn.min_create_dt --created_datetime
    ,h_dn.dn_update_dt --updated_datetime
    ,h_dn.min_create_by --created_by
    ,h_dn.dn_updated_by --updated_by
    ,h_dn.ship_to --ship_to_address
    ,h_dn.real_ship_to_address --real_shipto_address
    ,h_dn.planned_gi_date --planned_gi_date
    ,h_dn.actual_gi_date --actual_gi_date
    ,cust_receipt.receiving_confirmation_date
    ,h_dn.delivery_mode --delivery_mode
    ,h_dn.carrier --carrier_id
    ,h_dn.pick_location --pick_location_id
    ,h_dn.total_qty
    ,h_dn.plant
    ,h_dn.chinese_dncreatedt
    ,date_format(h_dn.min_create_dt,'yyyy-MM-dd') as dt
    ,date_format(h_dn.min_create_dt,'yyyy-MM') as year_mon
from tmp_h_dn h_dn
left outer join tmp_cust_receipt cust_receipt on h_dn.sap_delivery_no=cust_receipt.delivery_no ;
"
# Sales order delivery note lines
lines_sql="
use ${target_db_name};
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;

drop table if exists tmp_dwd_fact_sales_order_dn_detail_h_dn;
create table tmp_dwd_fact_sales_order_dn_detail_h_dn stored as orc as
select
    so_no 
    ,x.sap_delivery_no
    ,delivery_line
    ,material 
    ,case when trim(qr_code) <> '' and qr_code is not null then 1 else qty end as qty 
    ,batch
    ,qr_code         
    ,plant
    ,chinese_dncreatedt
    ,dn_create_dt--created_datetime
    ,ship_to --ship_to_address
    ,real_ship_to_address --real_shipto_address
    ,planned_gi_date --planned_gi_date
    ,actual_gi_date --actual_gi_date
    ,x.delivery_mode --delivery_mode
    ,x.carrier --carrier_id
    ,x.pick_location --pick_location_id    
from ods_sales_delivery x
where dt='$sync_date'
and qty!=0
and substr(dn_create_dt,1,1)='2';


insert overwrite table ${target_db_name}.dwd_fact_sales_order_dn_detail partition(year_mon)
select
    so_no 
    ,x.sap_delivery_no
    ,delivery_line
    ,material 
    ,qty 
    ,batch
    ,qr_code         
    ,plant
    ,chinese_dncreatedt
    ,dn_create_dt--created_datetime
    ,ship_to --ship_to_address
    ,real_ship_to_address --real_shipto_address
    ,planned_gi_date --planned_gi_date
    ,actual_gi_date --actual_gi_date
    ,cust_receipt.receiving_confirmation_date
    ,x.delivery_mode --delivery_mode
    ,x.carrier --carrier_id
    ,x.pick_location --pick_location_id
    ,date_format(dn_create_dt, 'yyyy-MM-dd') as dt  
    ,date_format(dn_create_dt, 'yyyy-MM') as year_mon       
from tmp_dwd_fact_sales_order_dn_detail_h_dn  x
left outer join tmp_cust_receipt cust_receipt on x.sap_delivery_no=cust_receipt.delivery_no ;
"
delete_sql="
drop table tmp_h_dn;
drop table tmp_cust_receipt;
drop table tmp_dwd_fact_sales_order_dn_detail_h_dn;
"
# 2. 执行SQL，并判断查询结果是否为空
# count=`$hive -e "select count(*) from ods_sales_delivery where dt='$sync_date'and substr(dn_create_dt,1,1)='2'" | tail -n1`

# if [ $count -eq 0 ]; then
#   echo "Error: Failed to import data, count is zero."
#   exit 1
# fi
# 3. 执行加载数据SQL
exec_sql=""

if [ -n "$1" ] ;then 
    case $1 in
    "header")
    echo "dwd_fact_sales_order_dn_info--------------------------"
        exec_sql="$dn_header_sql"
        ;;
    "lines")
    echo "dwd_fact_sales_order_dn_detail--------------------------"
        exec_sql="$lines_sql"
        ;;
    "all")
        exec_sql="$dn_header_sql""$lines_sql" 
        ;;
    *)
        echo "please use header, lines, all"
        ;;
    esac
else
    echo "all run-------------------------------------------"
    exec_sql="$dn_header_sql""$lines_sql" 
fi

$hive -e "$exec_sql" 
$hive -e "$delete_sql"

echo "End syncing so dn data into DWD layer on ${sync_date} .................." 