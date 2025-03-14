#!/bin/bash
# Function:
#   sync up sales order data from ods to dwd layer
# History:
# 2021-05-18    Donny   v1.0    init
# 2021-05-24    Donny   v1.1    update the fields

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
this_year=`date -d "${sync_date}" +%Y-01`
echo "start syncing so into DWD layer on ${sync_date} on ${this_year} .................."
dwd_dim_customer_maxdt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_dim_customer | awk 'BEGIN {max = 0} {if ($6+0 > max+0) max=$6} END {print $8}' | grep -oP 20'[^ ]*'`
dwd_dim_material_maxdt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_dim_material | awk 'BEGIN {max = 0} {if ($6+0 > max+0) max=$6} END {print $8}' | grep -oP 20'[^ ]*'`
# 1 Hive SQL string
so_sql="
use ${target_db_name};
-- 参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;

drop table if exists tmp_dwd_fact_sales_order_info_so;
create table tmp_dwd_fact_sales_order_info_so stored as orc as
    select distinct 
        so_no
       ,order_type
       ,order_reason
       ,reject_reason
       ,order_remarks
       ,so_create_dt
       ,chinese_socreatedt
       ,so_update_dt
       ,so_create_by
       ,so_updated_by
       ,so_status
       ,po_number
       ,soline_no
       ,material
       ,batch
       ,qty
       ,net_value
       ,currency
       ,unit
       ,request_delivery_date
       ,pick_up_plant
       ,customer_code
       ,ship_to_code
    from ods_sales_order --源表:TRANS_SalesOrder
    where dt='$sync_date'
        and substr(so_create_dt,1,1)='2';
drop table if exists tmp_dwd_fact_sales_order_info_socri;
create table tmp_dwd_fact_sales_order_info_socri stored as orc as
     select CAST(CAST(so_no as int) as string) as so_no
	      , min(to_date(request_delivery_date)) as request_delivery_date
	   from dwd_salesorder_createdinfo  --源表:TRANS_SalesOrder_CreatedInfo
	 where dt_month>='$this_year'  ---重刷的时候全量
    group by CAST(CAST(so_no as int) as string);
drop table if exists tmp_dwd_fact_sales_order_info_sku;
create table tmp_dwd_fact_sales_order_info_sku stored as orc as
    select distinct
         bu_sku.material_code 
        ,bu_sku.division_id
        ,bu_sku.division_display_name
        ,bu_sku.default_location
        ,bu_sku.sub_division
        ,bu_sku.sub_division_bak
        ,bu_sku.business_group
    from dwd_dim_material bu_sku --源表主表MDM_MaterialMaster 全量更新,次表MDM_DivisionMaster 全量更新
    where bu_sku.dt ='$dwd_dim_material_maxdt'
    ---in (select max(dt) from dwd_dim_material max_dt where dt>=date_sub('$sync_date',10))
;
drop table if exists tmp_dwd_fact_sales_order_info_cust;
create table tmp_dwd_fact_sales_order_info_cust stored as orc as
    select distinct 
        cust_account
        ,level3_code
        ,level4_code as customer_type
    from dwd_dim_customer  --源表:MDM_CustomerMaster,次表MDM_CustomerMaster_KNB1,MDM_CustomerMaster_KNVI,ods_customer_level
    where dt  in (select max(dt) from dwd_dim_customer where dt>=date_sub('$dwd_dim_customer_maxdt',7))
      ---='$dwd_dim_customer_maxdt'
;

drop table if exists tmp_dwd_fact_sales_order_info_new_rebate;
create table tmp_dwd_fact_sales_order_info_new_rebate stored as orc as
   select 
            so.so_no
           ,so.order_type
           ,so.order_reason
           ,so.reject_reason
           ,so.order_remarks
           ,so.so_create_dt --created_datetime
           ,so.chinese_socreatedt
           ,so.so_update_dt --updated_datetime
           ,so.so_create_by --created_by
           ,so.so_updated_by --updated_by
           ,so.so_status --order_status
           ,so.po_number --reference_po_number
           ,so.soline_no --line_number
           ,so.material --material
           ,so.batch --batch
           ,so.qty --qty
           ,nvl(so.unit, 'ea') as unit
           ,so.net_value --net_value
           ,so.currency
           ,so.request_delivery_date
           ,so.pick_up_plant
           ,so.customer_code
           ,so.ship_to_code
           ,sku.division_display_name as division
           ,cust.level3_code
           ,cust.customer_type
           ,sku.default_location
           ,sku.sub_division
		   ,sku.sub_division_bak
           ,sku.business_group
    from tmp_dwd_fact_sales_order_info_so so
    left outer join tmp_dwd_fact_sales_order_info_sku sku on so.material=sku.material_code
    left outer join tmp_dwd_fact_sales_order_info_cust cust on cust.cust_account=so.customer_code;
	
-- sync up SQL string
insert overwrite table dwd_fact_sales_order_info partition(dt)
select distinct
        so_orig.so_no
        ,so_orig.order_type
        ,so_orig.order_reason
        ,so_orig.reject_reason
        ,so_orig.order_remarks
        ,so_orig.so_create_dt --created_datetime        
        ,so_orig.so_update_dt --updated_datetime
        ,so_orig.so_create_by --created_by
        ,so_orig.so_updated_by --updated_by
        ,so_orig.so_status --order_status
        ,so_orig.po_number --reference_po_number
        ,so_orig.soline_no --line_number
        ,so_orig.material --material
        ,so_orig.batch --batch
        ,so_orig.qty --qty
        ,so_orig.unit
        ,so_orig.net_value --net_value
        ,so_orig.currency
        ,coalesce(socri.request_delivery_date, so_orig.request_delivery_date)
        ,so_orig.pick_up_plant
        ,so_orig.customer_code
        ,so_orig.ship_to_code
        ,so_orig.division
        ,so_orig.level3_code
        ,so_orig.customer_type
        ,null --so_orig.operation_type
        ,so_orig.default_location
        ,so_orig.sub_division
        ,0 as rebate_rate -- PION SP&针电极
        ,so_orig.business_group
        ,so_orig.chinese_socreatedt
        ,date_format(so_orig.so_create_dt,'yyyy-MM-dd')
from tmp_dwd_fact_sales_order_info_new_rebate as so_orig
left outer join tmp_dwd_fact_sales_order_info_socri socri on so_orig.so_no=socri.so_no 

; 
"
# 2. 执行SQL，并判断查询结果是否为空
count=`$hive -e "select count(*) from ods_sales_order where dt='$sync_date'and substr(so_create_dt,1,1)='2'" | tail -n1`

if [ $count -eq 0 ]; then
  echo "Error: Failed to import data, count is zero."
  exit 1
fi
# 3. 执行SQL
echo "$so_sql"
$hive -e "$so_sql"

echo "End syncing Sales order data into DWD layer on ${sync_date} .................."