#!/bin/bash
# Function:
#   sync up dwt_product_putaway_leadtime_slc_topic data to dwt layer
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

# salesdn monthly closed:
start_date=$(date -d "-31day ${sync_date}" "+ %Y-%m-%d")


echo "start syncing data into dws layer from: ${start_date} to: ${sync_date} .................."

dwt_sql="
use ${target_db_name};
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;

with batch_salesdn as (
select delry.so_no
     , delry.material
     , delry.batch
     , delry.delivery_id
     , delry.delry_qty
     , dn.dn_createdtime
     , dn.dn_pgitime
     , dn.dn_receivedtime
     , so.order_type
     , so.order_reason
     , so.ItemType
     , so.SOStuatus
     , so.customer_code
     , so.DealerType
     , so.sub_divison
     , so.pick_up_plant
     , so.SO_createdtime
     , CAST(YEAR(so.SO_createdtime) as string) as SearchPeriod 
from (
 select so_no, delivery_id, material, batch
      , count(1) as delry_qty
  from dwd_fact_sales_order_dn_detail
 where dt <= '${sync_date}'
   and dt >= date_add('${sync_date}',-2)
   and qr_code is not null 
 group by so_no, delivery_id, material, batch
 union all 
 select so_no, delivery_id, material, batch
      , sum(qty) as delry_qty
  from dwd_fact_sales_order_dn_detail
 where dt <= '${sync_date}'
   and dt >= date_add('${sync_date}',-2)
   and qr_code is null 
 group by so_no, delivery_id, material, batch ) delry
left outer join (
select so_no, delivery_id 
     , min(created_datetime) as dn_createdtime
     , min(actual_gi_date) as dn_pgitime
     , max(receiving_confirmation_date) as dn_receivedtime
  from dwd_fact_sales_order_dn_info
 where dt <= '${sync_date}'
   and dt >= '${start_date}'
group by so_no, delivery_id ) dn on delry.so_no = dn.so_no and delry.delivery_id = dn.delivery_id
left outer join (
select so_no, order_type, order_reason, order_status as SOStuatus
     , material ,item_type as ItemType 
     , customer_level3 as DealerType
     , sub_divison,pick_up_plant, customer_code
     , min(created_datetime) as SO_createdtime
  from dwd_fact_sales_order_info
 where dt <= '${sync_date}'
   and dt >= date_add('${start_date}',-365)
 group by so_no, order_type, order_reason, order_status
     , material,item_type , customer_level3
     , sub_divison, pick_up_plant, customer_code) so  on delry.so_no = so.so_no and delry.material = so.material
 group by delry.so_no
     , delry.material
     , delry.batch
     , delry.delivery_id
     , delry.delry_qty
     , dn.dn_createdtime
     , dn.dn_pgitime
     , dn.dn_receivedtime
     , so.order_type
     , so.order_reason
     , so.ItemType
     , so.SOStuatus
     , so.customer_code
     , so.DealerType
     , so.sub_divison
     , so.pick_up_plant
     , so.SO_createdtime
)

insert overwrite table dws_batchtracking_salesdn partition(dt)
select so_no
     , material
     , batch
     , delivery_id
     , delry_qty
     , dn_createdtime
     , dn_pgitime
     , dn_receivedtime
     , order_type
     , order_reason
     , ItemType
     , SOStuatus
     , customer_code
     , DealerType
     , sub_divison
     , pick_up_plant
     , SO_createdtime
     , SearchPeriod 
     , '' as extra_info
     , to_date(dn_createdtime) as dt
  from batch_salesdn; 
"
# 2. 执行加载数据SQL
$hive -e "$dwt_sql"

echo "End syncing dwt_product_putaway_leadtime_slc_topic data into DWS layer on $sync_date ....."