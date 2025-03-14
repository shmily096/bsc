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

with batch_inventoryonhand as (
  select a.*
        , b.country_of_origin
        , b.date_of_manufacture
    from (
    select trans_date
         , inventory_type
         , plant as site 
         , storage_loc as storage_location
         , material, batch
         , profic_center as profit_center 
         , sum(quantity) as total_qty
         , sum(unrestricted) as unrestricted_qty
         , sum(inspection) as inspection_qty
         , sum(blocked_material) as blocked_qty
     from dwd_fact_inventory_onhand 
    where Plant IN ('D835','D836','D837','D838')
    and dt = '${sync_date}'
  group by trans_date
         , inventory_type
         , plant
         , storage_loc
         , material, batch
         , profic_center ) a 
   left outer join (
             select material
                  , batch
                  , country_of_origin
                  , date_of_manuf as date_of_manufacture
               from dwd_dim_batch
               where dt = '${sync_date}' 
              ) b on a.material=b.material and a.batch = b.batch
)

insert overwrite table dws_batchtracking_inventoryonhand partition(dt)
select trans_date as transaction_date
     , inventory_type
     , site 
     , storage_location
     , material
     , batch
     , profit_center 
     , total_qty
     , unrestricted_qty
     , inspection_qty
     , blocked_qty
     , country_of_origin
     , date_of_manufacture
     , '' as extra_info
     , trans_date as dt
  from batch_inventoryonhand;
"
# 2. 执行加载数据SQL
$hive -e "$dwt_sql"

echo "End syncing dwt_product_putaway_leadtime_slc_topic data into DWS layer on $sync_date ....."