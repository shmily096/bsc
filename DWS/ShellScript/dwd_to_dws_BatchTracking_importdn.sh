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
    sync_date=$(date -d "-1day" +%F)
fi

# salesdn monthly closed:
#start_date=$(date -d "-30day ${sync_date}" "+ %Y-%m-%d")
start_date=$(date -d "-39day" "+%F")

echo "period sync up from ${start_date}....."

echo "start syncing data into dws layer from: ${start_date} to: ${sync_date} .................."

dwt_sql="
use ${target_db_name};
set hive.exec.reducers.max=8;
set mapred.reduce.tasks=8;
set hive.exec.parallel=false;
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;

with batch_importdetails as (
          select a.sto_no
               , a.delivery_no
               , a.material_code as material 
               , a.batch_number as batch 
               , a.qty as dn_qty
               , b.sto_qty 
               , b.sto_orderstatus 
               , b.ship_from_plant 
               , b.ship_to_plant 
               , b.sto_createtime 
               , b.sto_pgitime 
               , b.sto_migotime 
             from (
                 select *
                  from dwd_fact_import_export_dn_detail
                 where dt = '${sync_date}'
                 ) a 
         left outer join (
                 select sto_no 
                      , delivery_no as sto_deliveryno
                      , ship_from_plant , ship_to_plant 
                      , max(order_status) as sto_orderstatus
                      , min(created_datetime) as sto_createtime
                      , min(actual_good_issue_datetime) as sto_pgitime
                      , min(actual_migo_datetime) as sto_migotime
                      , sum(total_qty) as sto_qty
                  from dwd_fact_import_export_dn_info
                where dt <= '${sync_date}'
                  and dt >= '${start_date}'
              group by sto_no 
                     , delivery_no
                     , ship_from_plant , ship_to_plant) b on a.sto_no = b.sto_no and a.delivery_no = b.sto_deliveryno 
), batch_importdetails_all as (
  select *
    from batch_importdetails
  union all 
   select sto_no
     , delivery_no
     , material 
     , batch 
     , dn_qty
     , sto_qty
     , sto_orderstatus 
     , ship_from_plant 
     , ship_to_plant 
     , sto_createtime 
     , sto_pgitime 
     , sto_migotime
    from dws_batchtracking_importdn 
  where dt in (
    select to_date(sto_createtime) as dd 
      from batch_importdetails 
    group by to_date(sto_createtime)
  )
)

insert overwrite table dws_batchtracking_importdn partition(dt)
select distinct sto_no
     , delivery_no
     , material 
     , batch 
     , dn_qty
     , sto_qty
     , sto_orderstatus 
     , ship_from_plant 
     , ship_to_plant 
     , sto_createtime 
     , sto_pgitime 
     , sto_migotime
     , cast(Year(sto_createtime) as string) as SearchPeriod
     , '' as extra_info
     , to_date(sto_createtime) as dt
  from batch_importdetails_all; 
"
# 2. 执行加载数据SQL
$hive -e "$dwt_sql"

echo "End syncing data into DWS layer on $sync_date ....."