#!/bin/bash
# Function:
#   sync up inventory onhand  data to dwd layer
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

echo "start syncing inventory onhand data into DWD layer on ${sync_date} .................."

# 1 Hive SQL string
sto_sql="
-- 参数
----set mapreduce.job.queuename=hive;
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;


-- sync up SQL string
insert overwrite table ${target_db_name}.dwd_fact_inventory_onhand partition(dt)
select trans_date
       ,inventory_type
       ,plant_to --plant
       ,storage_loc
       ,profic_center
       ,material
       ,batch
       ,quantity
       ,unrestricted
       ,inspection
       ,blocked_material
       ,expiration_date
       ,standard_cost
       ,extended_cost
       ,update_date
       ,plant_from
       ,update_date as dt
    from ${target_db_name}.ods_inventory_onhand --源表TRANS_InventoryOnhand
    where dt='$sync_date'
    and substr(update_date,1,1)='2'; 
"
# 2. 执行SQL，并判断查询结果是否为空
count=`$hive -e "select count(*) from ods_inventory_onhand where dt='$sync_date'and substr(update_date,1,1)='2'" | tail -n1`

if [ $count -eq 0 ]; then
  echo "Error: Failed to import data, count is zero."
  exit 1
fi
# 3. 执行加载数据SQL
$hive -e "$sto_sql"

echo "End syncing inventory onhand data into DWD layer on ${sync_date} .................."
#大概是太占内存所以执行完成之后会把ods的一周前的一周的数据删除
sh /bscflow/ods/remove_a_week_ago_ods_onhand.sh