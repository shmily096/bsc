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
    next_date=$1
else
    sync_date=$(date -d '-1 day' +%F)
    next_date=$(date -d "$sync_date + 1 day" +"%Y-%m-%d") 
fi
echo "start syncing inventory onhand data into DWD layer on ${sync_date} ${next_date}.................."

# 1 Hive SQL string
sto_sql="
-- 参数
----set mapreduce.job.queuename=hive;
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
--set hive.exec.max.dynamic.partitions.pernode=100000;
--set hive.exec.max.dynamic.partitions=100000;
--set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
--set hive.exec.parallel=false;
drop table if exists tmp_dws_fact_inventory_onhand_by_mon;
create table  tmp_dws_fact_inventory_onhand_by_mon stored as orc as 
select  a.trans_date
       ,a.inventory_type
       ,a.plant --plant
       ,a.storage_loc
       ,a.profic_center
       ,a.material
       ,a.batch
       ,a.quantity
       ,a.unrestricted
       ,a.inspection
       ,a.blocked_material
       ,a.expiration_date
       ,a.standard_cost
       ,a.extended_cost
       ,a.update_date
       ,a.plant_from
	   ,b.standard_cost as ctm_standard_cost_rmb
	   ,b.standard_cost_usd as ctm_standard_cost_usd
	   ,b.division_display_name
       ,substr(date_add(a.update_date,-1),1,7) as year_mon
    from (select * from ${target_db_name}.dwd_fact_inventory_onhand where dt='$sync_date' )a --源表TRANS_InventoryOnhand
	left join (select material_code,standard_cost,standard_cost_usd,division_display_name from dwd_dim_material where dt='$next_date') b
		on a.material=b.material_code
    ; 

-- sync up SQL string
insert overwrite table ${target_db_name}.dws_fact_inventory_onhand_by_mon partition(year_mon)
select  trans_date
       ,inventory_type
       ,plant --plant
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
	   ,ctm_standard_cost_rmb
	   ,ctm_standard_cost_usd
	   ,division_display_name
       , year_mon
    from tmp_dws_fact_inventory_onhand_by_mon
    ; 
"
# 2. 执行SQL，并判断查询结果是否为空
count=`$hive -e "select count(*) from dwd_fact_inventory_onhand where dt='$sync_date'" | tail -n1`

if [ $count -eq 0 ]; then
  echo "Error: Failed to import data, count is zero."
  exit 1
fi
# 3. 执行加载数据SQL
$hive -e "$sto_sql"
#各qty的调度暂时先放这里后期一起上,下面这个只在58跑
sh /bscflow/dws/dwd_to_dws_finance_upn_qty.sh
echo "End syncing inventory onhand data into DWD layer on ${sync_date} .................."