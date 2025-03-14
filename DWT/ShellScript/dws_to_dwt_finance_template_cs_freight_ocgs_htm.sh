#!/bin/bash
# Function:
#   sync up xxxx 
# History:
# 2021-07-21    Donny   v1.0    init

# 设置必要的参数
target_db_name='opsdw' # 数据加载目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径
if [ -n "$1" ] ;then 
    sync_date=$1
else
    sync_date=$(date -d '-1 day' +%F)
fi
sync_year=${sync_date:0:4}
mm=${sync_date:5:2}
echo "start syncing data into dws layer on $sync_year :$mm .................."

sql_str="
use ${target_db_name};
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.parallel=false;

insert overwrite table ${target_db_name}.dwt_finance_monthly_cs_freight_ocgs_htm
select 
years, 
mon_n, 
mon_e, 
file_name,
disvision_old, 
disvision,
coalesce(b.excel,disvision) as bu,
items, 
country, 
items_2, 
value_all, 
currency, 
updatetime, 
sheet_name, 
versions, 
dt
FROM opsdw.dws_finance_monthly_cs_freight_ocgs_htm  a
left join dwd_dim_finance_disvision_mapping_hive_disvsion b on a.disvision=b.hive
where sheet_name||versions||dt in(
	select sheet_name||max(versions)||max(dt)
	from  opsdw.dws_finance_monthly_cs_freight_ocgs_htm
	group by sheet_name )
"
# 2. 执行加载数据SQL
$hive -e "$sql_str"

echo "End syncing data into DWS layer on  $sync_year :$mm  .................."