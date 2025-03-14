#!/bin/bash
# Function:
#   sync up dws_dsr_billed_cr_qty_netvalue_detail 
# History:
# 2023-10-23    slc   v1.0    init

# 参数
target_db_name='opsdw' # 目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

if [ -n "$1" ] ;then 
    sync_date=$1
else
    sync_date=$(date  +%F)
fi

echo "start syncing dws_dsr_billed_cr_qty_netvalue_detail data into DWS layer on ${sync_date} : ${sync_date[year]}"

# billed:
# dned:

sto_sql="
-- 参数
use ${target_db_name};
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;

drop table if exists tmp_dws_dsr_billed_cr_qty_netvalue_detail;
create table  tmp_dws_dsr_billed_cr_qty_netvalue_detail stored as orc as 
select 
	division,
	material,
	sum(value_all) as value_all,
	sum(qty) as qty,
	'CNY'currency,
	year_mon
from (
	select 
		division_display_name as division
		,material
		,dt_year||dt_month as year_mon
		,net_cr as  value_all 
		,cast(cr_qty as decimal(18,2)) qty
		from dws_dsr_cr_daily
		where dt_year>='2023'
		and material is not null
		union all
		select 
		division
		,material
		,dt_year||dt_month as year_mon
		,net_billed-COALESCE(billed_rebate,0) as  value_all 
		,cast(bill_qty as decimal(18,2))as qty
	from dws_dsr_billed_daily
	where dt_year>='2023'
		and material is not null)xx
group by division,year_mon,material
union all 
select 'Total'as division,material,sum(value_all) as value_all,sum(qty) as qty,'CNY'currency,year_mon
from (
	select 
		division_display_name as division
		,material
		,dt_year||dt_month as year_mon
		,net_cr as  value_all 
		,cast(cr_qty as decimal(18,2)) qty
		from dws_dsr_cr_daily
		where dt_year>='2023'
		and material is not null
		union all
		select 
		division
		,material
		,dt_year||dt_month as year_mon
		,net_billed-COALESCE(billed_rebate,0) as  value_all 
		,cast(bill_qty as decimal(18,2))as qty
	from dws_dsr_billed_daily
	where dt_year>='2023'
		and material is not null)xx
group by year_mon,material;

insert overwrite table dws_dsr_billed_cr_qty_netvalue_detail partition(year_mon)
SELECT 
		division,
		material,
		value_all,
		qty,
		currency,
		year_mon
from tmp_dws_dsr_billed_cr_qty_netvalue_detail

"
delete_tmp="
drop table tmp_dws_dsr_billed_cr_qty_netvalue_detail;
"
# 2. 执行加载数据SQL
echo "$sto_sql"
$hive -e "$sto_sql"
#第二部分收尾删除所有临时表
echo "two $delete_tmp"
$hive -e "$delete_tmp"
echo "End syncing dws_dsr_billed_cr_qty_netvalue_detail data into DWS layer on ${sync_date} .................."