#!/bin/bash
# Function:
#   sync up dws_to_dwt_kpi_by_bu_detail_PO_DN 
# History:
# 2024-01-18    Sway   v1.0    init

# 参数
target_db_name='opsdw' # 目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径
export LANG="en_US.UTF-8"

if [ -n "$1" ] ;then 
    sync_date=$1
	end_date=$1
else
    sync_date=$(date  +%F)
	end_date=$(date  +%F)
fi

# 获取下个月的日期（加上一个月）  
next_month_date=$(date -d "$current_date +1 month" +"%F") 

echo "start syncing dwd_trans_csgn_clear data into DWS layer on ${sync_date} : ${sync_date[year]}"



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
set hive.exec.reducers.max=8;
set mapred.reduce.tasks=8;
set hive.exec.parallel=false;
add jar /user/hive/numDay-1.0-SNAPSHOT.jar;
create temporary function myudf as 'org.example.Nmu';



--------------------------------------------------------------------------consignmentlist
insert overwrite table dwd_trans_consignmentlist partition(year_mon)
SELECT divisionid, division, customernumber,
customername, upn, category, dealertype, 
product_line1_name, 
case when product_line2_name='PE&PP&PE+' then SUBSTR(product_line1_name,0,2) else product_line2_name end product_line2_name, 
product_line3_name, product_line4_name, 
product_line5_name, qty, gross_price_usd, standard_cost_usd, updatedt, 
year_mon
FROM opsdw.dwd_trans_consignmentlist;

"



delete_tmp="
--drop table tmp_dwd_calendar_right15_workday;
"
# 2. 执行加载数据SQL
echo "$sto_sql"
$hive -e "$sto_sql"
#第二部分收尾删除所有临时表
echo "two $delete_tmp"
# $hive -e "$delete_tmp"
echo "End syncing dwd_trans_csgn_clear data into DWS layer on ${sync_date} .................."