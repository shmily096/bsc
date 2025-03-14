#!/bin/bash
# Function:
#   sync up dws_to_dwt_kpi_by_bu_detail_PO_DN 
# History:
# 2024-01-18    Sway   v1.0    init

# 参数
target_db_name='opsdw' # 目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

if [ -n "$1" ] ;then 
    sync_date=$1
	end_date=$1
else
    sync_date=$(date  +%F)
	end_date=$(date  +%F)
	sync_yearmon=$(date +%Y-%m-01) #当前月第一天
fi

echo "start syncing dws_to_dwt_kpi_by_bu_detail_PO_DN data into DWS layer on ${sync_date} : ${sync_date[year]}"
ods_kpi_report_maxdt=`hdfs dfs -ls /bsc/opsdw/ods/ods_kpi_report | tail -n 1 | awk -F'=' '{print $NF}'`
ods_kpi_complaint_maxdt=`hdfs dfs -ls /bsc/opsdw/ods/ods_kpi_complaint | tail -n 1 | awk -F'=' '{print $NF}'`
ods_kpi_complaint_maxdt=`hdfs dfs -ls /bsc/opsdw/ods/ods_kpi_complaint | tail -n 1 | awk -F'=' '{print $NF}'`

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
add jar /user/hive/numDay-1.0-SNAPSHOT.jar;
create temporary function myudf as 'org.example.Nmu';

--------------------------------------------------------------------------dwd_otds_itemlevel
insert overwrite table dwd_otds_itemlevel 
SELECT 
	vbeln, 
	posnr, 
	matnr, 
	division, 
	customer_req_date, 
	year_month, 
	pardate, 
	year_month_date, 
	bo_reason
    FROM opsdw.tmp_dwd_otds_itemlevel;

"
delete_tmp="
drop table tmp_dwd_fact_sales_order_info2;
drop table tmp_dwd_fact_sales_order_dn_info;
drop table tmp_dwd_po_dn_info;
drop table tmp_dwd_perfect_calendar;
drop table tmp_dwd_perfect_rate;
"
# 2. 执行加载数据SQL
echo "$sto_sql"
$hive -e "$sto_sql"
#第二部分收尾删除所有临时表
echo "two $delete_tmp"
# $hive -e "$delete_tmp"
echo "End syncing dwt_kpi_by_bu_detail data into DWS layer on ${sync_date} .................."