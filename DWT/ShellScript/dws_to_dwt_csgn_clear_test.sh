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
fi

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
add jar /user/hive/numDay-1.0-SNAPSHOT.jar;
create temporary function myudf as 'org.example.Nmu';



--------------------------------------------------------------------------dwd_lzo_trans_consignmenttracking
insert overwrite table dwd_lzo_trans_consignmenttracking partition(dt)
SELECT id, updatedt, active, divisionname, customercodesap, customernamesap, customer, customernumber, 
material, plant, materialdescription, batch, expiration, available, committed, deliverydocnum, postingdate, 
salesorder, orderdate, customerponumber, storagetype, ordertype, customerstatus, materialtype, consignmenttype, 
duedate, remainingdays, pgivsexpiration, pgivsexpirationscope, expirationvsreportdate, expirationvsreportdatescope, 
lastworkdate, alertmessenger, '$sync_date'
FROM opsdw.ods_trans_consignmenttracking;

"



delete_tmp="
drop table tmp_dwd_calendar_right15_workday;
"
# 2. 执行加载数据SQL
echo "$sto_sql"
$hive -e "$sto_sql"
#第二部分收尾删除所有临时表
echo "two $delete_tmp"
# $hive -e "$delete_tmp"
echo "End syncing dwd_trans_csgn_clear data into DWS layer on ${sync_date} .................."