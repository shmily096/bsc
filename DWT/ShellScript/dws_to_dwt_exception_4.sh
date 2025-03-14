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

# 获取下个月的日期（加上一个月）  
next_month_date=$(date -d "$current_date +1 month" +"%F") 

echo "start syncing dwd_trans_csgn_clear data into DWS layer on ${sync_date} : ${sync_date[year]}"
# dwd_dim_material_maxdt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_dim_material | tail -n 1 | awk -F'=' '{print $NF}'`
# ods_mdm_materialmaster_marc_maxdt=`hdfs dfs -ls /bsc/opsdw/ods/ods_mdm_materialmaster_marc | tail -n 1 | awk -F'=' '{print $NF}'`


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



drop table if exists tmp_dwd_dim_material;
create table  tmp_dwd_dim_material stored as orc as 
select DISTINCT material material_code from opsdw.ods_dq_maex_bsc
where dt='$ods_dq_maex_bsc_maxdt' and lower(\`grouping\`)  = 'approved' and ctry ='CN' and lr='CN';

drop table if exists tmp_dwd_dim_material_noapproved;
create table  tmp_dwd_dim_material_noapproved stored as orc as 
select DISTINCT material material_code from opsdw.ods_dq_maex_bsc
where dt='$ods_dq_maex_bsc_maxdt' and lower(\`grouping\`)  <> 'approved' and ctry ='CN' and lr='CN';


"


delete_tmp="
drop table tmp_materialmaster_marc;
"
# 2. 执行加载数据SQL
echo "$sto_sql"
$hive -e "$sto_sql"
#第二部分收尾删除所有临时表
echo "two $delete_tmp"
# $hive -e "$delete_tmp"
echo "End syncing dwd_trans_csgn_clear data into DWS layer on ${sync_date} .................."