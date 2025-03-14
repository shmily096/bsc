#!/bin/bash
# Function:
#   sync up dws_kpi_sales_waybill_timi 
# History:
# 2021-07-08    Donny   v1.0    init

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

echo "start syncing dws_kpi_sales_waybill_timi data into DWS layer on ${sync_date} : ${sync_date[year]}"

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
add jar /user/hive/numDay-1.0-SNAPSHOT.jar;
create temporary function myudf as 'org.example.Nmu';

insert overwrite table opsdw.dws_kpi_dwd_fact_import_export_dn_info_pdt partition(dt_yearmon,dt)
select 
sto_no 
,delivery_no
,reference_dn_no
,order_status
,ship_from_plant
,ship_to_plant
,total_qty
,actual_good_issue_datetime
,actual_migo_datetime
,fin_dim_id
,item_business_group
, CAST((unix_timestamp(actual_migo_datetime) - unix_timestamp(actual_good_issue_datetime))/(60 * 60)/24 AS float)  as lt_cd_hr
, (case when CAST((unix_timestamp(actual_migo_datetime) - unix_timestamp(actual_good_issue_datetime))/(60 * 60) AS float)
             - myudf(actual_good_issue_datetime,actual_migo_datetime)*24 <0 then 0
       else CAST((unix_timestamp(actual_migo_datetime) - unix_timestamp(actual_good_issue_datetime))/(60 * 60) AS float)
             - myudf(actual_good_issue_datetime,actual_migo_datetime)*24  
       end)/24 as lt_dw_hr  --如果结果是-1就是其中有空值或者null值，如果结果小于=0就是当天或者休息日发完的
, 'WH019' as KPI_no
, date_format(dt,'yyyy-MM')as dt_yearmon
, dt 
from opsdw.dwd_fact_import_export_dn_info 
                where dt>= date_add('$sync_date',-93)
                and actual_migo_datetime is not null ;
"
# 2. 执行加载数据SQL
echo "two $sto_sql"
$hive -e "$sto_sql"
echo "End syncing master data into DWD layer on ${sync_date} .................."