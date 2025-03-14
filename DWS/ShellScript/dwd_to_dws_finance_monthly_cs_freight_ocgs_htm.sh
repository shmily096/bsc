#!/bin/bash
# Function:
#   sync up dws_dsr_daily_trans 
# History:
# 2021-06-29    Donny   v1.0    init
# 设置必要的参数
target_db_name='opsdw' # 数据加载目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径
if [ -n "$2" ] ;then 
    sync_date=$2
else
    sync_date=$(date  +%F)
fi
sync_year=${sync_date:0:4}
dwd_finance_monthly_opex_cs_maxdt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_finance_monthly_opex_cs | awk 'BEGIN {max = 0} {if ($6+0 > max+0) max=$6} END {print $8}' | grep -oP 20'[^ ]*'`
dwd_finance_monthly_opex_freight_maxdt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_finance_monthly_opex_freight | awk 'BEGIN {max = 0} {if ($6+0 > max+0) max=$6} END {print $8}' | grep -oP 20'[^ ]*'`
dwd_finance_monthly_opex_ocogs_maxdt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_finance_monthly_opex_ocogs | awk 'BEGIN {max = 0} {if ($6+0 > max+0) max=$6} END {print $8}' | grep -oP 20'[^ ]*'`
dwd_finance_ocogs_report_china_hfm_maxdt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_finance_ocogs_report_china_hfm | awk 'BEGIN {max = 0} {if ($6+0 > max+0) max=$6} END {print $8}' | grep -oP 20'[^ ]*'`
echo "start syncing dws_dsr_daily_trans data into DWS layer on $sync_year : $sync_date .................."
htm_str="
use ${target_db_name};
-- 配置参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
--set hive.exec.max.dynamic.partitions.pernode=100000;
--set hive.exec.max.dynamic.partitions=100000;
drop table if exists tmp_htm;
create table tmp_htm stored as orc as
 SELECT concat('20', substring(a.year_mon, 3, 2)) AS years,
    COALESCE(b.mon_n, substring(a.year_mon, 6)) AS mon_n,
    substring(a.year_mon, 6) AS mon_e,
    a.file_name,
    a.target AS disvision_old,
    dm.shortname AS disvision,
    a.function_name AS items,
    a.china_total AS country,
    '' AS items_2,
    a.hfm_value AS value_all,
    a.currency,    
    a.updatetime,
    REGEXP_EXTRACT(a.file_name, 'xlsx_(.*)$', 1) as sheet_name,
    REGEXP_EXTRACT(a.file_name, '\\\\d{6}', 0) AS versions,
    substr(a.updatetime,1,10) as dt
   FROM (select * from opsdw.dwd_finance_ocogs_report_china_hfm where dt='$dwd_finance_ocogs_report_china_hfm_maxdt') a
     LEFT JOIN opsdw.dwd_dim_finance_months_mapping b ON upper(substring(a.year_mon, 6)) = upper(b.mon_e)
     LEFT JOIN opsdw.dwd_dim_finance_division_mapping dm ON a.target = dm.division_name;

INSERT OVERWRITE TABLE ${target_db_name}.dws_finance_monthly_cs_freight_ocgs_htm partition(sheet_name,versions,dt)
select 
    years,
    mon_n, 
    mon_e, 
    file_name, 
    disvision_old, 
    disvision, 
    items, 
    country, 
    items_2, 
    value_all,
    currency, 
    updatetime, 
    sheet_name,
    versions,
    dt
from tmp_htm
"
cs_str="
use ${target_db_name};
-- 配置参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
drop table if exists tmp_cs;
create table tmp_cs stored as orc as
  SELECT 
       concat('20', substring(a.year_mon, 3, 2)) AS years,
       COALESCE(b.mon_n, substring(a.year_mon, 6)) AS mon_n,
       substring(a.year_mon, 6) AS mon_e,
       a.file_name,
       a.function_name AS disvision_old,
       '' as disvision,
       a.target AS items,
       a.description AS country,
       '' AS items_2,
       a.cs_value AS value_all,
       a.currency,    
       a.updatetime,
        REGEXP_EXTRACT(a.file_name, 'xlsx_(.*)$', 1) as sheet_name,
        REGEXP_EXTRACT(a.file_name, '\\\\d{6}', 0) AS versions,
       substr(a.updatetime,1,10) as dt
   FROM (select * from opsdw.dwd_finance_monthly_opex_cs where dt='$dwd_finance_monthly_opex_cs_maxdt') a
       LEFT JOIN opsdw.dwd_dim_finance_months_mapping b ON upper(substring(a.year_mon, 6)) = upper(b.mon_e);

INSERT OVERWRITE TABLE ${target_db_name}.dws_finance_monthly_cs_freight_ocgs_htm partition(sheet_name,versions,dt)
select 
    years,
    mon_n, 
    mon_e, 
    file_name, 
    disvision_old, 
    disvision, 
    items, 
    country, 
    items_2, 
    value_all,
    currency, 
    updatetime, 
    sheet_name,
    versions,
    dt
from tmp_cs
"
fre_str="
use ${target_db_name};
-- 配置参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
drop table if exists tmp_fre;
create table tmp_fre stored as orc as
  SELECT 
       concat('20', substring(a.year_mon, 3, 2)) AS years,
       COALESCE(b.mon_n, substring(a.year_mon, 6)) AS mon_n,
       substring(a.year_mon, 6) AS mon_e,
       a.file_name,
       a.target AS disvision_old,
       dm.shortname AS disvision,
       a.total_sg AS items,
       a.china_total AS country,
       a.freight_name AS items_2,
       a.freight_value AS value_all,
       a.currency,       
       a.updatetime,
        REGEXP_EXTRACT(a.file_name, 'xlsx_(.*)$', 1) as sheet_name,
        REGEXP_EXTRACT(a.file_name, '\\\\d{6}', 0) AS versions,
       substr(a.updatetime,1,10) as dt
   FROM (select * from opsdw.dwd_finance_monthly_opex_freight where dt='$dwd_finance_monthly_opex_freight_maxdt') a
     LEFT JOIN opsdw.dwd_dim_finance_months_mapping b ON upper(substring(a.year_mon, 6)) = upper(b.mon_e)
     LEFT JOIN opsdw.dwd_dim_finance_division_mapping dm ON a.target = dm.division_name;

INSERT OVERWRITE TABLE ${target_db_name}.dws_finance_monthly_cs_freight_ocgs_htm partition(sheet_name,versions,dt)
select 
    years,
    mon_n, 
    mon_e, 
    file_name, 
    disvision_old, 
    disvision, 
    items, 
    country, 
    items_2, 
    value_all,
    currency, 
    updatetime, 
    sheet_name,
    versions,
    dt
from tmp_fre
"
ocgs_str="
use ${target_db_name};
-- 配置参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
drop table if exists tmp_ocgs;
create table tmp_ocgs stored as orc as
  SELECT concat('20', substring(a.year_mon, 3, 2)) AS years,
    COALESCE(b.mon_n, substring(a.year_mon, 6)) AS mon_n,
    substring(a.year_mon, 6) AS mon_e,
    a.file_name,
    a.target AS disvision_old,
    dm.shortname AS disvision,
    a.total_charges AS items,
    a.china_total AS country,
    '' AS items_2,
    a.ocogs_value AS value_all,
    a.currency,    
    a.updatetime,
    REGEXP_EXTRACT(a.file_name, 'xlsx_(.*)$', 1) as sheet_name,
    REGEXP_EXTRACT(a.file_name, '\\\\d{6}', 0) AS versions,
    substr(a.updatetime,1,10) as dt
   FROM (select * from opsdw.dwd_finance_monthly_opex_ocogs where dt='$dwd_finance_monthly_opex_ocogs_maxdt') a
     LEFT JOIN opsdw.dwd_dim_finance_months_mapping b ON upper(substring(a.year_mon, 6)) = upper(b.mon_e)
     LEFT JOIN opsdw.dwd_dim_finance_division_mapping dm ON a.target = dm.division_name
;

INSERT OVERWRITE TABLE ${target_db_name}.dws_finance_monthly_cs_freight_ocgs_htm partition(sheet_name,versions,dt)
select 
    years,
    mon_n, 
    mon_e, 
    file_name, 
    disvision_old, 
    disvision, 
    items, 
    country, 
    items_2, 
    value_all,
    currency, 
    updatetime, 
    sheet_name,
    versions,
    dt
from tmp_ocgs

"
# 2. 执行加载数据SQL
if [ "$1"x = "cs"x ];then
	echo "DWD $1 only run"	
	$hive -e "$cs_str"
	echo "DWD  finish cs data into DWD layer on ${sync_date} .................."
elif [ "$1"x = "fre"x ];then
    echo " DWD $1 only run"
	echo "$sync_date  ok"
	$hive -e "$fre_str"
	echo "DWD  finish fre data into DWD layer on ${sync_date} .................."
elif [ "$1"x = "ocgs"x ];then
    echo " DWD $1 only run"
	echo "$sync_date  ok"
	$hive -e "$ocgs_str"
elif [ "$1"x = "htm"x ];then
    echo " DWD $1 only run"
	echo "$sync_date  ok"
    $hive -e "$htm_str"
else
    echo "please give ok date "
fi
echo "End syncing dws_dsr_daily_trans data into DWS layer on $sync_year : $sync_date .................."