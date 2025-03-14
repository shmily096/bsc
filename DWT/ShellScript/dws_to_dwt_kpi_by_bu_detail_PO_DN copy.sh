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

echo "start syncing dws_to_dwt_kpi_by_bu_detail_PO_DN data into DWS layer on ${sync_date} : ${sync_date[year]}"
ods_kpi_report_maxdt=`hdfs dfs -ls /bsc/opsdw/ods/ods_kpi_report | tail -n 1 | awk -F'=' '{print $NF}'`
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


drop table if exists tmp_dwd_fact_sales_order_info2;
create table  tmp_dwd_fact_sales_order_info2 stored as orc as 
select so_no,material ,division_id  ,MAX(chinese_socreatedt) d1 
from dwd_fact_sales_order_info 
where dt>='2023-12-01' and order_type in ('KB','OR')
GROUP by so_no,material,division_id ;

drop table if exists tmp_dwd_fact_sales_order_dn_info;
create table  tmp_dwd_fact_sales_order_dn_info stored as orc as 
select 
so_no ,material ,chinese_dncreatedt d2,
b.year_month_date ,b.cal_year,b.cal_month, 
case when SUBSTR(month_weeknum,7,1)>4 then 4 else SUBSTR(month_weeknum,7,1) end week
from dwd_fact_sales_order_dn_detail a 
join dwd_dim_calendar b 
on date(a.chinese_dncreatedt)=b.year_month_date 
where a.year_mon >='2023-12-01';

drop table if exists tmp_dwd_po_dn_info;
create table  tmp_dwd_po_dn_info stored as orc as 
select so_no,division_id,year_month_date dt
,AVG(CAST((unix_timestamp(d2) - unix_timestamp(d1))/86400 AS decimal(18,6)))  as diff
from (
select a.so_no,a.division_id,a.d1 ,b.d2 ,b.year_month_date,
nvl(c.bo_reason,'0') bo_reason
from 
tmp_dwd_fact_sales_order_info2 a 
join tmp_dwd_fact_sales_order_dn_info b 
on a.so_no=b.so_no and a.material=b.material
left JOIN ods_otds_supply_week c
on (b.material =c.upn or case when a.division_id ='CRM' then SUBSTR(b.material,3,4) end = c.upn)
and b.cal_year =c.kpi_year and b.cal_month =c.kpi_month and b.week=c.kpi_week
) a 
where bo_reason='0'
group by so_no,division_id,year_month_date
;
--------------------------------------------------------------------------po-dn
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
----第一次刷全年
select 
null as plant,
dt,
division_id as bu,
CAST(SUM(diff) as decimal(18,6)) as value1,
CAST(COUNT(1) as decimal(18,6)) as value2,  
null as flag1, 
null as flag2, 
null as flag3,
'OC0101' kpi_code,
date_format(dt,'yyyy-MM') as year_mon
from tmp_dwd_po_dn_info
where division_id is not null
group by dt,division_id
union 
select 
null as plant,
dt,
'Total China' as bu,
CAST(SUM(diff) as decimal(18,6)) as value1,
CAST(COUNT(1) as decimal(18,6)) as value2,  
null as flag1, 
null as flag2, 
null as flag3,
'OC0101' kpi_code,
date_format(dt,'yyyy-MM') as year_mon
from tmp_dwd_po_dn_info 
group by dt;

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