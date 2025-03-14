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
ods_po_dn_cycletime_maxdt=`hdfs dfs -ls /bsc/opsdw/ods/ods_po_dn_cycletime | tail -n 1 | awk -F'=' '{print $NF}'`

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
a.so_no ,a.material,a.deliveryitem ,a.d2,p.po_dn_day,
b.year_month_date ,b.cal_year,b.cal_month, 
case when SUBSTR(month_weeknum,7,1)>4 then 4 else SUBSTR(month_weeknum,7,1) end week
from ( select so_no ,material,delivery_id||line_number deliveryitem ,MAX(chinese_dncreatedt)  d2
  from dwd_fact_sales_order_dn_detail where year_mon >='2023-12-01'
  group by so_no ,material,delivery_id||line_number) a 
  left join (select * from opsdw.ods_po_dn_cycletime where dt='$ods_po_dn_cycletime_maxdt') p 
  on a.deliveryitem=p.deliveryitem
join dwd_dim_calendar b 
on date(a.d2)=b.year_month_date;

drop table if exists tmp_ods_holiday_diff;
create table  tmp_ods_holiday_diff stored as orc as 
select '2024' h_year, '2024-02-09' h_startDT,'2024-02-18' h_endDT , 8 h_day, 'Chinese New Year' h_name;


drop table if exists tmp_dwd_po_dn_info;
create table  tmp_dwd_po_dn_info stored as orc as 
 select so_no,division_id,year_month_date dt,
case when po_dn_day is null then 
CAST((unix_timestamp(d2) - unix_timestamp(d1))/86400 AS decimal(18,6))-h_day  
else 1
end diff,
CAST((unix_timestamp(d2) - unix_timestamp(d1))/86400 AS decimal(18,6))-h_day diff2
from (
select a.so_no,a.division_id,a.d1 ,b.d2,b.po_dn_day ,b.year_month_date,
nvl(c.bo_reason,'0') bo_reason,nvl(d.h_day,0) h_day
from 
tmp_dwd_fact_sales_order_info2 a 
join tmp_dwd_fact_sales_order_dn_info b 
on a.so_no=b.so_no and a.material=b.material
left JOIN ods_otds_supply_week c
on (b.material =c.upn or case when a.division_id ='CRM' then SUBSTR(b.material,3,4) end = c.upn)
and b.cal_year =c.kpi_year and b.cal_month =c.kpi_month and b.week=c.kpi_week
left join tmp_ods_holiday_diff d 
on b.cal_year =d.h_year and b.d2 >=d.h_endDT and a.d1<=d.h_startDT
) a 
where bo_reason='0'
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
CAST(COUNT(DISTINCT so_no) as decimal(18,6)) as flag1,
CAST(SUM(diff2) as decimal(18,6)) as flag2, 
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
CAST(COUNT(DISTINCT so_no) as decimal(18,6)) as flag1,
CAST(SUM(diff2) as decimal(18,6)) as flag2, 
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