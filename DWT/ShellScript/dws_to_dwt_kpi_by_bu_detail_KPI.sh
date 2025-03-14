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


drop table if exists tmp_dwd_perfect_calendar;
create table tmp_dwd_perfect_calendar stored as orc as 
select 
case when l3_kpi ='Deliver item accuracy%' then 'PO0101'
	when l3_kpi ='Deliver quantity accuracy%' then 'PO0102'
	when l3_kpi ='Master data accuracy%' then 'PO0103'
	when l3_kpi ='Order processing accuracy%' then 'PO0301'
	when l3_kpi ='Delivery document accuracy%' then 'PO0302'
	when l2_kpi ='Goods in perfet condition%' then 'PO0201'
	end kpi_code
,year_month_date dt
,if(c.DN_qty is null,0,c.DN_qty) DN_qty
from 
(select DISTINCT l2_kpi ,l3_kpi  from opsdw.ods_kpi_category 
where l3_kpi <>'') a 
full join (
select year_month_date  from opsdw.dwd_dim_calendar 
where year_month_date>='2023-12-01' and year_month_date <= last_day(current_date)
) b
left join 
(
select dt,COUNT(1) DN_qty  from opsdw.dws_finance_upn_qty 
where  dt>='2023-12-01' and category ='outbound_delivery_qty' 
group by dt
) c 
on b.year_month_date=c.dt;

drop table if exists tmp_dwd_perfect_rate;
create table tmp_dwd_perfect_rate stored as orc as
select 
b.dt,
sum(if(a.l3_kpi is null,0,1))   as value1,
b.DN_qty as value2,  
b.kpi_code
--,a.l3_kpi
 from
(
select 
b.l2_kpi,b.l3_kpi,TO_DATE(a.closed_date) dt,
case when l3_kpi ='Deliver item accuracy%' then 'PO0101'
	when l3_kpi ='Deliver quantity accuracy%' then 'PO0102'
	when l3_kpi ='Master data accuracy%' then 'PO0103'
	when l3_kpi ='Order processing accuracy%' then 'PO0301'
	when l3_kpi ='Delivery document accuracy%' then 'PO0302'
	when l2_kpi ='Goods in perfet condition%' then 'PO0201'
	end kpi_code
from opsdw.ods_kpi_report a 
join opsdw.ods_kpi_category b 
on a.category =b.category 
and a.sub_category =b.sub_category 
and a.case_record_type =b.case_record_type 
and (a.resolution=b.resolution or b.resolution='')
where a.dt='$ods_kpi_report_maxdt' and b.l3_kpi <>''
) a
RIGHT JOIN tmp_dwd_perfect_calendar b 
on a.dt=b.dt and a.kpi_code=b.kpi_code
group by b.kpi_code,b.dt, b.DN_qty;


--------------------------------------------------------------------------CC Lead time
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
----第一次刷全年
select 
null as plant,
dt,
null as bu,
CAST(SUM(diff) as decimal(18,6)) as value1,
CAST(COUNT(1) as decimal(18,6)) as value2,  
null as flag1, 
null as flag2, 
null as flag3,
kpi_code,
date_format(dt,'yyyy-MM') as year_mon
from 
(
select 
case when lead_time ='Customer Feedback Resolve Time' then 'OC030102'
	when lead_time ='Customer Request Resolve Time' then 'OC030101'
	 end kpi_code,
TO_DATE(a.closed_date) dt
,CAST((unix_timestamp(a.closed_date) - unix_timestamp(a.opened_date))/86400.0 AS decimal(18,6))  as diff
from opsdw.ods_kpi_report a 
join opsdw.ods_kpi_category b 
on a.category =b.category 
and a.sub_category =b.sub_category 
and a.case_record_type =b.case_record_type 
and (a.resolution=b.resolution or b.resolution='')
where a.dt='$ods_kpi_report_maxdt'
) a 
group by dt,kpi_code;

--------------------------------------------------------------------------Perfect rate
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
----第一次刷全年
select 
null as plant,
dt,
null as bu,
value2-value1 value1,
value2,  
null as flag1, 
null as flag2, 
null as flag3,
kpi_code,
date_format(dt,'yyyy-MM') as year_mon
from
tmp_dwd_perfect_rate;

--------------------------------------------------------------------------Complaint
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
----第一次刷全年
select 
null as plant,
TO_DATE(from_unixtime(unix_timestamp(closedyearmonth||'01','yyyyMMdd'))) as dt,
division as bu,
sum(qa_pending_days) as value1,
count(1) as value2,  
null as flag1, 
null as flag2, 
null as flag3,
'OC030201' kpi_code,
date_format(from_unixtime(unix_timestamp(closedyearmonth||'01','yyyyMMdd')),'yyyy-MM') as year_mon
from opsdw.ods_kpi_complaint 
where dt='$ods_kpi_complaint_maxdt' and apply_no is not null
group by closedyearmonth ,division
UNION 
select 
null as plant,
TO_DATE(from_unixtime(unix_timestamp(closedyearmonth||'01','yyyyMMdd'))) as dt,
division as bu,
sum(bu_pending_days) as value1,
count(1) as value2,  
null as flag1, 
null as flag2, 
null as flag3,
'OC030202' kpi_code,
date_format(from_unixtime(unix_timestamp(closedyearmonth||'01','yyyyMMdd')),'yyyy-MM') as year_mon
from opsdw.ods_kpi_complaint 
where dt='$ods_kpi_complaint_maxdt' and apply_no is not null
group by closedyearmonth ,division
UNION 
select 
null as plant,
TO_DATE(from_unixtime(unix_timestamp(closedyearmonth||'01','yyyyMMdd'))) as dt,
division as bu,
sum(cc_pending_days) as value1,
count(1) as value2,  
null as flag1, 
null as flag2, 
null as flag3,
'OC030203' kpi_code,
date_format(from_unixtime(unix_timestamp(closedyearmonth||'01','yyyyMMdd')),'yyyy-MM') as year_mon
from opsdw.ods_kpi_complaint 
where dt='$ods_kpi_complaint_maxdt' and apply_no is not null
group by closedyearmonth ,division;

"
delete_tmp="
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